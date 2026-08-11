from fastapi import FastAPI, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
import json, os, re, uuid, asyncio, secrets, time, hashlib
from datetime import datetime, date
from pathlib import Path
import fitz
from dotenv import load_dotenv

BASE = Path(__file__).resolve().parent
STATIC = BASE / 'static'
STATIC.mkdir(exist_ok=True)

load_dotenv(BASE / '.env')  # allows a local .env with DATABASE_URL for `run.bat` / local dev

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError(
        'DATABASE_URL não definida. Crie um arquivo .env (veja .env.example) '
        'ou defina a variável de ambiente DATABASE_URL com a connection string do PostgreSQL.'
    )
# Some providers (Render/Heroku style) hand out "postgres://" URLs; psycopg wants "postgresql://".
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = 'postgresql://' + DATABASE_URL[len('postgres://'):]

app = FastAPI(title='Separação FULL', version='1.0.0')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=True, allow_methods=['*'], allow_headers=['*'])

class Hub:
    def __init__(self): self.connections=set()
    async def connect(self, ws):
        await ws.accept(); self.connections.add(ws)
    def disconnect(self, ws): self.connections.discard(ws)
    async def broadcast(self, payload):
        dead=[]
        for ws in list(self.connections):
            try: await ws.send_json(payload)
            except Exception: dead.append(ws)
        for ws in dead: self.disconnect(ws)

hub=Hub()

# --- Senha do painel administrativo -----------------------------------------
# Defina ADMIN_PASSWORD (no .env local ou nas variáveis de ambiente do Render)
# pra exigir login no painel admin. Se não for definida, o painel fica aberto
# (mesmo comportamento de antes) — assim quem ainda não configurou não fica
# travado por engano.
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', '').strip()
SESSION_HOURS = 12
_admin_sessions = {}  # token -> expira_em (timestamp)

def _new_admin_session():
    token = secrets.token_urlsafe(32)
    _admin_sessions[token] = time.time() + SESSION_HOURS*3600
    return token

def _check_admin_session(token):
    exp = _admin_sessions.get(token)
    if not exp or exp < time.time():
        _admin_sessions.pop(token, None)
        return False
    return True

async def require_admin(x_admin_token: str|None = Header(default=None)):
    if not ADMIN_PASSWORD:
        return  # senha não configurada: painel aberto
    if not x_admin_token or not _check_admin_session(x_admin_token):
        raise HTTPException(401, 'Sessão de administrador inválida ou expirada. Faça login novamente.')

class AdminLoginIn(BaseModel): senha: str

@app.post('/api/admin/login')
def admin_login(body: AdminLoginIn):
    if not ADMIN_PASSWORD:
        return {'ok': True, 'token': None, 'protegido': False}  # sem senha configurada
    if not secrets.compare_digest(body.senha, ADMIN_PASSWORD):
        raise HTTPException(401, 'Senha incorreta.')
    return {'ok': True, 'token': _new_admin_session(), 'protegido': True}

@app.get('/api/admin/check')
def admin_check(x_admin_token: str|None = Header(default=None)):
    if not ADMIN_PASSWORD: return {'protegido': False, 'autenticado': True}
    return {'protegido': True, 'autenticado': bool(x_admin_token and _check_admin_session(x_admin_token))}

def _admin_ok(x_admin_token: str|None) -> bool:
    if not ADMIN_PASSWORD: return True  # painel aberto se não configurado
    return bool(x_admin_token and _check_admin_session(x_admin_token))

# --- Senha individual de cada colaborador ------------------------------------
PBKDF2_ROUNDS = 200_000

def hash_password(senha: str, salt: str|None = None):
    salt = salt or secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac('sha256', senha.encode(), salt.encode(), PBKDF2_ROUNDS).hex()
    return h, salt

def verify_password(senha: str, salt: str, expected_hash: str) -> bool:
    h,_ = hash_password(senha, salt)
    return secrets.compare_digest(h, expected_hash)

SESSION_HOURS_WORKER = 12
_worker_sessions = {}  # token -> {'user_id':..., 'exp':...}

def _new_worker_session(user_id: str):
    token = secrets.token_urlsafe(32)
    _worker_sessions[token] = {'user_id': user_id, 'exp': time.time() + SESSION_HOURS_WORKER*3600}
    return token

def _worker_session_user(token: str|None):
    s = _worker_sessions.get(token) if token else None
    if not s or s['exp'] < time.time():
        _worker_sessions.pop(token, None)
        return None
    return s['user_id']

async def require_worker(x_worker_token: str|None = Header(default=None)) -> str:
    """Valida a sessão do colaborador e devolve o user_id autenticado."""
    uid_ = _worker_session_user(x_worker_token)
    if not uid_: raise HTTPException(401, 'Sessão de colaborador inválida ou expirada. Faça login novamente.')
    return uid_

async def require_admin_or_worker(x_admin_token: str|None = Header(default=None), x_worker_token: str|None = Header(default=None)):
    """Para ações compartilhadas (ex.: caixas/volumes), aceita admin OU qualquer colaborador autenticado."""
    if _admin_ok(x_admin_token): return
    if _worker_session_user(x_worker_token): return
    raise HTTPException(401, 'É necessário estar autenticado (admin ou colaborador) para esta ação.')

class WorkerLoginIn(BaseModel): user_id: str; senha: str

@app.post('/api/worker/login')
def worker_login(body: WorkerLoginIn):
    c=conn()
    u=c.execute('SELECT id,nome,senha_hash,senha_salt FROM users WHERE id=%s AND ativo=1',(body.user_id,)).fetchone()
    c.close()
    if not u: raise HTTPException(404,'Colaborador não encontrado.')
    if not u['senha_hash']:
        raise HTTPException(403,'Este colaborador ainda não tem senha definida. Peça para o administrador cadastrar uma.')
    if not verify_password(body.senha, u['senha_salt'], u['senha_hash']):
        raise HTTPException(401,'Senha incorreta.')
    return {'ok':True,'token':_new_worker_session(u['id']),'user_id':u['id'],'nome':u['nome']}

# Pool de conexões: evita abrir um handshake TCP+TLS novo com o Postgres a cada
# requisição (o maior custo de latência num banco remoto como o Neon). min_size=0
# porque bancos free-tier "dormem" quando ociosos — não faz sentido manter
# conexões abertas o tempo todo; check_connection descarta conexões mortas
# (ex.: depois do Neon hibernar) e abre uma nova automaticamente, sem precisar
# reiniciar o app.
POOL = ConnectionPool(
    DATABASE_URL,
    min_size=0,
    max_size=5,
    kwargs={'row_factory': dict_row, 'autocommit': False},
    check=ConnectionPool.check_connection,
    open=True,
)

class _PooledConn:
    """Se comporta como uma conexão psycopg normal, mas .close() devolve a
    conexão pro pool em vez de derrubá-la de verdade. Sempre dá rollback antes
    de devolver: garante que nenhuma transação/trava fique pendurada de um
    request pro próximo (ex.: rotas que fecham a conexão cedo, num erro 404,
    sem ter chamado commit/rollback explicitamente)."""
    def __init__(self, raw): object.__setattr__(self, '_raw', raw)
    def __getattr__(self, name): return getattr(object.__getattribute__(self, '_raw'), name)
    def close(self):
        raw = object.__getattribute__(self, '_raw')
        try: raw.rollback()
        except Exception: pass
        POOL.putconn(raw)

def conn():
    return _PooledConn(POOL.getconn())

def init_db():
    c=conn()
    c.execute('''
    CREATE TABLE IF NOT EXISTS fulls(
      id TEXT PRIMARY KEY, frete_ml TEXT, nome TEXT NOT NULL, previsao_data TEXT,
      status TEXT NOT NULL DEFAULT 'aguardando', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
      source_filename TEXT
    );
    CREATE TABLE IF NOT EXISTS products(
      id TEXT PRIMARY KEY, full_id TEXT NOT NULL REFERENCES fulls(id) ON DELETE CASCADE,
      codigo_ml TEXT, codigo_universal TEXT, sku TEXT, nome TEXT NOT NULL,
      quantidade INTEGER NOT NULL, separado INTEGER NOT NULL DEFAULT 0,
      instrucoes TEXT DEFAULT '', created_at TEXT NOT NULL,
      claimed_by TEXT, claimed_at TEXT, ord BIGSERIAL
    );
    CREATE TABLE IF NOT EXISTS users(
      id TEXT PRIMARY KEY, nome TEXT NOT NULL UNIQUE, ativo INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS assignments(
      id TEXT PRIMARY KEY, full_id TEXT NOT NULL REFERENCES fulls(id) ON DELETE CASCADE,
      product_id TEXT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
      user_id TEXT NOT NULL REFERENCES users(id), quantidade INTEGER NOT NULL,
      separado INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
      UNIQUE(product_id,user_id)
    );
    CREATE TABLE IF NOT EXISTS shipment_users(
      id TEXT PRIMARY KEY, full_id TEXT NOT NULL REFERENCES fulls(id) ON DELETE CASCADE,
      user_id TEXT NOT NULL REFERENCES users(id), created_at TEXT NOT NULL,
      UNIQUE(full_id,user_id)
    );
    CREATE TABLE IF NOT EXISTS volumes(
      id TEXT PRIMARY KEY, full_id TEXT NOT NULL REFERENCES fulls(id) ON DELETE CASCADE,
      seq INTEGER NOT NULL, tipo TEXT NOT NULL DEFAULT 'mista', status TEXT NOT NULL DEFAULT 'aberta',
      created_by TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS volume_items(
      id TEXT PRIMARY KEY, volume_id TEXT NOT NULL REFERENCES volumes(id) ON DELETE CASCADE,
      product_id TEXT NOT NULL REFERENCES products(id) ON DELETE CASCADE, quantidade INTEGER NOT NULL,
      UNIQUE(volume_id,product_id)
    );
    CREATE TABLE IF NOT EXISTS status_history(
      id TEXT PRIMARY KEY, full_id TEXT NOT NULL REFERENCES fulls(id) ON DELETE CASCADE,
      status TEXT NOT NULL, user_id TEXT, created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_products_full ON products(full_id);
    CREATE INDEX IF NOT EXISTS idx_shipment_users_full ON shipment_users(full_id);
    CREATE INDEX IF NOT EXISTS idx_shipment_users_user ON shipment_users(user_id);
    CREATE INDEX IF NOT EXISTS idx_assign_full ON assignments(full_id);
    CREATE INDEX IF NOT EXISTS idx_vol_full ON volumes(full_id);
    ''')
    # Migrations for databases created by older versions of this app.
    c.execute('ALTER TABLE products ADD COLUMN IF NOT EXISTS claimed_by TEXT')
    c.execute('ALTER TABLE products ADD COLUMN IF NOT EXISTS claimed_at TEXT')
    # Senha individual por colaborador. Colaboradores criados antes desta versão
    # ficam com senha_hash NULL, ou seja, sem senha definida ainda — o admin
    # precisa definir uma para cada um antes que eles consigam entrar.
    c.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS senha_hash TEXT')
    c.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS senha_salt TEXT')
    c.commit(); c.close()
init_db()

def now(): return datetime.now().isoformat(timespec='seconds')
def uid(prefix=''): return prefix + uuid.uuid4().hex[:12]

def parse_int(s):
    return int(re.sub(r'[^0-9]','',s))

def extract_pdf(path):
    doc=fitz.open(path)
    pages=[p.get_text('text') for p in doc]
    text='\n'.join(pages)
    text=text.replace('\r','')
    # Header information
    m=re.search(r'Frete\s*#\s*([A-Za-z0-9_-]+)', text, re.I)
    frete=m.group(1) if m else ''
    m=re.search(r'Produtos do envio:\s*(\d+)\s*\|\s*Total de unidades:\s*([\d.,]+)', text, re.I)
    expected_products=int(m.group(1)) if m else None
    expected_units=parse_int(m.group(2)) if m else None

    # Product blocks run from Código ML to Etiquetagem obrigatória.
    blocks=re.findall(r'(Código ML:\s*[^\n]+.*?)(?=Etiquetagem\s+obrigatória)', text, flags=re.I|re.S)
    products=[]
    for b in blocks:
        cm=re.search(r'Código ML:\s*([^\s]+)',b,re.I)
        cu=re.search(r'Código universal:\s*([^\n]+)',b,re.I)
        sk=re.search(r'SKU:\s*([^\n]+)',b,re.I)
        code_ml=cm.group(1).strip() if cm else ''
        universal=cu.group(1).strip() if cu else ''
        universal=re.sub(r'\s+SKU:.*$','',universal,flags=re.I).strip()
        sku=sk.group(1).strip() if sk else ''
        if sku == '-': sku=''
        # Text after SKU until end, cleaning labels and metadata.
        after=b
        if sk: after=b[sk.end():]
        lines=[x.strip() for x in after.splitlines() if x.strip()]
        name=' '.join(x for x in lines if not re.match(r'^(Código ML:|Código universal:|SKU:)',x,re.I))
        name=re.sub(r'\s+',' ',name).strip()
        products.append({'codigo_ml':code_ml,'codigo_universal':universal,'sku':sku,'nome':name})

    # Quantities are listed in a second table. In Mercado Livre PDFs the table may
    # continue on the next page, so collect numeric-only lines after the table header
    # from every page, stopping before the commercial annex on page 3.
    quantities=[]
    for page_text in pages:
        if 'PRODUTO\nUNIDADES' not in page_text:
            continue
        qty_text=page_text.split('PRODUTO',1)[1]
        # Start after the UNIDADES column/header.
        pos=qty_text.find('UNIDADES')
        qty_text=qty_text[pos+len('UNIDADES'):] if pos>=0 else qty_text
        for line in qty_text.splitlines():
            s=line.strip()
            if re.fullmatch(r'[0-9][0-9.,]*',s):
                quantities.append(parse_int(s))
    quantities=quantities[:len(products)]
    for i,p in enumerate(products): p['quantidade']=quantities[i] if i<len(quantities) else 0
    # Preparation instructions are best preserved as a PDF-derived summary. The exact per-product table text
    # is not always structurally separable, so we keep a safe generic note when present.
    for p in products:
        p['instrucoes']='Verificar embalagem, identificação e as instruções de preparação indicadas no PDF.'
    return {'frete_ml':frete,'expected_products':expected_products,'expected_units':expected_units,'products':products,'pages':len(doc)}

def full_summary(c, full_id):
    f=c.execute('SELECT * FROM fulls WHERE id=%s',(full_id,)).fetchone()
    if not f: raise HTTPException(404,'FULL não encontrado')
    products=c.execute('SELECT * FROM products WHERE full_id=%s ORDER BY ord',(full_id,)).fetchall()
    users=c.execute(f'SELECT {USER_PUBLIC_COLS} FROM users WHERE ativo=1 ORDER BY nome').fetchall()
    shipment_users=c.execute('SELECT su.*,u.nome user_nome FROM shipment_users su JOIN users u ON u.id=su.user_id WHERE su.full_id=%s ORDER BY u.nome',(full_id,)).fetchall()
    assigns=c.execute('SELECT a.*,u.nome user_nome,p.nome product_nome,p.sku,p.codigo_ml FROM assignments a JOIN users u ON u.id=a.user_id JOIN products p ON p.id=a.product_id WHERE a.full_id=%s ORDER BY u.nome,p.nome',(full_id,)).fetchall()
    vols=c.execute('SELECT * FROM volumes WHERE full_id=%s ORDER BY seq',(full_id,)).fetchall()
    vitems=c.execute('SELECT vi.*,p.nome product_nome,p.sku,p.codigo_ml FROM volume_items vi JOIN products p ON p.id=vi.product_id JOIN volumes v ON v.id=vi.volume_id WHERE v.full_id=%s ORDER BY v.seq,p.nome',(full_id,)).fetchall()
    p_rows=[]
    for p in products:
        claimed = assigns[0] if assigns and any(a['product_id']==p['id'] for a in assigns) else None
        if claimed:
            claimed = next(a for a in assigns if a['product_id']==p['id'])
        assigned=claimed['quantidade'] if claimed else 0
        separated=claimed['separado'] if claimed else 0
        boxed=sum(x['quantidade'] for x in vitems if x['product_id']==p['id'])
        p_rows.append(dict(p)|{'atribuido':assigned,'separado':separated,'em_volumes':boxed,'saldo_separar':p['quantidade']-separated,'saldo_volume':p['quantidade']-boxed,'disponivel':not bool(p['claimed_by']),'claimed_by':p['claimed_by'],'claimed_user_nome':(claimed['user_nome'] if claimed else None)})
    total=sum(p['quantidade'] for p in p_rows); sep=sum(p['separado'] for p in p_rows); boxed=sum(p['em_volumes'] for p in p_rows)
    vols_out=[]
    for v in vols:
        items=[dict(x) for x in vitems if x['volume_id']==v['id']]
        tipo='vazia' if not items else ('unica' if len(items)==1 else 'mista')
        if v['tipo']!=tipo: c.execute('UPDATE volumes SET tipo=%s,updated_at=%s WHERE id=%s',(tipo,now(),v['id']))
        vols_out.append(dict(v)|{'tipo':tipo,'items':items,'total_unidades':sum(x['quantidade'] for x in items)})
    return {'full':dict(f),'products':p_rows,'users':[dict(u) for u in users],'shipment_users':[dict(u) for u in shipment_users],'assignments':[dict(a) for a in assigns],'volumes':vols_out,'stats':{'produtos':len(p_rows),'unidades':total,'separado':sep,'em_volumes':boxed,'progresso':round((sep/total*100) if total else 0,1),'volumes':len(vols_out)}}

async def changed(full_id):
    c=conn(); data=full_summary(c,full_id); c.commit(); c.close(); await hub.broadcast({'type':'full_updated','full_id':full_id,'data':data})

class FullCreate(BaseModel):
    nome: str
    previsao_data: str=''
    frete_ml: str=''
    source_filename: str=''
    products: list[dict]

class UserCreate(BaseModel): nome: str; senha: str = Field(min_length=4)
class PasswordSetIn(BaseModel): senha: str = Field(min_length=4)
class ShipmentUserIn(BaseModel): user_id: str
class ClaimIn(BaseModel): user_id: str
class AssignmentIn(BaseModel): product_id: str; user_id: str; quantidade: int=Field(ge=0)
class ProgressIn(BaseModel): separado: int=Field(ge=0)
class StatusIn(BaseModel): status: str; user_id: str|None=None
class VolumeCreate(BaseModel): created_by: str|None=None
class VolumeItemIn(BaseModel): product_id: str; quantidade: int=Field(ge=0); modo: str='somar'
class ProductEdit(BaseModel):
    nome: str|None = None
    quantidade: int|None = Field(default=None, ge=0)
    sku: str|None = None
    codigo_ml: str|None = None
    codigo_universal: str|None = None
    instrucoes: str|None = None

@app.get('/api/health')
def health(): return {'ok':True}

@app.post('/api/import-pdf')
async def import_pdf(file: UploadFile=File(...), _=Depends(require_admin)):
    if not file.filename.lower().endswith('.pdf'): raise HTTPException(400,'Envie um arquivo PDF.')
    tmp=BASE/('_tmp_'+uid()+'.pdf')
    data=await file.read(); tmp.write_bytes(data)
    try: parsed=extract_pdf(tmp)
    except Exception as e: raise HTTPException(400,f'Não foi possível ler o PDF: {e}')
    finally:
        try: tmp.unlink()
        except: pass
    existing=None
    if parsed.get('frete_ml'):
        c=conn()
        f=c.execute('SELECT * FROM fulls WHERE frete_ml=%s ORDER BY created_at DESC LIMIT 1',(parsed['frete_ml'],)).fetchone()
        if f: existing=full_summary(c,f['id'])
        c.close()
    return {'filename':file.filename,'parsed':parsed,'existing':existing}

@app.post('/api/fulls')
async def create_full(body:FullCreate, _=Depends(require_admin)):
    if not body.products: raise HTTPException(400,'Nenhum produto informado.')
    c=conn(); fid=uid('full_'); t=now()
    c.execute('INSERT INTO fulls VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',(fid,body.frete_ml,body.nome,body.previsao_data,'aguardando',t,t,body.source_filename))
    for p in body.products:
        q=int(p.get('quantidade') or 0)
        if q<0: raise HTTPException(400,'Quantidade inválida.')
        c.execute('INSERT INTO products (id,full_id,codigo_ml,codigo_universal,sku,nome,quantidade,separado,instrucoes,created_at,claimed_by,claimed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(uid('prod_'),fid,p.get('codigo_ml',''),p.get('codigo_universal',''),p.get('sku',''),p.get('nome','Produto'),q,0,p.get('instrucoes',''),t,None,None))
    c.execute('INSERT INTO status_history VALUES (%s,%s,%s,%s,%s)',(uid('hist_'),fid,'aguardando',None,t))
    c.commit(); data=full_summary(c,fid); c.close(); await hub.broadcast({'type':'full_created','full_id':fid})
    return data

@app.put('/api/fulls/{full_id}/import-update')
async def import_update_full(full_id:str, body:FullCreate, _=Depends(require_admin)):
    c=conn()
    f=c.execute('SELECT * FROM fulls WHERE id=%s',(full_id,)).fetchone()
    if not f: c.close(); raise HTTPException(404,'Envio não encontrado.')
    existing=c.execute('SELECT * FROM products WHERE full_id=%s',(full_id,)).fetchall()
    def key(p_dict):
        cm=(p_dict.get('codigo_ml') or '').strip()
        if cm: return ('cm',cm)
        sk=(p_dict.get('sku') or '').strip()
        if sk: return ('sk',sk)
        return ('nm',(p_dict.get('nome') or '').strip().lower())
    existing_by_key={key(dict(e)):e for e in existing}
    matched_ids=set(); atualizados=[]; adicionados=[]; avisos=[]; t=now()
    for p in body.products:
        q=int(p.get('quantidade') or 0)
        if q<0: raise HTTPException(400,'Quantidade inválida.')
        k=key(p); ex=existing_by_key.get(k)
        if ex:
            matched_ids.add(ex['id'])
            sep_total=c.execute('SELECT COALESCE(SUM(separado),0) n FROM assignments WHERE product_id=%s',(ex['id'],)).fetchone()['n']
            boxed_total=c.execute('SELECT COALESCE(SUM(quantidade),0) n FROM volume_items WHERE product_id=%s',(ex['id'],)).fetchone()['n']
            minimo=max(sep_total,boxed_total)
            novo_nome=p.get('nome') or ex['nome']
            novo_q=q
            if q<minimo:
                novo_q=ex['quantidade']
                avisos.append(f"{ex['nome']}: quantidade do PDF ({q}) é menor que o já separado/em caixas ({minimo}); mantida em {ex['quantidade']}.")
            if novo_nome!=ex['nome'] or novo_q!=ex['quantidade']:
                c.execute('UPDATE products SET nome=%s,quantidade=%s,codigo_universal=%s,instrucoes=%s WHERE id=%s',(novo_nome,novo_q,p.get('codigo_universal',ex['codigo_universal']),p.get('instrucoes',ex['instrucoes']),ex['id']))
                atualizados.append({'nome':novo_nome,'quantidade_antes':ex['quantidade'],'quantidade_depois':novo_q})
        else:
            c.execute('INSERT INTO products (id,full_id,codigo_ml,codigo_universal,sku,nome,quantidade,separado,instrucoes,created_at,claimed_by,claimed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(uid('prod_'),full_id,p.get('codigo_ml',''),p.get('codigo_universal',''),p.get('sku',''),p.get('nome','Produto'),q,0,p.get('instrucoes',''),t,None,None))
            adicionados.append({'nome':p.get('nome','Produto'),'quantidade':q})
    nao_encontrados=[dict(e)['nome'] for e in existing if e['id'] not in matched_ids]
    c.execute('UPDATE fulls SET nome=%s,previsao_data=%s,frete_ml=%s,source_filename=%s,updated_at=%s WHERE id=%s',(body.nome,body.previsao_data,body.frete_ml,body.source_filename,t,full_id))
    c.commit(); data=full_summary(c,full_id); c.close()
    await hub.broadcast({'type':'full_updated','full_id':full_id,'data':data})
    return {**data,'relatorio':{'atualizados':atualizados,'adicionados':adicionados,'nao_encontrados_no_novo_arquivo':nao_encontrados,'avisos':avisos}}

@app.get('/api/fulls')
def list_fulls(_=Depends(require_admin_or_worker)):
    c=conn(); rows=c.execute('SELECT * FROM fulls ORDER BY COALESCE(previsao_data,\'9999-12-31\'),created_at DESC').fetchall(); out=[]
    for f in rows:
        d=full_summary(c,f['id']); out.append({'full':d['full'],'stats':d['stats']})
    c.close(); return out

@app.get('/api/fulls/{full_id}')
def get_full(full_id:str, _=Depends(require_admin_or_worker)):
    c=conn(); d=full_summary(c,full_id); c.commit(); c.close(); return d

@app.delete('/api/fulls/{full_id}')
async def delete_full(full_id:str, _=Depends(require_admin)):
    c=conn()
    if not c.execute('SELECT 1 FROM fulls WHERE id=%s',(full_id,)).fetchone():
        c.close(); raise HTTPException(404,'FULL não encontrado.')
    c.execute('DELETE FROM fulls WHERE id=%s',(full_id,))  # cascade remove produtos, atribuições, volumes e histórico
    c.commit(); c.close()
    await hub.broadcast({'type':'full_deleted','full_id':full_id})
    return {'ok':True}

USER_PUBLIC_COLS = 'id,nome,ativo,created_at,(senha_hash IS NOT NULL) AS tem_senha'

@app.post('/api/users')
async def create_user(body:UserCreate, _=Depends(require_admin)):
    name=body.nome.strip()
    if not name: raise HTTPException(400,'Nome obrigatório.')
    h,salt=hash_password(body.senha)
    c=conn()
    try:
        c.execute('INSERT INTO users (id,nome,ativo,created_at,senha_hash,senha_salt) VALUES (%s,%s,%s,%s,%s,%s)',(uid('usr_'),name,1,now(),h,salt)); c.commit()
    except psycopg.errors.UniqueViolation:
        c.rollback(); c.close(); raise HTTPException(409,'Colaborador já cadastrado.')
    rows=[dict(x) for x in c.execute(f'SELECT {USER_PUBLIC_COLS} FROM users WHERE ativo=1 ORDER BY nome')]; c.close(); await hub.broadcast({'type':'users_updated'}); return rows

@app.put('/api/users/{user_id}/password')
async def set_user_password(user_id:str, body:PasswordSetIn, _=Depends(require_admin)):
    c=conn()
    if not c.execute('SELECT 1 FROM users WHERE id=%s',(user_id,)).fetchone():
        c.close(); raise HTTPException(404,'Colaborador não encontrado.')
    h,salt=hash_password(body.senha)
    c.execute('UPDATE users SET senha_hash=%s,senha_salt=%s WHERE id=%s',(h,salt,user_id))
    c.commit(); c.close(); await hub.broadcast({'type':'users_updated'}); return {'ok':True}

@app.delete('/api/users/{user_id}')
async def delete_user(user_id:str, _=Depends(require_admin)):
    c=conn()
    if not c.execute('SELECT 1 FROM users WHERE id=%s',(user_id,)).fetchone():
        c.close(); raise HTTPException(404,'Colaborador não encontrado.')
    # Desativa em vez de apagar de vez: preserva o histórico de desempenho e não
    # quebra registros antigos (atribuições, caixas, histórico de status) que
    # apontam para este colaborador.
    c.execute('UPDATE users SET ativo=0 WHERE id=%s',(user_id,))
    c.execute('DELETE FROM shipment_users WHERE user_id=%s',(user_id,))
    c.execute('UPDATE products SET claimed_by=NULL,claimed_at=NULL WHERE claimed_by=%s',(user_id,))
    c.commit(); c.close()
    await hub.broadcast({'type':'users_updated'})
    return {'ok':True}

@app.get('/api/users')
def list_users():
    # Aberto de propósito: a tela de login (admin e colaborador) precisa da
    # lista de nomes antes de autenticar. Nunca inclui senha_hash/senha_salt.
    c=conn(); rows=[dict(x) for x in c.execute(f'SELECT {USER_PUBLIC_COLS} FROM users WHERE ativo=1 ORDER BY nome')]; c.close(); return rows

@app.put('/api/fulls/{full_id}/status')
async def set_status(full_id:str, body:StatusIn, _=Depends(require_admin)):
    allowed={'aguardando','em_separacao','pronto','enviado'}
    if body.status not in allowed: raise HTTPException(400,'Status inválido.')
    c=conn(); f=c.execute('SELECT * FROM fulls WHERE id=%s',(full_id,)).fetchone()
    if not f: raise HTTPException(404,'FULL não encontrado')
    if body.status=='pronto':
        d=full_summary(c,full_id)
        if d['stats']['separado']!=d['stats']['unidades'] or d['stats']['em_volumes']!=d['stats']['unidades']:
            raise HTTPException(400,'Para marcar como pronto, toda a quantidade deve estar separada e distribuída nos volumes.')
    t=now(); c.execute('UPDATE fulls SET status=%s,updated_at=%s WHERE id=%s',(body.status,t,full_id)); c.execute('INSERT INTO status_history VALUES (%s,%s,%s,%s,%s)',(uid('hist_'),full_id,body.status,body.user_id,t)); c.commit(); c.close(); await changed(full_id); return {'ok':True}

@app.post('/api/fulls/{full_id}/collaborators')
async def add_shipment_collaborator(full_id:str, body:ShipmentUserIn, _=Depends(require_admin)):
    c=conn()
    if not c.execute('SELECT 1 FROM fulls WHERE id=%s',(full_id,)).fetchone(): raise HTTPException(404,'FULL não encontrado.')
    if not c.execute('SELECT 1 FROM users WHERE id=%s AND ativo=1',(body.user_id,)).fetchone(): raise HTTPException(404,'Colaborador não encontrado.')
    try:
        c.execute('INSERT INTO shipment_users (id,full_id,user_id,created_at) VALUES (%s,%s,%s,%s)',(uid('su_'),full_id,body.user_id,now())); c.commit()
    except psycopg.errors.UniqueViolation:
        c.rollback(); c.close(); raise HTTPException(409,'Colaborador já está atribuído a este envio.')
    data=full_summary(c,full_id); c.close(); await hub.broadcast({'type':'full_updated','full_id':full_id,'data':data}); return data

@app.delete('/api/fulls/{full_id}/collaborators/{user_id}')
async def remove_shipment_collaborator(full_id:str,user_id:str, _=Depends(require_admin)):
    c=conn(); c.execute('DELETE FROM shipment_users WHERE full_id=%s AND user_id=%s',(full_id,user_id)); c.commit(); data=full_summary(c,full_id); c.close(); await hub.broadcast({'type':'full_updated','full_id':full_id,'data':data}); return data

@app.get('/api/performance')
def performance(_=Depends(require_admin_or_worker)):
    # Calculado sempre a partir do estado ATUAL do banco. Nada fica em cache.
    c=conn(); users=c.execute('SELECT * FROM users WHERE ativo=1 ORDER BY nome').fetchall(); out=[]
    for u in users:
        rows=c.execute("""SELECT a.*,f.nome full_nome,p.nome product_nome
                          FROM assignments a
                          JOIN fulls f ON f.id=a.full_id
                          JOIN products p ON p.id=a.product_id
                          WHERE a.user_id=%s AND p.claimed_by=%s AND p.full_id=f.id""",(u['id'],u['id'])).fetchall()
        assigned=sum(int(r['quantidade'] or 0) for r in rows)
        sep=sum(int(r['separado'] or 0) for r in rows)
        boxed=0
        for r in rows:
            boxed += int(c.execute("""SELECT COALESCE(SUM(vi.quantidade),0) n
                                      FROM volume_items vi
                                      JOIN volumes v ON v.id=vi.volume_id
                                      WHERE v.full_id=%s AND vi.product_id=%s""",(r['full_id'],r['product_id'])).fetchone()['n'] or 0)
        sep_pct=round(sep/assigned*100,1) if assigned else 0
        box_pct=round(boxed/assigned*100,1) if assigned else 0
        out.append({
            'user_id':u['id'],'nome':u['nome'],
            'envios':len(set(r['full_id'] for r in rows)),
            'produtos':len(rows),'atribuido':assigned,'separado':sep,
            'em_volumes':boxed,'progresso':sep_pct,
            'progresso_volumes':box_pct,
            'progresso_operacional':min(sep_pct,box_pct),
            'concluidos':sum(1 for r in rows if int(r['separado'] or 0)>=int(r['quantidade'] or 0))
        })
    c.close(); return out

@app.post('/api/products/{product_id}/claim')
async def claim_product(product_id:str, body:ClaimIn, current_user=Depends(require_worker)):
    if current_user != body.user_id: raise HTTPException(403,'Você só pode assumir produtos em seu próprio nome.')
    c=conn()
    try:
        # Row-level lock: holds this product row until commit/rollback so two
        # collaborators can't claim it at the same time.
        p=c.execute('SELECT * FROM products WHERE id=%s FOR UPDATE',(product_id,)).fetchone()
        if not p: raise HTTPException(404,'Produto não encontrado.')
        if not c.execute('SELECT 1 FROM shipment_users WHERE full_id=%s AND user_id=%s',(p['full_id'],body.user_id)).fetchone(): raise HTTPException(403,'Você não está atribuído a este envio.')
        if p['claimed_by'] and p['claimed_by'] != body.user_id: raise HTTPException(409,'Este produto já foi assumido por outro colaborador.')
        if not p['claimed_by']:
            t=now(); c.execute('UPDATE products SET claimed_by=%s,claimed_at=%s WHERE id=%s',(body.user_id,t,product_id))
            c.execute('''INSERT INTO assignments (id,full_id,product_id,user_id,quantidade,separado,created_at,updated_at)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                         ON CONFLICT (product_id,user_id) DO UPDATE SET
                           quantidade=EXCLUDED.quantidade, updated_at=EXCLUDED.updated_at''',
                      (uid('asg_'),p['full_id'],product_id,body.user_id,p['quantidade'],0,t,t))
            c.execute("UPDATE fulls SET status=CASE WHEN status='aguardando' THEN 'em_separacao' ELSE status END,updated_at=%s WHERE id=%s",(t,p['full_id']))
        c.commit(); fid=p['full_id']
    except HTTPException:
        c.rollback(); c.close(); raise
    except Exception:
        c.rollback(); c.close(); raise
    data=full_summary(c,fid); c.close(); await hub.broadcast({'type':'full_updated','full_id':fid,'data':data}); return data

@app.delete('/api/products/{product_id}/claim')
async def release_product(product_id:str, user_id:str, current_user=Depends(require_worker)):
    if current_user != user_id: raise HTTPException(403,'Você só pode liberar produtos em seu próprio nome.')
    c=conn()
    p=c.execute('SELECT * FROM products WHERE id=%s',(product_id,)).fetchone()
    if not p: raise HTTPException(404,'Produto não encontrado.')
    if p['claimed_by'] and p['claimed_by'] != user_id: raise HTTPException(403,'Este produto está em separação por outro colaborador.')
    boxed=c.execute('SELECT COALESCE(SUM(vi.quantidade),0) n FROM volume_items vi JOIN volumes v ON v.id=vi.volume_id WHERE v.full_id=%s AND vi.product_id=%s',(p['full_id'],product_id)).fetchone()['n']
    if boxed>0: raise HTTPException(400,'Remova primeiro este produto das caixas antes de tirá-lo da separação.')
    c.execute('DELETE FROM assignments WHERE product_id=%s AND user_id=%s',(product_id,user_id))
    c.execute('UPDATE products SET claimed_by=NULL,claimed_at=NULL WHERE id=%s',(product_id,))
    remaining_tasks=c.execute('SELECT COUNT(*) n FROM assignments WHERE full_id=%s',(p['full_id'],)).fetchone()['n']
    if remaining_tasks==0:
        c.execute("UPDATE fulls SET status='aguardando',updated_at=%s WHERE id=%s",(now(),p['full_id']))
    c.commit(); fid=p['full_id']; c.close(); await changed(fid); return {'ok':True}

@app.put('/api/products/{product_id}')
async def edit_product(product_id:str, body:ProductEdit, _=Depends(require_admin)):
    c=conn()
    p=c.execute('SELECT * FROM products WHERE id=%s',(product_id,)).fetchone()
    if not p: c.close(); raise HTTPException(404,'Produto não encontrado.')
    fields={}
    if body.nome is not None:
        nome=body.nome.strip()
        if not nome: c.close(); raise HTTPException(400,'Nome não pode ficar vazio.')
        fields['nome']=nome
    if body.sku is not None: fields['sku']=body.sku.strip()
    if body.codigo_ml is not None: fields['codigo_ml']=body.codigo_ml.strip()
    if body.codigo_universal is not None: fields['codigo_universal']=body.codigo_universal.strip()
    if body.instrucoes is not None: fields['instrucoes']=body.instrucoes
    if body.quantidade is not None:
        sep_total=c.execute('SELECT COALESCE(SUM(separado),0) n FROM assignments WHERE product_id=%s',(product_id,)).fetchone()['n']
        boxed_total=c.execute('SELECT COALESCE(SUM(quantidade),0) n FROM volume_items WHERE product_id=%s',(product_id,)).fetchone()['n']
        minimo=max(sep_total,boxed_total)
        if body.quantidade<minimo:
            c.close(); raise HTTPException(400,f'A quantidade não pode ser menor que {minimo} (já separado/em caixas).')
        fields['quantidade']=body.quantidade
    if not fields:
        data=full_summary(c,p['full_id']); c.commit(); c.close(); return data
    sets=','.join(f'{k}=%s' for k in fields)
    c.execute(f'UPDATE products SET {sets} WHERE id=%s',(*fields.values(),product_id))
    c.commit(); fid=p['full_id']; c.close(); await changed(fid); return {'ok':True}

@app.delete('/api/products/{product_id}')
async def delete_product(product_id:str, _=Depends(require_admin)):
    c=conn()
    p=c.execute('SELECT * FROM products WHERE id=%s',(product_id,)).fetchone()
    if not p: c.close(); raise HTTPException(404,'Produto não encontrado.')
    fid=p['full_id']
    c.execute('DELETE FROM products WHERE id=%s',(product_id,))  # cascade remove atribuições e itens de caixa deste produto
    c.commit(); c.close(); await changed(fid); return {'ok':True}

@app.post('/api/fulls/{full_id}/assignments')
async def save_assignment(full_id:str, body:AssignmentIn, _=Depends(require_admin)):
    c=conn();
    p=c.execute('SELECT * FROM products WHERE id=%s AND full_id=%s',(body.product_id,full_id)).fetchone()
    u=c.execute('SELECT * FROM users WHERE id=%s AND ativo=1',(body.user_id,)).fetchone()
    if not p or not u: raise HTTPException(404,'Produto ou colaborador não encontrado.')
    other=c.execute('SELECT COALESCE(SUM(quantidade),0) n FROM assignments WHERE product_id=%s AND user_id<>%s',(body.product_id,body.user_id)).fetchone()['n']
    if other+body.quantidade>p['quantidade']: raise HTTPException(400,'A quantidade atribuída ultrapassa o total do produto.')
    existing=c.execute('SELECT * FROM assignments WHERE product_id=%s AND user_id=%s',(body.product_id,body.user_id)).fetchone()
    t=now()
    if existing:
        if body.quantidade==0: c.execute('DELETE FROM assignments WHERE id=%s',(existing['id'],))
        else: c.execute('UPDATE assignments SET quantidade=%s,updated_at=%s WHERE id=%s',(body.quantidade,t,existing['id']))
    elif body.quantidade>0:
        c.execute('INSERT INTO assignments VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',(uid('asg_'),full_id,body.product_id,body.user_id,body.quantidade,0,t,t))
    # auto-start if anything is assigned
    c.execute('UPDATE fulls SET status=CASE WHEN status=\'aguardando\' AND EXISTS(SELECT 1 FROM assignments WHERE full_id=%s) THEN \'em_separacao\' ELSE status END,updated_at=%s WHERE id=%s',(full_id,t,full_id))
    c.commit(); c.close(); await changed(full_id); return {'ok':True}

@app.put('/api/assignments/{assignment_id}/progress')
async def progress(assignment_id:str, body:ProgressIn, current_user=Depends(require_worker)):
    c=conn(); a=c.execute('SELECT a.*,p.quantidade product_total FROM assignments a JOIN products p ON p.id=a.product_id WHERE a.id=%s',(assignment_id,)).fetchone()
    if not a: raise HTTPException(404,'Tarefa não encontrada.')
    if a['user_id'] != current_user: c.close(); raise HTTPException(403,'Você só pode atualizar o progresso das suas próprias tarefas.')
    p=c.execute('SELECT claimed_by FROM products WHERE id=%s',(a['product_id'],)).fetchone()
    if p and p['claimed_by'] and p['claimed_by'] != a['user_id']: raise HTTPException(409,'Esta tarefa foi assumida por outro colaborador.')
    max_allowed=a['quantidade']; val=min(body.separado,max_allowed)
    c.execute('UPDATE assignments SET separado=%s,updated_at=%s WHERE id=%s',(val,now(),assignment_id))
    c.commit(); fid=a['full_id']; c.close(); await changed(fid); return {'ok':True}

@app.post('/api/fulls/{full_id}/volumes')
async def create_volume(full_id:str, body:VolumeCreate, _=Depends(require_admin_or_worker)):
    c=conn();
    if not c.execute('SELECT 1 FROM fulls WHERE id=%s',(full_id,)).fetchone(): raise HTTPException(404,'FULL não encontrado.')
    seq=c.execute('SELECT COALESCE(MAX(seq),0)+1 n FROM volumes WHERE full_id=%s',(full_id,)).fetchone()['n']; t=now(); vid=uid('vol_')
    c.execute('INSERT INTO volumes VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',(vid,full_id,seq,'mista','aberta',body.created_by,t,t)); c.commit(); c.close(); await changed(full_id); return {'id':vid,'seq':seq}

@app.delete('/api/volumes/{volume_id}')
async def delete_volume(volume_id:str, _=Depends(require_admin_or_worker)):
    c=conn(); v=c.execute('SELECT * FROM volumes WHERE id=%s',(volume_id,)).fetchone()
    if not v: raise HTTPException(404,'Volume não encontrado.')
    c.execute('DELETE FROM volumes WHERE id=%s',(volume_id,)); c.commit(); fid=v['full_id']; c.close(); await changed(fid); return {'ok':True}

@app.post('/api/volumes/{volume_id}/items')
async def add_volume_item(volume_id:str, body:VolumeItemIn, _=Depends(require_admin_or_worker)):
    c=conn(); v=c.execute('SELECT * FROM volumes WHERE id=%s',(volume_id,)).fetchone();
    if not v: raise HTTPException(404,'Volume não encontrado.')
    if v['status']=='fechada': raise HTTPException(400,'Volume fechado não pode ser alterado.')
    p=c.execute('SELECT * FROM products WHERE id=%s AND full_id=%s',(body.product_id,v['full_id'])).fetchone()
    if not p: raise HTTPException(404,'Produto não pertence a este FULL.')
    existing=c.execute('SELECT * FROM volume_items WHERE volume_id=%s AND product_id=%s',(volume_id,body.product_id)).fetchone()
    if body.modo not in {'somar','definir'}: raise HTTPException(400,'Modo de alteração inválido.')
    atual=existing['quantidade'] if existing else 0
    nova=body.quantidade if body.modo=='definir' else atual+body.quantidade
    current_other=c.execute('SELECT COALESCE(SUM(vi.quantidade),0) n FROM volume_items vi JOIN volumes vv ON vv.id=vi.volume_id WHERE vi.product_id=%s AND vv.full_id=%s AND vi.volume_id<>%s',(body.product_id,v['full_id'],volume_id)).fetchone()['n']
    separated=c.execute('SELECT COALESCE(SUM(a.separado),0) n FROM assignments a WHERE a.product_id=%s',(body.product_id,)).fetchone()['n']
    if current_other+nova>separated: raise HTTPException(400,'Você só pode colocar em caixas a quantidade que já foi separada.')
    if current_other+nova>p['quantidade']: raise HTTPException(400,'Quantidade em volumes ultrapassa o total do produto.')
    t=now()
    if existing: c.execute('UPDATE volume_items SET quantidade=%s WHERE id=%s',(nova,existing['id']))
    elif nova>0: c.execute('INSERT INTO volume_items VALUES (%s,%s,%s,%s)',(uid('vi_'),volume_id,body.product_id,nova))
    if nova==0: c.execute('DELETE FROM volume_items WHERE volume_id=%s AND product_id=%s',(volume_id,body.product_id))
    c.execute('UPDATE volumes SET updated_at=%s WHERE id=%s',(t,volume_id)); c.commit(); fid=v['full_id']; c.close(); await changed(fid); return {'ok':True}

@app.delete('/api/volume-items/{item_id}')
async def delete_volume_item(item_id:str, _=Depends(require_admin_or_worker)):
    c=conn(); x=c.execute('SELECT vi.*,v.full_id,v.status FROM volume_items vi JOIN volumes v ON v.id=vi.volume_id WHERE vi.id=%s',(item_id,)).fetchone()
    if not x: raise HTTPException(404,'Item não encontrado.')
    if x['status']=='fechada': raise HTTPException(400,'Volume fechado não pode ser alterado.')
    c.execute('DELETE FROM volume_items WHERE id=%s',(item_id,)); c.commit(); fid=x['full_id']; c.close(); await changed(fid); return {'ok':True}

@app.put('/api/volumes/{volume_id}/status')
async def volume_status(volume_id:str, _=Depends(require_admin_or_worker)):
    c=conn(); v=c.execute('SELECT * FROM volumes WHERE id=%s',(volume_id,)).fetchone()
    if not v: raise HTTPException(404,'Volume não encontrado.')
    n=c.execute('SELECT COUNT(*) n FROM volume_items WHERE volume_id=%s',(volume_id,)).fetchone()['n']
    if n==0: raise HTTPException(400,'Não é possível fechar uma caixa vazia.')
    c.execute('UPDATE volumes SET status=CASE WHEN status=\'aberta\' THEN \'fechada\' ELSE \'aberta\' END,updated_at=%s WHERE id=%s',(now(),volume_id)); c.commit(); fid=v['full_id']; c.close(); await changed(fid); return {'ok':True}

@app.get('/api/fulls/{full_id}/history')
def history(full_id:str, _=Depends(require_admin_or_worker)):
    c=conn(); rows=[dict(x) for x in c.execute('SELECT h.*,u.nome user_nome FROM status_history h LEFT JOIN users u ON u.id=h.user_id WHERE h.full_id=%s ORDER BY h.created_at',(full_id,))]; c.close(); return rows

@app.websocket('/ws')
async def ws_endpoint(ws:WebSocket):
    await hub.connect(ws)
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect: hub.disconnect(ws)
    except Exception: hub.disconnect(ws)

app.mount('/static', StaticFiles(directory=STATIC), name='static')
@app.get('/')
def root(): return FileResponse(STATIC/'index.html')
