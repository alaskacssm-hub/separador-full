from fastapi import FastAPI, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
import psycopg
from psycopg.rows import dict_row
import json, os, re, uuid, asyncio
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

def conn():
    c=psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=False)
    return c

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
    users=c.execute('SELECT * FROM users WHERE ativo=1 ORDER BY nome').fetchall()
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

class UserCreate(BaseModel): nome: str
class ShipmentUserIn(BaseModel): user_id: str
class ClaimIn(BaseModel): user_id: str
class AssignmentIn(BaseModel): product_id: str; user_id: str; quantidade: int=Field(ge=0)
class ProgressIn(BaseModel): separado: int=Field(ge=0)
class StatusIn(BaseModel): status: str; user_id: str|None=None
class VolumeCreate(BaseModel): created_by: str|None=None
class VolumeItemIn(BaseModel): product_id: str; quantidade: int=Field(ge=0); modo: str='somar'

@app.get('/api/health')
def health(): return {'ok':True}

@app.post('/api/import-pdf')
async def import_pdf(file: UploadFile=File(...)):
    if not file.filename.lower().endswith('.pdf'): raise HTTPException(400,'Envie um arquivo PDF.')
    tmp=BASE/('_tmp_'+uid()+'.pdf')
    data=await file.read(); tmp.write_bytes(data)
    try: parsed=extract_pdf(tmp)
    except Exception as e: raise HTTPException(400,f'Não foi possível ler o PDF: {e}')
    finally:
        try: tmp.unlink()
        except: pass
    return {'filename':file.filename,'parsed':parsed}

@app.post('/api/fulls')
async def create_full(body:FullCreate):
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

@app.get('/api/fulls')
def list_fulls():
    c=conn(); rows=c.execute('SELECT * FROM fulls ORDER BY COALESCE(previsao_data,\'9999-12-31\'),created_at DESC').fetchall(); out=[]
    for f in rows:
        d=full_summary(c,f['id']); out.append({'full':d['full'],'stats':d['stats']})
    c.close(); return out

@app.get('/api/fulls/{full_id}')
def get_full(full_id:str):
    c=conn(); d=full_summary(c,full_id); c.commit(); c.close(); return d

@app.post('/api/users')
async def create_user(body:UserCreate):
    name=body.nome.strip()
    if not name: raise HTTPException(400,'Nome obrigatório.')
    c=conn()
    try:
        c.execute('INSERT INTO users VALUES (%s,%s,%s,%s)',(uid('usr_'),name,1,now())); c.commit()
    except psycopg.errors.UniqueViolation:
        c.rollback(); c.close(); raise HTTPException(409,'Colaborador já cadastrado.')
    rows=[dict(x) for x in c.execute('SELECT * FROM users WHERE ativo=1 ORDER BY nome')]; c.close(); await hub.broadcast({'type':'users_updated'}); return rows

@app.get('/api/users')
def list_users():
    c=conn(); rows=[dict(x) for x in c.execute('SELECT * FROM users WHERE ativo=1 ORDER BY nome')]; c.close(); return rows

@app.put('/api/fulls/{full_id}/status')
async def set_status(full_id:str, body:StatusIn):
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
async def add_shipment_collaborator(full_id:str, body:ShipmentUserIn):
    c=conn()
    if not c.execute('SELECT 1 FROM fulls WHERE id=%s',(full_id,)).fetchone(): raise HTTPException(404,'FULL não encontrado.')
    if not c.execute('SELECT 1 FROM users WHERE id=%s AND ativo=1',(body.user_id,)).fetchone(): raise HTTPException(404,'Colaborador não encontrado.')
    try:
        c.execute('INSERT INTO shipment_users (id,full_id,user_id,created_at) VALUES (%s,%s,%s,%s)',(uid('su_'),full_id,body.user_id,now())); c.commit()
    except psycopg.errors.UniqueViolation:
        c.rollback(); c.close(); raise HTTPException(409,'Colaborador já está atribuído a este envio.')
    data=full_summary(c,full_id); c.close(); await hub.broadcast({'type':'full_updated','full_id':full_id,'data':data}); return data

@app.delete('/api/fulls/{full_id}/collaborators/{user_id}')
async def remove_shipment_collaborator(full_id:str,user_id:str):
    c=conn(); c.execute('DELETE FROM shipment_users WHERE full_id=%s AND user_id=%s',(full_id,user_id)); c.commit(); data=full_summary(c,full_id); c.close(); await hub.broadcast({'type':'full_updated','full_id':full_id,'data':data}); return data

@app.get('/api/performance')
def performance():
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
async def claim_product(product_id:str, body:ClaimIn):
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
async def release_product(product_id:str, user_id:str):
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

@app.post('/api/fulls/{full_id}/assignments')
async def save_assignment(full_id:str, body:AssignmentIn):
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
async def progress(assignment_id:str, body:ProgressIn):
    c=conn(); a=c.execute('SELECT a.*,p.quantidade product_total FROM assignments a JOIN products p ON p.id=a.product_id WHERE a.id=%s',(assignment_id,)).fetchone()
    if not a: raise HTTPException(404,'Tarefa não encontrada.')
    p=c.execute('SELECT claimed_by FROM products WHERE id=%s',(a['product_id'],)).fetchone()
    if p and p['claimed_by'] and p['claimed_by'] != a['user_id']: raise HTTPException(409,'Esta tarefa foi assumida por outro colaborador.')
    max_allowed=a['quantidade']; val=min(body.separado,max_allowed)
    c.execute('UPDATE assignments SET separado=%s,updated_at=%s WHERE id=%s',(val,now(),assignment_id))
    c.commit(); fid=a['full_id']; c.close(); await changed(fid); return {'ok':True}

@app.post('/api/fulls/{full_id}/volumes')
async def create_volume(full_id:str, body:VolumeCreate):
    c=conn();
    if not c.execute('SELECT 1 FROM fulls WHERE id=%s',(full_id,)).fetchone(): raise HTTPException(404,'FULL não encontrado.')
    seq=c.execute('SELECT COALESCE(MAX(seq),0)+1 n FROM volumes WHERE full_id=%s',(full_id,)).fetchone()['n']; t=now(); vid=uid('vol_')
    c.execute('INSERT INTO volumes VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',(vid,full_id,seq,'mista','aberta',body.created_by,t,t)); c.commit(); c.close(); await changed(full_id); return {'id':vid,'seq':seq}

@app.delete('/api/volumes/{volume_id}')
async def delete_volume(volume_id:str):
    c=conn(); v=c.execute('SELECT * FROM volumes WHERE id=%s',(volume_id,)).fetchone()
    if not v: raise HTTPException(404,'Volume não encontrado.')
    c.execute('DELETE FROM volumes WHERE id=%s',(volume_id,)); c.commit(); fid=v['full_id']; c.close(); await changed(fid); return {'ok':True}

@app.post('/api/volumes/{volume_id}/items')
async def add_volume_item(volume_id:str, body:VolumeItemIn):
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
async def delete_volume_item(item_id:str):
    c=conn(); x=c.execute('SELECT vi.*,v.full_id,v.status FROM volume_items vi JOIN volumes v ON v.id=vi.volume_id WHERE vi.id=%s',(item_id,)).fetchone()
    if not x: raise HTTPException(404,'Item não encontrado.')
    if x['status']=='fechada': raise HTTPException(400,'Volume fechado não pode ser alterado.')
    c.execute('DELETE FROM volume_items WHERE id=%s',(item_id,)); c.commit(); fid=x['full_id']; c.close(); await changed(fid); return {'ok':True}

@app.put('/api/volumes/{volume_id}/status')
async def volume_status(volume_id:str):
    c=conn(); v=c.execute('SELECT * FROM volumes WHERE id=%s',(volume_id,)).fetchone()
    if not v: raise HTTPException(404,'Volume não encontrado.')
    n=c.execute('SELECT COUNT(*) n FROM volume_items WHERE volume_id=%s',(volume_id,)).fetchone()['n']
    if n==0: raise HTTPException(400,'Não é possível fechar uma caixa vazia.')
    c.execute('UPDATE volumes SET status=CASE WHEN status=\'aberta\' THEN \'fechada\' ELSE \'aberta\' END,updated_at=%s WHERE id=%s',(now(),volume_id)); c.commit(); fid=v['full_id']; c.close(); await changed(fid); return {'ok':True}

@app.get('/api/fulls/{full_id}/history')
def history(full_id:str):
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
