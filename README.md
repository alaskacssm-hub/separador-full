# FULL Control

Sistema de separação de envios FULL (Mercado Livre Fulfillment), com backend em **FastAPI**,
banco de dados **PostgreSQL** e front-end em HTML/JS puro servido pelo próprio backend.
Atualizações entre colaboradores acontecem em tempo real via WebSocket.

## Fluxo

### Colaborador
1. Seleciona o próprio nome.
2. Vê somente os envios aos quais foi atribuído.
3. Abre um envio e vê os produtos disponíveis.
4. Toca em "Separar este produto" — o produto fica reservado só para ele.
5. Informa a quantidade separada em "Produtos em separação".
6. Registra os produtos em caixas (volumes), podendo escolher caixa nova ou existente.
7. Pode editar, excluir, fechar e reabrir caixas.
8. Só pode remover um produto da própria separação depois de tirá-lo dos volumes.

### Caixas
Não existem capacidades fixas — uma caixa pode receber vários produtos e unidades.

### Administrador
- Cadastro/importação de envios (a partir do PDF de instruções do Mercado Livre)
- Cadastro de colaboradores e atribuição a envios
- Dashboard, acompanhamento em tempo real e desempenho individual
- Controle de volumes e status do envio

---

## Rodando localmente

### 1. Banco de dados
O app precisa de um PostgreSQL. O jeito mais rápido é criar um banco gratuito no
[Neon](https://neon.tech) (leva ~1 minuto, não precisa instalar nada):

1. Crie uma conta em neon.tech e um novo projeto.
2. Copie a *connection string* (algo como
   `postgresql://usuario:senha@ep-xxxx.sa-east-1.aws.neon.tech/neondb?sslmode=require`).

### 2. Configurar
Copie `.env.example` para `.env` e cole a connection string:

```
DATABASE_URL=postgresql://usuario:senha@ep-xxxx.aws.neon.tech/neondb?sslmode=require
```

### 3. Rodar
No Windows, dê dois cliques em `run.bat` (ele instala as dependências e abre o navegador
automaticamente). Em outro sistema:

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

Acesse `http://localhost:8000`. As tabelas são criadas automaticamente no primeiro start.

> Não abra `static/index.html` diretamente — o front-end depende da API do backend.

---

## Colocando no GitHub

```bash
git init
git add .
git commit -m "FULL Control"
git branch -M main
git remote add origin https://github.com/SEU_USUARIO/full-control.git
git push -u origin main
```

O `.gitignore` já exclui o `.env` (suas credenciais nunca vão para o repositório).

---

## Deploy online (link acessível 24/7, grátis)

**Banco de dados:** [Neon](https://neon.tech) — plano gratuito, "dorme" quando não tem uso e
acorda sozinho na próxima requisição, sem perder dados.

**Aplicação:** [Render](https://render.com) — plano gratuito, conecta direto no repositório do GitHub.

Passo a passo:

1. Suba o projeto no GitHub (seção acima).
2. Crie um banco no [Neon](https://neon.tech) e copie a connection string.
3. Em [render.com](https://render.com), clique em **New > Blueprint**, aponte para o seu
   repositório do GitHub (o `render.yaml` já está configurado no projeto).
4. Quando o Render pedir a variável `DATABASE_URL`, cole a connection string do Neon.
5. Aguarde o deploy — o Render te dá uma URL pública tipo
   `https://full-control.onrender.com`.

No plano gratuito do Render, o serviço "dorme" após ~15 minutos sem uso e demora alguns
segundos para acordar na primeira requisição seguinte — normal para uso interno/pequena equipe.
Se isso incomodar, dá para subir para o plano pago (a partir de ~US$7/mês) para manter sempre ativo.

---

## Desempenho
O desempenho é calculado sobre o estado atual do banco. Se uma separação ou caixa for
removida, ela deixa de contar. O progresso operacional considera separação e volumes.

## PDF
O sistema lê o PDF de instruções de preparação do Mercado Livre Fulfillment e confere o
total de produtos/unidades antes de criar o envio.
