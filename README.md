# POC v2 — Processamento Batch de Parquet → SQS → PostgreSQL

Segunda versão da prova de conceito. Agora com **fila SQS entre a leitura do Parquet e o banco**, demonstrando desacoplamento entre produtor e consumidor.

```
S3 Notification
      │
      ▼
┌──────────────────────┐
│  parquet_to_sqs.py    │  ← ECS Task 1 (produtor)
│  Le o Parquet em      │
│  streaming e envia    │
│  cada registro para   │
│  uma fila SQS         │
└──────────┬───────────┘
           │
           ▼
   ┌───────────────┐
   │  SQS Queue     │  ← Buffer de mensagens
   │  (poc-queue)   │    (at-least-once delivery)
   └───────┬───────┘
           │
           ▼
┌──────────────────────┐
│  process_sqs_to_db.py│  ← ECS Task 2 (consumidor)
│  Recebe lotes do SQS,│
│  valida e insere em  │
│  batch no PostgreSQL │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  merge_staging.py     │  ← Merge upsert
│  Staging → Final      │
└──────────────────────┘
```

## Índice

1. [Problema de negócio](#problema-de-negócio)
2. [Por que esta arquitetura?](#por-que-esta-arquitetura)
3. [Stack tecnológica](#stack-tecnológica)
4. [Pré-requisitos](#pré-requisitos)
5. [Setup do ambiente](#setup-do-ambiente)
6. [Execução do pipeline](#execução-do-pipeline)
7. [O papel de cada tabela](#o-papel-de-cada-tabela)
8. [Idempotência](#idempotência)
9. [SQS: decisões de design](#sqs-decisões-de-design)
10. [Comparação: v1 (direto) vs v2 (SQS)](#comparação-v1-direto-vs-v2-sqs)
11. [E se algo der errado?](#e-se-algo-der-errado)
12. [Troubleshooting](#troubleshooting)

---

## Problema de negócio

Instituições financeiras precisam processar diariamente arquivos de posição de custódia.
A v2 resolve o mesmo problema da v1, mas com uma **arquitetura desacoplada por fila**,
adequada para cenários onde:

- O produtor e consumidor podem escalar independentemente
- O volume de dados varia (a fila absorve picos)
- Diferentes times são responsáveis pela ingestão e pelo processamento
- Há necessidade de reprocessamento seletivo sem re-download do Parquet

---

## Por que esta arquitetura?

### Fluxo completo

```
S3 (Parquet)
    │
    ▼
parquet_to_sqs.py
    │  Leitura streaming via s3fs (Range GET)
    │  Para cada linha → mensagem JSON
    │  Envia em lotes de 10 para SQS
    │  Sem validacao — tudo vai para fila
    ▼
SQS Queue (poc-queue)
    │
    ▼
process_sqs_to_db.py
    │  Recebe ate 10 mensagens por vez (long polling)
    │  Valida cada registro
    │  Batch INSERT validos → staging
    │  Batch INSERT invalidos → error table
    │  Deleta mensagens processadas
    ▼
merge_staging.py
    │  Merge upsert em lotes
    │  Staging (PENDING) → Final
    ▼
custody_position (final)
```

### Por que SQS entre o Parquet e o banco?

A v1 inseria direto na staging (via batch INSERT). A v2 adiciona uma fila SQS:

```
v1 (direto):       S3 → process_file.py → staging → merge → final
v2 (SQS):          S3 → parquet_to_sqs.py → SQS → process_sqs_to_db.py → staging → merge → final
```

Benefícios da fila:

| Benefício | Explicação |
|-----------|------------|
| **Desacoplamento** | Produtor e consumidor escalam independentemente |
| **Buffer de picos** | SQS segura mensagens se o banco estiver lento |
| **Reprocessamento granular** | Mensagens que falharam voltam para a fila automaticamente |
| **Paralelismo** | Multiplos consumidores processam a mesma fila |
| **Resiliência** | Se o consumer morre, a mensagem volta pra fila em 30s |

### Custo da fila

Cada registro vira uma mensagem SQS. Para 1M registros: 1M mensagens SQS.
Na AWS, SQS standard custa $0.40/milhão de requests. O custo é baixo, mas existe.

---

## Stack tecnológica

| Componente | Tecnologia | Função |
|------------|-----------|--------|
| Container | Docker Compose | Orquestração local |
| S3 emulator | LocalStack | S3 + SQS simulados |
| Banco | PostgreSQL 16 | Armazenamento |
| Streaming S3 | s3fs / pyarrow | Leitura de Parquet via Range GET |
| SQS | boto3 (LocalStack) | Fila de mensagens |
| DB driver | psycopg2-binary | Conexão PostgreSQL |

---

## Pré-requisitos

- Docker e Docker Compose
- Python 3.12+
- ~2 GB de RAM livre

---

## Setup do ambiente

```bash
# Subir servicos (PostgreSQL + LocalStack com S3 + SQS)
docker compose up -d

# Verificar se estao saudaveis
docker compose ps

# Criar ambiente Python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Execução do pipeline

```bash
# Etapa 1 — Gerar arquivo Parquet
python scripts/create_sample_file.py
# Output: Arquivo gerado: ./data/input/custody_position.parquet (24 linhas)

# Etapa 2 — Enviar para o S3 local
python scripts/upload_to_s3.py
# Output: Bucket criado / Arquivo enviado

# Etapa 3 — Produtor: ler Parquet e enviar cada registro para o SQS
python scripts/parquet_to_sqs.py --bucket poc-bucket --key input/custody_position.parquet
# Output:
#   batch_id: 4a4e6dcf-...
#   Total linhas: 24 | Row groups: 1
#   RG 0: 24 linhas enviadas para SQS
#   enviadas: 24
#   fila SQS: http://localhost:4566/000000000000/poc-queue

# Etapa 4 — Consumidor: ler do SQS, validar e inserir no PostgreSQL
python scripts/process_sqs_to_db.py
# Output:
#   Lote: 10 msgs, 10 staging, 0 erros
#   Lote: 10 msgs, 10 staging, 0 erros
#   Lote: 4 msgs, 0 staging, 4 erros
#   Nenhuma mensagem na fila. Consumidor encerrado.
#   recebidas:  24
#   validas:    20
#   invalidas:  4
#   deletadas:  24

# Etapa 5 — Merge para tabela final
python scripts/merge_staging.py
# Output:
#   Registros pendentes antes: 20
#   Lote 10/20: ...
#   Lote 20/20: ...
```

### Ordem dos consumidores

O `process_sqs_to_db.py` pode ser executado **em paralelo** com o produtor,
ou **depois**. Se executar antes, a fila estará vazia e ele encerra.
Se executar depois, ele consome as mensagens enfileiradas.

Para simular processamento contínuo:

```bash
# Terminal 1: produtor
python scripts/parquet_to_sqs.py --bucket poc-bucket --key input/custody_position.parquet

# Terminal 2: consumidor (pode rodar antes, durante ou depois)
python scripts/process_sqs_to_db.py
```

---

## O papel de cada tabela

(Mesmo esquema da v1 — inalterado)

### `custody_position` (final)
| Coluna | Tipo | Função |
|--------|------|--------|
| `account_id` | VARCHAR | Conta |
| `asset_id` | VARCHAR | Ativo |
| `reference_date` | DATE | Data |
| `quantity` | NUMERIC(18,4) | Quantidade |
| `amount` | NUMERIC(18,2) | Valor |
| `updated_at` | TIMESTAMP | Atualização |
| **UK** | (account_id, asset_id, reference_date) | 1 posição |

### `custody_position_staging`
| Coluna | Função |
|--------|--------|
| `batch_id` | UUID do lote |
| `source_file` | Origem (s3://...) |
| `row_number` | Linha no arquivo |
| `record_hash` | SHA256 do registro |
| `status` | PENDING → MERGED |

### `custody_position_error`
| Coluna | Função |
|--------|--------|
| `payload` | JSONB com cópia do registro |
| `error_reason` | Motivo da rejeição |
| **UK** | (source_file, row_number) |

---

## Idempotência

### Produtor (parquet_to_sqs.py)
- Cada execução gera um novo `batch_id`
- Se rodar 2x, envia 2x as mensagens para o SQS
- Não há proteção contra duplicação no produtor — é proposital

### SQS
- SQS standard é **at-least-once**: uma mensagem pode chegar 2x ao consumidor
- O consumidor precisa ser idempotente

### Consumidor (process_sqs_to_db.py)
```sql
INSERT INTO staging ... ON CONFLICT (source_file, row_number) DO NOTHING
```
- Se a mesma mensagem chegar 2x, o segundo INSERT é ignorado
- Se o consumidor cai antes de deletar, a mensagem volta em 30s
  e é reprocessada — `ON CONFLICT` impede duplicação
- Se o consumidor cai depois de inserir mas antes de deletar,
  mesma proteção — staging não duplica

### Merge (merge_staging.py)
- Processa só `status = 'PENDING'`
- `FOR UPDATE SKIP LOCKED` para concorrência
- Após merge: `status = 'MERGED'` — não processa de novo

---

## SQS: decisões de design

### Por que enviar TODOS os registros para o SQS (inclusive inválidos)?

A validação poderia acontecer no produtor, enviando só os válidos para a fila.
Optamos por enviar **todos** porque:

| Motivo | Explicação |
|--------|------------|
| **Produtor mais rápido** | Só leitura + serialize + SQS. Sem DB |
| **Consumidor com lógica completa** | Validação + staging + erro no mesmo lugar |
| **Rastreabilidade** | Todo registro passou pela fila — mesmo os inválidos |
| **Desacoplamento** | Produtor não precisa conhecer regras de validação |

O custo: algumas mensagens a mais na fila (os registros inválidos).

### Tamanho do lote SQS

| Operação | Limite SQS | Usado |
|----------|-----------|-------|
| `send_message_batch` | 10 mensagens | ✅ 10 |
| `receive_message` | 10 mensagens | ✅ 10 (configurável) |
| Mensagem individual | 256 KB | ✅ << 256 KB |

Cada mensagem SQS contém o registro completo em JSON (~200 bytes).
Muito abaixo do limite de 256 KB.

### Long polling

O consumidor usa `WaitTimeSeconds=5` (long polling):
- Se a fila está vazia, espera até 5s por novas mensagens
- Reduz chamadas de API (menos custo)
- Se após 5s não há mensagens, assume que acabou e encerra

### Visibilidade (Visibility Timeout)

Quando o consumidor recebe uma mensagem, ela fica **invisível** por 30s.
Se o consumidor não deletar nesse tempo, a mensagem volta para a fila:

```
Consumidor recebe msg          → invisivel por 30s
Consumidor insere no DB + COMMIT + deleta msg → OK
                              ou
Consumidor CRASHA              → msg volta pra fila em 30s
                              → outro consumidor processa
```

---

## Comparação: v1 (direto) vs v2 (SQS)

| Característica | v1 (process_file.py) | v2 (SQS) |
|---------------|---------------------|----------|
| **Leitura** | Streaming de row groups | Streaming de row groups |
| **Validação** | No mesmo script | No consumidor |
| **INSERT** | Batch (`execute_values`) | Batch (`execute_values`) |
| **Acoplamento** | Alto (1 script faz tudo) | Baixo (produtor ≠ consumidor) |
| **Escalabilidade** | Vertical (mais workers) | Horizontal (mais consumers) |
| **Buffer de pico** | Sem buffer | Fila SQS |
| **Custo AWS extra** | Nenhum | SQS (~$0.40/1M msgs) |
| **Latência** | Menor (direto) | Maior (passa pela fila) |
| **Complexidade** | Menor | Moderada |
| **Reprocessamento granular** | Arquivo inteiro | Mensagens individuais |
| **Consistência** | Imediata | Eventual (delay da fila) |

---

## E se algo der errado?

### Produtor cai no meio do Parquet

```
Estado: parte do arquivo foi para SQS
Recuperacao:
  • Rodar parquet_to_sqs.py de novo
  • ON CONFLICT (source_file, row_number) na staging protege duplicatas
  • SQS terá mensagens duplicadas — mas staging ignora
```

### Consumidor cai depois de inserir mas antes de deletar

```
Estado: dados na staging, mensagem ainda na fila
Recuperacao:
  • Visibilidade expira em 30s → mensagem volta pra fila
  • Outro consumidor (ou o mesmo reiniciado) processa de novo
  • ON CONFLICT DO NOTHING → staging nao duplica
```

### SQS fica fora do ar (LocalStack cai)

```
Estado: produtor nao consegue enviar
Recuperacao:
  • docker compose restart
  • Rodar o produtor de novo
  • Mensagens nao enviadas = processadas de novo do Parquet
```

### Consumidor processa mensagem inválida

```
Estado: registro vai para custody_position_error
Recuperacao:
  • Corrigir o dado de origem
  • Regerar Parquet e reprocessar
  • Ou: UPDATE direto na staging (correção manual)
```

---

## Troubleshooting

| Problema | Causa | Solução |
|----------|-------|---------|
| SQS queue não existe | Primeira execução | `parquet_to_sqs.py` cria automaticamente |
| Consumer não pega mensagens | Fila vazia ou produtor não rodou | Execute o produtor primeiro |
| `Connection refused` no SQS | LocalStack sem SQS | `docker compose up -d` recria o container |
| Mensagens voltando para fila | Consumer lento ou crashou | Aumente `VISIBILITY_TIMEOUT` |
| Dados duplicados na staging | Mensagem entregue 2x | ON CONFLICT DO NOTHING protege — verifique se está ativo |
| Merge não processa staging | Status não é PENDING | `WHERE status = 'PENDING'` — verifique o status |
