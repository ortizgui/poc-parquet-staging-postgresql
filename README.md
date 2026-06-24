# POC v2 — Processamento Batch: S3 → SQS → SQS → PostgreSQL

Prova de conceito do fluxo completo com **duas filas SQS**, replicando o comportamento real de produção com S3 Event Notification.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    FLUXO DE PRODUCAO COM DUAS FILAS                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  S3                                       SQS #2 (registros)            │
│  ┌──────────────┐                        ┌──────────────────┐          │
│  │ custody_     │                        │ 1 msg = 1 linha  │          │
│  │ position     │                        │ do Parquet       │          │
│  │ .parquet     │                        └────────┬─────────┘          │
│  └──────┬───────┘                                 │                     │
│         │ S3 Event Notification                    │                     │
│         ▼                                          ▼                     │
│  ┌──────────────────┐              ┌──────────────────────────┐         │
│  │ SQS #1           │              │ consume_records_to_db.py │         │
│  │ poc-notification │              │ (ECS Task 2)             │         │
│  │ 1 msg = 1 arquivo│              │ Le lotes do SQS #2,      │         │
│  └────────┬─────────┘              │ valida, batch INSERT     │         │
│           │                        └────────────┬─────────────┘         │
│           ▼                                     │                       │
│  ┌──────────────────┐                            ▼                       │
│  │ consume_s3_event │              ┌──────────────────────────┐         │
│  │  .py (ECS Task 1)│              │ PostgreSQL (pocdb)       │         │
│  │ Le notificacao,  │              │ ┌──────────────────┐    │         │
│  │ processa Parquet,│              │ │ staging (20)     │    │         │
│  │ envia registros  │              │ │ error (4)        │    │         │
│  │ para SQS #2      │              │ └────────┬─────────┘    │         │
│  └──────────────────┘                       │                 │         │
│                                             ▼                 │         │
│                                   ┌──────────────────┐       │         │
│                                   │ merge_staging.py  │       │         │
│                                   │ Staging → Final   │       │         │
│                                   └──────────────────┘       │         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Indice

1. [O que cada etapa faz](#o-que-cada-etapa-faz)
2. [Stack](#stack)
3. [Setup](#setup)
4. [Execucao](#execucao)
5. [O papel de cada tabela](#o-papel-de-cada-tabela)
6. [Idempotencia](#idempotencia)
7. [E se algo der errado?](#e-se-algo-der-errado)

---

## O que cada etapa faz

### SQS #1 (poc-notification-queue) — Notificacao S3

Recebe **1 mensagem por arquivo** no formato que o S3 envia:

```json
{
  "Records": [{
    "eventName": "ObjectCreated:Put",
    "s3": {
      "bucket": { "name": "poc-bucket" },
      "object": { "key": "input/custody_position.parquet" }
    }
  }]
}
```

No LocalStack, simulamos com `simulate_s3_notification.py`.

### ECS Task 1 (consume_s3_event.py)

1. Le mensagem da SQS #1
2. Extrai bucket + key do evento S3
3. Le o Parquet em streaming (s3fs + row groups)
4. Envia **cada linha** como mensagem para a SQS #2
5. Deleta a notificacao da SQS #1

### SQS #2 (poc-record-queue) — Registros individuais

Recebe **1 mensagem por linha do Parquet**:

```json
{
  "batch_id": "uuid-do-processamento",
  "source_file": "s3://poc-bucket/input/arquivo.parquet",
  "row_number": 0,
  "record": {
    "account_id": "ACC001",
    "asset_id": "PETR4",
    "reference_date": "2025-01-15",
    "quantity": 1000.0,
    "amount": 25000.0
  }
}
```

### ECS Task 2 (consume_records_to_db.py)

1. Recebe ate 10 mensagens da SQS #2 (long polling 5s)
2. Para cada mensagem: valida o registro
3. Batch INSERT via `execute_values`: validos → staging, invalidos → error
4. COMMIT no PostgreSQL
5. Deleta as mensagens processadas da SQS #2
6. Se o container morre antes de deletar, a mensagem volta pra fila em 30s

### merge_staging.py (inalterado)

Upsert em lotes de 10.000: staging (PENDING) → tabela final.

---

## Stack

| Componente | Funcao |
|------------|--------|
| Docker Compose | PostgreSQL + LocalStack (S3 + SQS) |
| LocalStack | S3 + SQS simulados |
| PostgreSQL 16 | staging + error + final |
| s3fs + pyarrow | Leitura de Parquet via Range GET |
| boto3 | SQS producer/consumer |
| psycopg2 + execute_values | Batch INSERT no DB |

---

## Setup

```bash
docker compose up -d
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

---

## Execucao

```bash
# 1. Gerar arquivo Parquet
python scripts/create_sample_file.py
# Output: 24 linhas

# 2. Enviar para o S3
python scripts/upload_to_s3.py
# Output: Bucket criado / Arquivo enviado

# 3. SIMULAR: S3 envia notificacao para SQS #1
python scripts/simulate_s3_notification.py \
  --bucket poc-bucket \
  --key input/custody_position.parquet
# Output:
#   S3 Event enviado para poc-notification-queue
#   Bucket: poc-bucket
#   Key:    input/custody_position.parquet

# 4. ECS TASK 1: Consome SQS #1, le Parquet, envia registros para SQS #2
python scripts/consume_s3_event.py
# Output:
#   [PROCESS] batch_id=... arquivo=s3://poc-bucket/input/...
#   Linhas: 24 | Row groups: 1
#   RG 0: enviados para SQS #2
#   [OK] Notificacao processada. 24 registros enviados para SQS #2

# 5. ECS TASK 2: Consome SQS #2, valida, batch INSERT no PostgreSQL
python scripts/consume_records_to_db.py
# Output:
#   Lote: 10 msgs | staging: 10 | erro: 0
#   Lote: 10 msgs | staging: 10 | erro: 0
#   Lote: 4 msgs | staging: 0 | erro: 4
#   Recebidas: 24 | Validas: 20 | Invalidas: 4

# 6. Merge para tabela final
python scripts/merge_staging.py
# Output: 20 registros mergeados, 20 na tabela final
```

### Execucao em paralelo (simulando producao)

```bash
# Terminal 1 — Simula notificacao e processa
python scripts/simulate_s3_notification.py --bucket poc-bucket --key input/custody_position.parquet
python scripts/consume_s3_event.py

# Terminal 2 — Consome registros (pode rodar antes, durante ou depois)
python scripts/consume_records_to_db.py

# Terminal 3 — Merge (so depois que o consumer terminar)
python scripts/merge_staging.py
```

---

## Idempotencia

### Se a SQS #1 entregar a mesma notificacao 2x

```
SQS #1 (at-least-once)
  → consume_s3_event.py processa 2x o mesmo arquivo
  → SQS #2 recebe registros duplicados
  → consume_records_to_db.py:
      ON CONFLICT (source_file, row_number) DO NOTHING
      → staging ignora duplicatas
  → Nao duplica no DB. Processamento extra, mas dados consistentes.
```

### Se o ECS Task 2 morre antes de deletar da SQS #2

```
consume_records_to_db.py:
  1. Recebe 10 mensagens da SQS #2
  2. Foi inserido no DB   ← mas CRASHOU antes de deletar
  3. ...
  4. Visibilidade expira em 30s → msgs voltam pra SQS #2
  5. Outro consumer processa de novo
  6. ON CONFLICT DO NOTHING → staging nao duplica
  7. Mensagens sao deletadas na segunda tentativa
```

### Se o merge roda 2x

```
merge_staging.py:
  - So processa WHERE status = 'PENDING'
  - Apos merge: status = 'MERGED'
  - Segunda execucao: 0 PENDING → nada a fazer
```

---

## E se algo der errado?

| Problema | O que acontece | Recuperacao |
|----------|---------------|-------------|
| SQS #1 sem mensagem | Nao rodou simulate_s3_notification.py | Rodar simulate + consume_s3_event |
| SQS #2 vazia | consume_s3_event nao rodou ou Parquet vazio | Rodar consume_s3_event primeiro |
| Consumer morre no batch INSERT | Msgs voltam pra SQS #2 em 30s | Reprocessa (ON CONFLICT protege) |
| Container PostgreSQL cai | Consumer nao consegue inserir → msgs voltam pra fila | Aguardar DB subir, reprocessa |
| Arquivo Parquet corrompido | consume_s3_event falha | Corrigir arquivo, reenviar notificacao |

---

## Tabelas (mesmo schema da v1)

- `custody_position` — Final (UK: account_id, asset_id, reference_date)
- `custody_position_staging` — Staging (batch_id, source_file, row_number, record_hash, status)
- `custody_position_error` — Erros (payload JSONB, error_reason)
