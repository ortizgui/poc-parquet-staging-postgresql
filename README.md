# POC — Ingestão Massiva de Dados: Parquet → Staging → Principal

Prova de conceito do fluxo de ingestão massiva de dados com staging table e merge controlado.

## Arquitetura

```mermaid
flowchart TD
    subgraph S3 ["S3 (Origem dos Dados)"]
        PARQUET[Arquivos Parquet<br/>até 5.000 registros cada]
    end

    subgraph SNS ["SNS (Notificação)"]
        NOTIFICATION[SNS Topic<br/>Notifica novo arquivo]
    end

    subgraph ECS ["ECS Service (Consumer)"]
        CONSUMER[Consumer ECS<br/>Lê Parquet<br/>Bulk INSERT<br/>Staging Table]
    end

    subgraph STAGING ["PostgreSQL - Staging"]
        STAGING_TABLE[custody_position_staging<br/>Landing Zone<br/>Append-only<br/>Idempotente]
        ERROR_TABLE[custody_position_error<br/>Registros inválidos]
    end

    subgraph CRON ["Merge Job (Cron/Scheduler)"]
        MERGE[merge_staging.py<br/>Batch Upsert<br/>INSERT/UPDATE<br/>DELETE from Staging]
    end

    subgraph PRINCIPAL ["PostgreSQL - Principal"]
        FINAL_TABLE[custody_position<br/>Tabela Final<br/>Produção]
    end

    PARQUET -->|S3 Event| SNS
    SNS -->|Fanout| CONSUMER
    CONSUMER -->|Bulk INSERT| STAGING_TABLE
    CONSUMER -->|Invalid Rows| ERROR_TABLE
    STAGING_TABLE -->|Merge em batches| MERGE
    MERGE -->|INSERT/UPDATE| FINAL_TABLE
    MERGE -->|DELETE merged| STAGING_TABLE
```

## Fluxo de Dados

```
1. S3: Arquivos Parquet chegam (até 5.000 registros cada)
       ↓
2. SNS: Notificação enviada ao ECS Consumer
       ↓
3. ECS: Lê parquet e faz bulk insert na staging table
       ↓
4. Staging: Dados aguardam processamento
       ↓
5. Cron: merge_staging.py roda a cada X segundos
       ↓
6. Merge: INSERT novos + UPDATE modificados + DELETE da staging
       ↓
7. Principal: Dados disponíveis para aplicações
```

## Scripts Disponíveis

| Script | Função |
|--------|--------|
| `process_file.py` | Lê parquet do S3 e insere na staging (bulk insert) |
| `merge_staging.py` | Merge da staging para principal (batch + throttle) |
| `simulate_load.py` | Simula carga para validação (testa só o merge) |
| `generate_report.py` | Gera relatório HTML das métricas |
| `seed_database.py` | Preenche base com dados de teste |
| `setup_infra.py` | Cria infraestrutura S3/SNS/SQS no LocalStack |
| `consume_s3_event.py` | Consumer que polling SQS e chama process_file.py |
| `generate_parquets.py` | Gera múltiplos arquivos Parquet e sobe para S3 |
| `simulate_s3_notification.py` | Simula notificação SNS (S3 Event) |

## Teste Completo End-to-End

### Teste Aurora-like (40 arquivos)

```bash
./run_complete_test.sh --files 40 --records-per-file 5000 --existing 100000 --batch 2000 --delay 0.5
```

**Resultado**: 40 arquivos × 5.000 registros = **200.000 registros** totais

Parametros:
- `--files 40`: 40 arquivos Parquet
- `--records-per-file 5000`: 5.000 registros por arquivo
- `--existing 100000`: 100k registros ja existentes na tabela principal
- `--batch 2000`: batch size do merge (sweet spot identificado)
- `--delay 0.5`: delay entre batches (sweet spot para throttle)

### Teste Rápido (10 arquivos)

```bash
./run_complete_test.sh --files 10 --records-per-file 5000 --existing 100000
```

### Opções do run_complete_test.sh

| Opção | Default | Descrição |
|-------|---------|-----------|
| `--files` | 10 | Número de arquivos Parquet |
| `--records-per-file` | 5000 | Registros por arquivo |
| `--existing` | 100000 | Registros existentes na base |
| `--batch` | 2000 | Batch size do merge |
| `--delay` | 0.5 | Delay entre batches (segundos) |
| `--keep-docker` | false | Não recria Docker (mais rápido) |
| `--output` | metrics_*.csv | Arquivo CSV de saída |

## Uso Individual

### 1. Setup

```bash
# Subir serviços
docker compose up -d

# Setup infraestrutura (S3, SNS, SQS)
python3 scripts/setup_infra.py
```

### 2. Gerar e processar Parquets

```bash
# Gerar múltiplos arquivos Parquet e subir para S3
python3 scripts/generate_parquets.py --count 10 --records-per-file 5000

# Simular notificação SNS para cada arquivo
python3 scripts/simulate_s3_notification.py --bucket poc-bucket --key input/custody_xxxx.parquet

# OU: rodar o consumer que polling SQS automaticamente
python3 scripts/consume_s3_event.py
```

### 3. Merge para tabela principal (Cron)

```bash
# Com configurações padrão
python3 scripts/merge_staging.py

# Ou com configurações customizadas
MERGE_BATCH_SIZE=2000 MERGE_DELAY_SECONDS=0.5 python3 scripts/merge_staging.py
```

### 4. Simular carga de produção (apenas merge)

```bash
python3 scripts/simulate_load.py \
    --existing-records 500000 \
    --ingestion-size 1000000 \
    --update-ratio 60 \
    --batch-size 2000 \
    --delay 0.5 \
    --output-csv metrics.csv
```

### 5. Gerar relatório HTML

```bash
python3 scripts/generate_report.py metrics.csv
```

## Parâmetros

### run_complete_test.sh

| Variável | Default | Descrição |
|----------|---------|-----------|
| `--files` | 10 | Número de arquivos Parquet |
| `--records-per-file` | 5000 | Registros por arquivo |
| `--existing` | 100000 | Registros existentes na base |
| `--batch` | 2000 | Batch size do merge |
| `--delay` | 0.5 | Delay entre batches (segundos) |

### simulate_load.py

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `--existing-records` | 100000 | Registros já existentes na tabela principal |
| `--ingestion-size` | 10000 | Quantidade de registros para ingestação |
| `--update-ratio` | 60 | % de registros que atualizarão dados existentes |
| `--batch-size` | 2000 | Tamanho do batch de merge |
| `--delay` | 0.5 | Delay entre batches (segundos) |
| `--output-csv` | "" | Arquivo CSV para métricas |

### merge_staging.py

| Variável | Default | Descrição |
|----------|---------|-----------|
| `MERGE_BATCH_SIZE` | 2000 | Registros por batch |
| `MERGE_DELAY_SECONDS` | 0.5 | Pausa entre batches |

### generate_parquets.py

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `--count` | 10 | Número de arquivos Parquet |
| `--records-per-file` | 5000 | Registros por arquivo |
| `--prefix` | input/ | Prefixo da chave S3 |

## Merge Staging (merge_staging.py)

Este script é destinado a rodar como CRON/JobScheduler.

### Fluxo do Merge

```
Para cada batch:
  1. SELECT id FROM staging ORDER BY id LIMIT batch_size
  2. INSERT novos registros na principal (ON CONFLICT DO NOTHING)
  3. UPDATE registros existentes (apenas se mudou)
  4. DELETE da staging (após sucesso)
  5. COMMIT
  6. SLEEP (delay configurável)
```

### Características

- **Batch size configurável**: Processa N registros por vez
- **Delay entre batches**: Pausa para não impactar operações concorrentes
- **Advisory lock**: Evita execuções concorrentes
- **Idempotente**: Não processa o mesmo registro duas vezes
- **Métricas**: Tempo, throughput, progresso

## Resultados dos Testes

### Teste: 1M registros, 60% updates

| Métrica | Valor |
|---------|-------|
| Total Time | ~5 min |
| Throughput | ~3,000 regs/s |
| Pending Locks | 0 |
| Dead Tuples | Normal (limpo por autovacuum) |

### Estimativa Aurora

| Instância | 1M registros | 4M registros |
|-----------|-------------|--------------|
| r6g.xlarge (4 vCPU, 32GB) | ~12 min | ~46 min |

### Teste Completo (40 arquivos × 5000 registros)

| Métrica | Valor |
|---------|-------|
| Total Records | 200.000 |
| Throughput | ~3,000 regs/s |
| Batch Size | 2000 |
| Delay | 0.5s |

## Padrões de Resiliencia

### Idempotência

- Unique constraint em `(source_file, row_number)` garante que mesmo parquet processado 2x não duplica
- Merge usa DELETE após sucesso

### Retry

- Consumer ECS: retry automático via SQS visibility timeout
- Merge: se falhar, registros permanecem na staging para próxima execução

### Dead Letter Queue

- Registros inválidos vão para `custody_position_error`
- Payload JSONB preserva dados originais para investigação

## Stack

| Componente | Tecnologia |
|------------|------------|
| Database | PostgreSQL 16 |
| Object Storage | AWS S3 (LocalStack) |
| Notifications | AWS SNS |
| Compute | ECS Fargate (simulado localmente) |
| Language | Python 3.12 |
