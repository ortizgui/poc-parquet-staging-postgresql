# POC — Ingestão Massiva de Dados: Parquet → PostgreSQL (Direct Insert)

Prova de conceito do fluxo de ingestão massiva de dados com suporte a direct insert via .NET 10.

## Arquitetura

```mermaid
flowchart TD
    S3_BUCKET[S3<br/>Parquet Files]
    
    subgraph DIRECT ["Produção — Direct Insert (default)"]
        S3_EVENT[S3 Event Notification] --> SQS_DIRECT[SQS<br/>Captura Carteira]
        SQS_DIRECT --> ECS_CONSUMER[ECS Consumer<br/>Bulk Insert<br/>ON CONFLICT DO UPDATE]
        ECS_CONSUMER --> PRINCIPAL_DIRECT[custody_position<br/>Tabela Principal]
    end

    S3_BUCKET -->|default| S3_EVENT
```

## Fluxo de Dados

### Fluxo A — Direct Insert (Produção — default)

```
1. S3: Arquivos Parquet chegam (até 5.000 registros cada)
       ↓
2. S3 Event Notification → SQS (Captura Carteira)
       ↓
3. ECS Consumer: Lê parquet e faz bulk insert direto na principal
       ↓
4. ON CONFLICT (account_id, asset_id, reference_date) DO UPDATE
       ↓
5. custody_position: Dados disponíveis para aplicações
```

## Scripts Disponíveis

| Script | Descrição |
|--------|-----------|
| `setup_infra.py` | Cria S3 + S3 Bucket Notification → SQS (padrão). Use `--sns` para criar SNS também |
| `simulate_s3_notification.py` | Simula notificação S3. Use `--mode sns` (SNS) ou `--mode sqs` (SQS direto) |
| `generate_parquets.py` | Gera múltiplos arquivos Parquet e sobe para S3 |
| `generate_unique_test_data.py` | Gera dados únicos (sem duplicatas entre arquivos) |
| `seed_database.py` | Preenche base com dados de teste |
| `generate_report.py` | Gera relatório HTML das métricas |
| `upload_to_s3.py` | Utilitário de upload para S3 |
| `create_sample_file.py` | Cria arquivo de amostra |

## Teste Completo End-to-End

### Fluxo de Produção (Direct Insert)

```bash
./run_complete_test.sh --target direct --files 10 --records-per-file 5000 --consumers 3
```

**Resultado**: 10 arquivos × 5.000 registros = **50.000 registros** processados por 3 consumers paralelos

### Opções do run_complete_test.sh

| Opção | Default | Descrição |
|-------|---------|-----------|
| `--files` | 10 | Número de arquivos Parquet |
| `--records-per-file` | 5000 | Registros por arquivo |
| `--existing` | 100000 | Registros existentes na base |
| `--consumers` | 2 | Número de consumers paralelos |
| `--keep-docker` | false | Não recria Docker (mais rápido) |
| `--output` | metrics_*.csv | Arquivo CSV de saída |

## Uso Individual

### 1. Setup

```bash
# Subir serviços
docker compose up -d

# Setup infraestrutura (S3, SQS, SNS opcional)
python3 scripts/setup_infra.py
```

### 2. Gerar e processar Parquets — Fluxo de Produção (Direct Insert)

```bash
# Gerar múltiplos arquivos Parquet e subir para S3
python3 scripts/generate_parquets.py --count 10 --records-per-file 5000

# O worker .NET roda automaticamente via Docker Compose
# Para múltiplos consumers paralelos:
docker compose up -d --scale consumer=3
```

### 3. Gerar relatório HTML

```bash
python3 scripts/generate_report.py metrics.csv
```

## Worker .NET (`src/Worker/`)

O worker ECS é uma aplicação .NET 10 que substitui os scripts Python `consume_s3_event.py` e `process_file.py`.

### Estrutura

```
src/Worker/
├── Worker.csproj                 # Projeto .NET 10
├── Program.cs                    # Entry point (Host.CreateDefaultBuilder)
├── appsettings.json              # Configuração default
├── Dockerfile                    # Multi-stage build (sdk → runtime)
├── Models/
│   ├── S3EventNotification.cs    # Modelo do evento S3
│   └── ProcessResult.cs          # Resultado do processamento
└── Services/
    ├── SqsConsumerService.cs     # BackgroundService (polling SQS)
    ├── ParquetProcessor.cs       # Leitura + validação de Parquet
    └── DatabaseService.cs        # Bulk insert PostgreSQL
```

### Bibliotecas

- **AWSSDK.S3** — Download de Parquet do S3
- **AWSSDK.SQS** — Consumo de mensagens da fila
- **Parquet.Net** — Leitura de arquivos Parquet (row groups)
- **Npgsql** — Conexão PostgreSQL com bulk upsert
- **Microsoft.Extensions.Hosting** — BackgroundService lifecycle

### Funcionalidades

- Suporta `--target direct` (default) e `--target staging` (legado)
- `--consumer-id` para logging em múltiplas instâncias
- `--max-messages` para limitar número de mensagens processadas
- Polling de profundidade da fila SQS a cada mensagem
- ON CONFLICT DO UPDATE com RETURNING para métricas precisas
- Graceful shutdown via CancellationToken

## Parâmetros

### generate_parquets.py

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `--count` | 10 | Número de arquivos Parquet |
| `--records-per-file` | 5000 | Registros por arquivo |
| `--prefix` | input/ | Prefixo da chave S3 |

## Padrões de Resiliência

### Idempotência

`ON CONFLICT DO UPDATE`

### Retry

SQS visibility timeout

### Dead Letter Queue

`custody_position_error`

### Paralelismo

Múltiplos consumers

## Stack

| Componente | Tecnologia |
|------------|------------|
| Database | PostgreSQL 16 |
| Object Storage | AWS S3 (LocalStack) |
| Notifications | S3 Event Notification → SQS (padrão) ou SNS (opcional) |
| Compute | ECS Fargate (simulado localmente via consumer container) |
| Worker Runtime | .NET 10 (C#) — src/Worker/ |
| Infra/Scripts | Python 3.12 — scripts/ |
