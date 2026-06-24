# POC — Batch Processing Parquet → PostgreSQL

Prova de conceito de um pipeline de processamento batch de arquivos Parquet armazenados em S3 (emulado via LocalStack) para PostgreSQL, usando uma tabela de staging com validação por chunks e merge idempotente na tabela final.

## Visão Geral

1. **Docker Compose** sobe PostgreSQL 16 e LocalStack (S3 emulator).
2. **Script Python** gera um arquivo Parquet com ~25 registros (incluindo válidos, updates e inválidos).
3. **Upload** do arquivo para o bucket S3 local.
4. **Processamento** em chunks: baixa o Parquet, valida cada linha, insere válidos na staging e inválidos na tabela de erro.
5. **Merge** move os registros pendentes da staging para a tabela final (`custody_position`) com upsert (INSERT ON CONFLICT DO UPDATE).
6. **Idempotência**: reexecutar os scripts não duplica dados — a staging usa constraints `UNIQUE (source_file, row_number)` e `UNIQUE (source_file, record_hash)`, e o merge usa `ON CONFLICT`.

## Pré-requisitos

- Docker e Docker Compose
- Python 3.12+
- `pip` / `venv`

## Setup do Ambiente

```bash
# Subir ambiente (PostgreSQL + LocalStack)
docker compose up -d

# Aguardar serviços ficarem prontos
docker compose ps

# Criar ambiente Python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Execução do Pipeline

```bash
# Gerar arquivo Parquet (~25 registros)
python scripts/create_sample_file.py

# Enviar para S3 local
python scripts/upload_to_s3.py

# Processar arquivo em chunks (valida + staging)
python scripts/process_file.py --bucket poc-bucket --key input/custody_position.parquet --chunk-size 5

# Executar merge para tabela final
python scripts/merge_staging.py
```

## Consultas úteis no PostgreSQL

```bash
# Acessar o banco
docker exec -it poc-postgres psql -U pocuser -d pocdb

# Ver tabela final
SELECT * FROM custody_position ORDER BY account_id, asset_id, reference_date;

# Ver staging
SELECT status, COUNT(*) FROM custody_position_staging GROUP BY status;

# Ver erros
SELECT * FROM custody_position_error;
```

## Notas sobre Idempotência

- **`create_sample_file.py`**: sempre sobrescreve o mesmo arquivo — seguro reexecutar.
- **`upload_to_s3.py`**: sobrescreve o objeto no S3 — seguro reexecutar.
- **`process_file.py`**: cada execução gera um novo `batch_id`. As constraints `UNIQUE (source_file, row_number)` e `UNIQUE (source_file, record_hash)` impedem duplicação de registros. Registros já inseridos são ignorados com `ON CONFLICT DO NOTHING`.
- **`merge_staging.py`**: processa apenas registros com `status = 'PENDING'`. Após o merge, o status muda para `'MERGED'`. Reexecutar não reprocessa registros já mergeados.

## MiniStack / LocalStack

Este projeto utiliza a imagem `localstack/localstack:latest` com `SERVICES=s3` como emulador S3 compatível com MiniStack (modo gratuito). O LocalStack Community Edition é gratuito e oferece o serviço S3 sem necessidade de licença.

Para usar o MiniStack oficial, altere a imagem no `docker-compose.yml` para `ministackorg/ministack:latest` (se disponível) e ajuste as configurações conforme documentação.

## Troubleshooting

| Problema | Solução |
|----------|---------|
| `Connection refused` no S3 | Verifique se o LocalStack está rodando: `docker compose ps` |
| `Connection refused` no PostgreSQL | Aguarde o healthcheck passar: `docker compose ps` (status "healthy") |
| Erro `BucketAlreadyOwnedByYou` | Normal — o script trata como aviso. |
| `pyarrow` não instalou | Use `pip install pyarrow` separadamente; requer Python 3.9+ |
| Tabelas não criadas | Os scripts SQL em `./sql/` rodam apenas na primeira subida do container. Recrie com `docker compose down -v && docker compose up -d` |
