# POC — Batch Processing Parquet → PostgreSQL

Prova de conceito de um pipeline de processamento batch de arquivos Parquet armazenados em S3 (emulado via LocalStack) para PostgreSQL, usando uma tabela de staging com validação por chunks e merge idempotente na tabela final.

## Visão Geral

1. **Docker Compose** sobe PostgreSQL 16 e LocalStack (S3 emulator).
2. **Script Python** gera um arquivo Parquet com ~25 registros (incluindo válidos, updates e inválidos).
3. **Upload** do arquivo para o bucket S3 local.
4. **Processamento** em chunks: baixa o Parquet, valida cada linha, insere válidos na staging e inválidos na tabela de erro.
5. **Merge em lotes** (batch merge): processa registros pendentes da staging em lotes de 10.000 (configurável via `MERGE_BATCH_SIZE`). Cada lote:
   - **Passo 1**: insere na tabela final apenas registros que **não existem** (`WHERE NOT EXISTS`)
   - **Passo 2**: atualiza na tabela final registros que **já existem** (`UPDATE via JOIN`)
   - **Passo 3**: marca o lote como `MERGED` na staging
   - Cada lote é uma transação independente — evita transação monolithic

## Estrutura do Projeto

```
.
├── docker-compose.yml
├── .env
├── requirements.txt
├── README.md
├── data/input/
├── scripts/
│   ├── create_sample_file.py
│   ├── upload_to_s3.py
│   ├── process_file.py
│   └── merge_staging.py
└── sql/
    ├── 001_init.sql
    └── 002_seed_final.sql
```

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
# Gerar arquivo Parquet (~24 registros)
python scripts/create_sample_file.py

# Enviar para S3 local
python scripts/upload_to_s3.py

# Processar arquivo em chunks (valida + staging)
python scripts/process_file.py --bucket poc-bucket --key input/custody_position.parquet --chunk-size 5

# Executar merge para tabela final (batch merge)
python scripts/merge_staging.py

# Opcional: configurar tamanho do lote
MERGE_BATCH_SIZE=5000 python scripts/merge_staging.py
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
- **`merge_staging.py`**: processa apenas registros com `status = 'PENDING'`. Após o merge, o status muda para `'MERGED'`. Reexecutar não reprocessa registros já mergeados. O `FOR UPDATE SKIP LOCKED` garante que cada lote seja processado por apenas um worker.

---

## Arquitetura para Produção

### Por que merge em lotes (batch merge)?

O merge monolithic (única transação com milhões de registros) tem problemas graves em produção:

| Problema | Impacto |
|----------|---------|
| Transação gigante | Bloqueia a tabela final por minutos/horas |
| `max_locks_per_transaction` | Pode estourar o limite de locks do PostgreSQL |
| WAL enorme | Geração excessiva de Write-Ahead Log |
| Rollback catastrófico | Se falha no final, tudo é desfeito — horas perdidas |

O **batch merge** resolve isso:
- Cada lote de N registros (ex: 10.000) é uma transação curta
- Se um lote falha, só ele precisa ser reprocessado
- Vários workers podem processar lotes em paralelo
- A tabela final fica bloqueada por milissegundos, não horas

### Por que dois passos (INSERT + UPDATE separados) em vez de `ON CONFLICT DO UPDATE`?

```
Abordagem original (ON CONFLICT DO UPDATE):
  INSERT 1.000.000 linhas
  ON CONFLICT (chave) DO UPDATE SET quantity = EXCLUDED.quantity
  → Gera dead tuple para CADA linha existente atualizada
  → Table bloat na tabela final

Abordagem em dois passos:
  Passo 1: INSERT WHERE NOT EXISTS
  Passo 2: UPDATE via JOIN
  → Menos dead tuples (só o UPDATE gera)
  → Mais previsível para o planner do PostgreSQL
  → Permite VACUUM entre os passos se necessário
```

Na prática, para milhões de registros o `ON CONFLICT DO UPDATE` funciona bem — a diferença é sutil. O que realmente importa é o **batch** (evitar transação monolithic). O dois-passos é uma camada extra de segurança e clareza.

---

## VACUUM

### O que é?

VACUUM é um comando do PostgreSQL que **recupera espaço ocupado por registros mortos (dead tuples)**.

### Por que precisa?

O PostgreSQL usa **MVCC (Multi-Version Concurrency Control)**. Quando você faz um UPDATE, o PostgreSQL não modifica o registro original — ele cria uma **nova versão** (nova tupla) e marca a antiga como **morta (dead tuple)**. Com o tempo:

```
Antes do UPDATE:
  [PETR4, qty=1000]  ← tupla viva

Depois do UPDATE (qty=1500):
  [PETR4, qty=1000]  ← dead tuple (ocupando espaço)
  [PETR4, qty=1500]  ← tupla viva
```

Sem VACUUM:
- A tabela cresce infinitamente (table bloat)
- As queries ficam mais lentas (precisam escanear mais páginas)
- Indexes também incham
- Pode estourar o disk space

### Tipos de VACUUM

```sql
-- VACUUM padrão: marca espaço como reutilizável (não libera para o SO)
VACUUM custody_position;

-- VACUUM ANALYZE: VACUUM + atualiza estatísticas para o planner
VACUUM ANALYZE custody_position;

-- VACUUM FULL: libera espaço para o SO (bloqueia a tabela!)
VACUUM FULL custody_position;

-- AUTO VACUUM: o PostgreSQL faz automaticamente, mas pode não acompanhar
```

### Quando executar

```bash
# Após cada merge grande (recomendado)
docker exec poc-postgres psql -U pocuser -d pocdb -c "VACUUM ANALYZE custody_position"

# Verificar bloat
docker exec poc-postgres psql -U pocuser -d pocdb -c "
SELECT schemaname, relname, n_dead_tup, n_live_tup,
       round(n_dead_tup::numeric / NULLIF(n_live_tup, 0) * 100, 2) AS dead_pct
FROM pg_stat_user_tables
WHERE relname = 'custody_position';
"
```

> ⚠️ **AVISO**: `VACUUM FULL` é exclusivo e bloqueia a tabela. Use apenas em janelas de manutenção. O `VACUUM` padrão (sem FULL) roda concorrentemente com leituras e escritas.

---

## Particionamento (Item 6)

### O que é?

Particionamento divide uma tabela grande em **partições menores** baseadas em uma coluna-chave (ex: `reference_date`). O PostgreSQL roteia automaticamente cada registro para a partição correta.

### Exemplo para `custody_position`

```sql
-- Tabela particionada por mês (reference_date)
CREATE TABLE custody_position (
    id SERIAL,
    account_id VARCHAR NOT NULL,
    asset_id VARCHAR NOT NULL,
    reference_date DATE NOT NULL,
    quantity NUMERIC(18, 4) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, reference_date)  -- PK precisa incluir a coluna de partição
) PARTITION BY RANGE (reference_date);

-- Partições mensais
CREATE TABLE custody_position_2025_01 PARTITION OF custody_position
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE custody_position_2025_02 PARTITION OF custody_position
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

CREATE TABLE custody_position_default PARTITION OF custody_position DEFAULT;
```

### Benefícios

| Benefício | Explicação |
|-----------|------------|
| **Manutenção eficiente** | Drop de partição antiga é instantâneo (sem DELETE milhões de linhas) |
| **Query mais rápida** | PostgreSQL só escaneia as partições necessárias (partition pruning) |
| **Paralelismo** | Cada partição pode ser vacuum/indexada independentemente |
| **Archiving** | Partições antigas podem ser movidas para storage mais barato |

### Exemplo de partition pruning

```sql
-- PostgreSQL só escaneia a partição 2025_01
EXPLAIN SELECT * FROM custody_position
WHERE reference_date = '2025-01-15';
```

### Cuidados

| Cuidado | Por quê |
|---------|---------|
| PK precisa incluir a coluna de partição | Limitação do PostgreSQL |
| Não é adequado para tabelas com < 10M registros | Overhead desnecessário |
| Migração de tabela não-particionada para particionada requer rebuild | `CREATE TABLE ... PARTITION BY RANGE` + `INSERT INTO ... SELECT` |
| Constraints únicas entre partições não são possíveis | Cada partição tem seu próprio índice único |

> ⚠️ Para esta POC, o particionamento é desnecessário (< 100 registros). Em produção, considere quando a tabela ultrapassar **10-50 milhões de registros**.

---

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
| Merge lento | Ajuste `MERGE_BATCH_SIZE` no `.env` ou via variável de ambiente |
| Table bloat suspeito | Execute `VACUUM ANALYZE custody_position` |
