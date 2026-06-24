# POC — Processamento Batch de Parquet → PostgreSQL

Prova de conceito de um pipeline batch que lê arquivos Parquet do S3, valida registros em paralelo, insere em staging e faz merge upsert na tabela final — tudo rodando localmente com Docker.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PIPELINE DE PROCESSAMENTO BATCH                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  S3 (MiniStack)                                                      │
│  ┌──────────────────┐            ┌──────────────────────────────┐   │
│  │ custody_position │  Range    │  Python: process_file.py      │   │
│  │   .parquet       │──GET────▶│  ┌──────────────────────────┐ │   │
│  │   (24 linhas)    │           │  │ 1. Lê footer (metadata) │ │   │
│  └──────────────────┘           │  │ 2. Worker pool:         │ │   │
│       ▲                         │  │    ├─ Worker 1 → RG 0   │ │   │
│       │ upload                  │  │    ├─ Worker 2 → RG 1   │ │   │
│  ┌────┴───────────┐             │  │    └─ Worker N → RG N   │ │   │
│  │ create_sample  │             │  │ 3. Valida cada linha    │ │   │
│  │ _file.py       │             │  │ 4. Válido → staging     │ │   │
│  └────────────────┘             │  │ 5. Inválido → erro      │ │   │
│                                 └──┼───────────────────────────┘   │
│                                    │                               │
│           ┌────────────────────────┼───────────────────┐           │
│           │                        ▼                   │           │
│           │     ┌──────────────────────────────────┐   │           │
│           │     │     PostgreSQL (pocdb)            │   │           │
│           │     │                                  │   │           │
│           │     │  ┌──────────────────────────┐   │   │           │
│           │     │  │ custody_position_error   │   │   │           │
│     merge │     │  │ (registros inválidos)    │   │   │           │
│     staging.py  │  └──────────────────────────┘   │   │           │
│           │     │                                  │   │           │
│           │     │  ┌──────────────────────────┐   │   │           │
│           └─────│▶│ custody_position_staging  │───┘   │           │
│                 │  │ (PENDING → MERGED)       │       │           │
│                 │  └──────────┬───────────────┘       │           │
│                 │             │ upsert (batch)         │           │
│                 │             ▼                        │           │
│                 │  ┌──────────────────────────┐   │   │           │
│                 │  │ custody_position (final) │   │   │           │
│                 │  │ (3 seed + 17 novos)      │   │   │           │
│                 │  └──────────────────────────┘   │   │           │
│                 └──────────────────────────────────┘   │           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Índice

1. [Problema de negócio](#problema-de-negócio)
2. [Por que esta arquitetura?](#por-que-esta-arquitetura)
3. [Stack tecnológica](#stack-tecnológica)
4. [Pré-requisitos](#pré-requisitos)
5. [Setup do ambiente](#setup-do-ambiente)
6. [Execução do pipeline](#execução-do-pipeline)
7. [O papel de cada tabela](#o-papel-de-cada-tabela)
8. [Idempotência: camada por camada](#idempotência-camada-por-camada)
9. [Decisões de arquitetura](#decisões-de-arquitetura)
10. [Comparação: Row Groups vs SQS por Registro](#comparação-row-groups-vs-sqs-por-registro)
11. [E se algo der errado?](#e-se-algo-der-errado)
12. [VACUUM](#vacuum)
13. [Particionamento](#particionamento)
14. [MiniStack / LocalStack](#ministack--localstack)
15. [Troubleshooting](#troubleshooting)

---

## Problema de negócio

Instituições financeiras precisam processar diariamente **arquivos de posição de custódia** vindos de fontes externas (B3, custodiantes, administradores fiduciários). Estes arquivos contêm:

- **Registros novos**: ativos que entraram na carteira do cliente
- **Registros de atualização**: mesma posição do dia anterior, mas com saldo diferente
- **Registros inválidos**: dados inconsistentes que precisam ser reportados sem quebrar o fluxo

### Requisitos críticos

| Requisito | Por quê |
|-----------|---------|
| **Não perder dados** | Cada registro representa dinheiro real de clientes |
| **Idempotência** | Reprocessar o mesmo arquivo não pode gerar duplicatas |
| **Rastreabilidade** | Saber exatamente qual lote originou cada registro |
| **Isolamento** | Dados em processamento não podem poluir a tabela final |
| **Escalabilidade** | O mesmo design precisa funcionar para 24 ou 24 milhões de linhas |
| **Custo zero de licença** | Tudo open source, sem dependência de AWS real |

---

## Por que esta arquitetura?

### O fluxo em alto nível

```
ARQUIVO BRUTO              ÁREA DE PREPARAÇÃO              DADO CONFIÁVEL
─────────────────      ────────────────────────        ─────────────────
S3 (Parquet) ──────▶  custody_position_staging ──────▶  custody_position
                      (validação + batch_id)            (final, consistente)
                             │
                             └─▶ custody_position_error
                                 (diagnóstico)
```

Cada etapa tem uma responsabilidade única e isolada:

### 1. Staging — por que não inserir direto na final?

A staging existe como **área de preparação (buffer)** entre o dado bruto e o dado confiável:

```
SEM STAGING:                     COM STAGING:
─────                            ─────
S3 → Tabela Final                S3 → Staging → Merge → Final
                                  │
Se o processo morre no meio:      Se o merge morre:
  Metade dos dados na final       Staging intacta (PENDING)
  Sem saber o que já entrou       Reprocessa merge, não o download
  Precisa truncar e recomeçar     Dados brutos preservados na staging

Staging também permite:
  • Auditoria: batch_id, source_file, row_number por registro
  • Validação antes de expor à consulta
  • Rollback sem truncar tabela final
  • Reprocessamento seletivo (só o merge)
```

### 2. Batch merge — por que não um upsert gigante?

Um único `INSERT ... ON CONFLICT DO UPDATE` para milhões de linhas é uma **transação monolithic**:

```
Monolithic (ruim):                   Batch (bom):
─────────────────                    ────────────
BEGIN;                               BEGIN;  -- lote 1/100
  INSERT 10M linhas ON CONFLICT        INSERT WHERE NOT EXISTS
COMMIT; -- 45 minutos                  UPDATE via JOIN
                                       COMMIT; -- 3 segundos
Riscos:                               BEGIN;  -- lote 2/100
  • Lock na tabela final por horas      ...
  • WAL de 50 GB no disco            Se lote 5 falha:
  • Rollback desfaz TUDO               • Só lote 5 perdeu
  • max_locks_per_transaction          • Lotes 1-4 já comitaram
                                       • Reprocessa só lote 5
```

### 3. Streaming de row groups — por que não baixar o arquivo inteiro?

Arquivos Parquet são divididos internamente em **Row Groups** (grupos de linhas). Cada Row Group tem seus próprios dados e metadados:

```
Arquivo Parquet:
┌──────────────────────────────────────────────────────────┐
│ Magic │ Row Group 0  │ Row Group 1 │ ... │ Row Group N │ Footer │
│Bytes  │ (10k linhas) │ (10k linhas)│     │ (10k linhas)│(meta)  │
└──────────────────────────────────────────────────────────┘
         ▲              ▲                   ▲
         │              │                   │
    Worker 1       Worker 2            Worker N
    (Range GET)    (Range GET)         (Range GET)
```

Cada Worker baixa **apenas o que precisa** via Range GET — uma requisição HTTP que pede bytes específicos do arquivo. Sem download do arquivo inteiro. Sem estourar RAM.

---

## Stack tecnológica

| Componente | Tecnologia | Versão | Função |
|------------|-----------|--------|--------|
| Container | Docker Compose | - | Orquestração dos serviços |
| S3 emulator | LocalStack (MiniStack-compatível) | latest | Simula S3 sem conta AWS |
| Banco relacional | PostgreSQL | 16 | Armazenamento relacional |
| Streaming S3 | s3fs / fsspec | 2026.6+ | Leitura de Parquet via Range GET |
| Parquet | pyarrow | 17+ | Leitura de row groups |
| DataFrame | pandas | 2.2+ | Validação por linha |
| S3 SDK | boto3 | 1.34+ | Upload/download S3 |
| DB driver | psycopg2-binary | 2.9+ | Conexão PostgreSQL |
| Parquet generation | pyarrow + pandas | - | Criação do arquivo de exemplo |

---

## Pré-requisitos

- **Docker** e **Docker Compose** (para PostgreSQL e LocalStack)
- **Python 3.12+** (para os scripts)
- ~2 GB de RAM livre
- ~5 GB de disco livre

---

## Setup do ambiente

```bash
# 1. Subir os serviços (PostgreSQL + LocalStack)
docker compose up -d

# 2. Verificar se estão saudáveis
docker compose ps
# Esperado: ambos com status "healthy"

# 3. Criar ambiente Python e instalar dependências
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Execução do pipeline

```bash
# Etapa 1 — Gerar arquivo Parquet de exemplo (24 registros)
python scripts/create_sample_file.py
# Output:
#   Arquivo gerado: ./data/input/custody_position.parquet
#   Linhas: 24

# Etapa 2 — Enviar para o S3 local (MiniStack/LocalStack)
python scripts/upload_to_s3.py
# Output:
#   Bucket criado: poc-bucket
#   Arquivo enviado: s3://poc-bucket/input/custody_position.parquet

# Etapa 3 — Processar em streaming paralelo (row groups)
python scripts/process_file.py --bucket poc-bucket \
                               --key input/custody_position.parquet
# Opcional: tamanho do lote de validacao por worker (default 5)
# python scripts/process_file.py --bucket poc-bucket --key ... --chunk-size 10
# Output:
#   batch_id: 025c8e45-...
#   Total linhas: 24 | Row groups: 1
#   Row groups pendentes: 1/1
#   RG  0: 20 validos, 4 invalidos, 0 duplicatas, 24 linhas
#   Resumo: 20 validos, 4 invalidos

# Etapa 4 — Merge para tabela final (batch upsert)
python scripts/merge_staging.py
# Output:
#   Registros pendentes antes: 20
#   Lote 10/20: +7 novos / ~10 atualizados
#   Lote 20/20: +10 novos / ~10 atualizados
#   Total final na custody_position: 20

# Opcional: configurar batch size do merge
MERGE_BATCH_SIZE=5000 python scripts/merge_staging.py

# Opcional: configurar workers de processamento
PROCESS_MAX_WORKERS=8 python scripts/process_file.py ...
```

---

## O papel de cada tabela

### `custody_position` (tabela final)

A tabela "fonte da verdade". Contém apenas **dados consistentes** e prontos para consulta.

| Coluna | Tipo | Função |
|--------|------|--------|
| `account_id` | VARCHAR | Identificador da conta (ex: ACC001) |
| `asset_id` | VARCHAR | Código do ativo (ex: PETR4) |
| `reference_date` | DATE | Data de referência da posição |
| `quantity` | NUMERIC(18,4) | Quantidade do ativo |
| `amount` | NUMERIC(18,2) | Valor financeiro |
| `updated_at` | TIMESTAMP | Última atualização |
| **UK** | (account_id, asset_id, reference_date) | Garante 1 registro por posição |

### `custody_position_staging` (área de staging)

Recebe os dados crus do processamento. Cada lote é identificado por `batch_id`. Depois do merge, os registros mudam de `PENDING` para `MERGED`.

| Coluna | Tipo | Função |
|--------|------|--------|
| `batch_id` | UUID | Identificador único do processamento |
| `source_file` | VARCHAR | Origem do dado (ex: s3://...) |
| `row_number` | INTEGER | Número da linha no arquivo original |
| `record_hash` | VARCHAR | SHA256 dos dados (dedup adicional) |
| `status` | VARCHAR | PENDING → MERGED |
| `merged_at` | TIMESTAMP | Quando foi integrado à final |
| **UK1** | (source_file, row_number) | Impede duplicata do mesmo arquivo |
| **UK2** | (source_file, record_hash) | Impede duplicata do mesmo dado |

### `custody_position_error` (diagnóstico)

Registros que falharam validação. Permite auditoria sem travar o pipeline.

| Coluna | Tipo | Função |
|--------|------|--------|
| `batch_id` | UUID | Lote que detectou o erro |
| `payload` | JSONB | Cópia fiel do registro original |
| `error_reason` | TEXT | Motivo da rejeição (ex: "account_id is empty") |
| **UK** | (source_file, row_number) | Impede erro duplicado no reprocessamento |

---

## Idempotência: camada por camada

Idempotência significa que **executar a mesma operação N vezes produz o mesmo resultado que executar 1 vez**.

```
Camada 1 — process_file.py (download + staging)
───────────────────────────────────────────────
  Execução 1: baixa Parquet, insere 20 registros na staging
  Execução 2: baixa Parquet mesmo arquivo
    → ON CONFLICT (source_file, row_number) DO NOTHING
    → 0 novos registros (20 ignorados como duplicatas)
  Resultado: staging com 20 registros (não 40)

Camada 2 — merge_staging.py (staging → final)
───────────────────────────────────────────────
  Execução 1: 20 PENDING → INSERT + UPDATE na final → marca MERGED
  Execução 2: 0 PENDING (já foram mergeados)
    → WHERE status = 'PENDING' retorna vazio
    → Nada acontece
  Resultado: final com 20 registros (não 40)

Camada 3 — reprocessamento completo
───────────────────────────────────────────────
  process_file.py + merge_staging.py (de novo):
  • Staging: registros ignorados (já existem)
  • Merge: staging sem PENDING → nada a fazer
  • Final: inalterada
```

---

## Decisões de arquitetura

### 1. Por que `s3fs` em vez de `boto3` + `BytesIO`?

```
Critério              boto3 + BytesIO         s3fs (Range GET)
──────────────────  ──────────────────────   ─────────────────────
Download            Arquivo inteiro           Só o que precisa
RAM                 Tamanho do arquivo        Tamanho do row group
Paralelismo         Não (1 conexão)           Sim (N workers)
Range request       Manual (Range header)     Automático (fsspec)
Checkpoint          Requer lógica extra       ON CONFLICT + MAX()
```

`s3fs` abstrai Range GET requests e permite que o `pyarrow.ParquetFile` leia apenas os bytes necessários do S3 — sem baixar nada além do necessário.

### 2. Por que `ThreadPoolExecutor` em vez de `multiprocessing`?

| Característica | ThreadPoolExecutor | multiprocessing |
|----------------|-------------------|-----------------|
| Uso de CPU | Leve (I/O bound) | Pesado (CPU bound) |
| Memória | Compartilhada | Duplicada (N×RAM) |
| Complexidade | Baixa | Alta (pickle, serialização) |
| Ideal para | Download + DB I/O | Processamento computacional |

Nosso gargalo é **I/O** (download S3 + insert PostgreSQL), não CPU. Threads são a escolha correta.

### 3. Por que `ON CONFLICT DO NOTHING` em vez de verificar antes?

```sql
-- Abordagem "check-then-insert" (NÂO use em concorrência)
SELECT COUNT(*) FROM staging WHERE source_file = 'x' AND row_number = 5
-- Se não existe:
INSERT INTO staging ...

-- Abordagem "insert-or-ignore" (correta)
INSERT INTO staging ... ON CONFLICT (source_file, row_number) DO NOTHING
-- Se já existe, ignora silenciosamente
```

`ON CONFLICT DO NOTHING` é **atômico**: não há janela entre o check e o insert onde outra thread pode inserir o mesmo registro. É a abordagem correta para sistemas concorrentes.

### 4. Por que `FOR UPDATE SKIP LOCKED` no merge?

```
SEM SKIP LOCKED:                       COM SKIP LOCKED:
Worker 1: SELECT ... WHERE PENDING     Worker 1: SELECT ... FOR UPDATE SKIP LOCKED
Worker 2: SELECT ... WHERE PENDING     Worker 2: SELECT ... FOR UPDATE SKIP LOCKED
Ambos pegam OS MESMOS registros!       Cada um pega LOTES DIFERENTES!
Processo duplicado!                    Processo paralelo seguro!
```

`SKIP LOCKED` diz: "pule os registros que estão locked por outra transação". Isso permite múltiplos workers de merge rodando em paralelo sem pisar no mesmo lote.

### 5. Por que dois passos (INSERT + UPDATE) e não `ON CONFLICT DO UPDATE`?

```sql
-- Versão 1: ON CONFLICT DO UPDATE (1 passo)
INSERT INTO custody_position (...)
SELECT ... FROM staging
ON CONFLICT (account_id, asset_id, reference_date) DO UPDATE
SET quantity = EXCLUDED.quantity;

-- Versão 2: INSERT + UPDATE (2 passos)
INSERT INTO custody_position (...)
SELECT ... FROM staging s
WHERE NOT EXISTS (SELECT 1 FROM custody_position f WHERE ...);

UPDATE custody_position f
SET quantity = s.quantity
FROM staging s
WHERE f.chave = s.chave;
```

Na prática, para milhões de registros:

| Abordagem | Prós | Contras |
|-----------|------|---------|
| `ON CONFLICT DO UPDATE` | 1 comando, mais simples | Gera dead tuple para cada update |
| Dois passos | INSERT limpo + UPDATE explícito | 2 comandos, marginalmente mais lento |

**Ambas funcionam.** O dois-passos foi escolhido por clareza e por separar a contagem de inserts vs updates.

---

---

## Comparação: Row Groups vs SQS por Registro

### Contexto

Uma arquitetura alternativa seria transformar **cada registro** do arquivo Parquet em uma **mensagem individual em uma fila SQS**. Essa abordagem oferece alto nível de desacoplamento e reprocessamento granular, porém adiciona complexidade operacional significativa para um cenário cujo objetivo principal é realizar **carga massiva de dados em um banco PostgreSQL**.

Após análise, optou-se por uma estratégia baseada em **leitura de row groups em streaming**, inserção em tabela de staging e merge para a tabela final.

```
ABORDAGEM ESCOLHIDA (Row Groups Streaming):     ALTERNATIVA (SQS por registro):
───────────────────────────────                  ────────────────────────────────
S3 (Parquet)                                     S3 (Parquet)
    │                                                  │
    ▼                                                  ▼
Leitura do Footer (Range GET)                   Leitura do arquivo inteiro
    │                                                  │
    ▼                                                  ▼
Workers paralelos (ThreadPool)                  1.000.000 mensagens SQS
    │  cada um lê 1 row group                        │
    ▼                                                  ▼
Validação em lote                                1.000.000 consumers
    │                                                  │
    ▼                                                  ▼
Staging Table + Error Table                      Fila + DLQ + Retry + Monitoramento
    │                                                  │
    ▼                                                  ▼
Merge upsert (batch)                            1.000.000 inserts individuais
```

### 1. Menor Complexidade Arquitetural

Na abordagem baseada em eventos, **cada linha** do arquivo gera uma mensagem SQS:

```
Arquivo com 1.000.000 linhas
=
1.000.000 mensagens SQS
```

Além da fila principal, são necessários:

- Consumers (ECS/Lambda) com escalonamento
- Dead Letter Queue (DLQ) para falhas
- Controle de duplicidade na entrega
- Monitoramento de backlog (CloudWatch)
- Estratégias de retry com backoff

Na abordagem em **row groups**, o sistema trabalha diretamente sobre o arquivo original via **Range GET requests** — sem fila, sem consumers, sem DLQ. Cada worker do `ThreadPoolExecutor` abre sua própria conexão S3 (Range GET para um row group) e sua própria conexão PostgreSQL.

| Componente | SQS por registro | Row Groups streaming |
|------------|-----------------|---------------------|
| Mensageria | Fila SQS + DLQ | Nenhuma |
| Consumers | Lambda ou ECS (auto-scaling) | ThreadPoolExecutor (N workers) |
| Duplicidade | ID de dedup + lambda idempotente | ON CONFLICT DO NOTHING |
| Monitoramento | CloudWatch fila + consumer | Logs no terminal |
| Complexidade total | Alta | Baixa |

### 2. Melhor Eficiência para Cargas Massivas

O PostgreSQL é otimizado para **operações em lote**, não para inserts individuais.

Inserir 1 registro por vez gera:

- N round-trips de rede (1 por linha)
- N transações (ou 1 transação gigante)
- N vezes mais CPU no banco
- N vezes mais WAL (Write Ahead Log)

Nosso fluxo atual:

```
1.000.000 registros em um Parquet

Row groups (típico: 100k linhas por grupo):
  ~10 row groups
  10 Range GET requests
  10 workers paralelos
  10 inserts em lote na staging

Merge:
  Batch upsert de 10.000 em 10.000
  = 100 transações curtas
```

O volume de operações no banco é **drasticamente reduzido** — de 1.000.000 inserts individuais para ~100 operações em lote.

### 3. Menor Custo Operacional

Na estratégia SQS por registro:

| Recurso | Impacto |
|---------|---------|
| **SQS** | 1.000.000 mensagens por arquivo |
| **Lambda/ECS** | 1.000.000 invocações |
| **Rede** | 1.000.000 chamadas de API |
| **DB connections** | Centenas de connections por minuto |
| **Logs** | 1.000.000 entradas de log |
| **Monitoramento** | 1.000.000 métricas de fila |

Na abordagem em row groups:

| Recurso | Impacto |
|---------|---------|
| **S3** | 1 head + footer + N Range GETs (N = row groups) |
| **Threads** | N workers simultâneos (configurável) |
| **Rede** | N chamadas S3 + poucas chamadas DB |
| **DB connections** | N connections simultâneas |
| **Logs** | 1 linha por worker processado |
| **Monitoramento** | Métricas do próprio script |

O custo está relacionado à **quantidade de row groups** (estrutura interna do Parquet), não à quantidade de registros. Para arquivos grandes, você pode controlar o tamanho dos row groups na geração do Parquet.

### 4. Idempotência Continua Garantida

Uma preocupação comum ao abandonar o modelo de eventos individuais é perder a capacidade de evitar duplicações.

Isso é resolvido através de:

- **`batch_id`**: UUID único por execução do processamento
- **`source_file`**: identificador do arquivo de origem
- **`row_number`**: número da linha dentro do arquivo
- **`record_hash`**: SHA256 dos dados da linha para dedup fino
- **Constraints únicas** no banco PostgreSQL

```sql
-- Impede o mesmo arquivo+linha de ser inserido duas vezes
UNIQUE (source_file, row_number)

-- Impede o mesmo dado (hash) de ser inserido duas vezes
UNIQUE (source_file, record_hash)
```

Se o mesmo arquivo for processado novamente, o PostgreSQL ignora registros já existentes:

```sql
INSERT INTO custody_position_staging (...)
VALUES (...)
ON CONFLICT (source_file, row_number) DO NOTHING
```

Dessa forma, o processamento permanece seguro e idempotente **sem necessidade de fila**.

### 5. Reprocessamento Continua Possível

Embora não exista uma mensagem SQS para cada linha, ainda é possível reprocessar dados de forma controlada:

| Estratégia | Como fazer |
|------------|------------|
| **Reprocessar arquivo inteiro** | Executar `process_file.py` de novo (ON CONFLICT ignora staging duplicada) |
| **Reprocessar merge** | Executar `merge_staging.py` de novo (só processa PENDING restantes) |
| **Reprocessar apenas inválidos** | Corrigir dados e gerar novo Parquet com batch_id diferente |
| **Compensação manual** | UPDATE diretamente na staging com status = 'PENDING' e rodar merge |

No modelo SQS por registro, reprocessar exigiria reenfileirar N mensagens ou implementar um mecanismo de replay na DLQ.

### 6. Tratamento de Erros Mais Simples

Na abordagem SQS, cada falha gera:

1. Mensagem vai para DLQ
2. Alarme no CloudWatch
3. Operador precisa investigar
4. Decidir entre descartar, corrigir e reenfileirar
5. Se o erro é no dado (não no processamento), a DLQ enche sem solução

Na abordagem em row groups:

- **Registros válidos** → staging (status PENDING)
- **Registros inválidos** → `custody_position_error` com payload original + motivo
- **O processamento continua** — um registro inválido não quebra o lote

```
Exemplo real na POC:
24 registros processados
20 válidos → staging
  4 inválidos → custody_position_error
    • ACC006/INVL1: quantity is invalid: -100.0
    • (empty)/INVL2: account_id is empty
    • ACC007/(empty): asset_id is empty
    • ACC008/INVL4: amount is invalid: -500.0

Nenhuma fila, nenhuma DLQ, nenhuma interrupção.
```

### 7. Melhor Auditoria e Rastreabilidade

A tabela de staging cria um **histórico explícito** do processamento:

| Coluna | O que armazena |
|--------|---------------|
| `batch_id` | UUID da execução do processamento |
| `source_file` | s3://bucket/path/arquivo.parquet |
| `row_number` | Linha exata dentro do arquivo |
| `record_hash` | SHA256 do conteúdo da linha |
| `status` | PENDING → MERGED |
| `created_at` | Quando foi inserido na staging |
| `merged_at` | Quando foi integrado à final |

A tabela de erro armazena o `payload` completo em JSONB — cópia fiel do registro que falhou:

```sql
SELECT error_reason, payload->>'account_id' AS conta,
       payload->>'asset_id' AS ativo
FROM custody_position_error
WHERE batch_id = '025c8e45-...';
```

No modelo SQS, a rastreabilidade dependeria de logs do consumer e da mensagem na DLQ — mais difusa e sem estrutura relacional.

### 8. Separação Clara Entre Ingestão e Atualização

O fluxo divide o pipeline em **duas etapas independentes**:

```
Etapa 1 — Ingestão (process_file.py)
──────────────────────────────────────
  Responsabilidade: baixar, validar, armazenar na staging
  Pode rodar em janela noturna
  Não impacta consultas na tabela final

Etapa 2 — Atualização (merge_staging.py)
──────────────────────────────────────
  Responsabilidade: aplicar regras de negócio (upsert)
  Pode rodar após validação da staging
  Transação curta por lote (não bloqueia)
```

Essa separação reduz o **acoplamento** e simplifica a **manutenção**. Cada etapa pode evoluir independentemente — por exemplo, a ingestão pode ganhar novos validadores sem afetar o merge, e o merge pode mudar a estratégia de upsert sem revalidar os dados.

### Quando SQS por registro faz sentido

A abordagem baseada em eventos individuais **continua sendo a melhor escolha** quando:

- **Cada registro requer processamento complexo e heterogêneo** (ex: calls a APIs externas, enriquecimento, transformação)
- **Há integrações externas por registro** (ex: notificar sistema A para ativo X, sistema B para ativo Y)
- **O tempo de processamento varia significativamente entre mensagens** (ex: 1 registro leva 10ms, outro leva 30s — fila permite escalonamento natural)
- **Há necessidade de paralelismo extremo com backpressure** (a fila SQS é um buffer natural entre produtor e consumidor)
- **O sistema é orientado a eventos por design** (outros consumidores reagem a cada registro individualmente)

**Nenhum desses cenários se aplica ao nosso caso**, cujo objetivo é **carga massiva e atualização de dados em banco relacional** com máximo throughput e mínima complexidade.

### Conclusão da Comparação

| Critério | Row Groups (nossa solução) | SQS por registro |
|----------|---------------------------|------------------|
| Complexidade arquitetural | Baixa (1 script, 1 worker pool) | Alta (fila, consumer, DLQ, monitoramento) |
| Throughput no DB | Alto (batch insert) | Baixo (insert individual) |
| Custo operacional | Previsível (por arquivo) | Proporcional (por registro) |
| Idempotência | ON CONFLICT DO NOTHING | ID de dedup na fila |
| Reprocessamento | Simples (reprocessar arquivo) | Complexo (replay de fila) |
| Tratamento de erros | Tabela de erro + continua lote | DLQ + interrompe lote |
| Rastreabilidade | Structured (banco relacional) | Difusa (logs + DLQ) |
| Acoplamento ingestão/atualização | Baixo (separado por script) | Alto (consumer único) |
| Ideal para | Carga massiva em banco relacional | Processamento heterogêneo por registro |

A solução com row groups mantém características importantes como **idempotência, rastreabilidade e capacidade de reprocessamento**, ao mesmo tempo que evita a complexidade e o custo associados à criação e gerenciamento de milhões de mensagens individuais em filas SQS.


## E se algo der errado?

### Cenário 1: processo morre no meio do download

```
  Acontece: container cai, falta de memória, timeout de rede
  Consequência: staging vazia (nenhum row group foi processado)
  Recuperação: executar process_file.py de novo
    → Checkpoint detecta que nada foi feito
    → Reprocessa tudo
```

### Cenário 2: processo morre no meio do staging

```
  Acontece: DB connection drops durante insert de staging
  Consequência: row group parcialmente inserido
  Recuperação: executar process_file.py de novo
    → ON CONFLICT DO NOTHING: linhas já inseridas são ignoradas
    → Só insere as que faltam
```

### Cenário 3: merge morre no meio

```
  Acontece: deadlock, timeout, conexão perdida
  Consequência: lote parcialmente mergeado
    • staging ainda tem registros PENDING (não sofreram UPDATE MERGED)
  Recuperação: executar merge_staging.py de novo
    → FOR UPDATE SKIP LOCKED pega só os PENDING restantes
    → ON CONFLICT DO UPDATE na final = idempotente
```

### Cenário 4: merge roda duas vezes (acidentalmente)

```
  Acontece: schedule duplicado, execução manual + automática
  Consequência: merge_staging.py roda em paralelo
    • Worker 1 pega lote A (SKIP LOCKED)
    • Worker 2 pega lote B (SKIP LOCKED)
  Resultado: cada lote é processado uma vez
  Final: sem duplicatas, sem conflitos
```

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

> 💡 **Nota para RDS Aurora PostgreSQL**: O **Aurora gerencia o storage de forma distribuída** e o **autovacuum** já vem habilitado e configurado pela AWS com parâmetros adequados no `DB cluster parameter group`. Em produção com Aurora, você **não precisa se preocupar com VACUUM manual** — o serviço cuida disso automaticamente. O entendimento conceitual de dead tuples continua importante para dimensionamento e modelagem, mas a operação é transparente.

---

## Particionamento

### O que é?

Particionamento divide uma tabela grande em **partições menores** baseadas em uma coluna-chave (ex: `reference_date`). O PostgreSQL roteia automaticamente cada registro para a partição correta.

### Quando considerar

```
Tamanho da tabela        Recomendação
────────────────────     ────────────────────
< 10 milhões             Sem particionamento (overhead desnecessário)
10-50 milhões            Avaliar particionamento mensal
50+ milhões              Particionamento recomendado
```

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

### Cuidados

| Cuidado | Por quê |
|---------|---------|
| PK precisa incluir a coluna de partição | Limitação do PostgreSQL |
| Constraints únicas entre partições não são possíveis | Cada partição tem seu próprio índice único |
| Migração requer rebuild | `CREATE TABLE ... PARTITION BY RANGE` + `INSERT INTO ... SELECT` |

---

## MiniStack / LocalStack

Este projeto utiliza a imagem `localstack/localstack:latest` com `SERVICES=s3` como emulador S3
compatível com MiniStack (modo gratuito). O LocalStack Community Edition é gratuito e oferece
o serviço S3 sem necessidade de licença, chave AWS ou conta.

Para usar o MiniStack oficial, altere a imagem no `docker-compose.yml` para
`ministackorg/ministack:latest` (se disponível) e ajuste as configurações conforme documentação.

---

## Troubleshooting

| Problema | Causa provável | Solução |
|----------|---------------|---------|
| `Connection refused` no S3 | LocalStack não está pronto | `docker compose ps` — aguarde "healthy" |
| `Connection refused` no PostgreSQL | PostgreSQL não está pronto | `docker compose ps` — aguarde "healthy" |
| `MultiPartUpload` ou `BucketAlreadyOwnedByYou` | Bucket já existe | Normal — o script trata como aviso |
| `NoSuchBucket` no processamento | Bucket foi recriado | Execute `upload_to_s3.py` novamente |
| `pyarrow` não instalou | Dependência faltando | `pip install pyarrow` separadamente |
| Tabelas não existem | Volume PostgreSQL foi removido | `docker compose down -v && docker compose up -d` |
| Merge lento | Batch size muito pequeno | Ajuste `MERGE_BATCH_SIZE=50000` |
| Erro `seek` no S3 | StreamingBody não seekable | (já corrigido na versão atual com BytesIO) |
| Table bloat suspeito | Muitos dead tuples | `VACUUM ANALYZE custody_position` |

---

## Consultas úteis

```bash
# Acessar o banco
docker exec -it poc-postgres psql -U pocuser -d pocdb

# Tabela final — todos os registros
SELECT * FROM custody_position ORDER BY account_id, asset_id, reference_date;

# Staging — status dos lotes
SELECT batch_id, status, COUNT(*) AS registros
FROM custody_position_staging
GROUP BY batch_id, status
ORDER BY batch_id;

# Erros por lote
SELECT e.batch_id, e.error_reason, COUNT(*) AS ocorrencias
FROM custody_position_error e
GROUP BY e.batch_id, e.error_reason
ORDER BY e.batch_id;

# Data de atualização dos registros na final
SELECT account_id, asset_id, quantity, updated_at
FROM custody_position
WHERE updated_at > NOW() - INTERVAL '1 hour';
```
