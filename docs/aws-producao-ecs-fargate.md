# Ingestão de Parquet no AWS — ECS Fargate + S3 + SQS + RDS

Este documento descreve como a POC roda **em produção na AWS** e quais decisões de projeto
sustentam o objetivo central: **ingerir um arquivo Parquet maior que a memória do pod, sem
estourar a memória e sem ocupar disco.**

> Complementa o `README.md` (que cobre a POC local). Aqui o foco é o ambiente real.

---

## 1. O problema

Um serviço precisa ler arquivos Parquet de ~1 GB no S3 e fazer upsert no PostgreSQL. Rodando
em uma task pequena (meta de 512 MB a 2 GB de RAM), a implementação ingênua morre:

| Abordagem ingênua | O que acontece |
|---|---|
| `GetObject` → `MemoryStream` | 1 GB de arquivo = 1 GB de heap **antes** de ler a primeira linha |
| `GetObject` → arquivo local, depois ler | Funciona, mas exige disco do tamanho do objeto e baixa 100% dos bytes |
| Ler tudo e acumular `List<>`, inserir no fim | Pico cresce com o arquivo, não com o lote |

**A causa é sempre a mesma: tratar o arquivo como a unidade de trabalho.** No Parquet, a unidade
é o **row group**.

---

## 2. Fluxo end-to-end em produção

```
  ┌──────────┐   put object    ┌──────────────────────────┐
  │ Produtor │────────────────▶│ S3  (bucket de entrada)  │
  └──────────┘                 └────────────┬─────────────┘
                                            │ S3 Event Notification
                                            ▼
                                 ┌──────────────────────┐
                                 │  SQS (fila padrão)   │◀── DLQ (após N tentativas)
                                 └──────────┬───────────┘
                                            │ ReceiveMessage (1 msg = 1 arquivo)
                                            ▼
                       ┌────────────────────────────────────────┐
                       │   ECS Fargate — task do worker .NET    │
                       │   mem_limit / task memory: 512 MB–2 GB │
                       │                                        │
                       │  1. lê a mensagem → bucket + key       │
                       │  2. HeadObject → tamanho + ETag        │
                       │  3. abre S3RangeStream (Range GET)     │
                       │  4. ParquetReader lê o FOOTER          │
                       │  5. para cada row group:               │
                       │       lê só as colunas projetadas      │
                       │       → upsert em lote                 │
                       │       → descarta o lote (Clear)        │
                       │  6. DeleteMessage                      │
                       └────────────────┬───────────────────────┘
                                        │  ON CONFLICT DO UPDATE
                                        ▼
                              ┌──────────────────────┐
                              │   RDS PostgreSQL     │
                              └──────────────────────┘
                                        ▲
                    prometheus-net ──────┘  (métricas /metrics, 4 golden signals)
```

O worker é **stateless** e não guarda estado entre mensagens. Escala horizontalmente por
**número de tasks** (desired count do ECS service), não por threads internas.

---

## 3. Como a leitura parcial funciona

### 3.1 O Parquet tem um índice no fim do arquivo

Um arquivo Parquet é:

```
┌────────┬──────────────────────────────┬─────────────────────┬─────────┬────────┐
│ "PAR1" │  data pages (column chunks)  │  footer (Thrift)    │ len(4B) │ "PAR1" │
└────────┴──────────────────────────────┴─────────────────────┴─────────┴────────┘
   4 B              ~99,9% do arquivo           schema + offset e tamanho
                                                de CADA column chunk
                                                de CADA row group
```

O `footer` é um **diretório**: ele diz exatamente em qual byte do arquivo começa e termina cada
column chunk de cada row group. Ou seja, o leitor **não precisa varrer o arquivo** para saber
onde estão os dados — precisa do footer e de mais nada.

### 3.2 O que o `ParquetReader` pede ao stream

O `ParquetReader.CreateAsync(Stream)` só exige um stream **legível** e **seekable**. A sequência
de operações dele é:

1. `Seek` para o fim, lê 8 bytes → tamanho do footer
2. `Seek` para o início do footer, lê o footer → **o mapa**
3. Para cada row group: `Seek` + `Read` nos offsets das colunas pedidas

`S3RangeStream` traduz cada `Seek`/`Read` em uma requisição HTTP

```
GET /bucket/key
Range: bytes=<offset>-<offset+len-1>
```

Nada vai para disco, e nada é lido além do que o reader pedir.

### 3.3 Evidência: a sequência real de requisições

Trace de um arquivo de **1.040,8 MB** (58 row groups, 40 colunas, 5 projetadas), limite de 512 MB:

```
  req #1    offset=             0 len=   262,144     ← abre o arquivo
  req #2    offset= 1,091,378,029 len=         4     ← lê o magic no FIM do arquivo
  req #3    offset= 1,091,378,025 len=         8     ← lê tamanho do footer
  req #4    offset= 1,090,992,711 len=   385,314     ← LÊ O FOOTER (o mapa)
  req #5    offset=             4 len=   262,144     ← primeiro column chunk
  req #6    offset=       262,148 len=   262,144
  ...
  req #120  offset= 1,072,436,883 len=   262,144     ← último row group
```

Repare no salto entre row groups: **~17,7 MB**. O leitor pula de row group em row group — nunca
relê o que já passou, nunca lê o que não pediu.

**Resultado: 29,6 MB transferidos de 1.040,8 MB — 2,8% do objeto — em 120 requisições.**

### 3.4 Por que 2,8% e não os 12,5% das colunas

São lidas 5 de 40 colunas (= 12,5%), mas o tráfego cai para 2,8%. Motivo: as colunas **não lidas**
são as de texto largo e alta cardinalidade (UUIDs, descrições, campos livres), que dominam o
tamanho do arquivo. Você não paga por elas.

**Corolário importante:** a projeção de colunas é o maior ganho isolado, e ela não depende do modo
de leitura — vale igual para `S3Range` e `LocalFile`.

### 3.5 Armadilha: não alinhe as buscas em blocos

A tentação é arredondar cada `Seek` para blocos de, digamos, 8 MB — "aproveita o cache". Não
funciona: neste layout um column chunk comprimido tem ~450 KB, então um bloco alinhado de 8 MB
buscaria **~18× mais bytes que o necessário** — podendo ficar **pior que baixar o arquivo inteiro**.

A busca é **exata**, com piso (`MinFetchBytes` = 256 KB, para não fazer milhares de requisições de
poucos bytes) e teto (`Consumer:RangeBlockMb`).

---

## 4. Modos de leitura (`Consumer:ReadMode`)

| | `S3Range` (**default**, recomendado) | `LocalFile` |
|---|---|---|
| Como funciona | stream seekable sobre Range GET; lê o footer e busca só o necessário | baixa o objeto inteiro por streaming para arquivo temporário e lê local |
| Requisições por arquivo de 1 GB | ~120 | 1 |
| Bytes transferidos | 29,6 MB (2,8%) | 1.040,8 MB (100%) |
| Disco necessário | nenhum | tamanho do objeto |
| Latência até a primeira linha | lê o footer (1 requisição) e já começa | espera o download completo |
| Custo de rede | +120 GETs (irrisório) | 1 GET, tráfego maior |
| Quando usar | **ponto de partida em produção** | arquivo relido várias vezes; rede até o S3 lenta/instável; depuração com o arquivo no container |

Ambos os modos produzem **exatamente o mesmo resultado** — mesma contagem de linhas, mesmos erros,
mesmo pico de memória dentro da mesma faixa. A diferença é só como os bytes chegam.

---

## 5. Dimensionamento

### 5.1 Memória

O pico é `O(maior row group) + O(lote de flush)` — **não** `O(arquivo)`. O que define o maior row
group é o **writer** (`row_group_size`), não o leitor: um arquivo com um único row group gigante
não tem como ser paginado, e aí a projeção de colunas é a única alavanca.

Medições com limite de **512 MB** e arquivo de **1.040,8 MB** (2,03× o limite):

| Modo | Pico de memória | % do limite | `OOMKilled` | Erros |
|---|---|---|---|---|
| `S3Range` | 108–132 MiB | 21–26% | `false` | 0 |
| `LocalFile` | ~100 MiB | 19% | `false` | 0 |

Para referência, com o arquivo de 358,9 MB (20 row groups, 5 de 40 colunas):

| Modo | Pico | Bytes transferidos | Requisições |
|---|---|---|---|
| `S3Range` | 95 MiB | 10,4 MB (**2,9%**) | 44 |
| `LocalFile` | 91 MiB | 358,9 MB (100%) | 1 |

**Piso do runtime:** ~150–200 MB. Com 128 MB o container é morto pelo kernel no startup
(`OOMKilled`, exit 137) antes de processar qualquer coisa — AWS SDK + Npgsql + GC custam isso.

### 5.2 Task definition (referência)

```jsonc
{
  "family": "parquet-ingest-worker",
  "cpu": "1024",            // 1 vCPU
  "memory": "2048",         // 2 GB. A POC provou 512 MB; 2 GB da folga p/ row groups maiores
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "runtimePlatform": { "cpuArchitecture": "ARM64", "operatingSystemFamily": "LINUX" },
  "containerDefinitions": [{
    "name": "worker",
    "image": "<conta>.dkr.ecr.<região>.amazonaws.com/parquet-ingest-worker:<tag>",
    "environment": [
      { "name": "Consumer__ReadMode",                  "value": "S3Range" },
      { "name": "Consumer__RangeBlockMb",              "value": "8" },
      { "name": "Consumer__PinObjectVersion",          "value": "true" },
      { "name": "Consumer__VisibilityTimeoutSeconds",  "value": "300" },
      { "name": "Consumer__VisibilityHeartbeatSeconds","value": "15" },
      { "name": "Consumer__FlushBatchSize",            "value": "2000" },
      { "name": "DOTNET_gcServer",                     "value": "0" }
    ]
  }]
  // ephemeralStorage: NÃO é necessário no modo S3Range
}
```

> **ARM64 (Graviton)** é ~20% mais barato que x86 nesta carga. A imagem precisa ser publicada para
> a arquitetura correspondente (`docker buildx --platform linux/arm64`).

### 5.3 Disco

No modo `S3Range` não há requisito de disco: o default de 20 GB de `ephemeralStorage` do Fargate
sobra. No modo `LocalFile`, dimensione `ephemeralStorage` para o maior arquivo esperado **mais**
margem (o `TempPath` é um arquivo por mensagem em voo).

---

## 6. Consistência: pinning por ETag

**Este é o risco silencioso.** Cada Range GET é uma requisição **independente**. Se outro produtor
sobrescrever o mesmo objeto enquanto a ingestão corre, sem cuidado os bytes vêm de **versões
diferentes** — footer de uma, column chunks de outra. O resultado é um arquivo Frankenstein: ou o
parser explode de forma confusa, ou — pior — insere dado inconsistente sem erro nenhum.

A proteção implementada: o worker captura o `ETag` do objeto no `HeadObject` e envia
`If-Match` em **toda** requisição Range.

```
[range] If-Match: "ba20443ed8d6a543d4c83310a41b1acf-131"
```

Se o objeto mudar no meio da leitura, o S3 responde **412 Precondition Failed** em vez de devolver
bytes de outra versão. A mensagem falha de forma limpa, volta para a fila e é reprocessada quando o
objeto estiver estável. Desligável por `Consumer:PinObjectVersion=false` (não recomendado).

Em buckets **com versionamento**, o ideal adicional é fixar o `VersionId` — leitura determinística
mesmo com o objeto sendo reescrito (exige `s3:GetObjectVersion` na policy).

---

## 7. Confiabilidade

### 7.1 Visibility timeout + heartbeat

O visibility timeout do SQS conta a partir da **entrega**, não do fim do processamento. Sem
renovação, uma ingestão de 75 s com timeout de 30 s faz a mensagem reaparecer no meio do trabalho:
outro consumer baixa o mesmo arquivo, o `ApproximateReceiveCount` sobe e a mensagem vai para a DLQ
**mesmo com a ingestão terminando bem**. DLQ com falso positivo é pior que DLQ vazia — treina o
time a ignorar o alerta.

O consumer renova a visibilidade a cada `Consumer:VisibilityHeartbeatSeconds` enquanto processa.

**Regra:** o heartbeat precisa disparar **antes** de a visibility expirar. Ele não é um valor solto:
sem configuração explícita é derivado do timeout (1/5); com configuração explícita é **limitado à
metade do timeout**. O worker detecta e corrige sozinho:

```
[CONSUMER:default] VisibilityHeartbeatSeconds=60s >= VisibilityTimeoutSeconds=20s — a mensagem
reapareceria ANTES da renovacao. Reduzido para 10s (metade do timeout).
```

Valores de referência: `visibility = 300s` (≥ o tempo esperado de ingestão, para uma ingestão
inteira caber numa janela **mesmo se o heartbeat falhar**) e `heartbeat = 15s`.

Medido no arquivo de 1 GB: **1 entrega, 1 conclusão, 5 renovações, 0 redelivery**.

### 7.2 DLQ

Duas camadas:

1. **Redrive explícito no worker** — ao atingir `Consumer:MaxReceiveCount`, a mensagem vai para a
   DLQ com o corpo original + atributos de triagem (`FailureReason`, `ExceptionType`, `SourceQueue`,
   `ConsumerId`, `ReceiveCount`).
2. **`RedrivePolicy` do SQS** (`maxReceiveCount: 3`) — cobre o caso do consumer morrer sem tratar
   a falha (ex.: `OOMKilled`).

> `ApproximateReceiveCount` é **aproximado** (é literalmente o nome do atributo). Uma execução real
> fez 4 recebimentos antes do redrive com `maxReceiveCount: 3`. Daí as duas camadas.

### 7.3 Idempotência

`INSERT ... ON CONFLICT DO UPDATE` em statement único, com deduplicação **dentro do lote** — o
Postgres rejeita a mesma chave duas vezes no mesmo statement (`21000 cannot affect row a second
time`), e duplicata dentro do lote é normal neste volume. A primeira ocorrência vence.

Contagem exata de insert vs update via `RETURNING (xmax = 0)`. Reprocessar o mesmo arquivo resulta
em `+0 ins` e contagem estável.

---

## 8. Observabilidade — 4 golden signals

O worker expõe `/metrics` (prometheus-net). Some à stack Prometheus + Grafana + cadvisor
(orquestrada por `docker compose up` — zero clique manual).

| Signal | Métricas |
|---|---|
| **Traffic** | `poc_parquet_rows_processed_total`, `poc_parquet_bytes_downloaded_total`, `poc_parquet_range_requests_total` |
| **Latency** | `poc_parquet_rowgroup_read_seconds`, `poc_db_upsert_seconds` |
| **Errors** | `poc_ingest_invalid_records_total`, `poc_sqs_message_failures_total`, `poc_sqs_messages_sent_to_dlq_total`, `poc_sqs_dlq_depth` |
| **Saturation** | `container_memory_working_set_bytes` (cadvisor — **é do pod**, não do processo), throttling de CPU, + GC/working set do runtime |

**Por que cadvisor e não só as métricas do processo:** a pergunta é "o pod vai ser morto por
`OOMKilled`?", e quem decide isso é o limite do cgroup — não o heap gerenciado do .NET. Medir só o
heap dá falso negativo.

**Onde olhar primeiro:** comparação entre `poc_parquet_bytes_downloaded_total` e
`poc_ingest_last_file_bytes` mostra na hora se a projeção de colunas está funcionando (esperado:
~3% no arquivo real). Se subir para ~100%, o `ReadMode` virou `LocalFile` ou a projeção quebrou.

---

## 9. IAM — policy mínima

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LerParquetDoBucketDeEntrada",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::BUCKET-DE-ENTRADA/prefixo/*"
    },
    {
      "Sid": "ConsumirFila",
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:GetQueueAttributes",
        "sqs:GetQueueUrl"
      ],
      "Resource": "arn:aws:sqs:REGIÃO:CONTA:poc-notification-queue"
    },
    {
      "Sid": "RedriveParaDlq",
      "Effect": "Allow",
      "Action": ["sqs:SendMessage"],
      "Resource": "arn:aws:sqs:REGIÃO:CONTA:poc-notification-dlq"
    }
  ]
}
```

Notas:

- **`s3:ListBucket` não é necessário.** Range GET é `GetObject` como qualquer outro.
- `sqs:ChangeMessageVisibility` é **obrigatório** — sem ele o heartbeat falha e a redelivery volta.
- Com versionamento + `VersionId`: trocar `s3:GetObject` por `s3:GetObjectVersion`.
- Credenciais vêm da **task role** (nunca de env vars estáticas no container).

---

## 10. Custos

Para um arquivo de 1 GB, com projeção de 5 de 40 colunas:

| Item | `LocalFile` | `S3Range` |
|---|---|---|
| S3 GET requests | 1 | 120 |
| Custo de requests* | ~US$ 0,0000004 | ~US$ 0,000048 |
| Transferência S3 → task (mesma região) | US$ 0 | US$ 0 |
| Disco (`ephemeralStorage`) | 1 GB+ por task | 0 |

\* US$ 0,0004 por 1.000 requests.

**Ponto importante e contraintuitivo:** dentro da mesma região, a transferência S3 → ECS **não é
cobrada por byte**. Então o ganho do `S3Range` **não é financeiro** — o custo de 120 GETs é
irrelevante. O ganho é **memória, disco e tempo até a primeira linha**, que é exatamente o
problema que estamos resolvendo.

Onde o custo aparece de verdade: se o bucket estiver em **outra região** da task (aí transferência
inter-região é cobrada por GB e 2,8% é uma economia de 35×), ou se a taxa de requisições por
segundo crescer muito com muitos arquivos pequenos.

---

## 11. O que está provado e o que depende do ambiente real

Ser explícito aqui evita surpresa no primeiro deploy.

### Provado neste repositório

- A mecânica de leitura: footer → row group → column chunk, com **trace de requisição** (§3.3)
- Que um arquivo de **1.040,8 MB (2,03× o limite)** é ingerido sob **512 MB** sem `OOMKilled`
- Que a transferência cai para **2,8%** com projeção de colunas
- Idempotência (`+0 ins` em reprocesso), DLQ de mensagem inválida, heartbeat de visibility
- Pinning por ETag ativo (o header `If-Match` é enviado e o ETag é registrado no log)

### Depende do ambiente real (não é validável no emulador)

| Item | Por quê | Como validar no primeiro deploy |
|---|---|---|
| **Policy IAM** | o emulador não valida IAM | subir a task com a policy de §9 e conferir que não há `AccessDenied` |
| **Latência real de cada Range GET** | emulador é local (sub-ms); S3 real é ~5–30 ms por requisição | 120 requests × 20 ms ≈ **2,4 s** adicionados a uma ingestão de ~75 s — irrelevante, mas confirme |
| **Sizing do Fargate** | o ambiente de teste não é uma task Fargate | comparar `container_memory_working_set_bytes` com o `memory` da task definition |
| **RDS: pool de conexões e latência** | o Postgres local não tem a latência nem os limites de conexão do RDS | observar `poc_db_upsert_seconds` |
| **Throughput de rede da task** | Fargate tem limite por tamanho de task | ingestão de 1 GB pode saturar 0,5 vCPU em rede |

**O que dá confiança para os itens acima:** as chamadas ao S3 são feitas pelo **mesmo**
`AWSSDK.S3`, com o mesmo header `Range`, contra a mesma API. O emulador é S3-compatível e
implementa a mesma semântica de Range. A diferença é **latência e IAM**, não semântica.

---

## 12. Checklist de deploy

- [ ] Bucket de entrada com **versionamento habilitado** (permite fixar `VersionId`)
- [ ] Notificação de evento S3 (`s3:ObjectCreated:*`, sufixo `.parquet`) → fila SQS
- [ ] Fila com `RedrivePolicy` apontando para a DLQ (`maxReceiveCount: 3`)
- [ ] Alarme de CloudWatch na **profundidade da DLQ > 0**
- [ ] Alarme em `container_memory_working_set_bytes` próximo do limite da task
- [ ] Task role com a policy mínima de §9 (`sqs:ChangeMessageVisibility` incluído)
- [ ] `Consumer:ReadMode=S3Range` e `Consumer:PinObjectVersion=true`
- [ ] `Consumer:VisibilityHeartbeatSeconds` **menor** que `VisibilityTimeoutSeconds`
- [ ] Imagem publicada para a arquitetura da task (`ARM64` se Graviton)
- [ ] Logs do worker no CloudWatch Logs, com retenção definida
- [ ] `desired count` do service desligado no primeiro deploy — subir 1 task e validar
- [ ] Conferir no log: `Origem S3Range ... nada vai para disco (pinning por ETag: "...")`

---

## 13. Configuração de referência

| Chave | Default | Notas |
|---|---|---|
| `Consumer:ReadMode` | `S3Range` | `LocalFile` é alternativa, não o caminho de produção |
| `Consumer:RangeBlockMb` | `8` | teto de cada Range GET; **não** alinhe as buscas (§3.5) |
| `Consumer:PinObjectVersion` | `true` | `If-Match` em toda requisição |
| `Consumer:RangeTraceLog` | `false` | liga o log de cada Range GET (§3.3) |
| `Consumer:VisibilityTimeoutSeconds` | `300` | ≥ tempo esperado de ingestão |
| `Consumer:VisibilityHeartbeatSeconds` | `15` | derivado (timeout/5) se não informado; nunca ≥ timeout |
| `Consumer:MaxReceiveCount` | `3` | tentativas antes do redrive para a DLQ |
| `Consumer:FlushBatchSize` | `2000` | linhas por lote de upsert |
| `Consumer:TempPath` | `/tmp/poc-ingest` | usado só no modo `LocalFile` |
| `Consumer:TempRetentionHours` | `1` | idade mínima para limpar temporário órfão |

---

## 14. Limitações conhecidas

- **O row group é o piso.** Um arquivo gravado com um único row group gigante não pode ser paginado
  pelo leitor — o pico vira o row group. A alavanca nesse caso é a **projeção de colunas** e,
  idealmente, **reescrever o arquivo com row groups menores** (o writer é nosso).
- **Piso de memória do runtime:** ~150–200 MB. Não vale tentar 128 MB.
- **Acesso sequencial por design.** O `S3RangeStream` não é thread-safe; o consumo é um row group
  por vez. Paralelizar row groups exigiria múltiplos streams — possível, fora do escopo da POC.
- **Many small files:** o modelo é 1 mensagem = 1 arquivo grande. Para muitos arquivos pequenos, o
  custo de 120 GETs não aparece, mas o de abrir conexão por arquivo sim — aí vale batch.
