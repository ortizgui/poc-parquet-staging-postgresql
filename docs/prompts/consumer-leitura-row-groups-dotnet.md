# Prompt — Consumidor .NET: ingestão de Parquet por row group a partir do S3

> **Para um agente de IA.** Este documento é auto-contido: você vai **implementar ou portar**, em um
> consumidor .NET, a ingestão de Parquet **row group a row group** a partir do S3 que esta POC usa —
> de modo que um arquivo **maior que a memória do pod** seja ingerido sem OOM, de forma incremental,
> e falhe de forma limpa quando o writer emitir **um único row group gigante**.
> A implementação de referência real está neste repositório; cite/compare com ela.
> O lado do produtor é o outro prompt: [`producer-row-group-size.md`](producer-row-group-size.md).

---

## 1. Objetivo e tese

- **Tese:** o pico de memória do leitor é `O(maior row group × colunas lidas) + O(lote de flush)`,
  **não** `O(tamanho do arquivo)`. O row group é o piso.
- **Entregável:** um consumidor .NET 10 que:
  1. lê o objeto do S3 por **Range GET** (stream seekable, sem baixar o arquivo inteiro),
  2. itera **row group a row group**, projetando **apenas as colunas** que vão para o banco,
  3. faz **flush incremental** (upsert) e descarta o lote antes do próximo row group,
  4. mantém a memória **chapada** mesmo com arquivo de ~1 GB sob limite de 512 MB / 192 MB,
  5. **falha por memória** com 1 row group gigante — e isso é o resultado esperado do teste.

## 2. Implementação de referência (cite os arquivos/linhas)

| Arquivo | Papel |
|---|---|
| [`../../src/Worker/Services/S3RangeStream.cs`](../../src/Worker/Services/S3RangeStream.cs) | `Stream` **seekable** sobre S3 `GetObject` com `ByteRange`; buscas exatas (piso `MinFetchBytes` = 256 KB na linha 49; teto `Consumer:RangeBlockMb`), sem alinhamento a blocos; `If-Match` (ETag) em toda requisição (`EtagToMatch`, linhas 184–187) → **412** se o objeto for sobrescrito; **não é thread-safe**; contadores `TotalBytesFetched` / `Requests` (linhas 67–70). |
| [`../../src/Worker/Services/ParquetProcessor.cs`](../../src/Worker/Services/ParquetProcessor.cs) | `ProcessRowGroupsAsync` (linha 180): `ParquetReader.CreateAsync(origem)` (183), projeção dos 5 campos (186–191), loop `OpenRowGroupReader(rg)` (246) + `ReadColumnAsync` (248–252), flush no fim do RG (305), `_flushBatchSize` default 2000 (72); modo `LocalFile` baixa para `Consumer:TempPath` e apaga no `finally` (158–163). |
| [`../../src/Worker/Services/DatabaseService.cs`](../../src/Worker/Services/DatabaseService.cs) | Upsert em **um único statement** `INSERT ... ON CONFLICT DO UPDATE ... RETURNING (xmax = 0)` (67, 102, 106); dedup **dentro do lote** (o Postgres aborta com `21000 cannot affect row a second time`, linhas 71–74); parâmetros Npgsql limitados por `PostgreSQL:BatchSize` (33). |
| [`../../src/Worker/Services/SqsConsumerService.cs`](../../src/Worker/Services/SqsConsumerService.cs) | Heartbeat de **visibility** durante o processamento (187, 289); redrive para a **DLQ** com atributos de triagem `FailureReason`, `ExceptionType`, `SourceQueue`, `ConsumerId`, `ReceiveCount` (323–327). |
| [`../../src/Worker/Services/IngestMetrics.cs`](../../src/Worker/Services/IngestMetrics.cs) | Métricas `/metrics` (prometheus-net). |

## 3. Invariantes que NÃO podem ser quebradas

1. **Nunca** `MemoryStream` do arquivo inteiro (era exatamente o bug do baseline).
2. **Nunca** `List<>`/array segurando o arquivo inteiro — a lista só acumula o **lote** (`FlushBatchSize`).
3. Memória deve ser `O(maior RG) + O(lote de flush)`, **independente** do tamanho do arquivo.
4. Apagar o temporário no `finally` (o modo `LocalFile` deixa `.parquet` órfão em SIGKILL).
5. **Projeção de colunas** é o ganho mais barato num schema largo — leia só o que vai para o banco.
6. O **row group é o piso**: o código não pode tentar paginar abaixo dele.

## 4. Requisitos funcionais

### 4.1 Stream seekable sobre Range GET

- `GetObjectMetadata` (HeadObject) para obter `ContentLength`, `ETag` e `VersionId` —
  [`S3RangeStream.GetObjectInfoAsync`](../../src/Worker/Services/S3RangeStream.cs) (87–97).
- `Seek` só move o ponteiro (nada é transferido). `Read` busca a partir do offset **exato**, com
  piso de 256 KB e teto configurável; leituras dentro do intervalo já buscado não geram tráfego novo.
- **Pinning:** envie `If-Match` com o ETag do objeto em **toda** requisição. Se outro produtor
  sobrescrever o objeto no meio da leitura, o S3 responde **412** em vez de misturar bytes de versões
  diferentes. Em bucket versionado, fixe o `VersionId` (`s3:GetObjectVersion`).
- Um único consumidor sequencial (não é thread-safe por design).

### 4.2 Loop por row group com projeção

- `var reader = await ParquetReader.CreateAsync(stream)`; `reader.RowGroupCount` e
  `reader.Schema.GetDataFields()`.
- Para cada `rg`: `using var rgReader = reader.OpenRowGroupReader(rg)`, depois
  `await rgReader.ReadColumnAsync(field)` **apenas** para as colunas projetadas.
- Declare contadores (`inserted`, `updated`, `invalid`, `totalRows`) **fora** do loop.
- Ao sair do `using`, as colunas do RG ficam elegíveis ao GC antes do próximo RG.

### 4.3 Flush incremental e idempotência

- Acumule linhas válidas e chame o flush quando `validRows.Count >= Consumer:FlushBatchSize` (2000).
- O upsert é **um statement** por lote: `INSERT ... ON CONFLICT (account_id, asset_id, reference_date)
  DO UPDATE ... RETURNING (xmax = 0)`; `xmax = 0` ⇒ linha **inserida**, senão **update**.
- Dedup dentro do lote (primeira ocorrência vence) para evitar o erro `21000` do Postgres.
- Feche o último lote parcial no fim de cada row group.

### 4.4 Fila e resiliência

- Consuma SQS com `MaxNumberOfMessages = 1`, processamento sequencial (1 msg = 1 arquivo).
- **Heartbeat de visibility**: o timeout do SQS conta da **entrega**, não do fim. Renove a cada
  `Consumer:VisibilityHeartbeatSeconds` (default **15s**) enquanto processa; `VisibilityTimeoutSeconds`
  default **300s**. Regra: `heartbeat < timeout`; o worker deriva (timeout/5) e **limita à metade**
  quando explícito — nunca aceite `heartbeat >= timeout`.
- **DLQ**: ao atingir `Consumer:MaxReceiveCount` (default 3), envie a mensagem à DLQ com o corpo
  original + atributos de triagem; remova da fila principal. `RedrivePolicy` do SQS é a 2ª camada.

## 5. Mecânica da leitura parcial no S3 (o que você precisa saber)

- Um Parquet termina com `File Metadata` + `4-byte length` + `PAR1`. O leitor: `Seek` ao fim → lê 8
  bytes do tamanho do footer → `Seek` ao início do footer → lê o footer (o **mapa** de cada column
  chunk) → `Seek`/`Read` por column chunk.
- **S3 não suporta múltiplos ranges em um GET** → cada column chunk buscado vira **uma requisição**
  `Range: bytes=...` (resposta `206 Partial Content`). No arquivo de teste de ~1 GB:
  **~120 GETs / 29,6 MB / 2,8%** do objeto, com projeção de 5 de 40 colunas.
- **Não alinhe** as buscas a blocos grandes: neste layout um column chunk comprimido tem ~450 KB; um
  bloco alinhado de 8 MB buscaria ~18× mais bytes — podendo ficar **pior que baixar o arquivo inteiro**.
- Transferência **intra-região** S3→ECS não é cobrada por byte; latência real ~5–30 ms/req.

## 6. ECS / Fargate

- **IAM (task role):** `s3:GetObject` no bucket/prefixo (e `s3:GetObjectVersion` se pinar `VersionId`);
  `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility` (**obrigatório** — sem ele
  o heartbeat falha), `sqs:GetQueueAttributes`, `sqs:GetQueueUrl`; `sqs:SendMessage` na DLQ.
- **Rede:** **S3 Gateway VPC Endpoint** para subnets privadas (mantém o Range GET na rede da AWS sem
  NAT). No modo `S3Range` **não** é preciso `ephemeralStorage`.
- **Dimensionamento de memória:**
  `task_memory ≈ baseline (~30–60 MB) + maior_RG × (2–4)` com ~50% de folga.
  Paralelismo é **por task** (`desiredCount`), não por worker interno — `N` tasks pagam `N × pico`.
- **Sem `cadvisor` no Fargate:** ele é agente de host. Use **CloudWatch Container Insights**
  (`MemoryUtilized` / `MemoryReserved`, `CpuUtilized` / `CpuReserved`) + o **stop reason**/`OOMKilled`
  da task. Scrape do `/metrics` via ADOT/CloudWatch agent ou Amazon Managed Prometheus.

## 7. Modos de falha por memória (detecte os dois)

1. **`OOMKilled` do kernel/cgroup** — exit 137, `OOMKilled=true`, **geralmente sem exceção no log**.
2. **`System.OutOfMemoryException` gerenciada** — o runtime .NET lança antes do cgroup matar;
   `OOMKilled=false`, exit 0 (a task pode "terminar com sucesso" com **0 linhas**). Exemplo real:
   um RG de 1.803,6 MiB materializado em `.Data` (`DataField.UnpackDefinitions`,
   `ParquetProcessor.cs:262`).

Qual modo ocorre depende de `DOTNET_GCHeapHardLimitPercent` (a POC usa `0x4B` = 75% do cgroup). Como
o OOM gerenciado **não** aparece como `OOMKilled`, **alerte sobre os dois**: `OOMKilled`/stop reason
**e** `OutOfMemoryException` no log + profundidade da DLQ. O runner do A/B aceita os dois modos e
rejeita `timeout`/DLQ isolados.

## 8. Observabilidade (4 golden signals + métricas de aplicação)

| Signal | Métricas |
|---|---|
| Traffic | `poc_parquet_rows_processed_total`, `poc_parquet_bytes_downloaded_total`, `poc_parquet_range_requests_total`, `poc_ingest_files_total` |
| Latency | `poc_parquet_rowgroup_read_seconds`, `poc_db_upsert_seconds`, `poc_db_upsert_batch_size` |
| Errors | `poc_ingest_invalid_records_total`, `poc_sqs_message_failures_total`, `poc_sqs_messages_sent_to_dlq_total`, `poc_sqs_dlq_depth` |
| Saturation | working set do **pod** ÷ limite (cadvisor no compose; Container Insights no Fargate) + heap do .NET |

- Compare `poc_parquet_bytes_downloaded_total` com `poc_ingest_last_file_bytes`: leitura parcial
  saudável fica em **~3%**; se subir para ~100%, o `ReadMode` virou `LocalFile` ou a projeção quebrou.
- **Curva esperada:** o working set **sobe e estabiliza num platô** abaixo do limite — **não** dente
  de serra (o GC do .NET não devolve memória ao SO na hora).

## 9. Checklist de implementação

- [ ] `S3RangeStream : Stream` com `CanRead`/`CanSeek`, `Seek` sem I/O, `Read` com busca exata
      (piso 256 KB, teto configurável) e **`If-Match`**.
- [ ] `ParquetReader.CreateAsync(stream)` + loop `OpenRowGroupReader(rg)`.
- [ ] Projeção: ler **só** as colunas que vão para o banco.
- [ ] Acumulador do **lote** (não do arquivo) + flush a cada `FlushBatchSize`.
- [ ] Contadores fora do loop; `_metrics.RowGroupsRead.Inc()` e `RowsProcessed.Inc(rowCount)`.
- [ ] Upsert em 1 statement `ON CONFLICT ... RETURNING (xmax = 0)` + dedup in-batch.
- [ ] `LocalFile`: streaming para disco + delete no `finally`; limpeza de órfãos por prefixo/idade.
- [ ] Heartbeat de visibility + DLQ com atributos de triagem.
- [ ] Config: `ReadMode`, `RangeBlockMb`, `PinObjectVersion`, `FlushBatchSize`, `TempPath`,
      `VisibilityTimeoutSeconds`, `VisibilityHeartbeatSeconds`, `MaxReceiveCount`, `Metrics:Port`.
- [ ] Modo de falha por memória detectado nos **dois** modos; testes de aceite (§12).

## 10. Esqueleto C# (stream + loop por row group)

```csharp
// 1) Stream seekable sobre Range GET, com pinning por ETag.
var info = await S3RangeStream.GetObjectInfoAsync(_s3, bucket, key, ct);
await using var stream = new S3RangeStream(_s3, bucket, key, info.Length,
    maxFetchBytes: _rangeBlockMb * 1024 * 1024,
    etag: info.ETag, pinVersion: _pinObjectVersion, logger: _logger);

// 2) Reader Parquet + projecao.
await using var reader = await ParquetReader.CreateAsync(stream, cancellationToken: ct);
var fields = reader.Schema.GetDataFields();
var accountIdField = fields.First(f => f.Name == "account_id");
// ... asset_id, reference_date, quantity, amount ...

int totalRows = 0, inserted = 0, updated = 0, invalid = 0;   // fora do loop
var batch = new List<Row>(_flushBatchSize);

for (var rg = 0; rg < reader.RowGroupCount; rg++)
{
    ct.ThrowIfCancellationRequested();

    using (var rgReader = reader.OpenRowGroupReader(rg))
    {
        var accountIds = (string[])(await rgReader.ReadColumnAsync(accountIdField, ct)).Data;
        // ... demais colunas projetadas ...
        var rowCount = accountIds.Length;
        totalRows += rowCount;

        for (var j = 0; j < rowCount; j++)
        {
            batch.Add(new Row(accountIds[j] /* ... */));
            if (batch.Count >= _flushBatchSize)
            {
                var (ins, upd) = await _db.BulkInsertDirectAsync(batch, ct);  // 1 statement
                inserted += ins; updated += upd;
                batch.Clear();                                                 // descarta o lote
            }
        }
    }

    if (batch.Count > 0) { var (ins, upd) = await _db.BulkInsertDirectAsync(batch, ct);
                           inserted += ins; updated += upd; batch.Clear(); }

    _metrics.RowGroupsRead.Inc();
    _metrics.RowsProcessed.Inc(rowCount);
    _logger.LogInformation("[rg {Rg}/{Groups}] rows={Rows} | managed={Mb}MB workingSet={Ws}MB",
        rg + 1, reader.RowGroupCount, rowCount,
        GC.GetTotalMemory(false) / 1048576, Environment.WorkingSet / 1048576);
}
```

> `Row`, `BulkInsertDirectAsync`, `_metrics`, `_db`, `_rangeBlockMb`, `_flushBatchSize` etc. vêm do
> seu projeto; o esqueleto mostra **a forma** do fluxo (o real está em `ParquetProcessor.cs`).

## 11. Anti-padrões / armadilhas

| Anti-padrão | Consequência | Correção |
|---|---|---|
| `GetObject` → `MemoryStream` do arquivo todo | 1 GB de heap antes da 1ª linha | `S3RangeStream` (Range GET) |
| `List<>` acumulando o arquivo todo | Pico cresce com o arquivo | Acumule só o lote + `Clear()` |
| Alinhar `Range` a blocos de 8 MB | ~18× mais tráfego que o necessário | Busca exata (piso 256 KB, teto configurável) |
| Achar que `Parquet.Net` pagina abaixo do RG | OOM com 1 RG gigante | Projeção de colunas; reescrever a origem |
| Materializar todas as colunas do RG | Pico = RG inteiro × nº de colunas | Projete as 5 colunas de destino |
| Ignorar a corrida de ETag | Dado Frankenstein (footer de uma versão, chunks de outra) | `If-Match` (412) + `VersionId` |
| Paralelismo interno (`MaxWorkers>1`) sem fazer a conta de memória | Pico multiplicado dentro do pod | Neste repo o `MaxWorkers` foi **removido** — o paralelismo é por **task** (`desiredCount`); se reintroduzir workers in-process, `N` workers = `N × pico` |
| Contar só com `OutOfMemoryException` ou só `OOMKilled` | Perde um dos modos de falha | Detecte/alerta os dois (§7) |

## 12. Receita de teste (A/B com asserção)

O teste decisivo usa o **mesmo limite** e o **mesmo volume**, mudando só o row group:

```bash
# 1. Infra + stack
docker compose up -d
.venv/bin/python3 scripts/setup_infra.py

# 2. Parquets: muitos RGs (deve CONCLUIR) e 1 RG gigante (deve FALHAR por memoria)
.venv/bin/python3 scripts/generate_large_parquet.py --rows 1160000 \
    --row-group-rows 20000 --columns 40 --output data/ab_1160000_20000rg.parquet --upload
.venv/bin/python3 scripts/generate_large_parquet.py --rows 1160000 \
    --row-group-rows 1160000 --columns 40 --output data/ab_1160000_1rg.parquet --upload

# 3. A/B decisivo (com assecao; exit != 0 se a tese nao bater)
PATH="$PWD/.venv/bin:$PATH" bash scripts/test_rowgroup_ab.sh --rows 1160000 --limit-mb 192 --reuse
```

**Aceite medido nesta POC** (limite **192 MB**, 1.160.000 linhas × 40 colunas, 5 projetadas):

| Cenário | Row groups | Maior RG | Pico | Resultado | OOMKilled | Linhas |
|---|---|---|---|---|---|---|
| MUITOS_RG | 58 | 32,5 MiB | **109,4 MiB** | `ok` | `false` | +1.073.909 ins / ~85.920 upd / −0 err; S3Range 29,6 MB / 120 req / 2,8% |
| UM_RG | 1 | 1.803,6 MiB | **176,9 MiB** | `out_of_memory` (gerenciada) | `false` | 0 |

- **Critério de sucesso:** MUITOS_RG `ok` com pico **plano**; UM_RG **falha por memória** (kernel
  `OOMKilled` **ou** `OutOfMemoryException`). `timeout`/DLQ isolados **não** passam.
- Evidência bruta: [`../evidencias/README.md`](../evidencias/README.md)
  (`ab-limit192-final.log`, CSVs, worker logs, `ab-parquet-metadata.txt`).

## 13. Critérios de aceite

- [ ] Arquivo de ~1 GB ingerido sob o limite do pod (512 MB ou 192 MB) sem OOM, com working set
      **estável** (platô), não crescente com o arquivo.
- [ ] Mesma contagem de linhas que um processamento de referência; `0 erros` no caminho feliz.
- [ ] Reprocessar é **idempotente** (`+0 ins`, contagem estável).
- [ ] 1 row group gigante **falha por memória** (um dos dois modos), com a mensagem diagnóstica.
- [ ] Métricas `/metrics` expostas; projeção comprovada pelo tráfego (~3%).
- [ ] Temporário apagado no `finally`; nenhum `MemoryStream` do arquivo inteiro no código.

## 14. Referências

- [`../aws-producao-ecs-fargate.md`](../aws-producao-ecs-fargate.md) — fluxo E2E, IAM, sizing, ECS/Fargate, modos de falha.
- [`../replicar-testes-e-metricas.md`](../replicar-testes-e-metricas.md) — como rodar e medir.
- [`../memory-test-results.json`](../memory-test-results.json) — resultados e `modo_de_falha`.
- [`../evidencias/README.md`](../evidencias/README.md) — evidência bruta versionada.
- [`../../scripts/test_rowgroup_ab.sh`](../../scripts/test_rowgroup_ab.sh) — A/B com asserção.
- [`producer-row-group-size.md`](producer-row-group-size.md) — o lado do produtor.
