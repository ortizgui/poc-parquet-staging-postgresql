using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Amazon.S3;
using Parquet;
using Parquet.Data;
using PocWorker.Models;

namespace PocWorker.Services;

/// <summary>
/// Le um Parquet do S3 e faz upsert direto em custody_position.
///
/// Paginacao por ROW GROUP:
///   1. obtem um stream seekable do objeto (dois modos, ver <c>Consumer:ReadMode</c>);
///   2. le row group a row group, fatiando cada um em lotes de FlushBatchSize;
///   3. faz flush no banco e descarta o lote antes de seguir.
///
/// A memoria de pico e O(maior row group + lote de flush) — NAO O(tamanho do arquivo).
/// O row group e a unidade de I/O do Parquet, e quem o define e o WRITER
/// (row_group_size). Um arquivo com um unico row group gigante nao tem como ser
/// paginado pelo leitor: o piso e o proprio row group (atenuado pela projecao de colunas).
///
/// Projecao: apenas as colunas que alimentam custody_position sao lidas do stream.
///
/// MODOS DE LEITURA (Consumer:ReadMode):
///   S3Range  — stream seekable sobre Range GET (default). Le o footer e busca so os bytes
///              necessarios; nada vai para disco e, com projecao, a transferencia cai para a
///              fracao de colunas lidas. Mais requisicoes HTTP.
///   LocalFile — baixa o objeto por streaming para um arquivo temporario e le o arquivo local.
///              1 requisicao grande, leitura local; exige espaco em disco do tamanho do objeto.
/// </summary>
public class ParquetProcessor
{
    public const string ReadModeS3Range = "S3Range";
    public const string ReadModeLocalFile = "LocalFile";

    private readonly IAmazonS3 _s3;
    private readonly DatabaseService _db;
    private readonly IngestMetrics _metrics;
    private readonly ILogger<ParquetProcessor> _logger;
    private readonly string _tempPath;
    private readonly string _tempPrefix;
    private readonly int _flushBatchSize;
    private readonly string _readMode;
    private readonly int _rangeBlockBytes;
    private readonly bool _pinObjectVersion;
    private readonly bool _rangeTrace;

    private const int StreamBufferSize = 1 << 16; // 64 KiB

    private bool UseS3Range => !string.Equals(_readMode, ReadModeLocalFile, StringComparison.OrdinalIgnoreCase);

    public ParquetProcessor(
        IAmazonS3 s3,
        DatabaseService db,
        IngestMetrics metrics,
        IConfiguration config,
        ILogger<ParquetProcessor> logger)
    {
        _s3 = s3;
        _db = db;
        _metrics = metrics;
        _logger = logger;

        var configuredTemp = config.GetValue<string>("Consumer:TempPath");
        _tempPath = string.IsNullOrWhiteSpace(configuredTemp)
            ? Path.Combine(Path.GetTempPath(), "poc-ingest")
            : configuredTemp;

        _flushBatchSize = Math.Max(1, config.GetValue<int>("Consumer:FlushBatchSize", 2000));
        _readMode = config.GetValue<string>("Consumer:ReadMode") ?? ReadModeS3Range;
        _rangeBlockBytes = Math.Max(1, config.GetValue<int>("Consumer:RangeBlockMb", 8)) * 1024 * 1024;
        _pinObjectVersion = config.GetValue<bool>("Consumer:PinObjectVersion", true);
        _rangeTrace = config.GetValue<bool>("Consumer:RangeTraceLog", false);

        Directory.CreateDirectory(_tempPath);

        // Um SIGKILL (OOMKilled) nao roda o finally, entao o arquivo temporario fica orfao.
        // A limpeza roda SO na inicializacao, SO nos arquivos deste consumer (prefixo) e SO nos
        // que passaram do tempo de retencao — seguro mesmo se TempPath apontar para um volume
        // compartilhado entre tasks (EFS, volume de host no launch type EC2), onde apagar tudo
        // poderia remover um arquivo que outra task esta lendo.
        _tempPrefix = Sanitize(config.GetValue<string>("Consumer:ConsumerId") ?? "worker");
        var retentionHours = Math.Max(1, config.GetValue<int>("Consumer:TempRetentionHours", 1));
        CleanupStaleTempFiles(TimeSpan.FromHours(retentionHours));
    }

    public async Task<ProcessResult> ProcessFileAsync(
        string bucket, string key, string target, CancellationToken ct = default)
    {
        var sourceFile = $"s3://{bucket}/{key}";
        var batchId = Guid.NewGuid();

        FileStream? arquivoLocal = null;
        S3RangeStream? streamS3 = null;
        string? tempFile = null;
        long objectSize = 0;

        try
        {
            Stream origem;

            if (UseS3Range)
            {
                var info = await S3RangeStream.GetObjectInfoAsync(_s3, bucket, key, ct);
                objectSize = info.Length;
                streamS3 = new S3RangeStream(
                    _s3, bucket, key, objectSize, _rangeBlockBytes,
                    etag: info.ETag, pinVersion: _pinObjectVersion, logger: _logger, trace: _rangeTrace);
                origem = streamS3;

                _logger.LogInformation(
                    "Origem {Mode} para {Source}: {Mb:F1} MB no S3, bloco de {Block} MB, nada vai para disco " +
                    "(pinning por ETag: {Pin})",
                    _readMode, sourceFile, objectSize / 1024.0 / 1024.0, _rangeBlockBytes / 1024 / 1024,
                    _pinObjectVersion && !string.IsNullOrEmpty(info.ETag) ? info.ETag : "desativado");
            }
            else
            {
                tempFile = Path.Combine(_tempPath, $"{_tempPrefix}-{batchId:N}.parquet");
                var bytes = await DownloadToFileAsync(bucket, key, tempFile, ct);
                objectSize = bytes;
                _metrics.BytesDownloaded.Inc(bytes);
                arquivoLocal = new FileStream(
                    tempFile, FileMode.Open, FileAccess.Read, FileShare.Read, StreamBufferSize, useAsync: true);
                origem = arquivoLocal;

                _logger.LogInformation(
                    "Origem {Mode} para {Source}: {Mb:F1} MB em disco, flush de {Batch} linhas",
                    _readMode, sourceFile, bytes / 1024.0 / 1024.0, _flushBatchSize);
            }

            var result = await ProcessRowGroupsAsync(origem, sourceFile, batchId, target, ct);

            if (streamS3 is not null)
            {
                _metrics.BytesDownloaded.Inc(streamS3.TotalBytesFetched);
                _metrics.RangeRequests.Inc(streamS3.Requests);

                _logger.LogInformation(
                    "Origem {Mode}: {Mb:F1} MB transferidos em {Req} requisicoes ({Pct}) do objeto",
                    _readMode, streamS3.TotalBytesFetched / 1024.0 / 1024.0, streamS3.Requests,
                    objectSize > 0 ? $"{streamS3.TotalBytesFetched * 100.0 / objectSize:F1}%" : "n/d");
            }

            _metrics.FilesProcessed.Inc();
            _metrics.LastFileBytes.Set(objectSize);
            _metrics.LastFileRows.Set(result.TotalRows);

            _logger.LogInformation(
                "Result: +{Ins} ins ~{Upd} upd -{Err} err / {Total} total [{Source}]",
                result.Inserted, result.Updated, result.Invalid, result.TotalRows, sourceFile);

            return result;
        }
        finally
        {
            if (arquivoLocal is not null) await arquivoLocal.DisposeAsync();
            if (streamS3 is not null) await streamS3.DisposeAsync();
            if (tempFile is not null) TryDeleteTempFile(tempFile);
        }
    }

    /// <summary>
    /// Streaming do S3 para disco (modo LocalFile). O arquivo nunca vai inteiro para a memoria:
    /// era exatamente aqui que o pod estourava (MemoryStream com o objeto completo).
    /// </summary>
    private async Task<long> DownloadToFileAsync(string bucket, string key, string tempFile, CancellationToken ct)
    {
        using var response = await _s3.GetObjectAsync(bucket, key, ct);
        await using var file = new FileStream(
            tempFile, FileMode.Create, FileAccess.Write, FileShare.None, StreamBufferSize, useAsync: true);

        await response.ResponseStream.CopyToAsync(file, StreamBufferSize, ct);
        return file.Length;
    }

    private async Task<ProcessResult> ProcessRowGroupsAsync(
        Stream origem, string sourceFile, Guid batchId, string target, CancellationToken ct)
    {
        using var reader = await ParquetReader.CreateAsync(origem, cancellationToken: ct);

        // Projecao: so os campos que vao para custody_position sao lidos do arquivo.
        var dataFields = reader.Schema.GetDataFields();
        var accountIdField = dataFields.First(f => f.Name == "account_id");
        var assetIdField = dataFields.First(f => f.Name == "asset_id");
        var refDateField = dataFields.First(f => f.Name == "reference_date");
        var quantityField = dataFields.First(f => f.Name == "quantity");
        var amountField = dataFields.First(f => f.Name == "amount");

        var rowGroupCount = reader.RowGroupCount;
        int totalRows = 0, inserted = 0, updated = 0, invalid = 0, validOrdinal = 0;

        var validRows = new List<(string accountId, string assetId, DateTime refDate, decimal quantity, decimal amount)>(_flushBatchSize);
        var invalidRows = new List<(Guid batchId, string sourceFile, int rowNumber, string payload, string errorReason)>(64);

        async Task FlushAsync()
        {
            if (validRows.Count == 0) return;

            var batchCount = validRows.Count;
            var started = Stopwatch.GetTimestamp();

            if (target == "direct")
            {
                var (ins, upd) = await _db.BulkInsertDirectAsync(validRows, ct);
                inserted += ins;
                updated += upd;
                _metrics.RecordsInserted.Inc(ins);
                _metrics.RecordsUpdated.Inc(upd);
            }
            else
            {
                var stagingRows = new List<(Guid batchId, string sourceFile, int rowNumber, string recordHash,
                    string accountId, string assetId, DateTime refDate, decimal quantity, decimal amount)>(batchCount);

                for (var i = 0; i < batchCount; i++)
                {
                    var row = validRows[i];
                    stagingRows.Add((batchId, sourceFile, validOrdinal + i + 1,
                        ComputeRecordHash(row.accountId, row.assetId, row.refDate, row.quantity, row.amount),
                        row.accountId, row.assetId, row.refDate, row.quantity, row.amount));
                }

                await _db.BulkInsertStagingAsync(stagingRows, ct);
            }

            _metrics.DbUpsertSeconds.Observe(Stopwatch.GetElapsedTime(started).TotalSeconds);
            _metrics.DbBatchSize.Observe(batchCount);

            validOrdinal += batchCount;
            validRows.Clear();
        }

        for (var rg = 0; rg < rowGroupCount; rg++)
        {
            ct.ThrowIfCancellationRequested();

            var rowGroupStarted = Stopwatch.GetTimestamp();
            int rowCount;

            // As colunas vivem dentro do escopo do row group: ao sair do bloco elas
            // ficam elegiveis ao GC antes de carregar o proximo row group.
            using (var rgReader = reader.OpenRowGroupReader(rg))
            {
                var accountIdCol = await rgReader.ReadColumnAsync(accountIdField, ct);
                var assetIdCol = await rgReader.ReadColumnAsync(assetIdField, ct);
                var refDateCol = await rgReader.ReadColumnAsync(refDateField, ct);
                var quantityCol = await rgReader.ReadColumnAsync(quantityField, ct);
                var amountCol = await rgReader.ReadColumnAsync(amountField, ct);

                var accountIds = (string[])accountIdCol.Data;
                var assetIds = (string[])assetIdCol.Data;
                var refDates = refDateCol.Data switch
                {
                    DateTime?[] nullable => nullable.Select(d => d ?? DateTime.MinValue).ToArray(),
                    DateTime[] arr => arr,
                    _ => Array.Empty<DateTime>()
                };
                var quantities = ConvertToDecimalArray(quantityCol.Data);
                var amounts = ConvertToDecimalArray(amountCol.Data);

                rowCount = accountIds.Length;

                for (var j = 0; j < rowCount; j++)
                {
                    totalRows++;
                    var accountId = accountIds[j] ?? "";
                    var assetId = assetIds[j] ?? "";
                    var refDate = refDates[j];
                    var quantity = quantities[j];
                    var amount = amounts[j];

                    if (ValidateRow(accountId, assetId, refDate, quantity, amount, out var errors))
                    {
                        validRows.Add((accountId, assetId, refDate, quantity, amount));
                        if (validRows.Count >= _flushBatchSize)
                            await FlushAsync();
                    }
                    else
                    {
                        var payload = JsonSerializer.Serialize(new
                        {
                            account_id = accountId,
                            asset_id = assetId,
                            reference_date = refDate.ToString("yyyy-MM-dd"),
                            quantity,
                            amount
                        });

                        invalidRows.Add((batchId, sourceFile, totalRows, payload, string.Join("; ", errors)));
                        if (invalidRows.Count >= _flushBatchSize)
                        {
                            var flushed = await _db.InsertErrorsAsync(invalidRows, ct);
                            invalid += flushed;
                            _metrics.InvalidRecords.Inc(flushed);
                            invalidRows.Clear();
                        }
                    }
                }
            }

            await FlushAsync(); // fecha o ultimo lote parcial do row group

            if (invalidRows.Count > 0)
            {
                var flushed = await _db.InsertErrorsAsync(invalidRows, ct);
                invalid += flushed;
                _metrics.InvalidRecords.Inc(flushed);
                invalidRows.Clear();
            }

            _metrics.RowGroupsRead.Inc();
            _metrics.RowsProcessed.Inc(rowCount);
            _metrics.RowGroupReadSeconds.Observe(Stopwatch.GetElapsedTime(rowGroupStarted).TotalSeconds);

            _logger.LogInformation(
                "[rg {Rg}/{Groups}] rows={Rows:N0} | +{Ins} ins ~{Upd} upd -{Err} err | managed={ManagedMb:F0}MB workingSet={WsMb:F0}MB",
                rg + 1, rowGroupCount, rowCount, inserted, updated, invalid,
                GC.GetTotalMemory(false) / 1024.0 / 1024.0, Environment.WorkingSet / 1024.0 / 1024.0);
        }

        return new ProcessResult(inserted, updated, invalid, totalRows, sourceFile);
    }

    private static bool ValidateRow(string accountId, string assetId, DateTime refDate,
        decimal quantity, decimal amount, out List<string> errors)
    {
        errors = [];
        if (string.IsNullOrWhiteSpace(accountId)) errors.Add("account_id is empty");
        if (string.IsNullOrWhiteSpace(assetId)) errors.Add("asset_id is empty");
        if (refDate == default) errors.Add("reference_date is null");
        if (quantity < 0) errors.Add($"quantity is invalid: {quantity}");
        if (amount < 0) errors.Add($"amount is invalid: {amount}");
        return errors.Count == 0;
    }

    private static string ComputeRecordHash(string accountId, string assetId, DateTime refDate,
        decimal quantity, decimal amount)
    {
        var raw = $"{accountId}|{assetId}|{refDate:yyyy-MM-dd}|{quantity}|{amount}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(raw));
        return Convert.ToHexStringLower(hash);
    }

    private static decimal[] ConvertToDecimalArray(Array data)
    {
        var result = new decimal[data.Length];
        for (var i = 0; i < data.Length; i++)
        {
            var value = data.GetValue(i);
            result[i] = value switch
            {
                double d => (decimal)d,
                float f => (decimal)f,
                decimal m => m,
                int n => n,
                long l => l,
                _ => 0m
            };
        }
        return result;
    }

    private void CleanupStaleTempFiles(TimeSpan retention)
    {
        try
        {
            var cutoff = DateTime.UtcNow - retention;
            var removed = 0;

            foreach (var file in Directory.EnumerateFiles(_tempPath, $"{_tempPrefix}-*.parquet"))
            {
                if (File.GetLastWriteTimeUtc(file) >= cutoff) continue;

                try
                {
                    File.Delete(file);
                    removed++;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Nao foi possivel remover o temporario orfao {File}", file);
                }
            }

            if (removed > 0)
            {
                _logger.LogWarning(
                    "Removidos {N} arquivo(s) temporario(s) orfao(s), mais antigos que {H}h, de {Path}",
                    removed, retention.TotalHours, _tempPath);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Falha ao limpar temporarios antigos em {Path}", _tempPath);
        }
    }

    private static string Sanitize(string value)
    {
        var clean = new string(value.Where(c => char.IsLetterOrDigit(c) || c is '-' or '_').ToArray());
        return clean.Length > 0 ? clean : "worker";
    }

    private void TryDeleteTempFile(string tempFile)
    {
        try
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Nao foi possivel remover o arquivo temporario {File}", tempFile);
        }
    }
}
