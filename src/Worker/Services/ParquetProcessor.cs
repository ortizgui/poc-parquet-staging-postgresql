using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using Parquet;
using Parquet.Data;
using PocWorker.Models;

namespace PocWorker.Services;

public class ParquetProcessor
{
    private readonly IAmazonS3 _s3;
    private readonly DatabaseService _db;
    private readonly ILogger<ParquetProcessor> _logger;
    private readonly int _maxWorkers;

    public ParquetProcessor(IAmazonS3 s3, DatabaseService db, IConfiguration config, ILogger<ParquetProcessor> logger)
    {
        _s3 = s3;
        _db = db;
        _logger = logger;
        _maxWorkers = config.GetValue<int>("Consumer:MaxWorkers", 4);
    }

    public async Task<ProcessResult> ProcessFileAsync(
        string bucket, string key, string target, CancellationToken ct = default)
    {
        var sourceFile = $"s3://{bucket}/{key}";
        var batchId = Guid.NewGuid();

        _logger.LogInformation("Processing: {Source}", sourceFile);

        using var memStream = new MemoryStream();
        var getResponse = await _s3.GetObjectAsync(bucket, key, ct);
        await getResponse.ResponseStream.CopyToAsync(memStream, ct);
        memStream.Position = 0;

        using var reader = await ParquetReader.CreateAsync(memStream, cancellationToken: ct);
        var dataFields = reader.Schema.GetDataFields();

        var accountIdField = dataFields.First(f => f.Name == "account_id");
        var assetIdField = dataFields.First(f => f.Name == "asset_id");
        var refDateField = dataFields.First(f => f.Name == "reference_date");
        var quantityField = dataFields.First(f => f.Name == "quantity");
        var amountField = dataFields.First(f => f.Name == "amount");

        var validRows = new List<(string accountId, string assetId, DateTime refDate, decimal quantity, decimal amount)>();
        var invalidRows = new List<(Guid batchId, string sourceFile, int rowNumber, string payload, string errorReason)>();

        int globalRowNumber = 0;

        for (int rg = 0; rg < reader.RowGroupCount; rg++)
        {
            using var rgReader = reader.OpenRowGroupReader(rg);

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

            int rowCount = accountIds.Length;

            for (int j = 0; j < rowCount; j++)
            {
                globalRowNumber++;
                var accountId = accountIds[j] ?? "";
                var assetId = assetIds[j] ?? "";
                var refDate = refDates[j];
                var quantity = quantities[j];
                var amount = amounts[j];

                if (ValidateRow(accountId, assetId, refDate, quantity, amount, out var errors))
                {
                    validRows.Add((accountId, assetId, refDate, quantity, amount));
                }
                else
                {
                    var payload = JsonSerializer.Serialize(new
                    {
                        account_id = accountId,
                        asset_id = assetId,
                        reference_date = refDate.ToString("yyyy-MM-dd"),
                        quantity = quantity,
                        amount = amount
                    });

                    invalidRows.Add((batchId, sourceFile, globalRowNumber, payload, string.Join("; ", errors)));
                }
            }
        }

        int totalRows = globalRowNumber;
        int inserted = 0, updated = 0, invalid = 0;

        if (target == "direct")
        {
            (inserted, updated) = await _db.BulkInsertDirectAsync(validRows, ct);
        }
        else
        {
            var stagingRows = new List<(Guid batchId, string sourceFile, int rowNumber, string recordHash,
                string accountId, string assetId, DateTime refDate, decimal quantity, decimal amount)>();

            for (int i = 0; i < validRows.Count; i++)
            {
                var row = validRows[i];
                var recordHash = ComputeRecordHash(row.accountId, row.assetId, row.refDate, row.quantity, row.amount);
                stagingRows.Add((batchId, sourceFile, i + 1, recordHash,
                    row.accountId, row.assetId, row.refDate, row.quantity, row.amount));
            }

            await _db.BulkInsertStagingAsync(stagingRows, ct);
        }

        if (invalidRows.Count > 0)
        {
            invalid = await _db.InsertErrorsAsync(invalidRows, ct);
        }

        _logger.LogInformation(
            "Result: +{Ins} ins ~{Upd} upd -{Err} err / {Total} total [{Source}]",
            inserted, updated, invalid, totalRows, sourceFile);

        return new ProcessResult(inserted, updated, invalid, totalRows, sourceFile);
    }

    private bool ValidateRow(string accountId, string assetId, DateTime refDate,
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
        for (int i = 0; i < data.Length; i++)
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
}
