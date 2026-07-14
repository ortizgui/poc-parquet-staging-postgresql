using System.Data;
using System.Text;
using Npgsql;

namespace PocWorker.Services;

public class DatabaseService : IDisposable
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly int _batchSize = 5000;

    public DatabaseService(IConfiguration config)
    {
        var pg = config.GetSection("PostgreSQL");
        var host = pg["Host"] ?? "localhost";
        var port = pg["Port"] ?? "5432";
        var db = pg["Database"] ?? "pocdb";
        var user = pg["Username"] ?? "pocuser";
        var pass = pg["Password"] ?? "pocpass";
        var connectionString = $"Host={host};Port={port};Database={db};Username={user};Password={pass}";
        _dataSource = NpgsqlDataSource.Create(connectionString);
    }

    public async Task<(int inserted, int updated)> BulkInsertDirectAsync(
        List<(string accountId, string assetId, DateTime refDate, decimal quantity, decimal amount)> records,
        CancellationToken ct = default)
    {
        if (records.Count == 0)
            return (0, 0);

        await using var conn = await _dataSource.OpenConnectionAsync(ct);

        int totalInserted = 0;
        int totalUpdated = 0;

        for (int batchStart = 0; batchStart < records.Count; batchStart += _batchSize)
        {
            var batch = records.GetRange(batchStart, Math.Min(_batchSize, records.Count - batchStart));
            var (inserted, updated) = await InsertDirectBatchAsync(conn, batch, ct);
            totalInserted += inserted;
            totalUpdated += updated;
        }

        return (totalInserted, totalUpdated);
    }

    private static async Task<(int inserted, int updated)> InsertDirectBatchAsync(
        NpgsqlConnection conn,
        List<(string accountId, string assetId, DateTime refDate, decimal quantity, decimal amount)> batch,
        CancellationToken ct)
    {
        var insertSb = new StringBuilder();
        insertSb.Append("INSERT INTO custody_position (account_id, asset_id, reference_date, quantity, amount) VALUES ");
        var parameters = new List<NpgsqlParameter>();

        for (int i = 0; i < batch.Count; i++)
        {
            if (i > 0) insertSb.Append(", ");
            var idx = i * 5;
            insertSb.Append($"(${idx + 1}, ${idx + 2}, ${idx + 3}, ${idx + 4}, ${idx + 5})");
            parameters.Add(new NpgsqlParameter<string> { TypedValue = batch[i].accountId });
            parameters.Add(new NpgsqlParameter<string> { TypedValue = batch[i].assetId });
            parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = batch[i].refDate });
            parameters.Add(new NpgsqlParameter<decimal> { TypedValue = batch[i].quantity });
            parameters.Add(new NpgsqlParameter<decimal> { TypedValue = batch[i].amount });
        }

        insertSb.Append(" ON CONFLICT (account_id, asset_id, reference_date) DO NOTHING");

        await using var insertCmd = new NpgsqlCommand(insertSb.ToString(), conn);
        insertCmd.Parameters.AddRange(parameters.ToArray());
        int inserted = await insertCmd.ExecuteNonQueryAsync(ct);

        // UPDATE only rows that already exist and have different values
        var updateSb = new StringBuilder();
        updateSb.Append("UPDATE custody_position f SET quantity = v.quantity, amount = v.amount, updated_at = NOW() FROM (VALUES ");
        var updParams = new List<NpgsqlParameter>();

        for (int i = 0; i < batch.Count; i++)
        {
            if (i > 0) updateSb.Append(", ");
            var idx = i * 5;
            updateSb.Append($"(${idx + 1}::varchar, ${idx + 2}::varchar, ${idx + 3}::date, ${idx + 4}::numeric, ${idx + 5}::numeric)");
            updParams.Add(new NpgsqlParameter<string> { TypedValue = batch[i].accountId });
            updParams.Add(new NpgsqlParameter<string> { TypedValue = batch[i].assetId });
            updParams.Add(new NpgsqlParameter<DateTime> { TypedValue = batch[i].refDate });
            updParams.Add(new NpgsqlParameter<decimal> { TypedValue = batch[i].quantity });
            updParams.Add(new NpgsqlParameter<decimal> { TypedValue = batch[i].amount });
        }

        updateSb.Append(") AS v(account_id, asset_id, reference_date, quantity, amount) ");
        updateSb.Append("WHERE f.account_id = v.account_id AND f.asset_id = v.asset_id AND f.reference_date = v.reference_date ");
        updateSb.Append("AND (f.quantity IS DISTINCT FROM v.quantity OR f.amount IS DISTINCT FROM v.amount)");

        await using var updateCmd = new NpgsqlCommand(updateSb.ToString(), conn);
        updateCmd.Parameters.AddRange(updParams.ToArray());
        int updated = await updateCmd.ExecuteNonQueryAsync(ct);

        return (inserted, updated);
    }

    public async Task<int> BulkInsertStagingAsync(
        List<(Guid batchId, string sourceFile, int rowNumber, string recordHash,
              string accountId, string assetId, DateTime refDate, decimal quantity, decimal amount)> records,
        CancellationToken ct = default)
    {
        if (records.Count == 0)
            return 0;

        await using var conn = await _dataSource.OpenConnectionAsync(ct);

        var sb = new StringBuilder();
        sb.Append("INSERT INTO custody_position_staging ");
        sb.Append("(batch_id, source_file, row_number, record_hash, account_id, asset_id, reference_date, quantity, amount) ");
        sb.Append("VALUES ");
        var parameters = new List<NpgsqlParameter>();

        for (int i = 0; i < records.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            var idx = i * 9;
            sb.Append($"(${idx + 1}, ${idx + 2}, ${idx + 3}, ${idx + 4}, ${idx + 5}, ${idx + 6}, ${idx + 7}, ${idx + 8}, ${idx + 9})");
            parameters.Add(new NpgsqlParameter<Guid> { TypedValue = records[i].batchId });
            parameters.Add(new NpgsqlParameter<string> { TypedValue = records[i].sourceFile });
            parameters.Add(new NpgsqlParameter<int> { TypedValue = records[i].rowNumber });
            parameters.Add(new NpgsqlParameter<string> { TypedValue = records[i].recordHash });
            parameters.Add(new NpgsqlParameter<string> { TypedValue = records[i].accountId });
            parameters.Add(new NpgsqlParameter<string> { TypedValue = records[i].assetId });
            parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = records[i].refDate });
            parameters.Add(new NpgsqlParameter<decimal> { TypedValue = records[i].quantity });
            parameters.Add(new NpgsqlParameter<decimal> { TypedValue = records[i].amount });
        }

        sb.Append(" ON CONFLICT (source_file, row_number) DO NOTHING");

        await using var cmd = new NpgsqlCommand(sb.ToString(), conn);
        cmd.Parameters.AddRange(parameters.ToArray());
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<int> InsertErrorsAsync(
        List<(Guid batchId, string sourceFile, int rowNumber, string payload, string errorReason)> errors,
        CancellationToken ct = default)
    {
        if (errors.Count == 0)
            return 0;

        await using var conn = await _dataSource.OpenConnectionAsync(ct);

        var sb = new StringBuilder();
        sb.Append("INSERT INTO custody_position_error ");
        sb.Append("(batch_id, source_file, row_number, payload, error_reason) ");
        sb.Append("VALUES ");
        var parameters = new List<NpgsqlParameter>();

        for (int i = 0; i < errors.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            var idx = i * 5;
            sb.Append($"(${idx + 1}, ${idx + 2}, ${idx + 3}, ${idx + 4}::jsonb, ${idx + 5})");
            parameters.Add(new NpgsqlParameter<Guid> { TypedValue = errors[i].batchId });
            parameters.Add(new NpgsqlParameter<string> { TypedValue = errors[i].sourceFile });
            parameters.Add(new NpgsqlParameter<int> { TypedValue = errors[i].rowNumber });
            parameters.Add(new NpgsqlParameter<string> { TypedValue = errors[i].payload });
            parameters.Add(new NpgsqlParameter<string> { TypedValue = errors[i].errorReason });
        }

        sb.Append(" ON CONFLICT (source_file, row_number) DO NOTHING");

        await using var cmd = new NpgsqlCommand(sb.ToString(), conn);
        cmd.Parameters.AddRange(parameters.ToArray());
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public void Dispose()
    {
        _dataSource.Dispose();
    }
}
