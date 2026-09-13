using System.Text;
using Npgsql;

namespace PocWorker.Services;

/// <summary>
/// Acesso ao PostgreSQL.
///
/// O upsert e feito em UM unico statement (INSERT ... ON CONFLICT DO UPDATE ... RETURNING):
///   - uma passada de parametros em vez de duas (o caminho antigo montava INSERT e UPDATE
///     separados, dobrando os objetos NpgsqlParameter e o pico de memoria por lote);
///   - um round-trip em vez de dois;
///   - contagem exata de insert vs update pelo truque do xmax (xmax = 0 -> linha inserida).
///
/// Os lotes chegam ja fatiados pelo ParquetProcessor (Consumer:FlushBatchSize); o chunking
/// interno aqui e apenas uma trava de seguranca.
/// </summary>
public class DatabaseService : IDisposable
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly int _batchSize;

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
        _batchSize = Math.Max(1, pg.GetValue("BatchSize", 2000));
    }

    public async Task<(int inserted, int updated)> BulkInsertDirectAsync(
        List<(string accountId, string assetId, DateTime refDate, decimal quantity, decimal amount)> records,
        CancellationToken ct = default)
    {
        if (records.Count == 0)
            return (0, 0);

        await using var conn = await _dataSource.OpenConnectionAsync(ct);

        var totalInserted = 0;
        var totalUpdated = 0;

        for (var start = 0; start < records.Count; start += _batchSize)
        {
            var count = Math.Min(_batchSize, records.Count - start);
            var (inserted, updated) = await UpsertBatchAsync(conn, records, start, count, ct);
            totalInserted += inserted;
            totalUpdated += updated;
        }

        return (totalInserted, totalUpdated);
    }

    private static async Task<(int inserted, int updated)> UpsertBatchAsync(
        NpgsqlConnection conn,
        List<(string accountId, string assetId, DateTime refDate, decimal quantity, decimal amount)> records,
        int start,
        int count,
        CancellationToken ct)
    {
        var sql = new StringBuilder(count * 32);
        sql.Append("INSERT INTO custody_position (account_id, asset_id, reference_date, quantity, amount) VALUES ");

        await using var cmd = new NpgsqlCommand { Connection = conn };

        // ON CONFLICT DO UPDATE nao aceita a MESMA chave duas vezes dentro do mesmo
        // statement (o Postgres aborta com 21000 "cannot affect row a second time").
        // Duplicata de chave dentro do lote e normal neste volume de dados, entao
        // deduplicamos aqui: a primeira ocorrencia vence, de forma deterministica
        // (o caminho antigo, DO NOTHING + UPDATE, nao era determinista nesse caso).
        var seen = new HashSet<(string AccountId, string AssetId, DateTime RefDate)>(count);
        var written = 0;

        for (var i = 0; i < count; i++)
        {
            var row = records[start + i];

            if (!seen.Add((row.accountId, row.assetId, row.refDate)))
                continue;

            if (written > 0) sql.Append(", ");
            var idx = written * 5;
            sql.Append($"(${idx + 1}, ${idx + 2}, ${idx + 3}, ${idx + 4}, ${idx + 5})");

            cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.accountId });
            cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.assetId });
            cmd.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = row.refDate });
            cmd.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = row.quantity });
            cmd.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = row.amount });

            written++;
        }

        if (written == 0)
            return (0, 0);

        sql.Append(" ON CONFLICT (account_id, asset_id, reference_date) DO UPDATE SET ")
           .Append("quantity = EXCLUDED.quantity, amount = EXCLUDED.amount, updated_at = NOW() ")
           .Append("WHERE custody_position.quantity IS DISTINCT FROM EXCLUDED.quantity ")
           .Append("   OR custody_position.amount IS DISTINCT FROM EXCLUDED.amount ")
           .Append("RETURNING (xmax = 0)");

        cmd.CommandText = sql.ToString();

        var inserted = 0;
        var updated = 0;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.GetBoolean(0)) inserted++;
            else updated++;
        }

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

        var total = 0;

        for (var start = 0; start < records.Count; start += _batchSize)
        {
            var count = Math.Min(_batchSize, records.Count - start);

            var sql = new StringBuilder(count * 32);
            sql.Append("INSERT INTO custody_position_staging ");
            sql.Append("(batch_id, source_file, row_number, record_hash, account_id, asset_id, reference_date, quantity, amount) ");
            sql.Append("VALUES ");

            await using var cmd = new NpgsqlCommand { Connection = conn };

            for (var i = 0; i < count; i++)
            {
                if (i > 0) sql.Append(", ");
                var idx = i * 9;
                sql.Append($"(${idx + 1}, ${idx + 2}, ${idx + 3}, ${idx + 4}, ${idx + 5}, ${idx + 6}, ${idx + 7}, ${idx + 8}, ${idx + 9})");

                var row = records[start + i];
                cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = row.batchId });
                cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.sourceFile });
                cmd.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.rowNumber });
                cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.recordHash });
                cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.accountId });
                cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.assetId });
                cmd.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = row.refDate });
                cmd.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = row.quantity });
                cmd.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = row.amount });
            }

            sql.Append(" ON CONFLICT (source_file, row_number) DO NOTHING");
            cmd.CommandText = sql.ToString();

            total += await cmd.ExecuteNonQueryAsync(ct);
        }

        return total;
    }

    public async Task<int> InsertErrorsAsync(
        List<(Guid batchId, string sourceFile, int rowNumber, string payload, string errorReason)> errors,
        CancellationToken ct = default)
    {
        if (errors.Count == 0)
            return 0;

        await using var conn = await _dataSource.OpenConnectionAsync(ct);

        var total = 0;

        for (var start = 0; start < errors.Count; start += _batchSize)
        {
            var count = Math.Min(_batchSize, errors.Count - start);

            var sql = new StringBuilder(count * 24);
            sql.Append("INSERT INTO custody_position_error ");
            sql.Append("(batch_id, source_file, row_number, payload, error_reason) ");
            sql.Append("VALUES ");

            await using var cmd = new NpgsqlCommand { Connection = conn };

            for (var i = 0; i < count; i++)
            {
                if (i > 0) sql.Append(", ");
                var idx = i * 5;
                sql.Append($"(${idx + 1}, ${idx + 2}, ${idx + 3}, ${idx + 4}::jsonb, ${idx + 5})");

                var row = errors[start + i];
                cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = row.batchId });
                cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.sourceFile });
                cmd.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.rowNumber });
                cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.payload });
                cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.errorReason });
            }

            sql.Append(" ON CONFLICT (source_file, row_number) DO NOTHING");
            cmd.CommandText = sql.ToString();

            total += await cmd.ExecuteNonQueryAsync(ct);
        }

        return total;
    }

    public void Dispose()
    {
        _dataSource.Dispose();
    }
}
