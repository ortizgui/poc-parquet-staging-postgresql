using Prometheus;

namespace PocWorker.Services;

/// <summary>
/// Metricas da ingestao, expostas em /metrics pelo <see cref="MetricsServerService"/>.
///
/// Mapeamento nos 4 golden signals:
///   Traffic    -> poc_parquet_rows_processed_total, poc_parquet_bytes_downloaded_total
///   Latency    -> poc_parquet_rowgroup_read_seconds, poc_db_upsert_seconds
///   Errors     -> poc_ingest_invalid_records_total, poc_sqs_message_failures_total,
///                 poc_sqs_messages_sent_to_dlq_total, poc_sqs_dlq_depth
///   Saturation -> metricas de processo/GC do proprio prometheus-net
///                 + container_memory_working_set_bytes do cadvisor (nivel do pod)
/// </summary>
public class IngestMetrics
{
    public Counter FilesProcessed { get; } = Metrics.CreateCounter(
        "poc_ingest_files_total",
        "Arquivos Parquet processados por completo");

    public Counter RowsProcessed { get; } = Metrics.CreateCounter(
        "poc_parquet_rows_processed_total",
        "Linhas lidas do Parquet");

    public Counter RowGroupsRead { get; } = Metrics.CreateCounter(
        "poc_parquet_row_groups_total",
        "Row groups lidos (unidade de paginacao do Parquet)");

    public Counter BytesDownloaded { get; } = Metrics.CreateCounter(
        "poc_parquet_bytes_downloaded_total",
        "Bytes baixados do S3");

    public Counter RecordsInserted { get; } = Metrics.CreateCounter(
        "poc_db_records_inserted_total",
        "Registros inseridos em custody_position");

    public Counter RecordsUpdated { get; } = Metrics.CreateCounter(
        "poc_db_records_updated_total",
        "Registros atualizados em custody_position");

    public Counter InvalidRecords { get; } = Metrics.CreateCounter(
        "poc_ingest_invalid_records_total",
        "Registros invalidos gravados em custody_position_error");

    public Counter MessageFailures { get; } = Metrics.CreateCounter(
        "poc_sqs_message_failures_total",
        "Falhas de processamento de mensagem");

    public Counter MessageRetries { get; } = Metrics.CreateCounter(
        "poc_sqs_message_retries_total",
        "Mensagens devolvidas para a fila para nova tentativa");

    public Counter MessagesSentToDlq { get; } = Metrics.CreateCounter(
        "poc_sqs_messages_sent_to_dlq_total",
        "Mensagens movidas para a DLQ apos esgotar as tentativas");

    public Gauge QueueDepth { get; } = Metrics.CreateGauge(
        "poc_sqs_queue_depth",
        "Mensagens na fila principal (visiveis + em voo)");

    public Gauge DlqDepth { get; } = Metrics.CreateGauge(
        "poc_sqs_dlq_depth",
        "Mensagens na DLQ");

    public Histogram RowGroupReadSeconds { get; } = Metrics.CreateHistogram(
        "poc_parquet_rowgroup_read_seconds",
        "Tempo de leitura + descompressao + flush de um row group",
        new HistogramConfiguration { Buckets = Histogram.ExponentialBuckets(0.01, 2, 12) });

    public Histogram DbUpsertSeconds { get; } = Metrics.CreateHistogram(
        "poc_db_upsert_seconds",
        "Tempo do upsert de um lote",
        new HistogramConfiguration { Buckets = Histogram.ExponentialBuckets(0.005, 2, 12) });

    public Histogram DbBatchSize { get; } = Metrics.CreateHistogram(
        "poc_db_upsert_batch_size",
        "Linhas por lote de upsert",
        new HistogramConfiguration { Buckets = Histogram.ExponentialBuckets(100, 2, 8) });

    public Gauge LastFileBytes { get; } = Metrics.CreateGauge(
        "poc_ingest_last_file_bytes",
        "Tamanho do ultimo arquivo processado");

    public Gauge LastFileRows { get; } = Metrics.CreateGauge(
        "poc_ingest_last_file_rows",
        "Linhas do ultimo arquivo processado");
}
