using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using PocWorker.Models;

namespace PocWorker.Services;

/// <summary>
/// Consome a fila de notificacao (1 mensagem = 1 arquivo Parquet) e delega ao
/// <see cref="ParquetProcessor"/>.
///
/// Resiliencia:
///
/// 1. HEARTBEAT DE VISIBILITY.
///    O visibility timeout do SQS conta a partir da ENTREGA, nao do fim do processamento.
///    Ingerir um arquivo grande leva minutos; com um timeout curto a mensagem volta para a
///    fila NO MEIO da ingestao — outro consumer (ou o mesmo) baixa o mesmo arquivo de novo e
///    o ApproximateReceiveCount sobe sem que nada esteja errado. Resultado: trabalho duplicado
///    e redrive indevido para a DLQ. Enquanto processa, este servico estende a visibilidade a
///    cada Consumer:VisibilityHeartbeatSeconds.
///
/// 2. REDRIVE EXPLICITO.
///    Ao falhar Consumer:MaxReceiveCount vezes, o consumer envia a mensagem para a DLQ e a
///    remove da fila principal. O RedrivePolicy do SQS (configurado em scripts/setup_infra.py)
///    continua como segunda linha de defesa, mas aqui a decisao fica visivel: fica logada,
///    contada em metrica e carrega o motivo da falha na propria mensagem. Tambem funciona em
///    emulador, onde o redrive nativo pode nao ser implementado.
///
/// 3. METRICAS.
///    Profundidade da fila e da DLQ + contadores de falha e de redrive (golden signal Errors).
/// </summary>
public class SqsConsumerService : BackgroundService
{
    private readonly IAmazonSQS _sqs;
    private readonly ParquetProcessor _processor;
    private readonly IngestMetrics _metrics;
    private readonly ILogger<SqsConsumerService> _logger;
    private readonly IHostApplicationLifetime _appLifetime;

    private readonly string _queueName;
    private readonly string _dlqName;
    private readonly string _target;
    private readonly string _consumerId;
    private readonly int _maxMessages;
    private readonly int _pollWaitSeconds;
    private readonly int _visibilityTimeoutSeconds;
    private readonly int _visibilityHeartbeatSeconds;
    private readonly int _maxReceiveCount;

    private string? _queueUrl;
    private string? _dlqUrl;
    private int _messagesProcessed;
    private int _filesProcessed;
    private int _totalInserted;
    private int _totalUpdated;
    private DateTime _startTime;

    public SqsConsumerService(
        IAmazonSQS sqs,
        ParquetProcessor processor,
        IngestMetrics metrics,
        IConfiguration config,
        ILogger<SqsConsumerService> logger,
        IHostApplicationLifetime appLifetime)
    {
        _sqs = sqs;
        _processor = processor;
        _metrics = metrics;
        _logger = logger;
        _appLifetime = appLifetime;

        var consumer = config.GetSection("Consumer");
        _queueName = consumer["QueueName"] ?? "poc-notification-queue";
        _dlqName = consumer["DlqName"] ?? "poc-notification-dlq";
        _target = consumer["Target"] ?? "direct";
        _consumerId = consumer["ConsumerId"] ?? Environment.MachineName;
        _maxMessages = consumer.GetValue("MaxMessages", 0);
        _pollWaitSeconds = consumer.GetValue("PollWaitSeconds", 5);
        _visibilityTimeoutSeconds = Math.Max(10, consumer.GetValue("VisibilityTimeoutSeconds", 300));
        _visibilityHeartbeatSeconds = Math.Max(5, consumer.GetValue("VisibilityHeartbeatSeconds", 60));
        _maxReceiveCount = Math.Max(1, consumer.GetValue("MaxReceiveCount", 3));
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        var response = await _sqs.GetQueueUrlAsync(_queueName, cancellationToken);
        _queueUrl = response.QueueUrl;

        try
        {
            _dlqUrl = (await _sqs.GetQueueUrlAsync(_dlqName, cancellationToken)).QueueUrl;
        }
        catch (QueueDoesNotExistException)
        {
            _logger.LogWarning(
                "[CONSUMER:{Id}] DLQ '{Dlq}' nao existe — mensagens que falharem vao continuar voltando para a fila. Rode scripts/setup_infra.py",
                _consumerId, _dlqName);
        }

        _logger.LogInformation(
            "[CONSUMER:{Id}] Starting. Queue={Queue}, DLQ={Dlq}, Target={Target}, visibility={Vis}s (heartbeat {Hb}s), maxReceiveCount={Max}",
            _consumerId, _queueUrl, _dlqUrl ?? "-", _target,
            _visibilityTimeoutSeconds, _visibilityHeartbeatSeconds, _maxReceiveCount);

        _startTime = DateTime.UtcNow;
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var receiveResponse = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = _queueUrl,
                    MaxNumberOfMessages = 1,
                    WaitTimeSeconds = _pollWaitSeconds,
                    VisibilityTimeout = _visibilityTimeoutSeconds,
                    // necessario para decidir o redrive e para o log de tentativa
                    MessageSystemAttributeNames = ["ApproximateReceiveCount"]
                }, stoppingToken);

                if (receiveResponse.Messages.Count == 0)
                {
                    if (_messagesProcessed > 0 && _maxMessages > 0)
                    {
                        _logger.LogInformation("[CONSUMER:{Id}] Queue empty after {N} messages. Done.",
                            _consumerId, _messagesProcessed);
                        break;
                    }
                    continue;
                }

                foreach (var message in receiveResponse.Messages)
                {
                    await ProcessMessageAsync(message, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[CONSUMER:{Id}] Error in main loop", _consumerId);
                await Task.Delay(1000, stoppingToken);
            }
        }

        var elapsed = DateTime.UtcNow - _startTime;
        _logger.LogInformation(
            "[CONSUMER:{Id}] === SUMMARY === Files={Files}, Msgs={Msgs}, +{Ins}ins ~{Upd}upd, Elapsed={Elapsed}s",
            _consumerId, _filesProcessed, _messagesProcessed,
            _totalInserted, _totalUpdated, elapsed.TotalSeconds);
    }

    private async Task ProcessMessageAsync(Message message, CancellationToken ct)
    {
        var receiveCount = GetReceiveCount(message);

        // Enquanto a mensagem esta sendo processada, a visibilidade e estendida: e o que
        // impede a redelivery no meio da ingestao de um arquivo grande.
        using var heartbeat = StartVisibilityHeartbeat(message, ct);

        try
        {
            var body = JsonSerializer.Deserialize<S3EventNotification>(message.Body,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            if (body?.Records == null || body.Records.Count == 0)
            {
                _logger.LogWarning("[CONSUMER:{Id}] No records in message. Deleting malformed message.",
                    _consumerId);
                await _sqs.DeleteMessageAsync(_queueUrl, message.ReceiptHandle, ct);
                return;
            }

            foreach (var record in body.Records)
            {
                var bucket = record.S3?.Bucket?.Name;
                var key = record.S3?.Object?.Key;

                if (string.IsNullOrEmpty(bucket) || string.IsNullOrEmpty(key))
                {
                    _logger.LogWarning("[CONSUMER:{Id}] Invalid event: missing bucket/key", _consumerId);
                    continue;
                }

                if (!key.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
                {
                    _logger.LogInformation("[CONSUMER:{Id}] Skip non-parquet: {Key}", _consumerId, key);
                    continue;
                }

                _logger.LogInformation("[CONSUMER:{Id}] Processing: s3://{Bucket}/{Key} (tentativa {N}/{Max})",
                    _consumerId, bucket, key, receiveCount, _maxReceiveCount);

                var result = await _processor.ProcessFileAsync(bucket, key, _target, ct);

                Interlocked.Increment(ref _filesProcessed);
                Interlocked.Add(ref _totalInserted, result.Inserted);
                Interlocked.Add(ref _totalUpdated, result.Updated);
            }

            await _sqs.DeleteMessageAsync(_queueUrl, message.ReceiptHandle, ct);
            var count = Interlocked.Increment(ref _messagesProcessed);

            var depth = await GetQueueDepthAsync(_queueUrl, ct);
            if (depth >= 0) _metrics.QueueDepth.Set(depth);
            if (_dlqUrl is not null)
            {
                var dlqDepth = await GetQueueDepthAsync(_dlqUrl, ct);
                if (dlqDepth >= 0) _metrics.DlqDepth.Set(dlqDepth);
            }

            _logger.LogInformation(
                "[CONSUMER:{Id}] [OK] Msg #{MsgN}, {Files} file(s) processed, queue depth: {Depth}",
                _consumerId, count, body.Records.Count, depth);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "[CONSUMER:{Id}] Failed to deserialize message body. Deleting.", _consumerId);
            await _sqs.DeleteMessageAsync(_queueUrl, message.ReceiptHandle, ct);
        }
        catch (Exception ex)
        {
            _metrics.MessageFailures.Inc();

            if (receiveCount >= _maxReceiveCount)
            {
                await SendToDlqAsync(message, receiveCount, ex, ct);
            }
            else
            {
                _logger.LogError(ex,
                    "[CONSUMER:{Id}] Falha ao processar (tentativa {N}/{Max}). A mensagem volta apos o visibility timeout.",
                    _consumerId, receiveCount, _maxReceiveCount);
                _metrics.MessageRetries.Inc();
            }
        }
    }

    /// <summary>
    /// Estende a visibility timeout enquanto o processamento acontece. Sem isso, qualquer
    /// ingestao mais longa que Consumer:VisibilityTimeoutSeconds causa redelivery.
    /// </summary>
    private IDisposable StartVisibilityHeartbeat(Message message, CancellationToken ct)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var task = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(_visibilityHeartbeatSeconds), cts.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                try
                {
                    await _sqs.ChangeMessageVisibilityAsync(
                        _queueUrl, message.ReceiptHandle, _visibilityTimeoutSeconds, cts.Token);
                    _logger.LogDebug("[CONSUMER:{Id}] visibility renovada por {S}s", _consumerId, _visibilityTimeoutSeconds);
                }
                catch (Exception ex) when (!cts.IsCancellationRequested)
                {
                    _logger.LogWarning(ex, "[CONSUMER:{Id}] Nao foi possivel renovar a visibility", _consumerId);
                }
            }
        }, cts.Token);

        return new HeartbeatHandle(cts, task);
    }

    /// <summary>
    /// Move a mensagem para a DLQ e a remove da fila principal. O corpo e preservado e o
    /// motivo da falha viaja junto, para triagem.
    /// </summary>
    private async Task SendToDlqAsync(Message message, int receiveCount, Exception ex, CancellationToken ct)
    {
        if (_dlqUrl is null)
        {
            _logger.LogError(ex,
                "[CONSUMER:{Id}] Mensagem falhou {N} vezes e NAO ha DLQ configurada — ela vai continuar voltando para a fila.",
                _consumerId, receiveCount);
            return;
        }

        await _sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = _dlqUrl,
            MessageBody = message.Body,
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                ["FailureReason"] = new() { DataType = "String", StringValue = Truncate(ex.Message, 1024) },
                ["ExceptionType"] = new() { DataType = "String", StringValue = ex.GetType().Name },
                ["SourceQueue"] = new() { DataType = "String", StringValue = _queueName },
                ["ConsumerId"] = new() { DataType = "String", StringValue = _consumerId },
                ["ReceiveCount"] = new() { DataType = "Number", StringValue = receiveCount.ToString() }
            }
        }, ct);

        await _sqs.DeleteMessageAsync(_queueUrl, message.ReceiptHandle, ct);

        _metrics.MessagesSentToDlq.Inc();
        _logger.LogError(
            "[CONSUMER:{Id}] Mensagem movida para a DLQ '{Dlq}' apos {N} tentativas. Motivo: {Reason}",
            _consumerId, _dlqName, receiveCount, ex.Message);
    }

    private static int GetReceiveCount(Message message)
    {
        if (message.Attributes is not null
            && message.Attributes.TryGetValue("ApproximateReceiveCount", out var raw)
            && int.TryParse(raw, out var count))
        {
            return count;
        }
        return 1;
    }

    private async Task<int> GetQueueDepthAsync(string? queueUrl, CancellationToken ct)
    {
        if (queueUrl is null) return -1;

        try
        {
            var attrs = await _sqs.GetQueueAttributesAsync(queueUrl,
                ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"], ct);
            return attrs.ApproximateNumberOfMessages + attrs.ApproximateNumberOfMessagesNotVisible;
        }
        catch
        {
            return -1;
        }
    }

    private static string Truncate(string value, int max) =>
        value.Length <= max ? value : value[..max];

    private sealed class HeartbeatHandle(CancellationTokenSource cts, Task task) : IDisposable
    {
        public void Dispose()
        {
            try
            {
                cts.Cancel();
                task.Wait(TimeSpan.FromSeconds(3));
            }
            catch (AggregateException)
            {
                // cancelamento normal
            }
            catch (ObjectDisposedException)
            {
                // ja descartado
            }
            finally
            {
                cts.Dispose();
            }
        }
    }
}
