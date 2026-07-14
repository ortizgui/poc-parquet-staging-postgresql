using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using PocWorker.Models;

namespace PocWorker.Services;

public class SqsConsumerService : BackgroundService
{
    private readonly IAmazonSQS _sqs;
    private readonly ParquetProcessor _processor;
    private readonly ILogger<SqsConsumerService> _logger;
    private readonly IHostApplicationLifetime _appLifetime;
    private readonly IConfiguration _config;

    private readonly string _queueName;
    private readonly string _target;
    private readonly string _consumerId;
    private readonly int _maxMessages;
    private readonly int _pollWaitSeconds;

    private string? _queueUrl;
    private int _messagesProcessed;
    private int _filesProcessed;
    private int _totalInserted;
    private int _totalUpdated;
    private DateTime _startTime;

    public SqsConsumerService(
        IAmazonSQS sqs,
        ParquetProcessor processor,
        IConfiguration config,
        ILogger<SqsConsumerService> logger,
        IHostApplicationLifetime appLifetime)
    {
        _sqs = sqs;
        _processor = processor;
        _logger = logger;
        _appLifetime = appLifetime;
        _config = config;

        var consumer = config.GetSection("Consumer");
        _queueName = consumer["QueueName"] ?? "poc-notification-queue";
        _target = consumer["Target"] ?? "direct";
        _consumerId = consumer["ConsumerId"] ?? Environment.MachineName;
        _maxMessages = consumer.GetValue<int>("MaxMessages", 0);
        _pollWaitSeconds = consumer.GetValue<int>("PollWaitSeconds", 5);
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        var response = await _sqs.GetQueueUrlAsync(_queueName, cancellationToken);
        _queueUrl = response.QueueUrl;

        _logger.LogInformation("[CONSUMER:{Id}] Starting. Queue={Queue}, Target={Target}",
            _consumerId, _queueUrl, _target);

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
                    VisibilityTimeout = 30
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

                _logger.LogInformation("[CONSUMER:{Id}] Processing: s3://{Bucket}/{Key}",
                    _consumerId, bucket, key);

                var result = await _processor.ProcessFileAsync(bucket, key, _target, ct);

                Interlocked.Increment(ref _filesProcessed);
                Interlocked.Add(ref _totalInserted, result.Inserted);
                Interlocked.Add(ref _totalUpdated, result.Updated);
            }

            await _sqs.DeleteMessageAsync(_queueUrl, message.ReceiptHandle, ct);
            var count = Interlocked.Increment(ref _messagesProcessed);

            var depth = await GetQueueDepthAsync(ct);
            _logger.LogInformation(
                "[CONSUMER:{Id}] [OK] Msg #{MsgN}, {Files} file(s) processed, queue depth: {Depth}",
                _consumerId, count, body.Records.Count, depth);

            if (_maxMessages > 0 && _messagesProcessed >= _maxMessages)
            {
                _logger.LogInformation("[CONSUMER:{Id}] Max messages ({Max}) reached. Stopping.",
                    _consumerId, _maxMessages);
                _appLifetime.StopApplication();
            }
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "[CONSUMER:{Id}] Failed to deserialize message body. Deleting.",
                _consumerId);
            await _sqs.DeleteMessageAsync(_queueUrl, message.ReceiptHandle, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[CONSUMER:{Id}] Failed to process message. Will retry via visibility timeout.",
                _consumerId);
        }
    }

    private async Task<int> GetQueueDepthAsync(CancellationToken ct)
    {
        try
        {
            var attrs = await _sqs.GetQueueAttributesAsync(_queueUrl,
                ["ApproximateNumberOfMessages"], ct);
            return attrs.ApproximateNumberOfMessages;
        }
        catch
        {
            return -1;
        }
    }
}
