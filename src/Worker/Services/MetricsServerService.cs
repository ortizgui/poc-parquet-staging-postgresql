using Prometheus;

namespace PocWorker.Services;

/// <summary>
/// Expoe /metrics para o Prometheus fazer scrape.
/// Prometheus scrapes a porta do container: bind em todas as interfaces.
/// </summary>
public class MetricsServerService : BackgroundService
{
    private readonly ILogger<MetricsServerService> _logger;
    private readonly int _port;

    public MetricsServerService(IConfiguration config, ILogger<MetricsServerService> logger)
    {
        _logger = logger;
        _port = config.GetValue("Metrics:Port", 9464);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var server = new MetricServer(hostname: "*", port: _port);
        server.Start();

        _logger.LogInformation("Metricas expostas em http://0.0.0.0:{Port}/metrics", _port);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // shutdown normal
        }
        finally
        {
            await server.StopAsync();
        }
    }
}
