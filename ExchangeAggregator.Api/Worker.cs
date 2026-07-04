using ExchangeAggregator.Api.Configuration;
using ExchangeAggregator.Api.Pipeline;
using ExchangeAggregator.Api.WebSocket;
using ExchangeAggregator.Core.Metrics;
using Microsoft.Extensions.Options;

namespace ExchangeAggregator.Api;
public sealed class Worker : BackgroundService
{
    private readonly AggregatorOptions _config;
    private readonly TickProcessingPipeline _pipeline;
    private readonly TickMetrics _metrics;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<Worker> _logger;

    public Worker(
        IOptions<AggregatorOptions> config,
        TickProcessingPipeline pipeline,
        TickMetrics metrics,
        IServiceProvider serviceProvider,
        ILogger<Worker> logger)
    {
        _config = config.Value;
        _pipeline = pipeline;
        _metrics = metrics;
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Агрегатор запускается. Бирж: {Count}", _config.Exchanges.Count);

        // Запускаем периодический вывод метрик
        using var metricsTimer = new Timer(_ =>
        {
            var s = _metrics.Snapshot();
            _logger.LogInformation(
                "METRICS | Rcvd:{R} Parsed:{P} Dup:{D} Wrtn:{W} Err:{E} Drop:{Dr} Reconn:{Re} Backlog:{Bk}",
                s.Received, s.Parsed, s.Duplicates, s.Written,
                s.WriteErrors, s.Dropped, s.Reconnects, s.ChannelBacklog);
        }, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));

        // Запускаем WebSocket-клиенты — каждый в своей Task, независимо
        var clientTasks = new List<Task>();
        foreach (var exchange in _config.Exchanges)
        {
            // Каждый клиент получает свой lifetime scope (опционально, но чище)
            var client = new ExchangeWebSocketClient(
                exchange.Name,
                exchange.Url,
                _pipeline.Input,
                _metrics,
                Options.Create(_config),
                _serviceProvider.GetRequiredService<ILogger<ExchangeWebSocketClient>>());
            var task = client.RunAsync(stoppingToken);
            clientTasks.Add(task);
        }

        // Пайплайн обработки
        var pipelineTask = _pipeline.RunAsync(stoppingToken);

        // Ждём завершения всех задач
        var allTasks = new List<Task>(clientTasks) { pipelineTask };

        try
        {
            await Task.WhenAll(allTasks);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ошибка в пайплайне или клиенте");
        }

        _logger.LogInformation("Агрегатор остановлен");
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Graceful shutdown...");
        _pipeline.Stop(); // сигналим пайплайну — больше не принимаем новые данные
        await base.StopAsync(cancellationToken);
    }
}
