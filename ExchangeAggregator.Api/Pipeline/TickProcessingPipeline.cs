using System.Threading.Channels;
using ExchangeAggregator.Api.Configuration;
using ExchangeAggregator.Core.Interfaces;
using ExchangeAggregator.Core.Metrics;
using ExchangeAggregator.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ExchangeAggregator.Api.Pipeline;

/// <summary>
/// Сердце агрегатора — пайплайн обработки тиков.
/// 
/// Поток данных:
///   ExchangeWebSocketClient → Channel<string> (сырые JSON)
///   → парсинг (через ITickParser по имени биржи)
///   → нормализация (ITickNormalizer)
///   → дедупликация (IDeduplicator)
///   → батчинг (накопление в List<NormalizedTick>)
///   → ITickStore.WriteBatchAsync
/// 
/// Graceful shutdown: при CancellationToken — drain канала, дописываем батч.
/// </summary>
public sealed class TickProcessingPipeline : IDisposable
{
    private readonly Channel<string> _channel;
    private readonly IReadOnlyDictionary<string, ITickParser> _parsers;
    private readonly ITickNormalizer _normalizer;
    private readonly IDeduplicator _deduplicator;
    private readonly ITickStore _store;
    private readonly TickMetrics _metrics;
    private readonly ILogger _logger;
    private readonly int _batchSize;
    private readonly TimeSpan _flushInterval;
    private readonly TimeSpan _drainTimeout;
    private readonly CancellationTokenSource _cts = new();

    public ChannelWriter<string> Input => _channel.Writer;
    public ChannelReader<string> Output => _channel.Reader;

    public TickProcessingPipeline(
        IEnumerable<ITickParser> parsers,
        ITickNormalizer normalizer,
        IDeduplicator deduplicator,
        ITickStore store,
        TickMetrics metrics,
        IOptions<AggregatorOptions> options,
        ILogger<TickProcessingPipeline> logger)
    {
        _parsers = parsers.ToDictionary(p => p.Exchange);
        _normalizer = normalizer;
        _deduplicator = deduplicator;
        _store = store;
        _metrics = metrics;
        _logger = logger;
        _batchSize = options.Value.BatchSize;
        _flushInterval = TimeSpan.FromMilliseconds(options.Value.BatchFlushIntervalMs);
        _drainTimeout = TimeSpan.FromSeconds(options.Value.DrainTimeoutSeconds);

        _channel = Channel.CreateBounded<string>(new BoundedChannelOptions(options.Value.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.DropWrite, // не блокируем отправителя — дропаем
            SingleWriter = false,
            SingleReader = true
        });
    }

    /// <summary>
    /// Запускает пайплайн. Работает до отмены токена.
    /// При отмене — drain канала и дописывает последний батч.
    /// </summary>
    public async Task RunAsync(CancellationToken externalCt)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, externalCt);
        var ct = linkedCts.Token;

        try
        {
            await ProcessLoopAsync(ct);
        }
        catch (OperationCanceledException)
        {
            // Graceful shutdown — drain
            _logger.LogInformation("Завершение пайплайна, drain канала...");
            await DrainAndFlushAsync(externalCt);
        }
    }

    private async Task ProcessLoopAsync(CancellationToken ct)
    {
        var batch = new List<NormalizedTick>(_batchSize);
        var lastFlush = DateTimeOffset.UtcNow;

        while (await _channel.Reader.WaitToReadAsync(ct))
        {
            while (_channel.Reader.TryRead(out var json))
            {
                ProcessSingle(json, batch);

                if (batch.Count >= _batchSize)
                {
                    await FlushBatchAsync(batch, ct);
                    batch = new List<NormalizedTick>(_batchSize);
                    lastFlush = DateTimeOffset.UtcNow;
                }
            }

            // Флаш по времени
            if (batch.Count > 0 && DateTimeOffset.UtcNow - lastFlush >= _flushInterval)
            {
                await FlushBatchAsync(batch, ct);
                batch = new List<NormalizedTick>(_batchSize);
                lastFlush = DateTimeOffset.UtcNow;
            }

            // Обновляем метрику backlog-а
            _metrics.ChannelBacklog = _channel.Reader.Count;
        }
    }

    private void ProcessSingle(string json, List<NormalizedTick> batch)
    {
        RawTick? raw = null;
        ITickParser? matchedParser = null;

        // Пробуем все парсеры — первый, кто вернул не-null, выиграл
        foreach (var (name, parser) in _parsers)
        {
            raw = parser.TryParse(json);
            if (raw is not null)
            {
                matchedParser = parser;
                break;
            }
        }

        if (raw is null)
        {
            _logger.LogTrace("Не удалось разобрать сообщение: {Json}", json[..Math.Min(100, json.Length)]);
            return;
        }

        _metrics.IncrementParsed();

        var normalized = _normalizer.Normalize(raw);

        if (_deduplicator.IsDuplicate(normalized))
        {
            _metrics.IncrementDuplicates();
            return;
        }

        batch.Add(normalized);
    }

    private async Task FlushBatchAsync(List<NormalizedTick> batch, CancellationToken ct)
    {
        if (batch.Count == 0) return;

        var toWrite = batch.ToArray();
        var dropped = await _store.WriteBatchAsync(toWrite, ct);
        // dropped уже учтён в TickStore через метрики
    }

    private async Task DrainAndFlushAsync(CancellationToken ct)
    {
        var batch = new List<NormalizedTick>(_batchSize);

        try
        {
            using var timeoutCts = new CancellationTokenSource(_drainTimeout);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
            var drainCt = linked.Token;

            // Читаем остатки из канала
            while (await _channel.Reader.WaitToReadAsync(drainCt))
            {
                while (_channel.Reader.TryRead(out var json))
                {
                    ProcessSingle(json, batch);

                    if (batch.Count >= _batchSize)
                    {
                        await _store.WriteBatchAsync(batch, drainCt);
                        batch.Clear();
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Drain прерван по таймауту, осталось в канале: {Count}",
                _channel.Reader.Count);
        }

        // Последний батч
        if (batch.Count > 0)
        {
            try
            {
                using var finalCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await _store.WriteBatchAsync(batch, finalCts.Token);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Не удалось записать последний батч ({Count} тиков) при shutdown", batch.Count);
                _metrics.AddDropped(batch.Count);
            }
        }

        _logger.LogInformation("Drain завершён. Итого: Received={R}, Parsed={P}, Duplicates={D}, Written={W}, Errors={E}, Dropped={Dr}",
            _metrics.Received, _metrics.Parsed, _metrics.Duplicates, _metrics.Written,
            _metrics.WriteErrors, _metrics.Dropped);
    }

    public void Stop()
    {
        _cts.Cancel();
    }

    public void Dispose()
    {
        _cts.Dispose();
    }
}
