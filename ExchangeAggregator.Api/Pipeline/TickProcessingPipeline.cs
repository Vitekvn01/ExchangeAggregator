using System.Threading.Channels;
using ExchangeAggregator.Api.Configuration;
using ExchangeAggregator.Core.Interfaces;
using ExchangeAggregator.Core.Metrics;
using ExchangeAggregator.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ExchangeAggregator.Api.Pipeline;

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
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleWriter = false,
            SingleReader = true
        });
    }

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
            _logger.LogInformation("Завершение пайплайна, drain канала...");
        }
        finally
        {
            await DrainAndFlushAsync(externalCt);
        }
    }

    private async Task ProcessLoopAsync(CancellationToken ct)
    {
        var batch = new List<NormalizedTick>(_batchSize);
        var lastFlush = DateTimeOffset.UtcNow;

        try
        {
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

                if (batch.Count > 0 && DateTimeOffset.UtcNow - lastFlush >= _flushInterval)
                {
                    await FlushBatchAsync(batch, ct);
                    batch = new List<NormalizedTick>(_batchSize);
                    lastFlush = DateTimeOffset.UtcNow;
                }

                _metrics.ChannelBacklog = _channel.Reader.Count;
            }
        }
        finally
        {
            // Не теряем батч при отмене или закрытии канала
            if (batch.Count > 0)
            {
                try
                {
                    using var flushCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    await _store.WriteBatchAsync(batch, flushCts.Token);
                    _metrics.AddWritten(batch.Count);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Ошибка при сливе батча ({Count} тиков) при остановке", batch.Count);
                    _metrics.AddDropped(batch.Count);
                }
            }
        }
    }

    private void ProcessSingle(string json, List<NormalizedTick> batch)
    {
        RawTick? raw = null;

        foreach (var (_, parser) in _parsers)
        {
            raw = parser.TryParse(json);
            if (raw is not null)
                break;
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
        await _store.WriteBatchAsync(toWrite, ct);
        _metrics.AddWritten(toWrite.Length);
    }

    private async Task DrainAndFlushAsync(CancellationToken ct)
    {
        var batch = new List<NormalizedTick>(_batchSize);

        try
        {
            using var timeoutCts = new CancellationTokenSource(_drainTimeout);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
            var drainCt = linked.Token;

            while (await _channel.Reader.WaitToReadAsync(drainCt))
            {
                while (_channel.Reader.TryRead(out var json))
                {
                    ProcessSingle(json, batch);

                    if (batch.Count >= _batchSize)
                    {
                        await _store.WriteBatchAsync(batch, drainCt);
                        _metrics.AddWritten(batch.Count);
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

        if (batch.Count > 0)
        {
            try
            {
                using var finalCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await _store.WriteBatchAsync(batch, finalCts.Token);
                _metrics.AddWritten(batch.Count);
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