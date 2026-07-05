using ExchangeAggregator.Api.Configuration;
using ExchangeAggregator.Api.Pipeline;
using ExchangeAggregator.Core.Interfaces;
using ExchangeAggregator.Core.Metrics;
using ExchangeAggregator.Core.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace ExchangeAggregator.Tests;

public class PipelineDrainTests
{
    /// <summary>
    /// Complete() на канале → пайплайн штатно дочитывает всё и завершается.
    /// </summary>
    [Fact]
    public async Task CompleteChannel_DrainsAllTicks()
    {
        var metrics = new TickMetrics();
        var store = new CollectingTickStore();

        var options = Options.Create(new AggregatorOptions
        {
            BatchSize = 10,               // батч не накопится — проверим finally
            BatchFlushIntervalMs = 60_000,
            ChannelCapacity = 100,
            DrainTimeoutSeconds = 5
        });

        using var pipeline = new TickProcessingPipeline(
            new[] { new FakeParser("Binance") },
            new FakeNormalizer(),
            new FakeDeduplicator(),
            store,
            metrics,
            options,
            NullLogger<TickProcessingPipeline>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var runTask = pipeline.RunAsync(cts.Token);
        await Task.Delay(100);

        // Пишем 5 сообщений, batchSize=10 → батч не флашится
        for (int i = 0; i < 5; i++)
            pipeline.Input.TryWrite($$"""{"s":"BTCUSD","p":50000,"q":1,"t":1700000000000}""");

        pipeline.Input.Complete();
        await runTask;

        Assert.Equal(5, metrics.Parsed);
        Assert.Equal(5, metrics.Written);
        Assert.Equal(0, metrics.Duplicates);
        Assert.Equal(0, metrics.Dropped);
        Assert.Equal(5, store.TotalWritten);
    }

    /// <summary>
    /// Stop() при заполненном канале → finally флашит текущий батч,
    /// затем DrainAndFlushAsync добирает остатки из канала.
    /// </summary>
    [Fact]
    public async Task Stop_DrainsRemainingTicks()
    {
        var metrics = new TickMetrics();
        var store = new CollectingTickStore();

        var options = Options.Create(new AggregatorOptions
        {
            BatchSize = 100,              // большой — батч не флашится во время работы
            BatchFlushIntervalMs = 60_000,
            ChannelCapacity = 100,
            DrainTimeoutSeconds = 5
        });

        using var pipeline = new TickProcessingPipeline(
            new[] { new FakeParser("Binance") },
            new FakeNormalizer(),
            new FakeDeduplicator(),
            store,
            metrics,
            options,
            NullLogger<TickProcessingPipeline>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var runTask = pipeline.RunAsync(cts.Token);
        await Task.Delay(100);

        // Пишем 7 тиков
        for (int i = 0; i < 7; i++)
            pipeline.Input.TryWrite($$"""{"s":"BTCUSD","p":50000,"q":1,"t":1700000000000}""");

        // Даём пайплайну прочитать их в батч (без флаша, т.к. batchSize=100)
        await Task.Delay(200);

        // Stop() → отмена → finally флашит батч → DrainAndFlushAsync добирает остатки
        pipeline.Stop();
        await runTask;

        Assert.Equal(7, metrics.Parsed);
        Assert.Equal(7, metrics.Written);
        Assert.Equal(0, metrics.Duplicates);
        Assert.Equal(0, metrics.Dropped);
        Assert.Equal(7, store.TotalWritten);
    }

    // ── Fakes ──

    private sealed class FakeParser : ITickParser
    {
        public FakeParser(string exchange) => Exchange = exchange;
        public string Exchange { get; }

        public RawTick? TryParse(string json) => new()
        {
            Exchange = Exchange,
            Ticker = "BTCUSD",
            Price = 50000m,
            Volume = 1m,
            Timestamp = DateTimeOffset.UtcNow,
            RawJson = json
        };
    }

    private sealed class FakeNormalizer : ITickNormalizer
    {
        public NormalizedTick Normalize(RawTick rawTick) => new()
        {
            Ticker = rawTick.Ticker,
            Exchange = rawTick.Exchange,
            Price = rawTick.Price,
            Volume = rawTick.Volume,
            Timestamp = rawTick.Timestamp
        };
    }

    private sealed class FakeDeduplicator : IDeduplicator
    {
        public bool IsDuplicate(NormalizedTick tick) => false;
        public int Count => 0;
    }

    private sealed class CollectingTickStore : ITickStore
    {
        private int _totalWritten;
        public int TotalWritten => _totalWritten;

        public Task<int> WriteBatchAsync(IReadOnlyCollection<NormalizedTick> ticks, CancellationToken ct)
        {
            Interlocked.Add(ref _totalWritten, ticks.Count);
            return Task.FromResult(0);
        }
    }
}