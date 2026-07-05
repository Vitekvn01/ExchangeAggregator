using ExchangeAggregator.Core.Metrics;
using ExchangeAggregator.Core.Models;
using ExchangeAggregator.Infrastructure.Data;
using ExchangeAggregator.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;

/// <summary>
/// Тест TickStore: проверяем поведение при ошибках БД.
/// </summary>
public class TickStoreTests
{
    [Fact]
    public async Task WriteBatch_WritesSuccessfully_ReturnsZero()
    {
        var metrics = new TickMetrics();
        var store = CreateStore(metrics, working: true);

        var ticks = new[] { CreateTick("BTCUSD", "Binance", 50000m) };
        int dropped = await store.WriteBatchAsync(ticks, CancellationToken.None);

        Assert.Equal(0, dropped);
        Assert.Equal(1, metrics.Written);
        Assert.Equal(0, metrics.Dropped);
        Assert.Equal(0, metrics.WriteErrors);
    }

    [Fact]
    public async Task WriteBatch_DbFails_RetriesAndDrops()
    {
        var metrics = new TickMetrics();
        var store = CreateStore(metrics, working: false);

        var ticks = new[] { CreateTick("BTCUSD", "Binance", 50000m) };
        int dropped = await store.WriteBatchAsync(ticks, CancellationToken.None);

        Assert.Equal(1, dropped);
        Assert.Equal(0, metrics.Written);
        Assert.Equal(1, metrics.Dropped);
        Assert.True(metrics.WriteErrors > 0, "Должны быть залогированы ошибки записи");
    }

    private static TickStore CreateStore(TickMetrics metrics, bool working)
    {
        var factory = new FakeDbContextFactory(working);
        return new TickStore(factory, metrics, NullLogger<TickStore>.Instance);
    }

    private static NormalizedTick CreateTick(string ticker, string exchange, decimal price)
    {
        return new NormalizedTick
        {
            Ticker = ticker,
            Exchange = exchange,
            Price = price,
            Volume = 1.0m,
            Timestamp = DateTimeOffset.UtcNow
        };
    }
}