using ExchangeAggregator.Core.Metrics;
using ExchangeAggregator.Core.Models;
using ExchangeAggregator.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;

namespace ExchangeAggregator.Tests;

/// <summary>
/// Тест TickStore: проверяем поведение при ошибках БД.
/// 
/// Используем реальный DbContext с InMemory-провайдером.
/// Для теста ошибок — подменяем фабрику так, чтобы
/// SaveChangesAsync выбрасывал исключение.
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

        // DB не работает → после всех ретраев тики дропнуты
        Assert.Equal(1, dropped);
        Assert.Equal(0, metrics.Written);
        Assert.Equal(1, metrics.Dropped);
        Assert.True(metrics.WriteErrors > 0, "Должны быть залогированы ошибки записи");
    }

    // --- helpers ---

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

/// <summary>
/// Фейковая фабрика DbContext.
/// working=true  → нормальная InMemory БД (пишет успешно).
/// working=false → DbContext, который взрывается на SaveChangesAsync.
/// </summary>
public sealed class FakeDbContextFactory : IDbContextFactory<TickDbContext>
{
    private readonly bool _working;

    public FakeDbContextFactory(bool working) => _working = working;

    public TickDbContext CreateDbContext()
    {
        if (_working)
        {
            // InMemory — работает без реальной БД
            var opts = new DbContextOptionsBuilder<TickDbContext>()
                .UseInMemoryDatabase(Guid.NewGuid().ToString())
                .Options;
            return new TickDbContext(opts);
        }
        else
        {
            // FailingDbContext — SaveChangesAsync кидает исключение
            var opts = new DbContextOptionsBuilder<TickDbContext>()
                .UseInMemoryDatabase(Guid.NewGuid().ToString())
                .Options;
            return new FailingDbContext(opts);
        }
    }
}

/// <summary>
/// DbContext, который всегда падает на SaveChangesAsync.
/// </summary>
public sealed class FailingDbContext : TickDbContext
{
    public FailingDbContext(DbContextOptions<TickDbContext> opts) : base(opts) { }

    public override Task<int> SaveChangesAsync(CancellationToken ct = default)
    {
        throw new Exception("Simulated DB failure");
    }
}
