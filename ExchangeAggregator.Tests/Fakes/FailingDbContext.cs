using ExchangeAggregator.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;

namespace ExchangeAggregator.Tests.Fakes;

/// <summary>
/// DbContext, который всегда падает на SaveChangesAsync.
/// Используется для тестирования поведения TickStore при отказе БД.
/// </summary>
public sealed class FailingDbContext : TickDbContext
{
    public FailingDbContext(DbContextOptions<TickDbContext> opts) : base(opts) { }

    public override Task<int> SaveChangesAsync(CancellationToken ct = default)
    {
        throw new Exception("Simulated DB failure");
    }
}
