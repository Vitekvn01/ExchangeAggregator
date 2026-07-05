using ExchangeAggregator.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;

namespace ExchangeAggregator.Tests.Fakes;

public sealed class FakeDbContextFactory : IDbContextFactory<TickDbContext>
{
    private readonly bool _working;

    public FakeDbContextFactory(bool working) => _working = working;

    public TickDbContext CreateDbContext()
    {
        var opts = new DbContextOptionsBuilder<TickDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options;

        return _working
            ? new TickDbContext(opts)
            : new FailingDbContext(opts);
    }

    public async Task<TickDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default)
    {
        return CreateDbContext();
    }
}