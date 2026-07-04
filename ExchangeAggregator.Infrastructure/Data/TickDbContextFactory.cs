using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace ExchangeAggregator.Infrastructure.Data;

public sealed class TickDbContextFactory : IDesignTimeDbContextFactory<TickDbContext>
{
    public TickDbContext CreateDbContext(string[] args)
    {
        var options = new DbContextOptionsBuilder<TickDbContext>()
            .UseNpgsql("Host=localhost;Port=5435;Database=exchange_aggregator;Username=postgres;Password=postgres")
            .Options;
        return new TickDbContext(options);
    }
}
