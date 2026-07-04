using ExchangeAggregator.Infrastructure.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace ExchangeAggregator.Infrastructure.Data;

public sealed class TickDbContext(DbContextOptions<TickDbContext> options) : DbContext(options)
{
    public DbSet<TickEntity> Ticks => Set<TickEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TickEntity>(entity =>
        {
            entity.ToTable("ticks");

            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).UseIdentityColumn();

            entity.Property(e => e.Exchange).HasColumnType("varchar(50)").IsRequired();
            entity.Property(e => e.Ticker).HasColumnType("varchar(20)").IsRequired();
            entity.Property(e => e.Price).HasColumnType("decimal(18,8)").IsRequired();
            entity.Property(e => e.Volume).HasColumnType("decimal(18,8)").IsRequired();
            entity.Property(e => e.Timestamp).IsRequired();
            entity.Property(e => e.ReceivedAt).IsRequired();

            // Для дедупликации на уровне БД и аналитики
            entity.HasIndex(e => new { e.Exchange, e.Ticker, e.Timestamp });
            entity.HasIndex(e => e.ReceivedAt);
        });
    }
}
