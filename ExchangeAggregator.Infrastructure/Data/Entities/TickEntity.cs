namespace ExchangeAggregator.Infrastructure.Data.Entities;

/// <summary>
/// Чистый POCO для хранения тика. Вся конфигурация — в <see cref="TickDbContext.OnModelCreating"/>.
/// </summary>
public sealed class TickEntity
{
    public long Id { get; set; }
    public string Exchange { get; set; } = string.Empty;
    public string Ticker { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public decimal Volume { get; set; }
    public DateTimeOffset Timestamp { get; set; }
    public DateTimeOffset ReceivedAt { get; set; }
}
