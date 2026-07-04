namespace ExchangeAggregator.Core.Models;

/// <summary>
/// Единый внутренний формат котировки после нормализации.
/// Все тики от всех бирж приводятся к этому представлению.
/// </summary>
public sealed record NormalizedTick
{
    /// <summary>Тикер.</summary>
    public string Ticker { get; init; } = string.Empty;

    /// <summary>Цена.</summary>
    public decimal Price { get; init; }

    /// <summary>Объём.</summary>
    public decimal Volume { get; init; }

    /// <summary>Метка времени (UTC).</summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>Биржа-источник.</summary>
    public string Exchange { get; init; } = string.Empty;
}
