namespace ExchangeAggregator.Core.Models;

/// <summary>
/// Ключ дедупликации: тикер + биржа + timestamp с точностью до секунды.
/// Два тика считаются дубликатами, если они пришли от одной биржи,
/// относятся к одному тикеру и имеют одинаковую секунду.
/// </summary>
public readonly record struct DedupKey
{
    public string Ticker { get; init; }

    public string Exchange { get; init; }

    public long TimestampUnixSeconds { get; init; }

    public DedupKey(string ticker, string exchange, DateTimeOffset timestamp)
    {
        Ticker = ticker;
        Exchange = exchange;
        TimestampUnixSeconds = timestamp.ToUnixTimeSeconds();
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Ticker, Exchange, TimestampUnixSeconds);
    }
}
