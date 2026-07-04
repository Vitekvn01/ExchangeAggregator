namespace ExchangeAggregator.Core.Models;

/// <summary>
/// Ключ дедупликации: Ticker + Exchange + Timestamp (мс) + Price.
/// Дубликат — это тик с теми же ticker, exchange, timestamp и ценой.
/// Разная цена в одну миллисекунду/секунду — разные тики.
/// Это позволяет корректно работать с биржами, дающими время с разной точностью
/// (Binance — мс, Coinbase — секунды), без ложных срабатываний.
/// </summary>
public readonly record struct DedupKey
{
    public string Ticker { get; init; }
    public string Exchange { get; init; }
    public long TimestampUnixMs { get; init; }
    public decimal Price { get; init; }

    public DedupKey(string ticker, string exchange, DateTimeOffset timestamp, decimal price)
    {
        Ticker = ticker;
        Exchange = exchange;
        TimestampUnixMs = timestamp.ToUnixTimeMilliseconds();
        Price = price;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Ticker, Exchange, TimestampUnixMs, Price);
    }
}