namespace ExchangeAggregator.Core.Models;

/// <summary>
/// Представление тика в том виде, в каком он пришёл от конкретной биржи,
/// после разбора (парсинга) биржевого JSON.
/// Все биржевые форматы уже приведены к единым типам (decimal, DateTimeOffset),
/// но имена полей и состав могут отличаться — за это отвечает <see cref="ITickParser"/>.
/// </summary>
public sealed record RawTick
{
    /// <summary>Название биржи-источника (Binance, Kraken, Coinbase).</summary>
    public string Exchange { get; init; } = string.Empty;

    /// <summary>Тикер (символ) — например BTCUSD.</summary>
    public string Ticker { get; init; } = string.Empty;

    /// <summary>Цена (decimal для точности).</summary>
    public decimal Price { get; init; }

    /// <summary>Объём.</summary>
    public decimal Volume { get; init; }

    /// <summary>Метка времени биржи.</summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>Оригинальный JSON на случай отладки / аудита.</summary>
    public string? RawJson { get; init; }
}
