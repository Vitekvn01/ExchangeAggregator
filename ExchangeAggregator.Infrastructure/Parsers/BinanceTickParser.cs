using System.Text.Json;
using ExchangeAggregator.Core.Interfaces;
using ExchangeAggregator.Core.Models;

namespace ExchangeAggregator.Infrastructure.Parsers;

/// <summary>
/// Парсер для Binance.
/// Формат: {"s":"BTCUSD","p":50000.0,"q":1.5,"t":1700000000000}
///   s — тикер (symbol)
///   p — цена (price) — число
///   q — объём (quantity) — число
///   t — timestamp в миллисекундах Unix epoch
/// </summary>
public sealed class BinanceTickParser : ITickParser
{
    public string Exchange => "Binance";

    public RawTick? TryParse(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            // Проверяем наличие ключевых полей
            if (!root.TryGetProperty("s", out var sEl) ||
                !root.TryGetProperty("p", out var pEl) ||
                !root.TryGetProperty("q", out var qEl) ||
                !root.TryGetProperty("t", out var tEl))
            {
                return null;
            }

            var ticker = sEl.GetString();
            if (string.IsNullOrWhiteSpace(ticker)) return null;

            var price = pEl.GetDecimal();
            var volume = qEl.GetDecimal();
            var epochMs = tEl.GetInt64();

            var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(epochMs);

            return new RawTick
            {
                Exchange = Exchange,
                Ticker = ticker,
                Price = price,
                Volume = volume,
                Timestamp = timestamp,
                RawJson = json
            };
        }
        catch (JsonException)
        {
            return null; // невалидный JSON — не наш формат
        }
    }
}
