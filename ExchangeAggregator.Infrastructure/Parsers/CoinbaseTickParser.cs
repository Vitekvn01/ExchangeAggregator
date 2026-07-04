using System.Text.Json;
using ExchangeAggregator.Core.Interfaces;
using ExchangeAggregator.Core.Models;

namespace ExchangeAggregator.Infrastructure.Parsers;

/// <summary>
/// Парсер для имитатора "Coinbase".
/// Формат: {"ticker":"BTC-USD","price":50000.0,"size":1.5,"ts":"1700000000"}
///   ticker — тикер, разделитель дефис (отличие от Binance и Kraken)
///   price  — цена числом
///   size   — объём числом
///   ts     — timestamp в секундах Unix epoch строкой (отличие!)
/// </summary>
public sealed class CoinbaseTickParser : ITickParser
{
    public string Exchange => "Coinbase";

    public RawTick? TryParse(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (!root.TryGetProperty("ticker", out var tickerEl) ||
                !root.TryGetProperty("price", out var priceEl) ||
                !root.TryGetProperty("size", out var sizeEl) ||
                !root.TryGetProperty("ts", out var tsEl))
            {
                return null;
            }

            var tickerRaw = tickerEl.GetString();
            if (string.IsNullOrWhiteSpace(tickerRaw)) return null;

            var price = priceEl.GetDecimal();
            var volume = sizeEl.GetDecimal();

            // ts — Unix epoch в секундах, но приходит строкой (отличие!)
            var tsStr = tsEl.GetString();
            if (tsStr is null || !long.TryParse(tsStr,
                    System.Globalization.NumberStyles.Integer,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out var epochSeconds))
            {
                return null;
            }

            var timestamp = DateTimeOffset.FromUnixTimeSeconds(epochSeconds);

            // Нормализуем тикер: BTC-USD -> BTCUSD
            var ticker = tickerRaw.Replace("-", "");

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
            return null;
        }
    }
}
