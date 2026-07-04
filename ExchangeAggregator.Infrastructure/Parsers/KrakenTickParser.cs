using System.Text.Json;
using ExchangeAggregator.Core.Interfaces;
using ExchangeAggregator.Core.Models;

namespace ExchangeAggregator.Infrastructure.Parsers;

/// <summary>
/// Парсер для Kraken.
/// Формат: {"pair":"BTC/USD","last":"50000.00","vol":"1.5","time":"2024-01-01T00:00:00.000Z"}
///   pair  — тикер (формат XXX/YYY, точка вместо / в нормализации)
///   last  — цена (строка!)
///   vol   — объём (строка!)
///   time  — ISO 8601
/// </summary>
public sealed class KrakenTickParser : ITickParser
{
    public string Exchange => "Kraken";

    public RawTick? TryParse(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (!root.TryGetProperty("pair", out var pairEl) ||
                !root.TryGetProperty("last", out var lastEl) ||
                !root.TryGetProperty("vol", out var volEl) ||
                !root.TryGetProperty("time", out var timeEl))
            {
                return null;
            }

            var pair = pairEl.GetString();
            if (string.IsNullOrWhiteSpace(pair)) return null;

            // Цена строкой — парсим decimal
            var lastStr = lastEl.GetString();
            if (lastStr is null || !decimal.TryParse(lastStr,
                    System.Globalization.NumberStyles.Number,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out var price))
            {
                return null;
            }

            var volStr = volEl.GetString();
            if (volStr is null || !decimal.TryParse(volStr,
                    System.Globalization.NumberStyles.Number,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out var volume))
            {
                return null;
            }

            var timeStr = timeEl.GetString();
            if (timeStr is null || !DateTimeOffset.TryParse(timeStr,
                    System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.AssumeUniversal,
                    out var timestamp))
            {
                return null;
            }

            // Нормализуем тикер: BTC/USD -> BTCUSD
            var ticker = pair.Replace("/", "");

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
