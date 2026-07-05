using ExchangeAggregator.Core.Models;
using ExchangeAggregator.Infrastructure.Parsers;

namespace ExchangeAggregator.Tests;

/// <summary>
/// Тесты парсеров: проверяем, что каждый парсер корректно разбирает JSON своей биржи
/// и возвращает null на чужой формат.
/// </summary>
public class ParsersTests
{
    [Fact]
    public void BinanceParser_ParsesValidJson()
    {
        var parser = new BinanceTickParser();
        var json = """{"s":"BTCUSD","p":50000.50,"q":1.5,"t":1700000000123}""";

        RawTick? tick = parser.TryParse(json);

        Assert.NotNull(tick);
        Assert.Equal("Binance", tick!.Exchange);
        Assert.Equal("BTCUSD", tick.Ticker);
        Assert.Equal(50000.50m, tick.Price);
        Assert.Equal(1.5m, tick.Volume);
        Assert.Equal(1700000000123, tick.Timestamp.ToUnixTimeMilliseconds());
    }

    [Fact]
    public void BinanceParser_ReturnsNull_OnInvalidJson()
    {
        var parser = new BinanceTickParser();

        RawTick? tick = parser.TryParse("not json");

        Assert.Null(tick);
    }

    [Fact]
    public void BinanceParser_ReturnsNull_OnMissingField()
    {
        var parser = new BinanceTickParser();
        var json = """{"s":"BTCUSD","p":50000.50}"""; // нет q и t

        RawTick? tick = parser.TryParse(json);

        Assert.Null(tick);
    }

    [Fact]
    public void KrakenParser_ParsesValidJson()
    {
        var parser = new KrakenTickParser();
        var json = """{"pair":"BTC/USD","last":"50000.50","vol":"1.5","time":"2024-11-15T10:30:00.123Z"}""";

        RawTick? tick = parser.TryParse(json);

        Assert.NotNull(tick);
        Assert.Equal("Kraken", tick!.Exchange);
        Assert.Equal("BTCUSD", tick.Ticker); // слеш убран
        Assert.Equal(50000.50m, tick.Price);
        Assert.Equal(1.5m, tick.Volume);
    }

    [Fact]
    public void KrakenParser_ParsesPriceAsString() // ключевое отличие Kraken — цена строкой
    {
        var parser = new KrakenTickParser();
        var json = """{"pair":"ETH/USD","last":"3000.99","vol":"0.5","time":"2024-11-15T10:30:00.123Z"}""";

        RawTick? tick = parser.TryParse(json);

        Assert.NotNull(tick);
        Assert.Equal(3000.99m, tick!.Price);
        Assert.Equal("ETHUSD", tick.Ticker);
    }

    [Fact]
    public void CoinbaseParser_ParsesValidJson()
    {
        var parser = new CoinbaseTickParser();
        var json = """{"ticker":"BTC-USD","price":50000.50,"size":1.5,"ts":"1700000000"}""";

        RawTick? tick = parser.TryParse(json);

        Assert.NotNull(tick);
        Assert.Equal("Coinbase", tick!.Exchange);
        Assert.Equal("BTCUSD", tick.Ticker); // дефис убран
        Assert.Equal(50000.50m, tick.Price);
        Assert.Equal(1.5m, tick.Volume);
    }

    [Fact]
    public void CoinbaseParser_TimestampIsSeconds() // ключевое отличие Coinbase — секунды строкой
    {
        var parser = new CoinbaseTickParser();
        // ts = 1700000000 секунд → 1700000000000 миллисекунд
        var json = """{"ticker":"SOL-USD","price":150.0,"size":2.0,"ts":"1700000000"}""";

        RawTick? tick = parser.TryParse(json);

        Assert.NotNull(tick);
        Assert.Equal(1700000000000, tick!.Timestamp.ToUnixTimeMilliseconds());
    }

    [Fact]
    public void CoinbaseParser_ReturnsNull_OnMissingField()
    {
        var parser = new CoinbaseTickParser();
        var json = """{"ticker":"BTC-USD","price":50000.0}"""; // нет size и ts

        RawTick? tick = parser.TryParse(json);

        Assert.Null(tick);
    }

    /// <summary>
    /// Парсер одной биржи не должен разбирать JSON другой биржи.
    /// Это важно для корректной маршрутизации в пайплайне.
    /// </summary>
    [Fact]
    public void Parsers_DoNotCrossParse()
    {
        var binance = new BinanceTickParser();
        var kraken = new KrakenTickParser();
        var coinbase = new CoinbaseTickParser();

        var binanceJson = """{"s":"BTCUSD","p":50000.0,"q":1.0,"t":1700000000123}""";
        var krakenJson = """{"pair":"BTC/USD","last":"50000.0","vol":"1.0","time":"2024-11-15T10:30:00.000Z"}""";
        var coinbaseJson = """{"ticker":"BTC-USD","price":50000.0,"size":1.0,"ts":"1700000000"}""";

        // Binance должен парсить только свой JSON
        Assert.NotNull(binance.TryParse(binanceJson));
        Assert.Null(binance.TryParse(krakenJson));
        Assert.Null(binance.TryParse(coinbaseJson));

        // Kraken должен парсить только свой JSON
        Assert.Null(kraken.TryParse(binanceJson));
        Assert.NotNull(kraken.TryParse(krakenJson));
        Assert.Null(kraken.TryParse(coinbaseJson));

        // Coinbase должен парсить только свой JSON
        Assert.Null(coinbase.TryParse(binanceJson));
        Assert.Null(coinbase.TryParse(krakenJson));
        Assert.NotNull(coinbase.TryParse(coinbaseJson));
    }
}
