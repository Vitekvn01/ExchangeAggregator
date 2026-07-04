namespace ExchangeAggregator.Api.Configuration;

/// <summary>
/// Параметры подключения к одному имитатору биржи.
/// </summary>
public sealed class ExchangeConnectionOptions
{
    /// <summary>Название биржи (Binance, Kraken, Coinbase).</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>WebSocket URL (ws://localhost:5001/binance/ws).</summary>
    public string Url { get; set; } = string.Empty;
}

/// <summary>
/// Глобальные настройки агрегатора.
/// </summary>
public sealed class AggregatorOptions
{
    /// <summary>Список бирж для подключения.</summary>
    public List<ExchangeConnectionOptions> Exchanges { get; set; } = new();

    /// <summary>Начальная задержка переподключения (мс).</summary>
    public int ReconnectDelayMinMs { get; set; } = 500;

    /// <summary>Максимальная задержка переподключения (мс).</summary>
    public int ReconnectDelayMaxMs { get; set; } = 30_000;

    /// <summary>Idle-таймаут сокета: если данных нет дольше — считаем зависшим (сек).</summary>
    public int IdleTimeoutSeconds { get; set; } = 30;

    /// <summary>Размер батча для записи в БД.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>Максимальный интервал между сбросом батча (мс).</summary>
    public int BatchFlushIntervalMs { get; set; } = 500;

    /// <summary>Ёмкость канала обработки (backpressure).</summary>
    public int ChannelCapacity { get; set; } = 10_000;

    /// <summary>Таймаут drain при graceful shutdown (сек).</summary>
    public int DrainTimeoutSeconds { get; set; } = 15;
}
