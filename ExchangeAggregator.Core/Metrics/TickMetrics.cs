using System.Threading;

namespace ExchangeAggregator.Core.Metrics;

/// <summary>
/// Потокобезопасные счётчики для мониторинга работы агрегатора.
/// Использует Interlocked для атомарных операций без блокировок.
/// </summary>
public class TickMetrics
{
    private long _received;
    private long _parsed;
    private long _duplicates;
    private long _written;
    private long _writeErrors;
    private long _dropped;
    private long _reconnects;
    private long _channelBacklog;

    // --- Счётчики ---

    /// <summary>Получено сырых сообщений от WebSocket.</summary>
    public long Received => Interlocked.Read(ref _received);

    /// <summary>Успешно разобрано (парсинг).</summary>
    public long Parsed => Interlocked.Read(ref _parsed);

    /// <summary>Отфильтровано дубликатов.</summary>
    public long Duplicates => Interlocked.Read(ref _duplicates);

    /// <summary>Записано в БД.</summary>
    public long Written => Interlocked.Read(ref _written);

    /// <summary>Ошибок записи в БД.</summary>
    public long WriteErrors => Interlocked.Read(ref _writeErrors);

    /// <summary>Отброшено тиков (потеряны безвозвратно — overflow буфера и т.п.).</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>Количество переподключений.</summary>
    public long Reconnects => Interlocked.Read(ref _reconnects);

    /// <summary>Текущий backlog канала (приблизительный срез).</summary>
    public long ChannelBacklog
    {
        get => Interlocked.Read(ref _channelBacklog);
        set => Interlocked.Exchange(ref _channelBacklog, value);
    }

    // --- Increment helpers ---

    public void IncrementReceived() => Interlocked.Increment(ref _received);
    public void AddReceived(long count) => Interlocked.Add(ref _received, count);

    public void IncrementParsed() => Interlocked.Increment(ref _parsed);
    public void AddParsed(long count) => Interlocked.Add(ref _parsed, count);

    public void IncrementDuplicates() => Interlocked.Increment(ref _duplicates);
    public void AddDuplicates(long count) => Interlocked.Add(ref _duplicates, count);

    public void IncrementWritten() => Interlocked.Increment(ref _written);
    public void AddWritten(long count) => Interlocked.Add(ref _written, count);

    public void IncrementWriteErrors() => Interlocked.Increment(ref _writeErrors);
    public void AddWriteErrors(long count) => Interlocked.Add(ref _writeErrors, count);

    public void IncrementDropped() => Interlocked.Increment(ref _dropped);
    public void AddDropped(long count) => Interlocked.Add(ref _dropped, count);

    public void IncrementReconnects() => Interlocked.Increment(ref _reconnects);

    /// <summary>
    /// Возвращает снимок всех метрик для логирования/отображения.
    /// </summary>
    public MetricsSnapshot Snapshot() => new()
    {
        Received = Received,
        Parsed = Parsed,
        Duplicates = Duplicates,
        Written = Written,
        WriteErrors = WriteErrors,
        Dropped = Dropped,
        Reconnects = Reconnects,
        ChannelBacklog = ChannelBacklog
    };
}

/// <summary>
/// Неизменяемый снимок метрик.
/// </summary>
public sealed record MetricsSnapshot
{
    public long Received { get; init; }
    public long Parsed { get; init; }
    public long Duplicates { get; init; }
    public long Written { get; init; }
    public long WriteErrors { get; init; }
    public long Dropped { get; init; }
    public long Reconnects { get; init; }
    public long ChannelBacklog { get; init; }
}
