using ExchangeAggregator.Core.Models;

namespace ExchangeAggregator.Core.Interfaces;

/// <summary>
/// Дедупликатор: отсеивает повторяющиеся тики.
/// Ключ — <see cref="DedupKey"/>: Ticker + Exchange + Timestamp (с точностью до секунды).
/// Окно дедупликации — 5 минут: записи старше окна периодически вытесняются.
/// Реализация обязана быть потокобезопасной.
/// </summary>
public interface IDeduplicator
{
    /// <summary>
    /// Проверяет, был ли этот тик уже обработан (дубликат).
    /// Если не был — регистрирует его и возвращает false.
    /// Если был — возвращает true.
    /// </summary>
    bool IsDuplicate(NormalizedTick tick);

    /// <summary>
    /// Текущее количество записей в окне дедупликации (для мониторинга).
    /// </summary>
    int Count { get; }
}
