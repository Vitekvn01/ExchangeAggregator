using System.Collections.Concurrent;
using ExchangeAggregator.Core.Interfaces;
using ExchangeAggregator.Core.Models;

namespace ExchangeAggregator.Infrastructure.Deduplication;

/// <summary>
/// Потокобезопасный дедупликатор на ConcurrentDictionary.
/// Ключ: Ticker + Exchange + TimestampUnixSeconds (<see cref="DedupKey"/>).
/// Окно: 5 минут — записи старше окна периодически вычищаются таймером.
/// </summary>
public sealed class Deduplicator : IDeduplicator, IDisposable
{
    // Ключ + момент добавления (ticks) для очистки по возрасту
    private readonly ConcurrentDictionary<DedupKey, long> _seen = new();
    private readonly TimeSpan _window;
    private readonly Timer _cleanupTimer;

    public int Count => _seen.Count;

    /// <param name="window">Окно удержания ключей (рекомендуется 5 минут).</param>
    public Deduplicator(TimeSpan window)
    {
        _window = window;
        // Запускаем очистку раз в минуту — компромисс между памятью и CPU
        _cleanupTimer = new Timer(Cleanup, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    /// <summary>
    /// Проверяет, дубликат ли этот тик.
    /// Если ключа ещё нет — добавляет его и возвращает false (не дубликат).
    /// Если ключ есть — возвращает true (дубликат).
    /// Потокобезопасен: TryAdd атомарен в ConcurrentDictionary.
    /// </summary>
    public bool IsDuplicate(NormalizedTick tick)
    {
        var key = new DedupKey(tick.Ticker, tick.Exchange, tick.Timestamp, tick.Price);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;

        // TryAdd возвращает true если ключа не было (мы его только что добавили)
        // → это НЕ дубликат, возвращаем false.
        // TryAdd возвращает false если ключ уже существует → дубликат.
        return !_seen.TryAdd(key, nowTicks);
    }

    private void Cleanup(object? state)
    {
        try
        {
            var threshold = DateTimeOffset.UtcNow.Add(-_window).Ticks;
            foreach (var kvp in _seen)
            {
                if (kvp.Value < threshold)
                {
                    _seen.TryRemove(kvp.Key, out _);
                }
            }
        }
        catch
        {
            // Таймер не должен ронять процесс — молча пропускаем
        }
    }

    public void Dispose()
    {
        _cleanupTimer.Dispose();
    }
}
