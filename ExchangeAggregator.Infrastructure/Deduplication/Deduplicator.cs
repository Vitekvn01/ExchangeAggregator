using System.Collections.Concurrent;
using ExchangeAggregator.Core.Interfaces;
using ExchangeAggregator.Core.Models;
using Microsoft.Extensions.Logging;

namespace ExchangeAggregator.Infrastructure.Deduplication;

/// <summary>
/// Потокобезопасный дедупликатор на ConcurrentDictionary.
/// Ключ: Ticker + Exchange + TimestampUnixMs + Price (<see cref="DedupKey"/>).
/// Окно: 5 минут — записи старше окна периодически вычищаются таймером.
/// </summary>
public sealed class Deduplicator : IDeduplicator, IDisposable
{
    private readonly ConcurrentDictionary<DedupKey, long> _seen = new();
    private readonly TimeSpan _window;
    private readonly Timer _cleanupTimer;
    private readonly ILogger<Deduplicator> _logger;

    public int Count => _seen.Count;

    /// <param name="window">Окно удержания ключей (рекомендуется 5 минут).</param>
    /// <param name="logger">Опциональный логер для диагностики очистки.</param>
    public Deduplicator(TimeSpan window, ILogger<Deduplicator>? logger = null)
    {
        _window = window;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<Deduplicator>.Instance;
        _cleanupTimer = new Timer(Cleanup, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    public bool IsDuplicate(NormalizedTick tick)
    {
        var key = new DedupKey(tick.Ticker, tick.Exchange, tick.Timestamp, tick.Price);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        return !_seen.TryAdd(key, nowTicks);
    }

    private void Cleanup(object? state)
    {
        try
        {
            var threshold = DateTimeOffset.UtcNow.Add(-_window).Ticks;
            var removed = 0;
            foreach (var kvp in _seen)
            {
                if (kvp.Value < threshold)
                {
                    if (_seen.TryRemove(kvp.Key, out _))
                        removed++;
                }
            }

            if (removed > 0)
            {
                _logger.LogDebug("Deduplicator cleanup: удалено {Count} ключей старше окна {Window}",
                    removed, _window);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ошибка при очистке дедупликатора");
        }
    }

    public void Dispose()
    {
        _cleanupTimer.Dispose();
    }
}