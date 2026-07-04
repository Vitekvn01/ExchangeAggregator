using ExchangeAggregator.Core.Interfaces;
using ExchangeAggregator.Core.Metrics;
using ExchangeAggregator.Core.Models;
using ExchangeAggregator.Infrastructure.Data.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace ExchangeAggregator.Infrastructure.Data;

/// <summary>
/// Хранилище тиков с батчингом и retry при ошибках БД.
/// 
/// Стратегия при ошибке БД:
/// 1. До 3 ретраев с экспоненциальной задержкой.
/// 2. Если после ретраев не удалось — возвращает количество не записанных тиков,
///    а вызывающий код учитывает их в счётчике Dropped.
/// 3. Каждая ошибка пишется в лог и инкрементит метрику WriteErrors.
/// 
/// Никаких молчаливых потерь: вызывающий код всегда знает результат.
/// </summary>
public sealed class TickStore : ITickStore
{
    private readonly TickDbContext _db;
    private readonly TickMetrics _metrics;
    private readonly ILogger<TickStore> _logger;
    private const int MaxRetries = 3;
    private static readonly TimeSpan[] RetryDelays =
    [
        TimeSpan.FromMilliseconds(100),
        TimeSpan.FromMilliseconds(500),
        TimeSpan.FromSeconds(2)
    ];

    public TickStore(TickDbContext db, TickMetrics metrics, ILogger<TickStore> logger)
    {
        _db = db;
        _metrics = metrics;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<int> WriteBatchAsync(IReadOnlyCollection<NormalizedTick> ticks, CancellationToken ct)
    {
        if (ticks.Count == 0) return 0;

        var entities = ticks.Select(t => new TickEntity
        {
            Exchange = t.Exchange,
            Ticker = t.Ticker,
            Price = t.Price,
            Volume = t.Volume,
            Timestamp = t.Timestamp,
            ReceivedAt = DateTimeOffset.UtcNow
        }).ToList();

        for (var attempt = 0; attempt <= MaxRetries; attempt++)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                await _db.Ticks.AddRangeAsync(entities, ct);
                await _db.SaveChangesAsync(ct);

                _metrics.AddWritten(entities.Count);
                return 0; // все записаны
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex) when (attempt < MaxRetries)
            {
                _logger.LogWarning(ex,
                    "Retry {Attempt}/{Max} записи батча ({Count} тиков) — {Error}",
                    attempt + 1, MaxRetries, entities.Count, ex.Message);

                _metrics.IncrementWriteErrors();
                await Task.Delay(RetryDelays[attempt], ct);
            }
        }

        // Финальная попытка
        try
        {
            await _db.Ticks.AddRangeAsync(entities, ct);
            await _db.SaveChangesAsync(ct);
            _metrics.AddWritten(entities.Count);
            return 0;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Не удалось записать батч ({Count} тиков) после {Max} ретраев — данные потеряны",
                entities.Count, MaxRetries);

            _metrics.IncrementWriteErrors();
            _metrics.AddDropped(entities.Count);
            return entities.Count;
        }
    }
}
