using ExchangeAggregator.Core.Interfaces;
using ExchangeAggregator.Core.Metrics;
using ExchangeAggregator.Core.Models;
using ExchangeAggregator.Infrastructure.Data.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace ExchangeAggregator.Infrastructure.Data;

public sealed class TickStore : ITickStore
{
    private readonly IDbContextFactory<TickDbContext> _dbFactory;
    private readonly TickMetrics _metrics;
    private readonly ILogger<TickStore> _logger;
    private const int MaxRetries = 3;

    public TickStore(IDbContextFactory<TickDbContext> dbFactory, TickMetrics metrics, ILogger<TickStore> logger)
    {
        _dbFactory = dbFactory;
        _metrics = metrics;
        _logger = logger;
    }

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
                await using var db = await _dbFactory.CreateDbContextAsync(ct);
                await db.Ticks.AddRangeAsync(entities, ct);
                await db.SaveChangesAsync(ct);
                // ✅ AddWritten вызывается вызывающей стороной (pipeline)
                return 0;
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex) when (attempt < MaxRetries)
            {
                _logger.LogWarning(ex,
                    "Retry {Attempt}/{Max} записи батча ({Count} тиков)",
                    attempt + 1, MaxRetries, entities.Count);
                _metrics.IncrementWriteErrors();
                await Task.Delay(TimeSpan.FromMilliseconds(Math.Pow(2, attempt) * 100), ct);
            }
            catch (Exception ex)
            {
                // Последняя попытка провалилась — дропаем
                _logger.LogError(ex,
                    "Не удалось записать батч ({Count} тиков) после {Max} ретраев",
                    entities.Count, MaxRetries);
                _metrics.IncrementWriteErrors();
                _metrics.AddDropped(entities.Count);
                return entities.Count;
            }
        }

        return entities.Count; // недостижимо
    }
}