using ExchangeAggregator.Core.Models;

namespace ExchangeAggregator.Core.Interfaces;

/// <summary>
/// Хранилище нормализованных тиков.
/// Сохраняет батчами, обрабатывает ошибки записи осознанно (retry / fallback).
/// </summary>
public interface ITickStore
{
    /// <summary>
    /// Записать батч нормализованных тиков в БД.
    /// В случае ошибки — реализует retry-логику и/или возвращает количество
    /// НЕ записанных тиков (чтобы вызывающий код мог их учесть).
    /// </summary>
    /// <param name="ticks">Батч тиков.</param>
    /// <param name="ct">Токен отмены.</param>
    /// <returns>Количество тиков, которые НЕ удалось записать.</returns>
    Task<int> WriteBatchAsync(IReadOnlyCollection<NormalizedTick> ticks, CancellationToken ct);
}
