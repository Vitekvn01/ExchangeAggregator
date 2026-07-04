using ExchangeAggregator.Core.Models;

namespace ExchangeAggregator.Core.Interfaces;

/// <summary>
/// Нормализатор: превращает биржевой <see cref="RawTick"/> в единый <see cref="NormalizedTick"/>.
/// Одна реализация для всех бирж — маршрутизация по Exchange уже учтена в RawTick.
/// </summary>
public interface ITickNormalizer
{
    /// <summary>
    /// Нормализует raw-тик. Входной RawTick уже прошёл парсинг и приведён к типам.
    /// </summary>
    NormalizedTick Normalize(RawTick rawTick);
}
