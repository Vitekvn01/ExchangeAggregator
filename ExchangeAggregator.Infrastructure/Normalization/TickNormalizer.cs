using ExchangeAggregator.Core.Interfaces;
using ExchangeAggregator.Core.Models;

namespace ExchangeAggregator.Infrastructure.Normalization;

/// <summary>
/// Нормализатор: RawTick → NormalizedTick.
/// Тривиальный маппинг, т.к. RawTick уже содержит все поля в правильных типах.
/// </summary>
public sealed class TickNormalizer : ITickNormalizer
{
    public NormalizedTick Normalize(RawTick rawTick)
    {
        return new NormalizedTick
        {
            Ticker = rawTick.Ticker,
            Price = rawTick.Price,
            Volume = rawTick.Volume,
            Timestamp = rawTick.Timestamp,
            Exchange = rawTick.Exchange
        };
    }
}
