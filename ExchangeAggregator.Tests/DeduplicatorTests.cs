using ExchangeAggregator.Core.Models;
using ExchangeAggregator.Infrastructure.Deduplication;

namespace ExchangeAggregator.Tests;

/// <summary>
/// Тесты дедупликатора.
/// 
/// Тест 1: базовый — одинаковый тик второй раз = дубликат.
/// Тест 2: разная цена = не дубликат (ключевое после фикса Coinbase).
/// Тест 3: конкурентная запись из 8 потоков — потокобезопасность.
/// </summary>
public class DeduplicatorTests
{
    [Fact]
    public void SameTickTwice_IsDuplicate()
    {
        using var dedup = new Deduplicator(TimeSpan.FromMinutes(5));
        var tick = CreateTick("BTCUSD", "Binance", 50000m, 1700000000123);

        bool first = dedup.IsDuplicate(tick);  // первый раз — не дубликат
        bool second = dedup.IsDuplicate(tick); // второй раз — дубликат

        Assert.False(first);
        Assert.True(second);
        Assert.Equal(1, dedup.Count);
    }

    [Fact]
    public void DifferentPrice_SameTimestamp_NotDuplicate()
    {
        using var dedup = new Deduplicator(TimeSpan.FromMinutes(5));

        // Имитация Coinbase: два тика в одну секунду, но с разной ценой
        var tick1 = CreateTick("BTCUSD", "Coinbase", 50000m, 1700000000_000);
        var tick2 = CreateTick("BTCUSD", "Coinbase", 50001m, 1700000000_000);

        bool first = dedup.IsDuplicate(tick1);
        bool second = dedup.IsDuplicate(tick2);

        Assert.False(first);
        Assert.False(second); // разная цена → не дубликат!
        Assert.Equal(2, dedup.Count);
    }

    [Fact]
    public void DifferentExchange_SameOtherFields_NotDuplicate()
    {
        using var dedup = new Deduplicator(TimeSpan.FromMinutes(5));

        var binanceTick = CreateTick("BTCUSD", "Binance", 50000m, 1700000000123);
        var krakenTick = CreateTick("BTCUSD", "Kraken", 50000m, 1700000000123);

        Assert.False(dedup.IsDuplicate(binanceTick));
        Assert.False(dedup.IsDuplicate(krakenTick)); // разные биржи → не дубликат
    }

    /// <summary>
    /// Ломающий тест: потокобезопасность дедупликатора.
    /// 8 потоков одновременно пишут по 100_000 тиков.
    /// Проверяем: нет исключений, нет ложных дубликатов, Count корректен.
    /// </summary>
    [Fact]
    public void ConcurrentWrites_NoRaceConditions()
    {
        using var dedup = new Deduplicator(TimeSpan.FromMinutes(5));
        const int threadCount = 8;
        const int ticksPerThread = 100_000;

        var threads = new Thread[threadCount];
        var duplicates = new long[threadCount];
        var exceptions = new Exception?[threadCount];

        for (int t = 0; t < threadCount; t++)
        {
            int threadIndex = t;
            threads[t] = new Thread(() =>
            {
                try
                {
                    long dupCount = 0;
                    for (int i = 0; i < ticksPerThread; i++)
                    {
                        // Каждый тик уникален: своя цена и свой timestamp
                        var tick = new NormalizedTick
                        {
                            Ticker = "BTCUSD",
                            Exchange = "Binance",
                            Price = 50000m + (threadIndex * ticksPerThread + i) * 0.01m,
                            Volume = 1.0m,
                            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(
                                1700000000000 + threadIndex * ticksPerThread + i)
                        };

                        if (dedup.IsDuplicate(tick))
                            dupCount++;
                    }
                    duplicates[threadIndex] = dupCount;
                }
                catch (Exception ex)
                {
                    exceptions[threadIndex] = ex;
                }
            });
            threads[t].Start();
        }

        for (int t = 0; t < threadCount; t++)
            threads[t].Join();

        // Не было исключений
        for (int t = 0; t < threadCount; t++)
            Assert.Null(exceptions[t]);

        // Дубликатов быть не должно — каждый тик уникален
        long totalDuplicates = 0;
        for (int t = 0; t < threadCount; t++)
            totalDuplicates += duplicates[t];
        Assert.Equal(0, totalDuplicates);

        // Count = общему числу уникальных тиков
        Assert.Equal(threadCount * ticksPerThread, dedup.Count);
    }

    // --- helpers ---

    private static NormalizedTick CreateTick(string ticker, string exchange, decimal price, long timestampMs)
    {
        return new NormalizedTick
        {
            Ticker = ticker,
            Exchange = exchange,
            Price = price,
            Volume = 1.0m,
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(timestampMs)
        };
    }
}
