using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;
using ExchangeAggregator.Api.Configuration;
using ExchangeAggregator.Api.WebSocket;
using ExchangeAggregator.Core.Metrics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace ExchangeAggregator.Tests;

/// <summary>
/// Интеграционный тест переподключения ExchangeWebSocketClient.
///
/// Сценарий:
///  1. Поднимаем мини-WebSocket-сервер (Binance-формат) на случайном порту.
///  2. Запускаем ExchangeWebSocketClient.
///  3. Убеждаемся, что клиент получает тики (сервер 1 жив).
///  4. Роняем сервер 1.
///  5. Ждём, пока клиент обнаружит обрыв и начнёт backoff-попытки.
///  6. Поднимаем сервер 2 на том же порту.
///  7. Убеждаемся, что клиент переподключился и снова получает тики.
///  8. Проверяем Reconnects >= 2 (первое подключение + переподключение).
/// </summary>
public class ReconnectionTests
{
    [Fact]
    public async Task Client_Reconnects_AfterServerCrash_AndKeepsReceivingData()
    {
        // ── arrange ──
        var metrics = new TickMetrics();
        var channel = Channel.CreateBounded<string>(new BoundedChannelOptions(10_000)
        {
            FullMode = BoundedChannelFullMode.DropWrite
        });

        var options = Options.Create(new AggregatorOptions
        {
            ReconnectDelayMinMs = 100,
            ReconnectDelayMaxMs = 1_000,
            IdleTimeoutSeconds = 3,
            ChannelCapacity = 10_000
        });

        // ── сервер 1 ──
        var (server1, port) = await StartBinanceServerAsync();
        var url = $"ws://localhost:{port}/binance/ws";

        var client = new ExchangeWebSocketClient(
            "Binance", url, channel.Writer, metrics, options,
            NullLogger<ExchangeWebSocketClient>.Instance);

        using var clientCts = new CancellationTokenSource();
        var clientTask = client.RunAsync(clientCts.Token);

        // ── act 1: тики от сервера 1 ──
        using var readCts1 = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var ticks1 = await DrainTicksAsync(channel.Reader, minCount: 3, readCts1.Token);
        Assert.True(ticks1 >= 3,
            $"Шаг 1 — недополучено тиков от сервера 1: получили {ticks1}, хотели минимум 3");

        long reconnectsBeforeCrash = metrics.Reconnects;
        Assert.True(reconnectsBeforeCrash >= 1,
            $"После первого подключения Reconnects должен быть >= 1, а он {reconnectsBeforeCrash}");

        // ── act 2: роняем сервер 1 ──
        await StopServerAsync(server1);

        // Ждём idle-таймаут (3с) + backoff (~100мс) + запас
        await Task.Delay(TimeSpan.FromSeconds(5), CancellationToken.None);

        // ── act 3: сервер 2 на том же порту ──
        var (server2, _) = await StartBinanceServerAsync(port);

        // Ждём переподключения
        await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

        // ── act 4: тики от сервера 2 ──
        using var readCts2 = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var ticks2 = await DrainTicksAsync(channel.Reader, minCount: 3, readCts2.Token);
        Assert.True(ticks2 >= 3,
            $"Шаг 4 — недополучено тиков после переподключения: получили {ticks2}, хотели минимум 3");

        long reconnectsAfter = metrics.Reconnects;
        Assert.True(reconnectsAfter >= 2,
            $"После переподключения Reconnects должен быть >= 2 (было {reconnectsBeforeCrash}, стало {reconnectsAfter})");

        // ── cleanup ──
        await StopServerAsync(server2);
        clientCts.Cancel();
        try { await clientTask; } catch (OperationCanceledException) { }
    }

    /// <summary>
    /// Интеграционный тест: при падении одной биржи остальные продолжают получать данные.
    ///
    /// Сценарий:
    ///  1. Поднимаем сервер Binance и сервер Kraken на разных портах.
    ///  2. Запускаем двух ExchangeWebSocketClient (каждый в свой канал).
    ///  3. Оба получают тики (Reconnects >= 1 для каждого).
    ///  4. Роняем сервер Binance.
    ///  5. Kraken продолжает получать тики (без перерыва).
    ///  6. Binance-клиент теряет соединение, начинает backoff.
    ///  7. Поднимаем сервер Binance заново на том же порту.
    ///  8. Binance-клиент переподключается и снова получает тики.
    /// </summary>
    [Fact]
    public async Task OneExchangeCrash_DoesNotAffectOthers()
    {
        // ── arrange: два сервера, два канала, два клиента ──
        var metrics = new TickMetrics();

        var binanceChannel = Channel.CreateBounded<string>(new BoundedChannelOptions(10_000)
        {
            FullMode = BoundedChannelFullMode.DropWrite
        });
        var krakenChannel = Channel.CreateBounded<string>(new BoundedChannelOptions(10_000)
        {
            FullMode = BoundedChannelFullMode.DropWrite
        });

        var options = Options.Create(new AggregatorOptions
        {
            ReconnectDelayMinMs = 100,
            ReconnectDelayMaxMs = 1_000,
            IdleTimeoutSeconds = 3,
            ChannelCapacity = 10_000
        });

        // Серверы
        var (serverBinance, binancePort) = await StartBinanceServerAsync();
        var (serverKraken, krakenPort) = await StartBinanceServerAsync(); // формат не важен, важна изоляция

        var binanceUrl = $"ws://localhost:{binancePort}/binance/ws";
        var krakenUrl = $"ws://localhost:{krakenPort}/binance/ws";

        var binanceClient = new ExchangeWebSocketClient(
            "Binance", binanceUrl, binanceChannel.Writer, metrics, options,
            NullLogger<ExchangeWebSocketClient>.Instance);
        var krakenClient = new ExchangeWebSocketClient(
            "Kraken", krakenUrl, krakenChannel.Writer, metrics, options,
            NullLogger<ExchangeWebSocketClient>.Instance);

        using var cts = new CancellationTokenSource();
        var binanceTask = binanceClient.RunAsync(cts.Token);
        var krakenTask = krakenClient.RunAsync(cts.Token);

        // ── act 1: оба получают тики ──
        using var readCts1 = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var binanceTicks1 = await DrainTicksAsync(binanceChannel.Reader, minCount: 3, readCts1.Token);
        var krakenTicks1 = await DrainTicksAsync(krakenChannel.Reader, minCount: 3, readCts1.Token);

        Assert.True(binanceTicks1 >= 3,
            $"Шаг 1 — Binance недополучил: {binanceTicks1}");
        Assert.True(krakenTicks1 >= 3,
            $"Шаг 1 — Kraken недополучил: {krakenTicks1}");

        // ── act 2: роняем Binance ──
        await StopServerAsync(serverBinance);
        await Task.Delay(TimeSpan.FromSeconds(5), CancellationToken.None);

        // ── act 3: Kraken продолжает получать тики ──
        using var readCts2 = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var krakenTicks2 = await DrainTicksAsync(krakenChannel.Reader, minCount: 3, readCts2.Token);
        Assert.True(krakenTicks2 >= 3,
            $"Шаг 3 — Kraken должен продолжать получать тики после падения Binance, получили: {krakenTicks2}");

        // ── act 4: переподнимаем Binance ──
        var (serverBinance2, _) = await StartBinanceServerAsync(binancePort);
        await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

        // ── act 5: Binance снова получает тики ──
        using var readCts3 = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var binanceTicks2 = await DrainTicksAsync(binanceChannel.Reader, minCount: 3, readCts3.Token);
        Assert.True(binanceTicks2 >= 3,
            $"Шаг 5 — Binance не восстановился после переподключения: {binanceTicks2}");

        // ── акцент: Kraken ни разу не прерывался ──
        // krakenClient не терял соединение → Reconnects у него (в этом клиенте) = 1.
        // Но метрика общая, так что просто убеждаемся что данные шли непрерывно.

        long totalReconnects = metrics.Reconnects;
        Assert.True(totalReconnects >= 3, // Binance: 2 (initial+reconnect), Kraken: 1 (initial) = 3
            $"Общий Reconnects должен быть >= 3 (Binance×2 + Kraken×1), фактически: {totalReconnects}");

        // ── cleanup ──
        await StopServerAsync(serverBinance2);
        await StopServerAsync(serverKraken);
        cts.Cancel();
        try { await Task.WhenAll(binanceTask, krakenTask); } catch (OperationCanceledException) { }
    }

    // ─────────────────────────────────────────────────────────────────
    //  Helpers
    // ─────────────────────────────────────────────────────────────────
    /// <summary>
    /// Поднимает Kestrel с endpoint /binance/ws на заданном (или случайном) порту.
    /// Использует WebApplication (современный API) + StartAsync.
    /// Возвращает (IHost, реальный порт).
    /// </summary>
    private static async Task<(IHost Host, int Port)> StartBinanceServerAsync(int port = 0)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseKestrel(opts =>
        {
                opts.Listen(IPAddress.Loopback, port);
        });
        builder.Logging.ClearProviders();

        var app = builder.Build();
                    app.UseWebSockets();
                    app.Run(HandleBinanceAsync);

        await app.StartAsync();

        // app.Urls заполняется после StartAsync, берём первый адрес
        var address = app.Urls.First();
        var actualPort = new Uri(address).Port;
        return (app, actualPort);
    }

    /// <summary>
    /// WebSocket handler — бесконечно шлёт тики в Binance-формате каждые 10 мс.
    /// </summary>
    private static async Task HandleBinanceAsync(HttpContext ctx)
    {
        if (!ctx.WebSockets.IsWebSocketRequest)
        {
            ctx.Response.StatusCode = 400;
            return;
        }

        var socket = await ctx.WebSockets.AcceptWebSocketAsync();
        var rng = new Random();
        var tickers = new[] { "BTCUSD", "ETHUSD", "SOLUSD" };
        var basePrices = new Dictionary<string, decimal>
        {
            ["BTCUSD"] = 50000m, ["ETHUSD"] = 3000m, ["SOLUSD"] = 150m
        };
        try
        {
            while (socket.State == WebSocketState.Open)
            {
                await Task.Delay(10, ctx.RequestAborted);

                var ticker = tickers[rng.Next(tickers.Length)];
                var bp = basePrices[ticker];
                var fluctuation = (decimal)((rng.NextDouble() - 0.5) * 0.02);
                var price = Math.Round(bp * (1m + fluctuation), 2);
                var volume = Math.Round((decimal)(rng.NextDouble() * 10), 4);
                var epochMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                var priceStr = price.ToString(System.Globalization.CultureInfo.InvariantCulture);
                var volStr = volume.ToString(System.Globalization.CultureInfo.InvariantCulture);

                var json =
                    $$"""{"s":"{{ticker}}","p":{{priceStr}},"q":{{volStr}},"t":{{epochMs}}}""";
                var bytes = Encoding.UTF8.GetBytes(json);
                await socket.SendAsync(
                    new ArraySegment<byte>(bytes),
                    WebSocketMessageType.Text,
                    true,
                    ctx.RequestAborted);
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
        finally
        {
            socket.Dispose();
        }
    }

    /// <summary>
    /// Читает из канала минимум minCount сообщений, но не дольше ct.
    /// Возвращает реальное количество прочитанных.
    /// </summary>
    private static async Task<int> DrainTicksAsync(
        ChannelReader<string> reader, int minCount, CancellationToken ct)
    {
        var count = 0;
        try
        {
            while (count < minCount && await reader.WaitToReadAsync(ct))
            {
                while (reader.TryRead(out _))
                    count++;
            }
        }
        catch (OperationCanceledException) { }
        return count;
    }

    private static async Task StopServerAsync(IHost host)
    {
        try
        {
            await host.StopAsync(TimeSpan.FromSeconds(3));
        }
        catch { }
        host.Dispose();
    }
}
