
using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseKestrel(options =>
{
    // Три порта — три имитатора бирж
    options.Listen(IPAddress.Loopback, 5001);
    options.Listen(IPAddress.Loopback, 5002);
    options.Listen(IPAddress.Loopback, 5003);
});
builder.Logging.ClearProviders();
builder.Logging.AddConsole();

var app = builder.Build();
app.UseWebSockets();

var rng = new Random();
var tickers = new[] { "BTCUSD", "ETHUSD", "SOLUSD" };
var basePrices = new Dictionary<string, decimal>
{
    ["BTCUSD"] = 50000m, ["ETHUSD"] = 3000m, ["SOLUSD"] = 150m
};

// Форматы трёх бирж (шаблоны с плейсхолдерами)
// Binance: цена числом, unixtime ms
// Kraken: цена строкой, ISO-8601
// Coinbase: цена числом, unixtime seconds строкой

// --- /binance/ws ---
app.Map("/binance/ws", async (HttpContext ctx) =>
    await HandleExchange(ctx, "Binance", (ticker, price, vol) =>
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return $$"""{"s":"{{ticker}}","p":{{price}},"q":{{vol}},"t":{{nowMs}}}""";
    }, rng));

// --- /kraken/ws ---
app.Map("/kraken/ws", async (HttpContext ctx) =>
    await HandleExchange(ctx, "Kraken", (ticker, price, vol) =>
    {
        // Kraken: ticker = BTC/USD, цена строкой, время ISO
        var pair = $"{ticker[..3]}/{ticker[3..]}"; // BTCUSD -> BTC/USD
        var nowIso = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        return $$"""{"pair":"{{pair}}","last":"{{price}}","vol":"{{vol}}","time":"{{nowIso}}"}""";
    }, rng));

// --- /coinbase/ws ---
app.Map("/coinbase/ws", async (HttpContext ctx) =>
    await HandleExchange(ctx, "Coinbase", (ticker, price, vol) =>
    {
        // Coinbase: ticker = BTC-USD, время unixtime seconds строкой
        var sym = $"{ticker[..3]}-{ticker[3..]}"; // BTCUSD -> BTC-USD
        var nowSec = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        return $$"""{"ticker":"{{sym}}","price":{{price}},"size":{{vol}},"ts":"{{nowSec}}"}""";
    }, rng));

Console.WriteLine("=== Exchange Simulators ===");
Console.WriteLine("Binance:  ws://localhost:5001/binance/ws");
Console.WriteLine("Kraken:   ws://localhost:5002/kraken/ws");
Console.WriteLine("Coinbase: ws://localhost:5003/coinbase/ws");
Console.WriteLine("===========================");

await app.RunAsync();

static async Task HandleExchange(
    HttpContext ctx,
    string name,
    Func<string, decimal, decimal, string> formatTick,
    Random rng)
{
    if (!ctx.WebSockets.IsWebSocketRequest)
    {
        ctx.Response.StatusCode = 400;
        return;
    }

    var socket = await ctx.WebSockets.AcceptWebSocketAsync();
    Console.WriteLine($"[{name}] client connected");

    using var cts = new CancellationTokenSource();
    var tickers = new[] { "BTCUSD", "ETHUSD", "SOLUSD" };
    var basePrices = new Dictionary<string, decimal>
    {
        ["BTCUSD"] = 50000m, ["ETHUSD"] = 3000m, ["SOLUSD"] = 150m
    };

    // Задача генерации тиков
    var generateTask = GenerateTicksAsync(socket, name, formatTick, tickers, basePrices, rng, cts.Token);

    // Задача чтения (ждёт close frame)
    var buffer = new byte[1024];
    try
    {
        while (socket.State == WebSocketState.Open)
        {
            var result = await socket.ReceiveAsync(buffer, CancellationToken.None);
            if (result.MessageType == WebSocketMessageType.Close)
                break;
        }
    }
    catch (WebSocketException) { }
    finally
    {
        cts.Cancel();
        await generateTask;

        if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
        {
            try { await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None); }
            catch { }
        }

        socket.Dispose();
        Console.WriteLine($"[{name}] client disconnected");
    }
}

static async Task GenerateTicksAsync(
    WebSocket socket,
    string name,
    Func<string, decimal, decimal, string> formatTick,
    string[] tickers,
    Dictionary<string, decimal> basePrices,
    Random rng,
    CancellationToken ct)
{
    var duplicateCounter = 0;

    while (!ct.IsCancellationRequested && socket.State == WebSocketState.Open)
    {
        try
        {
            await Task.Delay(10, ct); // ~100 тиков/сек на биржу

            var ticker = tickers[rng.Next(tickers.Length)];
            basePrices.TryGetValue(ticker, out var bp);

            var fluctuation = (decimal)((rng.NextDouble() - 0.5) * 0.02);
            var price = Math.Round(bp * (1m + fluctuation), 2);
            var volume = Math.Round((decimal)(rng.NextDouble() * 10), 4);

            var json = formatTick(ticker, price, volume);
            var bytes = Encoding.UTF8.GetBytes(json);

            await socket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, ct);

            // Дубликат каждый 20-й
            duplicateCounter++;
            if (duplicateCounter % 20 == 0)
            {
                await Task.Delay(3, ct);
                await socket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, ct);
            }

            // Случайный обрыв каждые 30-60 сек
            if (rng.Next(1000) < 3) // 0.3% шанс на каждом тике = ~раз в 3-5 сек (при 100/сек)
            {
                Console.WriteLine($"[{name}] crashing connection...");
                try { await socket.CloseAsync(WebSocketCloseStatus.EndpointUnavailable, "simulated crash", CancellationToken.None); }
                catch { }
                break;
            }
        }
        catch (WebSocketException)
        {
            break;
        }
        catch (OperationCanceledException)
        {
            break;
        }
    }
}
