using System.Globalization;
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
    options.Listen(IPAddress.Loopback, 5001);
    options.Listen(IPAddress.Loopback, 5002);
    options.Listen(IPAddress.Loopback, 5003);
});
builder.Logging.ClearProviders();
builder.Logging.AddConsole();

var app = builder.Build();
app.UseWebSockets();
var rng = new Random();

string Fmt(decimal d) => d.ToString(CultureInfo.InvariantCulture);

app.Map("/binance/ws", async (HttpContext ctx) =>
    await HandleExchange(ctx, "Binance", (ticker, price, vol) =>
        $$"""{"s":"{{ticker}}","p":{{Fmt(price)}},"q":{{Fmt(vol)}},"t":{{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}}}""", rng));

app.Map("/kraken/ws", async (HttpContext ctx) =>
    await HandleExchange(ctx, "Kraken", (ticker, price, vol) =>
    {
        var pair = $"{ticker[..3]}/{ticker[3..]}";
        var iso = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        return $$"""{"pair":"{{pair}}","last":"{{Fmt(price)}}","vol":"{{Fmt(vol)}}","time":"{{iso}}"}""";
    }, rng));

app.Map("/coinbase/ws", async (HttpContext ctx) =>
    await HandleExchange(ctx, "Coinbase", (ticker, price, vol) =>
    {
        var sym = $"{ticker[..3]}-{ticker[3..]}";
        var sec = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        return $$"""{"ticker":"{{sym}}","price":{{Fmt(price)}},"size":{{Fmt(vol)}},"ts":"{{sec}}"}""";
    }, rng));

Console.WriteLine("=== Exchange Simulators ===");
Console.WriteLine("Binance:  ws://localhost:5001/binance/ws");
Console.WriteLine("Kraken:   ws://localhost:5002/kraken/ws");
Console.WriteLine("Coinbase: ws://localhost:5003/coinbase/ws");
Console.WriteLine("===========================");

await app.RunAsync();

static async Task HandleExchange(HttpContext ctx, string name, Func<string, decimal, decimal, string> formatTick, Random rng)
{
    if (!ctx.WebSockets.IsWebSocketRequest) { ctx.Response.StatusCode = 400; return; }
    var socket = await ctx.WebSockets.AcceptWebSocketAsync();
    Console.WriteLine($"[{name}] client connected");
    using var cts = new CancellationTokenSource();
    var tickers = new[] { "BTCUSD", "ETHUSD", "SOLUSD" };
    var basePrices = new Dictionary<string, decimal> { ["BTCUSD"] = 50000m, ["ETHUSD"] = 3000m, ["SOLUSD"] = 150m };
    var genTask = GenerateTicksAsync(socket, name, formatTick, tickers, basePrices, rng, cts.Token);
    var buf = new byte[1024];
    try
    {
        while (socket.State == WebSocketState.Open)
        {
            var result = await socket.ReceiveAsync(buf, CancellationToken.None);
            if (result.MessageType == WebSocketMessageType.Close) break;
        }
    }
    catch (WebSocketException) { }
    finally
    {
        cts.Cancel(); await genTask;
        if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
            try { await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None); } catch { }
        socket.Dispose();
        Console.WriteLine($"[{name}] client disconnected");
    }
}

static async Task GenerateTicksAsync(WebSocket socket, string name, Func<string, decimal, decimal, string> formatTick, string[] tickers, Dictionary<string, decimal> basePrices, Random rng, CancellationToken ct)
{
    var dup = 0;
    while (!ct.IsCancellationRequested && socket.State == WebSocketState.Open)
    {
        try
        {
            await Task.Delay(10, ct);
            var ticker = tickers[rng.Next(tickers.Length)];
            basePrices.TryGetValue(ticker, out var bp);
            var fluctuation = (decimal)((rng.NextDouble() - 0.5) * 0.02);
            var price = Math.Round(bp * (1m + fluctuation), 2);
            var volume = Math.Round((decimal)(rng.NextDouble() * 10), 4);
            var json = formatTick(ticker, price, volume);
            var bytes = Encoding.UTF8.GetBytes(json);
            var seg = new ArraySegment<byte>(bytes);
            await socket.SendAsync(seg, WebSocketMessageType.Text, true, ct);
            dup++;
            if (dup % 20 == 0)
            {
                await Task.Delay(3, ct);
                await socket.SendAsync(seg, WebSocketMessageType.Text, true, ct);
            }
            if (rng.Next(1000) < 3)
            {
                Console.WriteLine($"[{name}] crashing connection...");
                try { await socket.CloseAsync(WebSocketCloseStatus.EndpointUnavailable, "crash", CancellationToken.None); } catch { }
                break;
            }
        }
        catch (WebSocketException) { break; }
        catch (OperationCanceledException) { break; }
    }
}