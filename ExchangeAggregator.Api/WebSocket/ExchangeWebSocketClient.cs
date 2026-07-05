using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;
using ExchangeAggregator.Api.Configuration;
using ExchangeAggregator.Core.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ExchangeAggregator.Api.WebSocket;

/// <summary>
/// Обёртка над ClientWebSocket для подключения к одному имитатору биржи.
/// 
/// - Экспоненциальный backoff при обрывах (многократный, не одноразовый).
/// - Idle-таймаут: если данных нет дольше заданного порога — считаем соединение зависшим и переподключаемся.
/// - Читает сообщения и пишет их в выходной канал.
/// </summary>
public sealed class ExchangeWebSocketClient : IDisposable
{
    private readonly string _name;
    private readonly string _url;
    private readonly ChannelWriter<string> _output;
    private readonly TickMetrics _metrics;
    private readonly ILogger _logger;
    private readonly TimeSpan _reconnectDelayMin;
    private readonly TimeSpan _reconnectDelayMax;
    private readonly TimeSpan _idleTimeout;

    private ClientWebSocket? _socket;
    private CancellationTokenSource? _internalCts;

    public ExchangeWebSocketClient(
        string name,
        string url,
        ChannelWriter<string> output,
        TickMetrics metrics,
        IOptions<AggregatorOptions> options,
        ILogger<ExchangeWebSocketClient> logger)
    {
        _name = name;
        _url = url;
        _output = output;
        _metrics = metrics;
        _logger = logger;
        _reconnectDelayMin = TimeSpan.FromMilliseconds(options.Value.ReconnectDelayMinMs);
        _reconnectDelayMax = TimeSpan.FromMilliseconds(options.Value.ReconnectDelayMaxMs);
        _idleTimeout = TimeSpan.FromSeconds(options.Value.IdleTimeoutSeconds);
    }

    /// <summary>
    /// Запускает бесконечный цикл чтения с переподключениями. Работает до отмены ct.
    /// </summary>
    public async Task RunAsync(CancellationToken ct)
    {
        var attempt = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (attempt > 0)
                {
                    var delay = GetBackoffDelay(attempt);
                    _logger.LogInformation("[{Exchange}] переподключение через {Delay}мс (попытка {Attempt})",
                        _name, delay.TotalMilliseconds, attempt);
                    await Task.Delay(delay, ct);
                }

                // Освобождаем предыдущий CTS перед созданием нового — предотвращаем утечку
                _internalCts?.Dispose();
                _internalCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                await ConnectAndReadAsync(_internalCts.Token);
                attempt = 0; // успешно — сбрасываем счётчик
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break; // штатная остановка
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[{Exchange}] ошибка в цикле чтения, переподключаемся", _name);
                attempt++;
            }
            finally
            {
                await DisposeSocketAsync();
            }
        }
    }

    private async Task ConnectAndReadAsync(CancellationToken ct)
    {
        _socket = new ClientWebSocket();
        await _socket.ConnectAsync(new Uri(_url), ct);

        _metrics.IncrementReconnects();
        _logger.LogInformation("[{Exchange}] подключён к {Url}", _name, _url);

        var buffer = new byte[4096];

        while (_socket.State == WebSocketState.Open && !ct.IsCancellationRequested)
        {
            // Idle-таймаут: каждое ReceiveAsync ждёт не дольше _idleTimeout
            using var timeoutCts = new CancellationTokenSource(_idleTimeout);
            using var receiveCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

            try
            {
                var result = await _socket.ReceiveAsync(buffer, receiveCts.Token);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    _logger.LogWarning("[{Exchange}] сервер закрыл соединение", _name);
                    break;
                }

                if (result.MessageType == WebSocketMessageType.Text)
                {
                    var json = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    _metrics.IncrementReceived();

                    if (!_output.TryWrite(json))
                    {
                        _metrics.IncrementDropped();
                        _logger.LogWarning("[{Exchange}] канал переполнен, тик отброшен", _name);
                    }
                }
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
            {
                _logger.LogWarning("[{Exchange}] idle-таймаут ({Timeout}с), переподключаемся",
                    _name, _idleTimeout.TotalSeconds);
                break;
            }
        }
    }

    private TimeSpan GetBackoffDelay(int attempt)
    {
        // Экспоненциальный backoff с jitter ±25%
        var ms = _reconnectDelayMin.TotalMilliseconds * Math.Pow(2, Math.Min(attempt - 1, 8));
        ms = Math.Min(ms, _reconnectDelayMax.TotalMilliseconds);
        var jitter = Random.Shared.Next(-25, 25) / 100.0; // ±25%
        ms += ms * jitter;
        return TimeSpan.FromMilliseconds(Math.Max(ms, 0));
    }

    private async Task DisposeSocketAsync()
    {
        if (_socket is { State: not (WebSocketState.Closed or WebSocketState.Aborted) })
        {
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                await _socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "", cts.Token);
            }
            catch
            {
                // Сокет может быть уже в невалидном состоянии — молча закрываем
            }
        }
        _socket?.Dispose();
        _socket = null;
    }

    public void Dispose()
    {
        _internalCts?.Cancel();
        _internalCts?.Dispose();
        _socket?.Dispose();
    }
}