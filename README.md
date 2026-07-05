# ExchangeAggregator

Агрегатор криптовалютных котировок с трёх бирж (Binance, Kraken, Coinbase) в реальном времени.
Подключается к WebSocket-имитаторам, получает тики, нормализует, дедуплицирует и пишет в PostgreSQL.

---

## Быстрый старт

### 1. Поднять PostgreSQL

```bash
docker run -d --name pg-agg \
  -e POSTGRES_PASSWORD=postgres \
  -p 5435:5432 \
  postgres:16

docker exec -it pg-agg psql -U postgres -c "CREATE DATABASE exchange_aggregator;"
```

### 2. Применить миграции

```bash
dotnet ef database update \
  --project ExchangeAggregator.Infrastructure \
  --startup-project ExchangeAggregator.Api
```

### 3. Запустить имитаторы бирж

```bash
dotnet run --project ExchangeAggregator.Simulators
```

Поднимутся три WebSocket-сервера:

| Биржа     | Порт | Endpoint               |
|-----------|------|------------------------|
| Binance   | 5001 | `/binance/ws`          |
| Kraken    | 5002 | `/kraken/ws`           |
| Coinbase  | 5003 | `/coinbase/ws`         |

Имитаторы шлют тики каждые ~10 мс со случайной флуктуацией цены ±1%. Примерно 0.3% тиков
намеренно теряются (обрыв socket) — чтобы проверить логику переподключения.

### 4. Запустить агрегатор

```bash
dotnet run --project ExchangeAggregator.Api
```

### 5. Запустить тесты

```bash
dotnet test ExchangeAggregator.Tests
```

---

## Архитектура

```
  Simulators (Kestrel, 3 порта)
       │           │           │
  ┌────▼───┐ ┌─────▼────┐ ┌───▼──────┐
  │Binance │ │  Kraken  │ │ Coinbase │   WebSocket-клиенты (3 потока)
  │Client  │ │  Client  │ │  Client  │   + backoff-переподключение
  └────┬───┘ └────┬─────┘ └────┬─────┘
       │          │            │
       └──────────┼────────────┘
                  │  Channel<string> (bounded, DropWrite)
           ┌──────▼───────┐
           │   Pipeline    │              Parse → Normalize → Dedup → Batch → Store
           └──────┬───────┘
                  │  batch insert (retry 3×)
           ┌──────▼───────┐
           │  PostgreSQL   │
           └──────────────┘
```

**3 слоя решения:**

| Слой | Проект | Содержит |
|---|---|---|
| **Core** | `ExchangeAggregator.Core` | Модели (`RawTick`, `NormalizedTick`, `DedupKey`), интерфейсы (`ITickParser`, `IDeduplicator`, `ITickNormalizer`, `ITickStore`), метрики `TickMetrics` |
| **Infrastructure** | `ExchangeAggregator.Infrastructure` | Парсеры бирж, дедупликатор, нормализатор, `TickStore` (EF Core + миграции) |
| **Api** | `ExchangeAggregator.Api` | WebSocket-клиент, пайплайн, `Worker` (BackgroundService), DI-регистрация, `Program.cs` |

---

## Инженерные решения и trade-offs

### 1. Парсинг: TryParse + маршрутизация по имени биржи

Каждый `ITickParser` реализует `TryParse(string json) → RawTick?`. Парсер возвращает `null`,
если JSON не его формата. Пайплайн перебирает словарь `_parsers` до первого успеха.

```
foreach parser in parsers:
    raw = parser.TryParse(json)
    if raw != null → break
```

**Особенности форматов, которые пришлось обработать:**

- **Binance:** поля `s/p/q/t` — числа, timestamp в миллисекундах.
- **Kraken:** поля `last` и `vol` — **строки**, требуют `decimal.Parse()`. Timestamp — ISO 8601.
  `/` в тикере убирается: `BTC/USD → BTCUSD`.
- **Coinbase:** `ts` — **Unix-секунды строкой**, умножаем на 1000. `-` в тикере убирается:
  `BTC-USD → BTCUSD`.

**Trade-off:** решение "пробуем все парсеры подряд" — O(N) по числу бирж на каждый тик.
При 3 биржах незаметно. При 50+ имело бы смысл добавить быструю эвристику (первый символ JSON,
заголовок типа `"e":"kline"` у Binance и т.п.).

### 2. Нормализация: одна реализация для всех бирж

`ITickNormalizer` получает `RawTick` (уже с едиными типами: `decimal`, `DateTimeOffset`)
и отдаёт `NormalizedTick`. Вся маршрутизация по бирже — на уровне парсера; нормализатору
не нужно знать, откуда тик. Это позволяет добавлять биржи без изменения нормализатора.

### 3. Дедупликация: ConcurrentDictionary с ключом цена-в-ключе

**Ключ:** `DedupKey(Ticker + Exchange + TimestampUnixMs + Price)`.

Цена входит в ключ. Это принципиально: Coinbase шлёт тики раз в секунду, и два тика
в одну секунду с разной ценой — **разные тики, не дубликаты**. Без цены в ключе мы бы
теряли реальные изменения цены внутри секунды.

**Окно:** 5 минут. Таймер раз в минуту вычищает ключи старше окна.

```
кладём в словарь:  TryAdd(key, nowTicks)
не кладётся → уже был → дубликат
кладётся      → новый тик, пропускаем дальше
```
### 4. Батчевая запись в БД с retry

Pipeline накапливает NormalizedTick в батч (настраиваемый размер + интервал сброса).
При флаше батча вызывает `ITickStore.WriteBatchAsync`, который преобразует модели
в EF-entities и делает `AddRangeAsync` + `SaveChangesAsync`.

**Retry-логика:** до 3 попыток с экспоненциальным backoff (100ms → 200ms → 400ms).
Если все попытки провалились — батч дропается, тики считаются потерянными (`Dropped`).

### 5. Graceful shutdown и drain канала

При остановке приложения (`SIGTERM` / `Ctrl+C`):

1. `Worker.StopAsync` вызывает `Pipeline.Stop()` — отменяет внутренний `CancellationTokenSource`.
2. `ProcessLoopAsync` завершает цикл, сливая текущий батч в БД.
3. `DrainAndFlushAsync` дочитывает оставшиеся тики из канала с таймаутом
   (`DrainTimeoutSeconds`, по умолчанию 15 сек). Это гарантирует, что данные,
   уже отправленные WebSocket-клиентами в канал, не будут потеряны.

**Важно:** Drain не использует внешний `CancellationToken` — после `base.StopAsync`
он уже cancelled. Drain опирается только на собственный таймаут.

### 6. Переподключение WebSocket-клиентов

Каждый клиент (`ExchangeWebSocketClient`) работает в бесконечном цикле:

- При обрыве соединения — экспоненциальный backoff с jitter ±25%.
- Idle-таймаут: если данных нет дольше `IdleTimeoutSeconds` (по умолчанию 30 сек) —
  соединение считается зависшим, клиент переподключается.
- Клиенты изолированы: падение одной биржи не влияет на остальные.

### 7. Метрики и мониторинг

Все метрики потокобезопасны (`Interlocked`) и выводятся раз в 10 секунд:

| Метрика | Описание |
|---|---|
| `Received` | Получено сырых сообщений от WebSocket |
| `Parsed` | Успешно разобрано парсерами |
| `Duplicates` | Отфильтровано дедупликатором |
| `Written` | Записано в БД |
| `WriteErrors` | Ошибок при записи в БД (после ретраев) |
| `Dropped` | Безвозвратно потеряно (overflow канала + провал записи после ретраев) |
| `Reconnects` | Количество переподключений |
| `ChannelBacklog` | Текущая глубина канала обработки |

**Ответственность за метрику `Written`:** метрика увеличивается **только в pipeline**
после успешного вызова `WriteBatchAsync`. `TickStore` не трогает `Written` — он
отвечает только за запись в БД и увеличивает `WriteErrors`/`Dropped` при отказах.

### 8. Конфигурация

Все настройки в `appsettings.json`, секция `Aggregator`:

```json
{
  "Aggregator": {
    "Exchanges": [
      { "Name": "Binance",  "Url": "ws://localhost:5001/binance/ws" },
      { "Name": "Kraken",   "Url": "ws://localhost:5002/kraken/ws" },
      { "Name": "Coinbase", "Url": "ws://localhost:5003/coinbase/ws" }
    ],
    "ReconnectDelayMinMs": 500,
    "ReconnectDelayMaxMs": 30000,
    "IdleTimeoutSeconds": 30,
    "BatchSize": 100,
    "BatchFlushIntervalMs": 500,
    "ChannelCapacity": 10000,
    "DrainTimeoutSeconds": 15
  }
}
```

| Параметр | По умолчанию | Описание |
|---|---|---|
| `ReconnectDelayMinMs` | 500 | Начальная задержка перед переподключением |
| `ReconnectDelayMaxMs` | 30000 | Максимальная задержка (после экспоненциального роста) |
| `IdleTimeoutSeconds` | 30 | Таймаут бездействия сокета |
| `BatchSize` | 100 | Размер батча для записи в БД |
| `BatchFlushIntervalMs` | 500 | Максимальный интервал между сбросом батча |
| `ChannelCapacity` | 10000 | Ёмкость канала обработки (backpressure) |
| `DrainTimeoutSeconds` | 15 | Таймаут слива канала при остановке |

## Известные ограничения

### 1. Дедупликация — только в пределах жизни процесса

Ключи дедупликации хранятся в памяти (`ConcurrentDictionary`). При рестарте агрегатора
состояние теряется. За 5 минут (окно дедупликации) после рестарта возможны дубликаты,
если имитаторы перешлют старые тики. В продакшене стоило бы вынести ключи в Redis
с TTL = окно дедупликации.

### 2. Канал использует DropWrite при переполнении

При `ChannelCapacity = 10000` и `FullMode = DropWrite` новые тики отбрасываются,
если pipeline не успевает обрабатывать. Это осознанный выбор: лучше дропнуть свежий тик,
чем заблокировать WebSocket-клиентов. Каждый дроп учтён в метрике `Dropped`.

### 3. Нет персистентного буфера на случай краша процесса

Если процесс упадёт во время drain — тики в канале и батче потеряны безвозвратно.
В высоконагруженной системе можно было бы использовать persistent queue (Redis Streams,
Kafka), но для задания это избыточно.

### 4. Маршрутизация парсеров — O(N) по числу бирж

Пайплайн перебирает все парсеры для каждого тика. При 3 биржах — незаметно.
При 50+ стоило бы добавить быструю эвристику (первый символ JSON, поле-дискриминатор).

### 5. Нет health checks и readiness probes

Для Kubernetes-окружения стоило бы добавить `/health` (liveness) и `/ready` (readiness —
проверяет подключение к БД и наличие активных WebSocket-соединений). Не реализовано,
так как задание не подразумевает оркестрацию.

### 6. Нет гарантии порядка сообщений

Тики от разных бирж приходят в канал асинхронно — порядок обработки не гарантирован.
Для задачи агрегации это допустимо (каждый тик самодостаточен), но если бы требовалась
строгая очерёдность — потребовался бы другой дизайн.