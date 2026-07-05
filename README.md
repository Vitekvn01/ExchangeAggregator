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