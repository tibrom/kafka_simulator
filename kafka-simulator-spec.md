# kafka-sim — Спецификация CLI-симулятора Kafka-активности

Дата: 2026-09-05  
Статус: черновик

---

## Цель

Автономный CLI-инструмент, который подключается к реальному Kafka-кластеру и создаёт правдоподобную активность: топики, продюсеры, консьюмер-группы с разными поведениями. Нужен для ручного и регрессионного тестирования kfvisor на живых данных без необходимости поднимать реальные бизнес-сервисы.

---

## Требования верхнего уровня

- Язык: **Rust** (удобно переиспользовать rdkafka, уже используемый в kfvisor)
- Дистрибутив: один бинарник `kafka-sim`
- Конфигурация: один YAML-файл, путь задаётся аргументом (`--config sim.yaml`)
- Логирование: структурированное, уровень задаётся через env `RUST_LOG`
- Graceful shutdown: `Ctrl+C` — дождаться завершения текущих транзакций, вывести итоговую статистику

---

## Структура конфигурационного файла

```yaml
# sim.yaml

connection:
  bootstrap_servers:
    - "localhost:9092"
  sasl:
    mechanism: none            # none | plain | scram-sha-256 | scram-sha-512
    username: ""
    password: ""
  tls:
    enabled: false
    ca_cert_path: ""

# Топики, которые симулятор создаст при запуске (если не существуют).
topics:
  - name: orders
    partitions: 6
    replication_factor: 1
    config:
      retention.ms: 3600000   # 1 час — чтобы данные не накапливались

  - name: payments
    partitions: 3
    replication_factor: 1

  - name: notifications
    partitions: 12
    replication_factor: 1

# Продюсеры.
producers:
  - id: orders-producer
    topic: orders
    rate: 50                   # сообщений/сек в среднем
    burst:
      enabled: true
      every_seconds: 60        # каждые 60 сек
      multiplier: 5.0          # ×5 от базовой скорости
      duration_seconds: 10
    message:
      format: json
      template: |
        {
          "order_id": "{{uuid}}",
          "customer_id": "{{int 1000 9999}}",
          "amount": {{float 5.0 500.0}},
          "status": "{{choice pending processing shipped}}",
          "created_at": "{{iso_now}}"
        }
      key_strategy: field        # none | random | field | round_robin
      key_field: customer_id     # используется только при key_strategy: field

  - id: payments-producer
    topic: payments
    rate: 20
    message:
      format: json
      template: |
        {
          "payment_id": "{{uuid}}",
          "order_id": "{{uuid}}",
          "amount": {{float 5.0 500.0}},
          "method": "{{choice card wire crypto}}"
        }
      key_strategy: random

  - id: notifications-producer
    topic: notifications
    rate: 200
    message:
      format: string
      value: "User {{int 1 10000}} — event {{choice login logout purchase}}"
      key_strategy: none        # все сообщения без ключа → равномерно по партициям

# Консьюмер-группы.
consumer_groups:
  - id: order-processor
    topics: [orders]
    members: 3                  # количество одновременных консьюмеров
    behavior:
      type: normal              # normal | slow | lagging | intermittent | crashing
      processing_ms: 10         # время «обработки» одного сообщения (имитация бизнес-логики)
      commit_every: 100         # коммитить offset каждые N сообщений

  - id: payment-validator
    topics: [payments]
    members: 2
    behavior:
      type: slow
      processing_ms: 800        # медленный процессор → lag будет расти
      commit_every: 1

  - id: notification-sender
    topics: [notifications]
    members: 6
    behavior:
      type: normal
      processing_ms: 2
      commit_every: 500

  - id: audit-logger
    topics: [orders, payments]  # один consumer group читает несколько топиков
    members: 1
    behavior:
      type: intermittent        # периодически «засыпает»
      processing_ms: 50
      sleep_every_seconds: 30
      sleep_duration_seconds: 15

# Сценарии — последовательности событий, изменяющие поведение во время работы.
# Если не указаны, симулятор просто работает в базовом режиме бесконечно.
scenarios:
  - name: "Lagging consumer recovery"
    steps:
      - at_seconds: 0
        action: start_all

      - at_seconds: 60
        action: set_behavior
        target: payment-validator
        behavior:
          type: lagging
          processing_ms: 5000    # имитация полной остановки → lag растёт

      - at_seconds: 180
        action: set_behavior
        target: payment-validator
        behavior:
          type: normal           # «починили» — lag начинает сокращаться
          processing_ms: 50

      - at_seconds: 300
        action: stop_all

  - name: "Rebalance storm"
    steps:
      - at_seconds: 0
        action: start_all

      - at_seconds: 30
        action: add_members
        target: order-processor
        count: 5                  # добавить 5 консьюмеров → rebalance

      - at_seconds: 45
        action: remove_members
        target: order-processor
        count: 7                  # убрать 7 → rebalance снова

      - at_seconds: 300
        action: stop_all
```

---

## Типы поведений консьюмеров

| Тип | Описание |
|-----|----------|
| `normal` | Читает и коммитит в заданном темпе. Lag минимален. |
| `slow` | Обработка медленнее продакшн-rate → lag постоянно растёт. |
| `lagging` | Очень медленная обработка или полная остановка. Имитирует зависший consumer. |
| `intermittent` | Периодически «засыпает» на `sleep_duration_seconds`, потом восстанавливается. Вызывает periodic lag spikes. |
| `crashing` | С заданной вероятностью «падает» и перезапускается, вызывая rebalance. |
| `burst_consumer` | Нормально работает, но временами читает очень быстро (имитация backfill). |

---

## Стратегии ключей продюсера

| Стратегия | Описание |
|-----------|----------|
| `none` | Ключ не задаётся → rdkafka распределяет round-robin по партициям. |
| `random` | UUID на каждое сообщение → равномерное распределение. |
| `round_robin` | Явное распределение по партициям по очереди. |
| `field` | Значение указанного JSON-поля как ключ. Позволяет имитировать hot key (один customer_id → одна партиция получает весь его трафик). |
| `skewed` | 20% ключей получают 80% трафика → hot partition сценарий. |

---

## Шаблонные переменные сообщений

| Переменная | Пример результата |
|------------|-------------------|
| `{{uuid}}` | `"a3f8c2d1-..."` |
| `{{int MIN MAX}}` | `4271` |
| `{{float MIN MAX}}` | `123.45` |
| `{{choice val1 val2 val3}}` | `"val2"` |
| `{{iso_now}}` | `"2026-09-05T14:23:11.593Z"` |
| `{{seq}}` | монотонно возрастающий счётчик `0, 1, 2, ...` |
| `{{repeat N char}}` | строка из N символов (для имитации больших payload) |

---

## Выходные данные

При запуске программа выводит периодический (каждые 10 сек) отчёт:

```
[kafka-sim] 14:23:11 | producers: orders=51/s payments=19/s notifications=203/s
[kafka-sim] 14:23:11 | consumers: order-processor lag=0  payment-validator lag=12840  audit-logger lag=180
[kafka-sim] 14:23:11 | events: rebalances=0 errors=0 total_produced=7240 total_consumed=7058
```

При остановке — финальная сводка в JSON (опционально, через `--stats-output stats.json`).

---

## Аргументы командной строки

```
kafka-sim [OPTIONS]

Options:
  -c, --config <PATH>         Путь к YAML-конфигу [default: sim.yaml]
  -s, --scenario <NAME>       Запустить конкретный сценарий (иначе — бесконечный базовый режим)
      --dry-run               Проверить конфиг и подключение, не запуская продьюсеры/консьюмеры
      --stats-output <PATH>   Записать итоговую статистику в JSON-файл
      --no-create-topics      Не создавать топики, ожидать что они уже существуют
  -h, --help                  Показать справку
```

---

## Ключевые сценарии для тестирования kfvisor

| # | Сценарий | Что проверяет в kfvisor |
|---|----------|------------------------|
| 1 | Нормальная работа, lag ≈ 0 | Базовое отображение групп, корректный lag=0 |
| 2 | Медленный консьюмер — lag растёт | Цветовая индикация lag, обновление в реальном времени |
| 3 | Добавление / удаление консьюмеров | Rebalancing badge, изменение members |
| 4 | Hot key (skewed key strategy) | Неравномерное распределение по партициям в Offsets tab |
| 5 | Intermittent consumer | Периодический spike lag, восстановление |
| 6 | Группа полностью остановлена | Empty state, диагностический хинт |
| 7 | Много топиков (50+) | Производительность списка, lazy load |
| 8 | Большие сообщения (100KB+) | Корректный размер в UI, truncation |
| 9 | Несколько групп на один топик | Отображение в topic detail consumers tab |
| 10 | Offset reset после lagging | Проверка dry-run UI (будущий спринт) |

---

## Ограничения и неочевидные решения

**Imitate-only, не мокать**: симулятор работает с реальным Kafka, не с моком. Это принципиально — тестируем kfvisor против настоящего брокера.

**Consumer group naming**: группы именуются `kafka-sim-{id}` чтобы не пересекаться с реальными группами. Флаг `--prefix` позволяет изменить.

**Cleanup**: по умолчанию симулятор **не удаляет** топики и группы при остановке — это важно для воспроизведения конкретного состояния. Явный `--cleanup` удалит всё что создал.

**Партиции и replication**: симулятор не управляет ISR и репликацией. Для ISR/under-replicated сценариев нужен отдельный Kafka-кластер с конкретной топологией.

**Идемпотентность создания топиков**: при повторном запуске с теми же именами не падает — проверяет существование и пропускает создание.

---

## Технический стек

- `rdkafka` — продьюсеры и консьюмеры
- `tokio` — async runtime, один task на каждый продьюсер/консьюмер
- `serde_yaml` — парсинг конфига
- `clap` — CLI аргументы
- `tracing` / `tracing-subscriber` — логирование

---

## Структура проекта (предлагаемая)

```
kafka-sim/
├── Cargo.toml
├── sim.yaml                  # пример конфига
├── scenarios/
│   ├── basic.yaml
│   ├── lag-recovery.yaml
│   └── rebalance-storm.yaml
└── src/
    ├── main.rs
    ├── config.rs             # десериализация YAML
    ├── template.rs           # генерация сообщений из шаблонов
    ├── producer.rs           # продьюсер-агент
    ├── consumer.rs           # консьюмер-агент
    ├── scenario.rs           # планировщик сценариев
    └── stats.rs              # сбор и вывод статистики
```
