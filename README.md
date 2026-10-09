# kafka-sim — CLI-симулятор Kafka-активности

Инструмент для создания правдоподобной нагрузки на реальный Kafka-кластер: топики, продюсеры с шаблонами сообщений, консьюмер-группы с разными поведениями, сценарии с таймингом. Предназначен для тестирования kfvisor на живых данных без реальных бизнес-сервисов.

---

## Сборка

Требования: Rust 1.70+, CMake (для сборки librdkafka из исходников).

```bash
# macOS
brew install cmake

# Сборка
cargo build --release

# Бинарник будет в:
./target/release/kafka-sim
```

---

## Быстрый старт

```bash
# Проверить конфиг и подключение к Kafka (без запуска продьюсеров/консьюмеров)
kafka-sim --config sim.yaml --dry-run

# Запустить в базовом режиме (работает бесконечно, Ctrl+C для остановки)
kafka-sim --config sim.yaml

# Запустить конкретный сценарий
kafka-sim --config sim.yaml --scenario "Lagging consumer recovery"

# Сохранить итоговую статистику в JSON
kafka-sim --config sim.yaml --stats-output stats.json
```

---

## Аргументы командной строки

| Аргумент | По умолчанию | Описание |
|----------|-------------|----------|
| `-c`, `--config <PATH>` | `sim.yaml` | Путь к YAML-конфигу |
| `-s`, `--scenario <NAME>` | — | Запустить именованный сценарий |
| `--dry-run` | — | Проверить конфиг и соединение, затем выйти |
| `--stats-output <PATH>` | — | Записать итоговую статистику в JSON-файл |
| `--no-create-topics` | — | Не создавать топики, ожидать что они уже есть |
| `--prefix <STR>` | `kafka-sim` | Префикс для ID консьюмер-групп |
| `--cleanup` | — | Удалить созданные топики при выходе |

Уровень логирования задаётся через переменную окружения:

```bash
RUST_LOG=info kafka-sim --config sim.yaml
RUST_LOG=debug kafka-sim --config sim.yaml  # подробные логи
```

---

## Структура конфига (`sim.yaml`)

### Подключение

```yaml
connection:
  bootstrap_servers:
    - "localhost:9092"
  sasl:
    mechanism: none          # none | plain | scram-sha-256 | scram-sha-512
    username: ""
    password: ""
  tls:
    enabled: false
    ca_cert_path: ""
```

### Топики

Создаются при запуске, если не существуют. При повторном запуске с теми же именами — пропускаются без ошибки.

```yaml
topics:
  - name: orders
    partitions: 6
    replication_factor: 1
    config:
      retention.ms: "3600000"   # любые конфиги топика Kafka
```

### Продюсеры

```yaml
producers:
  - id: orders-producer
    topic: orders
    rate: 50                    # сообщений/сек (среднее)
    burst:                      # опционально: всплески нагрузки
      enabled: true
      every_seconds: 60         # каждые 60 сек
      multiplier: 5.0           # ×5 от базовой скорости
      duration_seconds: 10      # длительность всплеска
    message:
      format: json              # json | string
      template: |
        {
          "order_id": "{{uuid}}",
          "amount": {{float 5.0 500.0}},
          "status": "{{choice pending processing shipped}}",
          "created_at": "{{iso_now}}"
        }
      key_strategy: field       # none | random | field | round_robin | skewed
      key_field: customer_id    # только при key_strategy: field
```

#### Шаблонные переменные

| Переменная | Пример результата | Описание |
|------------|-------------------|----------|
| `{{uuid}}` | `a3f8c2d1-...` | UUID v4 |
| `{{int 1 9999}}` | `4271` | Случайное целое в диапазоне |
| `{{float 5.0 500.0}}` | `123.45` | Случайное дробное в диапазоне |
| `{{choice a b c}}` | `b` | Случайный выбор из списка |
| `{{iso_now}}` | `2026-09-05T14:23:11.593Z` | Текущее время UTC |
| `{{seq}}` | `0, 1, 2, ...` | Монотонно возрастающий счётчик |
| `{{repeat 100 x}}` | `xxx...` (100 символов) | Повторение символа N раз |

#### Стратегии ключей

| Стратегия | Поведение |
|-----------|-----------|
| `none` | Без ключа — rdkafka round-robin по партициям |
| `random` | UUID на каждое сообщение — равномерное распределение |
| `field` | Значение JSON-поля как ключ (имитирует hot key на одного customer) |
| `round_robin` | Явный цикл по партициям по очереди |
| `skewed` | 20% ключей получают 80% трафика — горячие партиции |

### Консьюмер-группы

```yaml
consumer_groups:
  - id: order-processor
    topics: [orders]
    members: 3                  # параллельных консьюмеров
    behavior:
      type: normal              # normal | slow | lagging | intermittent | crashing
      processing_ms: 10         # имитация времени обработки одного сообщения
      commit_every: 100         # коммитить offset каждые N сообщений

  - id: audit-logger
    topics: [orders, payments]  # один consumer group на несколько топиков
    members: 1
    behavior:
      type: intermittent
      processing_ms: 50
      sleep_every_seconds: 30   # периодически «засыпать»
      sleep_duration_seconds: 15
```

#### Типы поведений консьюмеров

| Тип | Описание |
|-----|----------|
| `normal` | Читает и коммитит в заданном темпе, lag ≈ 0 |
| `slow` | Медленная обработка, lag постоянно растёт |
| `lagging` | Очень медленная или полная остановка, имитирует зависший consumer |
| `intermittent` | Периодически «засыпает», создаёт periodic lag spikes |
| `crashing` | Случайно «падает» и перезапускается, вызывая rebalance |

ID группы в Kafka будет `{prefix}-{id}`, например `kafka-sim-order-processor`.

### Сценарии

Сценарии позволяют изменять поведение симулятора по времени. Если сценарий не задан — симулятор работает в базовом режиме бесконечно.

```yaml
scenarios:
  - name: "Lagging consumer recovery"
    steps:
      - at_seconds: 0
        action: start_all                   # запустить всё

      - at_seconds: 60
        action: set_behavior                # изменить поведение консьюмера
        target: payment-validator
        behavior:
          type: lagging
          processing_ms: 5000

      - at_seconds: 180
        action: set_behavior
        target: payment-validator
        behavior:
          type: normal
          processing_ms: 50

      - at_seconds: 300
        action: stop_all                    # остановить всё

  - name: "Rebalance storm"
    steps:
      - at_seconds: 0
        action: start_all

      - at_seconds: 30
        action: add_members                 # добавить консьюмеров → rebalance
        target: order-processor
        count: 5

      - at_seconds: 45
        action: remove_members             # убрать консьюмеров → rebalance
        target: order-processor
        count: 7

      - at_seconds: 300
        action: stop_all
```

#### Доступные действия сценариев

| Действие | Параметры | Описание |
|----------|-----------|----------|
| `start_all` | — | Запустить все продьюсеры и консьюмеры |
| `stop_all` | — | Остановить всё (graceful shutdown) |
| `set_behavior` | `target`, `behavior` | Изменить поведение консьюмер-группы на лету |
| `add_members` | `target`, `count` | Добавить N консьюмеров в группу → rebalance |
| `remove_members` | `target`, `count` | Убрать N консьюмеров из группы → rebalance |

---

## Периодический отчёт

Каждые 10 секунд в stdout выводится:

```
[kafka-sim] 14:23:11 | producers: orders-producer=51/s  payments-producer=19/s  notifications-producer=203/s
[kafka-sim] 14:23:11 | consumers: order-processor lag=0  payment-validator lag=12840  audit-logger lag=180
[kafka-sim] 14:23:11 | events: rebalances=0  errors=0  total_produced=7240  total_consumed=7058
```

При остановке (Ctrl+C) симулятор дожидается завершения текущих операций и выводит финальную сводку.

---

## Готовые конфиги сценариев

В папке `scenarios/` находятся самодостаточные конфиги для типичных тест-кейсов:

| Файл | Описание |
|------|----------|
| `scenarios/basic.yaml` | Нормальная работа, lag ≈ 0 |
| `scenarios/lag-recovery.yaml` | Lag растёт, затем консьюмер «восстанавливается» |
| `scenarios/rebalance-storm.yaml` | Волна rebalance через add/remove members |

```bash
kafka-sim --config scenarios/lag-recovery.yaml --scenario "Lagging consumer recovery"
```

---

## Структура проекта

```
kafka_simulator/
├── Cargo.toml
├── sim.yaml                  # основной пример конфига
├── scenarios/
│   ├── basic.yaml
│   ├── lag-recovery.yaml
│   └── rebalance-storm.yaml
└── src/
    ├── main.rs               # CLI, оркестрация, graceful shutdown
    ├── config.rs             # десериализация YAML
    ├── template.rs           # генерация сообщений из шаблонов
    ├── producer.rs           # продьюсер-агент
    ├── consumer.rs           # консьюмер-агент + lag polling
    ├── scenario.rs           # планировщик сценариев
    └── stats.rs              # сбор и вывод статистики
```
