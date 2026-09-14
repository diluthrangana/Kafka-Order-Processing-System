# Kafka Order Processing System: Full Implementation Plan

This plan is written so that it can be followed step by step, including by a smaller AI coding model. Do the phases in order. Do not start a phase until the previous phase's "Done when" check passes.

---

## 0. Requirements Recap (from the assignment)

| # | Requirement | How this plan satisfies it |
|---|---|---|
| 1 | Produce and consume **order messages** | `producer.py` sends orders, `consumer.py` reads them |
| 2 | **Avro serialization** with schema `order.avsc` (`orderId: string`, `product: string`, `price: float`) | Confluent Schema Registry + `AvroSerializer` / `AvroDeserializer` |
| 3 | **Real-time aggregation** (running average of prices) | `RunningAverage` class in the consumer, updated on every successful message (overall + per product) |
| 4 | **Retry logic** for temporary failures | In-consumer retry with exponential backoff (1s, 2s, 4s), max 3 retries |
| 5 | **Dead Letter Queue** for permanently failed messages | Failed messages forwarded to topic `orders-dlq` with error headers; `dlq_consumer.py` to inspect them |
| 6 | **Live demo** | Scripted demo in Section 9 |
| 7 | **Git repository** | Structure, `.gitignore`, commit plan, and README in Section 8 |

---

## 1. Technology Choices

| Component | Choice | Reason |
|---|---|---|
| Language | **Python 3.10+** | Least boilerplate, official Confluent client supports Avro well, easiest for an AI model to get right |
| Kafka client | `confluent-kafka` (with `avro` extra) | Official Confluent library, includes Schema Registry + Avro support |
| Kafka broker | `confluentinc/cp-kafka:7.6.1` in **KRaft mode** (no ZooKeeper) | Single container, modern setup |
| Schema storage | `confluentinc/cp-schema-registry:7.6.1` | Standard way to do Avro with Kafka |
| Visual UI (for demo) | `provectuslabs/kafka-ui` | Shows topics, messages, DLQ, and schemas in a browser |
| Infrastructure | Docker Compose | One command to start everything |

**Prerequisites to install:** Docker Desktop, Python 3.10+, Git, VS Code (optional).

---

## 2. System Architecture

```
                         ┌──────────────────────┐
                         │   Schema Registry    │
                         │   (localhost:8081)   │
                         └──────────▲───────────┘
                    registers/fetches│order.avsc schema
                                     │
┌──────────────┐   Avro bytes   ┌────┴─────┐    poll     ┌──────────────────────────┐
│ producer.py  ├───────────────►│ "orders" ├────────────►│       consumer.py        │
│ (random      │                │  topic   │             │ 1. Deserialize (Avro)    │
│  orders)     │                └──────────┘             │ 2. Validate              │
└──────────────┘                                         │ 3. Process + RETRY       │
                                                         │ 4. Update running avg    │
                                                         │ 5. Commit offset         │
                                                         └────────────┬─────────────┘
                                                                      │ on permanent failure
                                                                      ▼
                                                              ┌──────────────┐     ┌──────────────────┐
                                                              │ "orders-dlq" ├────►│ dlq_consumer.py  │
                                                              │    topic     │     │ (inspect/print)  │
                                                              └──────────────┘     └──────────────────┘
```

### Failure classification (important, explain this in the demo)

| Failure type | Example | Action |
|---|---|---|
| **Deserialization error** | Message bytes are not valid Avro | Send to DLQ immediately (retrying will never fix it) |
| **Permanent / validation error** | `price <= 0`, empty `orderId` or `product` | Send to DLQ immediately |
| **Temporary error** | Simulated downstream timeout (random) | Retry up to 3 times with backoff 1s → 2s → 4s |
| **Retries exhausted** | Temporary error still failing after 3 retries | Send to DLQ with `retry.count = 3` |

### Delivery guarantee
- Auto-commit is **disabled**. The consumer commits the offset **only after** the message is either processed successfully or safely written to the DLQ. This gives **at-least-once** processing and guarantees no message is silently lost.

---

## 3. Final Project Structure

```
kafka-order-system/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── .gitignore
├── schemas/
│   └── order.avsc
├── scripts/
│   └── create_topics.sh
└── src/
    ├── config.py
    ├── producer.py
    ├── consumer.py
    ├── dlq_consumer.py
    └── send_bad_message.py
```

All Python files are in one flat `src/` folder so that `from config import ...` works without packaging. Always run scripts from the repo root, e.g. `python src/producer.py`.

---

## 4. Phase 1: Infrastructure (Docker)

### 4.1 `docker-compose.yml`

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    hostname: kafka
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.1
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
      - schema-registry
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081
      DYNAMIC_CONFIG_ENABLED: "true"
```

Key point about listeners: containers talk to Kafka at `kafka:29092`; Python on the host machine uses `localhost:9092`.

### 4.2 Start and create topics

```bash
docker compose up -d
docker compose ps        # all 3 containers should be "running"
```

Create topics (these commands work in bash, PowerShell, and CMD):

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic orders --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic orders-dlq --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Put the same commands in `scripts/create_topics.sh` (add `#!/usr/bin/env bash` and `set -e` at the top).

### Done when
- `docker compose ps` shows 3 running containers.
- Topic list shows `orders` and `orders-dlq`.
- http://localhost:8080 opens Kafka UI and shows the cluster.
- http://localhost:8081/subjects returns `[]`.

**Git commit:** `chore: add docker-compose with Kafka (KRaft), Schema Registry and Kafka UI`

---

## 5. Phase 2: Schema, Config, Dependencies

### 5.1 `schemas/order.avsc`

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.assignment.orders",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "product", "type": "string" },
    { "name": "price",   "type": "float" }
  ]
}
```

Must match the assignment exactly: 3 fields, `price` is `float` (not `double`).

**Note:** Avro `float` is 32-bit, so `123.45` may come back as `123.44999694824219`. This is normal. Always print prices with `:.2f`.

### 5.2 `requirements.txt`

```
confluent-kafka[avro]>=2.3.0
```

### 5.3 Python virtual environment

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt
```

### 5.4 `src/config.py`

```python
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"

CONSUMER_GROUP = "order-processor"
DLQ_INSPECTOR_GROUP = "dlq-inspector"

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1.0

SCHEMA_PATH = os.path.join(BASE_DIR, "schemas", "order.avsc")


def load_schema_str():
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return f.read()
```

### 5.5 `.gitignore`

```
.venv/
__pycache__/
*.pyc
.env
.idea/
.vscode/
```

### Done when
- `python -c "import confluent_kafka; print(confluent_kafka.version())"` prints a version.

**Git commit:** `feat: add Avro order schema, config and dependencies`

---

## 6. Phase 3: Producer

### 6.1 Behaviour
- Generates orders with sequential `orderId` starting at `1001`, random `product` from `Item1`–`Item5`, random `price` between 10.00 and 500.00.
- A configurable percentage (`--invalid-rate`, default 10%) gets a **negative price** on purpose, so the DLQ can be demonstrated.
- Uses `orderId` as the message key.
- Uses `acks=all` and idempotence for reliable delivery.
- Prints a delivery report for every message.

### 6.2 `src/producer.py`

```python
import argparse
import random
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

import config

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def delivery_report(err, msg):
    key = msg.key().decode("utf-8") if msg.key() else None
    if err is not None:
        print(f"[PRODUCER] Delivery FAILED for key={key}: {err}")
    else:
        print(f"[PRODUCER] Delivered key={key} -> {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")


def make_order(order_id, invalid_rate):
    price = round(random.uniform(10.0, 500.0), 2)
    if random.random() < invalid_rate:
        price = -price  # intentionally invalid -> permanent failure -> DLQ
    return {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        "price": price,
    }


def main():
    parser = argparse.ArgumentParser(description="Order producer (Avro)")
    parser.add_argument("--count", type=int, default=0, help="Number of orders to send (0 = run forever)")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between messages")
    parser.add_argument("--start-id", type=int, default=1001, help="First orderId")
    parser.add_argument("--invalid-rate", type=float, default=0.1, help="Fraction of orders with invalid (negative) price")
    args = parser.parse_args()

    schema_registry_client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, config.load_schema_str(), lambda obj, ctx: obj)
    key_serializer = StringSerializer("utf_8")

    producer = Producer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
    })

    order_id = args.start_id
    sent = 0
    print(f"[PRODUCER] Sending to topic '{config.ORDERS_TOPIC}'. Press Ctrl+C to stop.")
    try:
        while args.count == 0 or sent < args.count:
            order = make_order(order_id, args.invalid_rate)
            producer.produce(
                topic=config.ORDERS_TOPIC,
                key=key_serializer(order["orderId"]),
                value=avro_serializer(order, SerializationContext(config.ORDERS_TOPIC, MessageField.VALUE)),
                on_delivery=delivery_report,
            )
            producer.poll(0)  # trigger delivery callbacks
            print(f"[PRODUCER] Produced order {order['orderId']} | {order['product']} | price={order['price']:.2f}")
            order_id += 1
            sent += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[PRODUCER] Stopping...")
    finally:
        producer.flush(10)
        print(f"[PRODUCER] Done. Sent {sent} orders.")


if __name__ == "__main__":
    main()
```

### Done when
- `python src/producer.py --count 5` prints 5 "Delivered" lines.
- http://localhost:8081/subjects now returns `["orders-value"]`.
- Kafka UI → Topics → `orders` → Messages shows the 5 orders decoded as JSON.

**Git commit:** `feat: add Avro order producer with random prices`

---

## 7. Phase 4: Consumer (Aggregation + Retry + DLQ)

### 7.1 Behaviour, in order, for every message
1. Poll a message from `orders`.
2. **Deserialize** the raw bytes with `AvroDeserializer`. The consumer reads raw bytes (no deserializer on the `Consumer` itself) so that if decoding fails, the original bytes can still be sent to the DLQ.
   - If it fails → DLQ with `error.type = DESERIALIZATION_ERROR`.
3. **Validate** (orderId and product non-empty, price > 0).
   - If it fails → DLQ with `error.type = VALIDATION_ERROR`, no retries.
4. **Process** with a simulated temporary failure chance (`--fail-rate`, default 0.3).
   - On temporary failure → retry after 1s, 2s, 4s.
   - After 3 failed retries → DLQ with `error.type = RETRIES_EXHAUSTED`.
5. On success → update running averages and print them.
6. **Commit the offset** (synchronously) only after step 5 or after the DLQ write succeeded.

### 7.2 `src/consumer.py`

```python
import argparse
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import config


# ---------- Error types ----------
class TemporaryProcessingError(Exception):
    """Recoverable error, e.g. a downstream service timeout. Should be retried."""


class PermanentProcessingError(Exception):
    """Unrecoverable error, e.g. invalid data. Must NOT be retried."""


class RetriesExhaustedError(Exception):
    def __init__(self, message, attempts):
        super().__init__(message)
        self.attempts = attempts


# ---------- Real-time aggregation ----------
class RunningAverage:
    def __init__(self):
        self.count = 0
        self.total = 0.0
        self.per_product = {}  # product -> [count, total]

    def add(self, product, price):
        self.count += 1
        self.total += price
        stats = self.per_product.setdefault(product, [0, 0.0])
        stats[0] += 1
        stats[1] += price

    def overall_average(self):
        return self.total / self.count if self.count else 0.0

    def product_average(self, product):
        c, t = self.per_product.get(product, [0, 0.0])
        return t / c if c else 0.0

    def summary(self):
        parts = [f"{p}={t / c:.2f}" for p, (c, t) in sorted(self.per_product.items())]
        return " | ".join(parts)


# ---------- Business logic ----------
def validate(order):
    if not order.get("orderId"):
        raise PermanentProcessingError("Missing orderId")
    if not order.get("product"):
        raise PermanentProcessingError("Missing product")
    price = order.get("price")
    if price is None or price <= 0:
        raise PermanentProcessingError(f"Invalid price: {price}")


def process(order, stats, fail_rate):
    validate(order)
    if random.random() < fail_rate:
        raise TemporaryProcessingError("Simulated temporary failure (downstream service timeout)")
    stats.add(order["product"], order["price"])


def process_with_retry(order, stats, fail_rate):
    """Returns the number of retries that were needed. Raises on permanent failure."""
    retries = 0
    while True:
        try:
            process(order, stats, fail_rate)
            return retries
        except TemporaryProcessingError as e:
            if retries >= config.MAX_RETRIES:
                raise RetriesExhaustedError(
                    f"Gave up after {config.MAX_RETRIES} retries. Last error: {e}", retries
                ) from e
            retries += 1
            backoff = config.BASE_BACKOFF_SECONDS * (2 ** (retries - 1))
            print(f"[RETRY]    order={order['orderId']} retry {retries}/{config.MAX_RETRIES} "
                  f"in {backoff:.0f}s (reason: {e})")
            time.sleep(backoff)


# ---------- DLQ ----------
def send_to_dlq(dlq_producer, msg, error_type, reason, retry_count):
    headers = [
        ("dlq.error.type", error_type),
        ("dlq.error.reason", reason),
        ("dlq.original.topic", msg.topic()),
        ("dlq.original.partition", str(msg.partition())),
        ("dlq.original.offset", str(msg.offset())),
        ("dlq.retry.count", str(retry_count)),
        ("dlq.failed.at", datetime.now(timezone.utc).isoformat()),
    ]
    # Forward the ORIGINAL raw bytes so nothing is lost or altered.
    dlq_producer.produce(config.DLQ_TOPIC, key=msg.key(), value=msg.value(), headers=headers)
    remaining = dlq_producer.flush(10)  # make sure it is written BEFORE committing the offset
    if remaining > 0:
        raise RuntimeError("DLQ write did not complete; offset will not be committed")
    key = msg.key().decode("utf-8") if msg.key() else None
    print(f"[DLQ]      key={key} sent to '{config.DLQ_TOPIC}' | type={error_type} | reason={reason}")


# ---------- Main loop ----------
def main():
    parser = argparse.ArgumentParser(description="Order consumer with running average, retry and DLQ")
    parser.add_argument("--fail-rate", type=float, default=0.3,
                        help="Probability of a simulated temporary failure per attempt (0.0 - 1.0)")
    args = parser.parse_args()

    schema_registry_client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client, config.load_schema_str())

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    dlq_producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS, "acks": "all"})

    stats = RunningAverage()
    running = True

    def shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    consumer.subscribe([config.ORDERS_TOPIC])
    print(f"[CONSUMER] Listening on '{config.ORDERS_TOPIC}' (fail-rate={args.fail_rate}). Ctrl+C to stop.")

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[CONSUMER] Kafka error: {msg.error()}")
                continue

            # 1. Deserialize
            try:
                order = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            except Exception as e:
                send_to_dlq(dlq_producer, msg, "DESERIALIZATION_ERROR", str(e), 0)
                consumer.commit(message=msg, asynchronous=False)
                continue

            if order is None:  # tombstone / empty value
                consumer.commit(message=msg, asynchronous=False)
                continue

            # 2-4. Validate + process with retry
            try:
                retries = process_with_retry(order, stats, args.fail_rate)
                retry_note = f" (after {retries} retries)" if retries else ""
                print(f"[OK]       order={order['orderId']} {order['product']} price={order['price']:.2f}{retry_note}"
                      f" | RUNNING AVG={stats.overall_average():.2f} (n={stats.count})"
                      f" | {order['product']} avg={stats.product_average(order['product']):.2f}")
            except PermanentProcessingError as e:
                send_to_dlq(dlq_producer, msg, "VALIDATION_ERROR", str(e), 0)
            except RetriesExhaustedError as e:
                send_to_dlq(dlq_producer, msg, "RETRIES_EXHAUSTED", str(e), e.attempts)

            # 5. Commit only after success or successful DLQ write
            consumer.commit(message=msg, asynchronous=False)
    finally:
        print("\n[CONSUMER] Shutting down...")
        print(f"[CONSUMER] Final running average: {stats.overall_average():.2f} over {stats.count} orders")
        if stats.count:
            print(f"[CONSUMER] Per product: {stats.summary()}")
        consumer.close()
        dlq_producer.flush(10)


if __name__ == "__main__":
    main()
```

### 7.3 `src/dlq_consumer.py` (inspect the Dead Letter Queue)

```python
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import config


def main():
    schema_registry_client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client, config.load_schema_str())

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": config.DLQ_INSPECTOR_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([config.DLQ_TOPIC])
    print(f"[DLQ-INSPECTOR] Reading '{config.DLQ_TOPIC}'. Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[DLQ-INSPECTOR] Kafka error: {msg.error()}")
                continue

            headers = {k: (v.decode("utf-8") if v else "") for k, v in (msg.headers() or [])}
            original_topic = headers.get("dlq.original.topic", config.ORDERS_TOPIC)
            try:
                payload = avro_deserializer(msg.value(), SerializationContext(original_topic, MessageField.VALUE))
            except Exception:
                payload = f"<undecodable payload, {len(msg.value() or b'')} bytes>"

            print("-" * 70)
            print(f"Payload     : {payload}")
            print(f"Error type  : {headers.get('dlq.error.type')}")
            print(f"Reason      : {headers.get('dlq.error.reason')}")
            print(f"Retries     : {headers.get('dlq.retry.count')}")
            print(f"Origin      : {original_topic} "
                  f"[partition {headers.get('dlq.original.partition')}, offset {headers.get('dlq.original.offset')}]")
            print(f"Failed at   : {headers.get('dlq.failed.at')}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

### 7.4 `src/send_bad_message.py` (demo a deserialization failure)

```python
from confluent_kafka import Producer

import config

producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS})
producer.produce(config.ORDERS_TOPIC, key=b"BAD-1", value=b"this is not avro data")
producer.flush(10)
print("[BAD] Sent a non-Avro message to 'orders'")
```

### Done when (test each one separately)

| Test | Command | Expected output |
|---|---|---|
| Normal flow | Consumer: `python src/consumer.py --fail-rate 0` / Producer: `python src/producer.py --count 10 --invalid-rate 0` | 10 `[OK]` lines, running average changes each time |
| Retry succeeds | Consumer with `--fail-rate 0.3` | Some `[RETRY]` lines followed by `[OK] ... (after N retries)` |
| Retries exhausted | Consumer with `--fail-rate 0.95` | `[RETRY]` 1/3, 2/3, 3/3 then `[DLQ] ... type=RETRIES_EXHAUSTED` |
| Validation → DLQ | Producer with `--invalid-rate 0.5` | `[DLQ] ... type=VALIDATION_ERROR reason=Invalid price: -...` with **no** retries |
| Bad bytes → DLQ | `python src/send_bad_message.py` | `[DLQ] key=BAD-1 ... type=DESERIALIZATION_ERROR` |
| DLQ inspection | `python src/dlq_consumer.py` | Every DLQ message printed with its headers |
| No message loss | Stop the consumer with Ctrl+C, produce 5 more, restart consumer | Consumer picks up exactly the 5 new orders (offsets were committed) |

Note: the running average lives in memory, so it resets when the consumer restarts. Mention this as a known limitation (see Section 10 for an optional improvement).

**Git commits (one per feature, in this order):**
1. `feat: add Avro consumer with running average aggregation`
2. `feat: add retry with exponential backoff for temporary failures`
3. `feat: add dead letter queue with error headers`
4. `feat: add DLQ inspector and bad-message test script`

---

## 8. Phase 5: Git Repository and README

### 8.1 Repository setup

```bash
git init
git add .
git commit -m "chore: initial project structure"
git branch -M main
git remote add origin https://github.com/<your-username>/kafka-order-system.git
git push -u origin main
```

Tip: commit after **each** phase (commit messages given above). A clear commit history shows independent work.

### 8.2 `README.md` must contain these sections

1. **Project title and one-paragraph overview**
2. **Architecture diagram** (copy the ASCII diagram from Section 2)
3. **Tech stack** (table from Section 1)
4. **Avro schema** (show `order.avsc` and explain Schema Registry)
5. **Features**
   - Running average (overall + per product)
   - Retry: max 3 retries, backoff 1s/2s/4s, only for temporary errors
   - DLQ: topic name, which errors go there, list of headers
   - At-least-once delivery via manual offset commit
6. **Prerequisites** (Docker, Python 3.10+, Git)
7. **How to run** (step-by-step commands from Phases 1–4)
8. **How to test each feature** (the table from 7.4)
9. **Project structure** (tree from Section 3)
10. **Known limitations & future improvements** (Section 10)

---

## 9. Phase 6: Live Demo Script (about 8–10 minutes)

Open **4 terminals** (all with the venv activated, in the repo root) plus a browser tab at http://localhost:8080.

| Step | Terminal / Screen | Action | What to say |
|---|---|---|---|
| 1 | T1 | `docker compose up -d` and `docker compose ps` | "Kafka in KRaft mode, Schema Registry, and Kafka UI are running." |
| 2 | Browser | Show topics `orders` and `orders-dlq` | "Two topics: main orders topic and the dead letter queue." |
| 3 | Editor | Show `schemas/order.avsc` | "Every message is Avro-encoded using this schema." |
| 4 | T2 | `python src/consumer.py --fail-rate 0.3` | "Consumer is waiting for orders." |
| 5 | T3 | `python src/producer.py --interval 1.5` | "Producer sends random orders every 1.5 seconds." |
| 6 | T2 | Point to `[OK]` lines | "The running average updates in real time, overall and per product." |
| 7 | T2 | Point to `[RETRY]` lines | "Temporary failures are retried with exponential backoff, then succeed." |
| 8 | T2 | Point to a `VALIDATION_ERROR` DLQ line | "Invalid prices are permanent failures, so they go straight to the DLQ without retrying." |
| 9 | T1 | `python src/send_bad_message.py` | "A corrupt, non-Avro message also goes to the DLQ instead of crashing the consumer." |
| 10 | T2 | Stop consumer, restart with `--fail-rate 0.95` | "Now the downstream service is almost always down: after 3 retries the message goes to the DLQ." |
| 11 | T4 | `python src/dlq_consumer.py` | "Every DLQ message keeps its original payload plus the error type, reason, retry count and original offset." |
| 12 | Browser | Schema Registry tab → `orders-value` | "The schema is registered centrally, so producer and consumer always agree." |
| 13 | Browser | GitHub repo | Show commit history and README. |

Before the demo: run everything once, and clear old data with `docker compose down -v` then `docker compose up -d` and recreate topics, so the demo starts clean.

---

## 10. Known Limitations and Optional Improvements (mention in README/viva)

- **In-memory average:** resets on restart. Improvement: publish averages to a compacted topic `order-price-averages` or use Kafka Streams / Faust for a stateful store.
- **Blocking retries:** while retrying, that consumer does not process other messages. Improvement: non-blocking retry topics (`orders-retry-1`, `orders-retry-2`) with delayed consumers.
- **Single consumer instance:** running average is global only when one instance runs. With several instances each would hold a partial average.
- **At-least-once:** a crash right after processing but before commit could process a message twice. Improvement: idempotent processing using `orderId`.

---

## 11. Likely Viva Questions (prepare short answers)

1. **Why Avro instead of JSON?** Compact binary format, schema enforced, supports schema evolution.
2. **What does Schema Registry do?** Stores schemas centrally; each message carries only a small schema ID, not the full schema.
3. **Difference between a temporary and a permanent failure?** Temporary may succeed on retry (timeout); permanent never will (bad data). Retrying permanent failures wastes time and blocks the partition.
4. **Why exponential backoff?** Gives the failing dependency time to recover and avoids hammering it.
5. **Why is auto-commit disabled?** To commit only after the message is fully handled, so no message is lost if the consumer crashes.
6. **Why send the original bytes to the DLQ?** So the message can be inspected or replayed exactly as received, even if it could not be decoded.
7. **What is KRaft?** Kafka's built-in consensus mode that replaces ZooKeeper.

---

## 12. Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `Failed to resolve 'kafka:29092'` from Python | Using the internal address from the host | Use `localhost:9092` in `config.py` |
| `Connection refused` on 8081 | Schema Registry still starting | Wait 20–30 seconds, check `docker logs schema-registry` |
| Consumer prints nothing | Offsets already committed for that group | Produce new messages, or change `CONSUMER_GROUP`, or `docker compose down -v` |
| `SerializationError` in producer | Price is not a number, or field names do not match schema | Field names must be exactly `orderId`, `product`, `price` |
| Schema compatibility error after editing `.avsc` | Old incompatible schema is registered | `docker compose down -v` to reset during development |
| `ModuleNotFoundError: config` | Running from the wrong folder | Run from repo root: `python src/consumer.py` |
| `pip install` fails on Windows | Old pip / Python | `python -m pip install --upgrade pip` then retry; use Python 3.10–3.12 |

---

## 13. How to Use This Plan with an AI Coding Model

Give the model **one phase at a time**. Suggested prompts:

1. "Create `docker-compose.yml` exactly as in Phase 1 of this plan. Do not change ports or listener names."
2. "Create `schemas/order.avsc`, `requirements.txt`, `.gitignore` and `src/config.py` exactly as in Phase 2."
3. "Create `src/producer.py` as in Phase 3. Keep the command-line arguments `--count`, `--interval`, `--start-id`, `--invalid-rate`."
4. "Create `src/consumer.py` as in Phase 4. Keep manual offset commit, the three error types, retry with 1s/2s/4s backoff, and DLQ headers."
5. "Create `src/dlq_consumer.py` and `src/send_bad_message.py` as in Phase 4."
6. "Write `README.md` with the 10 sections listed in Phase 5."

Rules to give the model:
- Do not rename topics, fields, or files.
- Do not switch libraries (use `confluent-kafka` only).
- Do not enable auto-commit in `consumer.py`.
- After each file, list the exact command to test it.

If something fails, paste the **full error message** to the model together with the file that produced it.

---

## 14. Final Submission Checklist

- [ ] `docker compose up -d` starts all services without errors
- [ ] `order.avsc` has exactly `orderId` (string), `product` (string), `price` (float)
- [ ] Producer sends Avro-serialized orders (schema visible at `/subjects`)
- [ ] Consumer shows running average (overall + per product)
- [ ] Temporary failures are retried with backoff
- [ ] Retries-exhausted messages go to DLQ
- [ ] Invalid messages go to DLQ without retry
- [ ] Non-Avro message goes to DLQ without crashing the consumer
- [ ] DLQ messages include error headers
- [ ] README complete with run and test instructions
- [ ] Clean Git history with meaningful commits, pushed to GitHub
- [ ] Demo rehearsed at least once end to end
