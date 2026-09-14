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
