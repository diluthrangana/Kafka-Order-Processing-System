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
