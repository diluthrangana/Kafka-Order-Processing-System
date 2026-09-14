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
