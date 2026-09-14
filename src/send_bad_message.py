from confluent_kafka import Producer

import config

producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS})
producer.produce(config.ORDERS_TOPIC, key=b"BAD-1", value=b"this is not avro data")
producer.flush(10)
print("[BAD] Sent a non-Avro message to 'orders'")
