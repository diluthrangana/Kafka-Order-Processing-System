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
