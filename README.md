# Kafka Order Processing System

A robust, real-time order processing pipeline built with Python and Apache Kafka. This project demonstrates enterprise-grade streaming patterns including Avro serialization, real-time metric aggregation, exponential backoff retries, and Dead Letter Queue management.

It also includes a beautiful, interactive web dashboard to visualize the pipeline in action.

## Features

- **Real-time Aggregation**: Calculates running price averages (overall and per-product) on the fly as orders are consumed.
- **Resilient Processing**: Implements exponential backoff (1s, 2s, 4s) for handling temporary downstream failures.
- **Dead Letter Queue (DLQ)**: Bad data (validation/deserialization errors) and exhausted retries are automatically routed to a separate `orders-dlq` topic without blocking the main pipeline.
- **Avro Schema Validation**: Enforces strong typing and schema evolution using Confluent Schema Registry.
- **At-Least-Once Delivery**: Manual offset commits ensure no messages are lost during unexpected crashes.
- **Live Interactive Dashboard**: A glassmorphism-styled UI to monitor metrics, inject test messages, and inspect the DLQ in real time.

## Architecture

At a high level, the system consists of a producer generating order events, a Kafka broker handling the streams, and a consumer processing them. 

- **Main Topic:** `orders` 
- **DLQ Topic:** `orders-dlq` 

1. The **Producer** (`src/producer.py`) serializes orders using Avro and publishes them.
2. The **Consumer** (`src/consumer.py`) polls the topic. It validates data and updates running averages.
3. If processing fails temporarily, the consumer retries. If it fails permanently (or retries are exhausted), the raw message is shipped to the DLQ along with diagnostic headers.
4. The **DLQ Inspector** (`src/dlq_consumer.py`) or the Web UI can be used to analyze failed messages.

## Prerequisites

- Python 3.9+
- A running Kafka Cluster (Broker + Schema Registry)
  *(If you don't have one locally, you can run a local Confluent Platform or Apache Kafka instance)*

## Quick Start

### 1. Installation

Clone the repository and install the dependencies:

```bash
git clone https://github.com/yourusername/Kafka-Order-Processing-System.git
cd Kafka-Order-Processing-System

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

### 2. Configuration

Make sure your Kafka broker and schema registry are running. By default, the system looks for:
- Kafka Broker: `localhost:9092`
- Schema Registry: `http://localhost:8081`

You can override these in `src/config.py` or via environment variables (`BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`).

### 3. Running the Live Dashboard (Recommended)

The easiest way to see the system in action is via the built-in UI dashboard. 

```bash
# Start a local HTTP server on port 3000
python -m http.server 3000
```
Open `http://localhost:3000` in your browser. From here you can pause/resume the producer, inject bad messages, and watch the DLQ and running averages update in real time.

### 4. Running the Python Pipeline Manually

If you want to run the actual Python Kafka clients against your local cluster, open separate terminal windows and run:

**Start the Consumer:**
```bash
python src/consumer.py --fail-rate 0.3
```

**Start the Producer:**
```bash
python src/producer.py --count 10 --interval 1.0 --invalid-rate 0.2
```

**Inspect the DLQ:**
```bash
python src/dlq_consumer.py
```

## Testing Failure Scenarios

- **Validation Errors:** Pass `--invalid-rate 1.0` to the producer to send orders with negative prices. They will go straight to the DLQ.
- **Deserialization Errors:** Run `python src/send_bad_message.py` to inject raw, non-Avro text into the topic.
- **Retries Exhausted:** Pass `--fail-rate 1.0` to the consumer to force every attempt to fail. It will retry 3 times and then route to the DLQ.

## License

MIT License. See `LICENSE` for more information.
