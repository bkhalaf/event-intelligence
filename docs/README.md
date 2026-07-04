# Event Intelligence

> **This project is under active development and not yet production-ready.**

Cloud-native real-time event streaming pipeline built on Apache Kafka and Apache Flink. Designed for scalable event ingestion, transformation, and storage.

## Architecture

```
                         +-------------------+
                         |   Producer API    |
                         |  (FastAPI :5000)  |
                         +---------+---------+
                                   |
                                   v
+-------------+         +-------------------+         +-------------------+
|  Zookeeper  |<------->|      Kafka        |<------->|    Kafka UI       |
|    :2181    |         |   :9092 / :29092  |         |     :8080         |
+-------------+         +---------+---------+         +-------------------+
                                   |
                                   | test-topic
                                   v
                         +-------------------+
                         |    Flink Job      |
                         | (JobManager :8081)|
                         | (TaskManager)     |
                         +---------+---------+
                                   |
                                   | output-topic (transformed)
                                   v
                         +-------------------+
                         |    Consumer       |
                         | (Python script)   |
                         +---------+---------+
                                   |
                                   v
                          +----------------+
                          |   PostgreSQL   |
                          |     :5433      |
                          +----------------+
                                   ^
                                   |
                         +-------------------+
                         |      db-seed      |
                         |   (seed.py once)  |
                         +-------------------+
```

### Data Flow

1. **Ingest** -- The FastAPI producer API receives messages via `POST /order` and publishes them to Kafka's `test-topic`.
2. **Transform** -- An Apache Flink streaming job reads from `test-topic`, transforms each message to uppercase, and writes the result to `output-topic`.
3. **Store** -- The Dockerized consumer reads from `output-topic` and persists the original and transformed messages to PostgreSQL.
4. **Seed** -- A startup seed script inserts initial demo records into PostgreSQL so the dashboard has data on first run.

### Services

| Service | Image / Build | Port | Purpose |
|---|---|---|---|
| Zookeeper | `confluentinc/cp-zookeeper:7.5.0` | 2181 | Kafka coordination |
| Kafka | `confluentinc/cp-kafka:7.5.0` | 9092, 29092 | Message broker |
| Producer API | Custom (Dockerfile) | 5000 | REST API to publish messages |
| Flink JobManager | `flink:1.18-java11` | 8081 | Flink cluster coordinator |
| Flink TaskManager | `flink:1.18-java11` | -- | Flink task execution |
| Flink Job | Custom (flink-job/Dockerfile) | -- | Stream transformation job |
| Kafka UI | `provectuslabs/kafka-ui:latest` | 8080 | Kafka topic monitoring |
| PostgreSQL | `postgres:15` | 5433 | Persistent storage |
| Kafka Init | Startup helper that creates required Kafka topics | -- | Ensures `test-topic` and `output-topic` exist before Flink starts |
| Consumer | Custom (Dockerfile) | -- | Reads `output-topic` and writes to PostgreSQL |
| DB Seed | Custom (Dockerfile) | -- | Inserts initial demo rows once at startup |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- [Python 3.11+](https://www.python.org/downloads/)

## Getting Started

### 1. Start the infrastructure

```bash
docker compose -f docker_compose.yaml up --build -d
```

This starts Zookeeper, Kafka, the Producer API, the Flink cluster and job, Kafka UI, and PostgreSQL.

After startup, allow a short warm-up period so Kafka topics are initialized and the Flink job reaches the running state.

### 2. Verify services are running

```bash
docker compose -f docker_compose.yaml ps
```

Check the dashboards:

- **Producer API docs**: http://localhost:5000/docs
- **Flink Dashboard**: http://localhost:8081
- **Kafka UI**: http://localhost:8080

### 3. Install Python dependencies (for the consumer)

```bash
pip install -r requirements.txt
```

### 4. Run the consumer

```bash
python consumer.py
```

By default the consumer listens to `output-topic`. You can specify a different topic:

```bash
python consumer.py my-topic
```

### 5. Send a test message

```bash
curl -X POST http://localhost:5000/send \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello from Event Intelligence"}'
```

The message flows through the full pipeline: Producer API -> Kafka -> Flink (uppercase transform) -> Consumer -> PostgreSQL.

### 6. Query stored results

```bash
docker exec -it postgres psql -U admin -d kafka_events -c "SELECT * FROM kafka_messages;"
```

## Project Structure

```
event-intelligence/
├── producer.py              # FastAPI producer API
├── consumer.py              # Kafka consumer, persists to PostgreSQL
├── seed.py                  # Inserts initial demo records into PostgreSQL
├── Dockerfile               # Producer API container
├── docker_compose.yaml      # Full stack orchestration
├── init.sql                 # PostgreSQL schema initialization
├── requirements.txt         # Python dependencies
├── flink-job/
│   ├── flink_job.py         # PyFlink stream transformation job
│   └── Dockerfile           # Flink job container
├── customer-order-page/
│   ├── index.html           # Order page UI
│   ├── style.css            # Order page styling
│   ├── script.js            # Order page behavior
│   └── images/             # Order page assets
└── LICENSE                  # Apache 2.0

```

## API Reference

### `POST /order`

Send a message to the Kafka pipeline.

**Request body:**
```json
{
  "message": "your message here"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Message sent to Kafka",
  "topic": "test-topic",
  "partition": 0,
  "offset": 1
}
```

### `GET /health`

Health check endpoint. Returns `{"status": "healthy"}`.

## Stopping the Stack

```bash
docker compose -f docker_compose.yaml down
```

To also remove volumes (deletes stored data):

```bash
docker compose -f docker_compose.yaml down -v
```

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
