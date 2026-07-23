# Event Intelligence

> **This project is under active development and not yet production-ready.**

Cloud-native real-time event streaming pipeline built on Apache Kafka and Apache Flink (PyFlink), with a configurable AI agent for automated order analysis. Designed for scalable event ingestion, transformation, storage, and real-time analytics.

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
             +-------------------+     +---------------------------+
             |    Consumer       |     |         AI Agent          |
             | (Python script)   |     | (configurable provider:   |
             +---------+---------+     |  Gemini / Groq / Ollama / |
                       |               |  OpenAI via YAML config)  |
                       |               +-------------+-------------+
                       v                             |
                +----------------+                   |
                |   PostgreSQL   |<------------------+
                |     :5433      |
                +----------------+
                       ^
                       |
        +-------------------+           +-------------------+
        |      db-seed      |           |     Dashboard      |
        |   (seed.py once)  |           | (reads /get_metrics,|
        +-------------------+           |  /sales_branch, ...) |
                                        +-------------------+
```

### Data Flow

1. **Ingest** -- The FastAPI producer API receives orders via `POST /order` and publishes them to Kafka's `test-topic`.
2. **Transform** -- An Apache Flink (PyFlink) streaming job reads from `test-topic`, transforms each message to uppercase, and writes the result to `output-topic`.
3. **Store** -- The Dockerized consumer reads from `output-topic` and persists the original and transformed messages to PostgreSQL.
4. **Analyze** -- The AI Agent reads from `output-topic` in parallel with the consumer, analyzes each order using a configurable AI provider (Gemini, Groq, Ollama, or OpenAI -- selected via `config/agent_config.yaml`), with automatic retry and exponential backoff, and persists the analysis to PostgreSQL's `agent_results` table.
5. **Serve** -- The analytics dashboard (`frontend/dashboard-page/`) reads aggregated metrics from the Producer API and displays them in real time. A separate customer-facing order page (`frontend/order-page/`) lets users submit new orders directly to the pipeline.
6. **Seed** -- A startup seed script inserts initial demo records into PostgreSQL (only if the table is empty) so the dashboard has data on first run.

### Services

| Service | Image / Build | Port(s) | Purpose |
|---|---|---|---|
| Zookeeper | `confluentinc/cp-zookeeper:7.5.0` | 2181 | Kafka coordination |
| Kafka | `confluentinc/cp-kafka:7.6.0` | 9092, 29092 | Message broker |
| Kafka Init | `confluentinc/cp-kafka:7.5.0` (startup helper) | -- | One-time job that creates `test-topic` and `output-topic` before Flink starts |
| Producer API | Custom (`Dockerfile`) | 5000 | FastAPI REST API: receives orders, exposes dashboard metrics endpoints |
| Flink JobManager | `flink:1.18-java11` | 8081 | Flink cluster coordinator |
| Flink TaskManager | `flink:1.18-java11` | -- | Flink task execution (2 task slots) |
| Flink Job | Custom (`flink-job/Dockerfile`) | -- | PyFlink stream transformation job |
| Kafka UI | `provectuslabs/kafka-ui:latest` | 8080 | Kafka topic monitoring |
| PostgreSQL | `postgres:16-alpine` | 5433 → 5432 | Persistent storage |
| Consumer | Custom (`Dockerfile`) | -- | Reads `output-topic`, writes raw + transformed messages to PostgreSQL |
| AI Agent | Custom (`Dockerfile`) | -- | Reads `output-topic`, analyzes orders via the configured AI provider, stores results in PostgreSQL |
| DB Seed | Custom (`Dockerfile`) | -- | Inserts demo rows once at startup, only if the table is empty |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- An API key for at least one supported AI provider (Gemini, Groq, or OpenAI), **or** a running local [Ollama](https://ollama.com/) instance -- see [AI Agent](#ai-agent) below
- [Python 3.11+](https://www.python.org/downloads/) -- only needed to run the test suite locally outside Docker (see [Testing](#testing))

## Getting Started

### 1. Configure the AI Agent provider

Open `config/agent_config.yaml` and set `active_provider` to one of `gemini`, `groq`, `ollama`, or `openai`, and fill in the matching `api_key` (for Gemini/Groq/OpenAI). See [AI Agent](#ai-agent) for full details.

> ⚠️ **Never commit a real API key.** The file is tracked with placeholder values (e.g. `PUT_YOUR_GEMINI_API_KEY`) -- fill in real keys locally only, and make sure they are removed again before pushing.

### 2. Start the full stack

```bash
docker compose -f docker_compose.yaml up --build -d
```

This single command starts every service: Zookeeper, Kafka, Kafka Init, the Flink cluster and job, the Producer API, the Consumer, the AI Agent, PostgreSQL, and the DB Seed job, plus Kafka UI. Everything runs inside containers -- no manual `pip install` or separately running a script is required.

### 3. Allow a short warm-up period

Kafka, the Flink job, and the AI Agent all need a few seconds to finish initializing. `consumer` and `agent` both have `restart: on-failure`, so they retry automatically if they start before Kafka is fully ready.

`flink-job`, however, has `restart: "no"` -- if it fails to connect to Kafka on its very first attempt, it stops permanently instead of retrying. If the Flink Dashboard (see below) shows 0 running jobs after start-up, restart it manually:

```bash
docker compose -f docker_compose.yaml up -d flink-job
```

### 4. Verify services are running

```bash
docker compose -f docker_compose.yaml ps
```

Check the dashboards:

- **Producer API docs**: http://localhost:5000/docs
- **Flink Dashboard**: http://localhost:8081 (confirm a job is listed under *Running Jobs*)
- **Kafka UI**: http://localhost:8080

### 5. Submit a test order

```bash
curl -X POST http://localhost:5000/order \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Gaza",
    "customer_name": "Test Customer",
    "payment_method": "Cash",
    "notes": "Test order",
    "items": [
      {"product_id": "E1004", "product_name": "Gaming Mouse", "quantity": 1, "unit_price": 150}
    ]
  }'
```

The order flows through the full pipeline: Producer API → Kafka (`test-topic`) → Flink (uppercase transform) → `output-topic` → Consumer (→ PostgreSQL) and AI Agent (→ PostgreSQL) in parallel.

### 6. Query stored results

```bash
docker exec -it postgres psql -U admin -d kafka_events -c "SELECT * FROM kafka_messages ORDER BY id DESC LIMIT 5;"
docker exec -it postgres psql -U admin -d kafka_events -c "SELECT * FROM agent_results ORDER BY id DESC LIMIT 5;"
```

Or open the analytics dashboard directly -- see [Frontend Interfaces](#frontend-interfaces).

## Project Structure

```
event-intelligence/
├── config/
│   └── agent_config.yaml     # AI provider configuration (active provider + credentials)
├── frontend/
│   ├── dashboard-page/         # Static real-time analytics dashboard
│   │   ├── index.html
│   │   ├── script.js
│   │   └── style.css
│   ├── order-page/
│   │   ├── images/
│   │   ├── index.html
│   │   ├── script.js
│   │   └── style.css
│   └── store-page/
│       ├── images/
│       ├── index.html
│       ├── script.js
│       └── style.css
├── docs/
│   ├── INFRASTRUCTURE.md     # Infrastructure decisions & benchmarking notes
│   └── README.md             
├── flink-job/
│   ├── Dockerfile
│   └── flink_job.py
├── scripts/
│   ├── agent.py              # AI Agent: consumes output-topic, calls the active AI provider
│   ├── consumer.py           # Kafka consumer, persists to PostgreSQL
│   ├── dashboard_utils.py    # Parsing/aggregation logic used by the dashboard endpoints
│   ├── producer.py           # FastAPI producer API + dashboard endpoints
│   └── seed.py                # Inserts demo records into PostgreSQL on first run
├── sql/
│   └── init.sql              # PostgreSQL schema (kafka_messages, agent_results)
├── tests/
│   ├── test_dashboard_utils.py
│   ├── test_integration_pipeline.py
│   ├── test_producer.py
│   ├── test_producer_api.py
│   └── test_seed.py
├── Dockerfile                 # Producer API / consumer / agent / db-seed container image
├── docker_compose.yaml       # Full stack orchestration
├── pytest.ini                 # pytest configuration (import paths, markers)
├── requirements.txt          # Python dependencies
└── LICENSE
```

## API Reference

All endpoints are served by the Producer API (`scripts/producer.py`) on port `5000`.

### `POST /order`

Submit a new order to the Kafka pipeline.

**Request body:**
```json
{
  "branch": "Gaza",
  "customer_name": "Ahmad",
  "payment_method": "Cash",
  "notes": "Urgent delivery",
  "items": [
    {"product_id": "E1004", "product_name": "Gaming Mouse", "quantity": 1, "unit_price": 150}
  ]
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Order sent to Kafka",
  "topic": "test-topic",
  "partition": 0,
  "offset": 1
}
```

### `GET /health`

Health check endpoint. Returns `{"status": "healthy"}`.

### `GET /get_metrics`

Returns aggregated dashboard metrics: total orders, revenue, products sold, number of branches, and the last-updated timestamp.

### `GET /latest_orders`

Returns the most recent orders (parsed into structured fields), newest first.

### `GET /sales_branch`

Returns total sales aggregated per branch.

### `GET /branch_performance`

Returns per-branch statistics: order count, revenue, products sold, and average order value.

## AI Agent

The AI Agent (`scripts/agent.py`) is a Dockerized Python service that consumes processed orders from `output-topic` **in parallel with the Consumer**, sends each order to an AI provider for a short natural-language analysis, and stores the result in PostgreSQL's `agent_results` table.

### Provider-agnostic design

Instead of being tied to a single AI provider, the agent is built to be **fully configurable** through `config/agent_config.yaml`. One field, `active_provider`, decides which provider is used at runtime, and the agent automatically routes each request to the matching handler function:

```yaml
active_provider: "gemini"   # one of: gemini, groq, ollama, openai

providers:
  gemini:
    url: "https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-lite-latest:generateContent"
    api_key: "PUT_YOUR_GEMINI_API_KEY"
    model: "gemini-flash-lite-latest"

  groq:
    url: "https://api.groq.com/openai/v1/chat/completions"
    api_key: "PUT_YOUR_GROQ_API_KEY_HERE"
    model: "llama-3.1-8b-instant"

  ollama:
    url: "http://<your-ollama-host>:11434/api/generate"
    model: "llama3"

  openai:
    url: "https://api.openai.com/v1/chat/completions"
    api_key: "PUT_YOUR_OPENAI_API_KEY_HERE"
    model: "gpt-4o-mini"
```

Switching providers requires **no code changes** -- just edit `active_provider` (and make sure the matching `api_key` / `url` is filled in) and restart the `agent` container.

### Currently active provider

The pipeline currently runs with **`gemini`** as the active provider, using Google AI Studio's `gemini-flash-lite-latest` model. This model was chosen after testing showed `gemini-2.5-flash` returning 404 for new accounts and `gemini-flash-latest` repeatedly hitting capacity errors.

### Reliability: retries with exponential backoff

Every call to the active provider goes through a retry loop (`analyze_with_model`, up to 3 attempts) with exponential backoff (2s → 4s → 8s between attempts) and per-message exception handling. If a call still fails after all retries, the error is logged and that message is skipped -- it does not crash the agent or block the next message.

### Known limitation

The `ollama` provider entry currently requires a local network IP address to be filled in manually, which only works on the machine where that address is reachable and is not portable across environments/teammates. This is worth revisiting if `ollama` is ever set as the active provider outside a single developer's machine.

### Security note

`config/agent_config.yaml` is committed with **placeholder values only** (e.g. `PUT_YOUR_GEMINI_API_KEY`). Fill in real keys locally, and double-check they are removed again before pushing. Moving these credentials to a git-ignored `.env` file is a recommended future improvement to reduce the risk of accidentally committing a real key.

### Query AI analysis results

```bash
docker exec -it postgres psql -U admin -d kafka_events -c "SELECT * FROM agent_results ORDER BY id DESC LIMIT 5;"
```

## Frontend Interfaces

The project includes two independent static front-ends, both plain HTML/CSS/JS (no build step) and both calling the Producer API directly at `http://localhost:5000`.

### `frontend/dashboard-page/` -- Analytics Dashboard

A read-only, real-time monitoring view. On load it calls `/get_metrics`, `/sales_branch`, `/latest_orders`, and `/branch_performance`, and renders:
- Summary cards (total orders, revenue, products, branches)
- A pie chart of sales by branch (Chart.js)
- A live feed of the most recent orders
- A branch-performance comparison table

### `frontend/order-page/` -- Order Submission Page

A simple customer-facing form for placing new orders. It lets a user pick a branch, payment method, and product quantities, previews the JSON payload before sending, and submits it via `POST /order`.

## Testing

The project uses `pytest`, split into two categories.

### Unit tests (29 tests, no infrastructure required)

Cover the pure logic in `dashboard_utils.py`, `producer.py`, and `seed.py`, plus the FastAPI endpoints (Kafka calls are mocked, so these run instantly without Docker):

```bash
pytest -v
```

| File | What it covers |
|---|---|
| `test_dashboard_utils.py` | Order/message parsing, metrics calculation, sales/branch aggregation, latest-orders sorting |
| `test_producer.py` | Order message formatting |
| `test_producer_api.py` | `/health` and `/order` endpoints (success, validation error, Kafka failure) |
| `test_seed.py` | Demo message generation matches the expected parsing format |

### Integration test (1 test, requires the full Docker stack running)

Verifies the complete pipeline end-to-end: sends a real order through `POST /order`, polls PostgreSQL until it appears (or times out after 30 seconds), and cleans up the test record afterward.

```bash
docker compose -f docker_compose.yaml up -d
pytest -v -m integration
```

By default, plain `pytest -v` automatically skips this test (see `pytest.ini`), so day-to-day development does not require Docker to be running.

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