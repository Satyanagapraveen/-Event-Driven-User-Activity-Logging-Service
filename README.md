# Event-Driven User Activity Logging Service

A robust backend service for consuming, processing, and querying user activity
events using an event-driven architecture with RabbitMQ, MongoDB, and FastAPI.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Generating Test Events](#generating-test-events)
- [API Documentation](#api-documentation)
- [Running Tests](#running-tests)
- [Environment Variables](#environment-variables)
- [Design Decisions](#design-decisions)
- [Idempotency Strategy](#idempotency-strategy)

## Overview

This service forms the core of an event-driven user activity logging system.
It consumes user activity events from a RabbitMQ message queue, enriches them
with metadata, persists them idempotently into MongoDB, and exposes a RESTful
API for querying historical data with filtering and pagination.

### Key Features

- **Asynchronous message consumption** from RabbitMQ with manual acknowledgment
- **Event enrichment** with unique identifiers and processing timestamps
- **Idempotent storage** using MongoDB upsert with unique indexes
- **RESTful query API** with filtering by user, event type, and date range
- **Pagination** support via limit and offset parameters
- **Structured JSON logging** for all critical operations
- **Graceful shutdown** handling SIGTERM and SIGINT signals
- **Exponential backoff reconnection** for message queue and database
- **Fully containerized** with Docker Compose and health checks
- **Automated test suite** with unit and integration tests

## Architecture

```mermaid
flowchart TD
  producer[producer.py<br/>Test script]
  rabbitmq[(RabbitMQ<br/>Queue: user_activity)]
  consumer[Consumer Service]
  broker[MessageBroker<br/>Connection, ACK/NACK, retry]
  processor[EventProcessor<br/>Validate and enrich]
  storage[NoSqlStorage<br/>Idempotent upsert]
  mongodb[(MongoDB<br/>Database: user_activity_logs<br/>Collection: events)]
  api[API Service<br/>FastAPI + Uvicorn]
  query[NoSqlQuery<br/>Filtered queries + pagination]
  client[Client<br/>Browser, Postman, curl]

  producer -->|Publishes JSON events| rabbitmq
  rabbitmq -->|Consumes events with manual ACK| consumer
  consumer --> broker --> processor --> storage -->|Writes enriched events| mongodb
  api --> query -->|Reads activity logs| mongodb
  client -->|HTTP GET /api/v1/activity-logs| api
```

### Data Flow

1. **Publish**: A producer sends a JSON event to the `user_activity` queue in
   RabbitMQ. The queue is declared as `durable` with persistent messages
   (`delivery_mode=2`) to survive broker restarts.

2. **Consume**: The consumer service connects to RabbitMQ with
   `prefetch_count=1` (one message at a time) and `auto_ack=False` (manual
   acknowledgment). Each message is delivered as raw bytes.

3. **Validate**: The `EventProcessor` decodes the bytes as UTF-8, parses the
   JSON, and validates against the `RawUserActivityEvent` Pydantic model.
   Malformed messages are rejected (NACK with `requeue=False`).

4. **Enrich**: Valid events receive a UUIDv4 `event_id` and a UTC
   `processing_timestamp`. Structured JSON logs are emitted at each step.

5. **Store**: The enriched event is persisted to MongoDB using an atomic
   `update_one` with `$setOnInsert` and `upsert=True`. A unique index on
   `event_id` ensures duplicate messages are silently ignored.

6. **Acknowledge**: On successful storage, the consumer sends an ACK to
   RabbitMQ. On failure, it sends a NACK. The broker removes acknowledged
   messages from the queue.

7. **Query**: The API service exposes `GET /api/v1/activity-logs` with optional
   query parameters (`user_id`, `event_type`, `start_date`, `end_date`, `limit`,
   `offset`). Queries use MongoDB compound indexes for efficient filtering and
   sorting.

## Tech Stack

| Component            | Technology                   | Purpose                                                           |
| -------------------- | ---------------------------- | ----------------------------------------------------------------- |
| **Message Queue**    | RabbitMQ 3 (management)      | Decouple event producers from consumers, reliable async delivery  |
| **Database**         | MongoDB 7.0                  | Flexible document storage, compound indexes for query performance |
| **Consumer**         | Python 3.13 + Pika           | Message consumption with manual ACK/NACK, connection retry        |
| **API Server**       | FastAPI + Uvicorn            | Async REST API with automatic OpenAPI docs, Pydantic validation   |
| **Data Validation**  | Pydantic v2                  | Request/response validation, event schema enforcement             |
| **MongoDB Driver**   | Motor (async) + PyMongo      | Async queries for API, sync access via `.delegate` for consumer   |
| **Containerization** | Docker + Docker Compose      | Reproducible builds, service orchestration, health checks         |
| **Testing**          | pytest + pytest-asyncio      | Unit tests (consumer), integration tests (API)                    |
| **Test MongoDB**     | mongomock                    | In-memory MongoDB for integration tests without external deps     |
| **Logging**          | Python logging (JSON format) | Structured, machine-parseable log output                          |

## Project Structure

```text
user-activity-logging/
├── consumer-service/ # Background worker service
│   ├── Dockerfile # Container build instructions
│   ├── requirements.txt # Python dependencies (pika, motor, pydantic)
│   ├── pytest.ini # Pytest async configuration
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py # Composition root, wires dependencies
│   │   ├── models.py # Pydantic schemas (Raw + Enriched events)
│   │   └── services/
│   │       ├── __init__.py
│   │       ├── message_broker.py # RabbitMQ connection, ACK/NACK, retry
│   │       ├── event_processor.py # Validation, enrichment, orchestration
│   │       └── nosql_storage.py # MongoDB writes, unique indexes, upsert
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py # Shared fixtures (MockStorage)
│       └── test_event_processor.py # Unit tests (6 tests)
│
├── api-service/ # HTTP API service
│   ├── Dockerfile # Container build instructions (includes curl)
│   ├── requirements.txt # Python dependencies (fastapi, uvicorn, motor)
│   ├── pytest.ini # Pytest async configuration
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py # FastAPI app with lifespan, router mounting
│   │   ├── models.py # Pydantic schemas (Response + QueryParams)
│   │   ├── routers/
│   │   │   ├── __init__.py
│   │   │   └── activity_logs.py # GET /api/v1/activity-logs endpoint
│   │   └── services/
│   │       ├── __init__.py
│   │       └── nosql_query.py # MongoDB reads with filtering + pagination
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py # Fixtures (mongomock, seeded data)
│       └── test_activity_logs_api.py # Integration tests (10 tests)
│
├── docker-compose.yaml # Orchestrates all 4 services + health checks
├── .env.example # Environment variable template (safe to commit)
├── .gitignore # Excludes .env, pycache, .venv, logs
├── producer.py # Test script to publish sample events
└── README.md
```

### Separation of Concerns

Each file has a single responsibility:

- **models.py** — Data shape definition (what an event looks like)
- **message_broker.py** — Transport layer (how messages arrive)
- **event_processor.py** — Business logic (what to do with messages)
- **nosql_storage.py** / **nosql_query.py** — Persistence layer (how data is stored/retrieved)
- **main.py** — Composition root (wires everything together, reads config)
- **routers/activity_logs.py** — HTTP layer (how clients interact)

This separation means you can swap MongoDB for Elasticsearch by changing only
`nosql_storage.py` and `nosql_query.py`. The processor and broker remain
untouched.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (version 20.10+)
- [Docker Compose](https://docs.docker.com/compose/install/) (version 2.0+)
- [Python 3.13](https://www.python.org/downloads/) (for running `producer.py` locally)
- [pip](https://pip.pypa.io/en/stable/installation/) (for installing `pika` locally)

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/Satyanagapraveen/-Event-Driven-User-Activity-Logging-Service
cd user-activity-logging
```

### 2. Set Up Environment Variables

```bash
cp .env.example .env
```

The default values work out of the box for local development. See
Environment Variables for details on each variable.

### 3. Start All Services

```bash
docker compose up -d
```

This command:

- Pulls RabbitMQ and MongoDB images (first run only)
- Builds the consumer and API service images (first run only)
- Creates a shared Docker network
- Starts all four services in dependency order
- Waits for health checks to pass before starting dependents

### 4. Verify Services Are Healthy

```bash
docker compose ps
```

Expected output: All services show Up status. MongoDB and RabbitMQ show
(healthy). The API service shows (healthy) after 30 seconds.

```text
NAME               STATUS
api-service        Up (healthy)
consumer-service   Up
mongodb            Up (healthy)
rabbitmq           Up (healthy)
```

### 5. Test the API Health Endpoint

```bash
curl -f http://localhost:8000/health
```

Expected response:

```json
{ "status": "healthy" }
```

### 6. Access RabbitMQ Management UI (Optional)

Open http://localhost:15672 in your browser.

Username: guest

Password: guest

You can monitor the user_activity queue, message rates, and consumer
connections in real time.

### 7. Access FastAPI Docs (Optional)

Open http://localhost:8000/docs for the auto-generated Swagger UI. You can
test API endpoints interactively.

## Generating Test Events

A producer script is included to publish sample user activity events to the
message queue. This script runs **on your host machine** (not inside Docker)
and connects to RabbitMQ via the exposed port `5672`.

### Install Producer Dependencies

```bash
pip install pika
```

### Run the Producer

```bash
python producer.py
```

Expected output:

```text
[x] Sent 'login' event for user 'user-001'
[x] Sent 'view_product' event for user 'user-002'
[x] Sent 'add_to_cart' event for user 'user-001'
[x] Sent 'register' event for user 'user-003'
[x] Sent 'purchase' event for user 'user-002'
[✓] All events published successfully
```

What happens:

- The producer connects to RabbitMQ on `localhost:5672`.
- It declares the `user_activity` queue as durable.
- Five events are published with different `event_type` values.
- The consumer enriches them and stores them in MongoDB.
- Events are immediately queryable via the API.

### Verify Events Were Stored

```bash
docker compose exec mongodb mongosh -u root -p example --eval \
  "db.getSiblingDB('user_activity_logs').events.countDocuments()"
```

Expected output: `5` (or multiples of `5` if you run the producer multiple times).

## API Documentation

### Base URL

```text
http://localhost:8000
```

### Endpoint: List Activity Logs

`GET /api/v1/activity-logs`

Retrieves user activity events with optional filtering, sorting, and pagination.
Results are sorted by timestamp descending (newest first).

### Query Parameters

| Parameter    | Type     | Required | Default | Description                                  |
| ------------ | -------- | -------- | ------- | -------------------------------------------- |
| `user_id`    | string   | No       | —       | Filter by user identifier                    |
| `event_type` | string   | No       | —       | Filter by event type (e.g., login, purchase) |
| `start_date` | ISO 8601 | No       | —       | Return events with timestamp >= start_date   |
| `end_date`   | ISO 8601 | No       | —       | Return events with timestamp <= end_date     |
| `limit`      | integer  | No       | 10      | Maximum records to return (1-100)            |
| `offset`     | integer  | No       | 0       | Records to skip for pagination               |

### Response Format

Status 200 OK returns a JSON array of activity log objects:

```json
[
  {
    "event_id": "051c8c7a-7c86-4056-8ac1-489ad76ca7bf",
    "user_id": "user-001",
    "event_type": "login",
    "timestamp": "2026-07-08T13:23:09.997000",
    "processing_timestamp": "2026-07-08T13:23:10.045000",
    "details": {
      "ip_address": "192.168.1.1"
    }
  }
]
```

| Field                  | Type            | Description                                   |
| ---------------------- | --------------- | --------------------------------------------- |
| `event_id`             | string (UUIDv4) | Unique identifier generated by the consumer   |
| `user_id`              | string          | User who performed the action                 |
| `event_type`           | string          | Category of the event                         |
| `timestamp`            | ISO 8601        | Original event time (from producer)           |
| `processing_timestamp` | ISO 8601        | When the consumer processed the event         |
| `details`              | object or null  | Event-specific payload (varies by event_type) |

### Example Requests

#### Fetch All Events (Default Pagination)

```bash
curl -s "http://localhost:8000/api/v1/activity-logs" | python -m json.tool
```

#### Filter by User

```bash
curl -s "http://localhost:8000/api/v1/activity-logs?user_id=user-001" | python -m json.tool
```

#### Filter by Event Type

```bash
curl -s "http://localhost:8000/api/v1/activity-logs?event_type=login" | python -m json.tool
```

#### Filter by Date Range

```bash
curl -s "http://localhost:8000/api/v1/activity-logs?start_date=2023-10-01T00:00:00Z&end_date=2023-10-31T23:59:59Z" | python -m json.tool
```

#### Combine Filters

```bash
curl -s "http://localhost:8000/api/v1/activity-logs?user_id=user-001&event_type=login&limit=5" | python -m json.tool
```

#### Pagination: Page 2

```bash
curl -s "http://localhost:8000/api/v1/activity-logs?limit=3&offset=3" | python -m json.tool
```

#### Example: Invalid Limit

```bash
curl -s "http://localhost:8000/api/v1/activity-logs?limit=500"
```

#### Example: Invalid Date

```bash
curl -s "http://localhost:8000/api/v1/activity-logs?start_date=not-a-date"
```

## Running Tests

### Consumer Unit Tests

Tests the EventProcessor class in isolation using a mock storage layer. No
external services required.

```bash
docker compose exec consumer-service python -m pytest tests/ -v
```

Expected output:

```text
tests/test_event_processor.py::test_process_valid_event PASSED
tests/test_event_processor.py::test_process_missing_required_field PASSED
tests/test_event_processor.py::test_process_invalid_json PASSED
tests/test_event_processor.py::test_process_non_utf8_bytes PASSED
tests/test_event_processor.py::test_process_storage_failure PASSED
tests/test_event_processor.py::test_idempotent_event_id_generation PASSED
```

### API Integration Tests

Tests the full API request/response cycle using mongomock (in-memory MongoDB)
and FastAPI's ASGITransport (no network calls). Covers all query parameters,
pagination, and error handling.

```bash
docker compose exec api-service python -m pytest tests/ -v
```

Expected output:

```text
tests/test_activity_logs_api.py::test_get_all_events_default_pagination PASSED
tests/test_activity_logs_api.py::test_pagination_limit PASSED
tests/test_activity_logs_api.py::test_pagination_offset PASSED
tests/test_activity_logs_api.py::test_filter_by_user_id PASSED
tests/test_activity_logs_api.py::test_filter_by_event_type PASSED
tests/test_activity_logs_api.py::test_filter_by_date_range PASSED
tests/test_activity_logs_api.py::test_filter_by_user_and_type PASSED
tests/test_activity_logs_api.py::test_invalid_limit_returns_error PASSED
tests/test_activity_logs_api.py::test_invalid_date_returns_error PASSED
tests/test_activity_logs_api.py::test_no_results PASSED
```

### Test Strategy

| Test Type   | What It Verifies                                             | Mock Strategy                 |
| ----------- | ------------------------------------------------------------ | ----------------------------- |
| Unit        | EventProcessor validation, enrichment, failure handling      | MockStorage (in-memory list)  |
| Integration | API query parameters, filtering, pagination, error responses | mongomock (in-memory MongoDB) |

Unit tests verify business logic in isolation. Fast execution, no I/O.

Integration tests verify the HTTP layer, Pydantic validation, and database
query logic work together correctly.

---

## Environment Variables

All configuration is externalized through environment variables. The `.env.example`
file provides a template with development defaults.

| Variable        | Default                                   | Used By            | Description               |
| --------------- | ----------------------------------------- | ------------------ | ------------------------- |
| `MQ_HOST`       | `localhost`                               | Consumer, Producer | RabbitMQ hostname         |
| `MQ_PORT`       | `5672`                                    | Consumer, Producer | RabbitMQ AMQP port        |
| `MQ_USER`       | `guest`                                   | Consumer, Producer | RabbitMQ username         |
| `MQ_PASS`       | `guest`                                   | Consumer, Producer | RabbitMQ password         |
| `MQ_QUEUE_NAME` | `user_activity`                           | Consumer, Producer | Queue name for events     |
| `MONGO_URI`     | `mongodb://root:example@localhost:27017/` | Consumer, API      | MongoDB connection string |
| `MONGO_DB_NAME` | `user_activity_logs`                      | Consumer, API      | MongoDB database name     |
| `API_HOST`      | `0.0.0.0`                                 | API                | Uvicorn bind address      |
| `API_PORT`      | `8000`                                    | API                | Uvicorn listen port       |

**Note for Docker**: When running inside Docker, `docker-compose.yaml` overrides
`MQ_HOST` to `rabbitmq` and `MONGO_URI` to reference the `mongodb` service name.
Docker's internal DNS resolves these to container IPs automatically.

**Note for Local Development**: When running `producer.py` on your host, use
`localhost` as the hostname. Docker maps container ports to your host's
`localhost`.

---

## Design Decisions

### Why RabbitMQ?

- **Mature and battle-tested**: Used in production at thousands of companies.
- **Management UI included**: The `-management` image provides a web dashboard
  for monitoring queues, message rates, and connections during development.
- **Flexible routing**: Supports exchanges, bindings, and routing keys for
  complex topologies if the system grows.
- **Manual acknowledgment**: `auto_ack=False` ensures no messages are lost if
  the consumer crashes mid-processing.

### Why MongoDB?

- **Flexible schema**: The `details` field varies by `event_type` (`login` has
  `ip_address`, `purchase` has `order_id`). MongoDB documents accommodate this
  without schema migrations.
- **Compound indexes**: Queries filtering by `user_id` or `event_type` with
  date sorting use efficient compound B-tree indexes.
- **Atomic upsert**: `update_one` with `$setOnInsert` and `upsert=True`
  provides idempotent inserts in a single atomic operation.
- **Horizontal scalability**: MongoDB supports sharding for high-volume
  production deployments.

### Why FastAPI?

- **Native async/await**: Motor (async MongoDB driver) integrates seamlessly
  with FastAPI's async request handling.
- **Automatic Pydantic validation**: Query parameters are validated against
  Pydantic models with zero boilerplate code.
- **Auto-generated docs**: `/docs` (Swagger) and `/redoc` endpoints are
  generated automatically from type hints.
- **Separation of concerns**: Routers keep endpoints organized. The lifespan
  context manager handles startup/shutdown cleanly.

### Why Separate Consumer and API Services?

- **Independent scaling**: If query load increases, scale the API service
  horizontally without affecting message consumption. If event volume spikes,
  scale the consumer independently.
- **Independent deployment**: The consumer can be updated without taking the
  API offline, and vice versa.
- **Fault isolation**: If the API crashes, the consumer continues processing
  events. Messages accumulate in RabbitMQ (durable queue) and are processed
  when the consumer recovers.
- **Technology flexibility**: The consumer could be rewritten in Go or Rust for
  performance without changing the API service.

### Why Pika Blocking Connection for Consumer?

- **Simplicity**: The consumer is a single-threaded worker processing one
  message at a time (`prefetch_count=1`). An async connection adds complexity
  with no throughput benefit in this scenario.
- **Straightforward retry logic**: Blocking I/O with `time.sleep()` for
  exponential backoff is simpler to reason about than async timers.
- **MongoDB access via `.delegate`**: The synchronous `pymongo` API (accessed
  through Motor's `.delegate`) integrates naturally with a synchronous consumer.
  No event loop management required.

## Idempotency Strategy

Distributed message systems guarantee **at-least-once delivery**, meaning the
same message may be delivered multiple times (network glitches, consumer
restarts before ACK). The system must handle this without creating duplicate
records.

### Implementation

The idempotency mechanism has two layers:

**Layer 1: Unique Database Index**

A unique index on `event_id` prevents duplicate inserts at the database level:

```python
await collection.create_index("event_id", unique=True)
```

If a duplicate `event_id` is inserted, MongoDB rejects it with a duplicate key
error. This is the safety net.

**Layer 2: Atomic Upsert with `$setOnInsert`**

Instead of `insert_one` (which raises exceptions on duplicates), the system
uses an atomic upsert:

```python
result = await collection.update_one(
    {"event_id": event.event_id},
    {"$setOnInsert": document},
    upsert=True,
)
```

How it works:

| Delivery | Operation                               | Result                                                                       |
| -------- | --------------------------------------- | ---------------------------------------------------------------------------- |
| First    | No document with this `event_id` exists | `upsert=True` inserts the document and `$setOnInsert` populates all fields.  |
| Second   | Document already exists                 | `upsert=True` matches the existing document and `$setOnInsert` does nothing. |

Why `$setOnInsert` instead of `insert_one` + `try/except`:

- No exceptions for normal operations: duplicates are expected in distributed systems.
- Atomicity: the check-and-insert happens in a single database operation.
- Clean logs: duplicates are logged at WARNING level, normal inserts at INFO.

### Event ID Generation

Each event receives a UUIDv4 (random) identifier during processing:

```python
event_id = str(uuid4())  # e.g., "a3f2b1c4-5d6e-7f8a-9b0c-1d2e3f4a5b6c"
```

UUIDv4 was chosen over deterministic alternatives because repeated but valid
events should still be stored separately when they occur at different times.

### Stopping the Application

To stop all services and remove containers:

```bash
docker compose down
```

To also remove the MongoDB data volume (reset the database):

```bash
docker compose down -v
```

To stop without removing containers (preserve state for later restart):

```bash
docker compose stop
```

### Common Issues

#### Port Already in Use

If port 8000, 5672, 15672, or 27017 is already in use:

```bash
# Find what's using the port
lsof -i :8000
# Or on Windows
netstat -ano | findstr :8000
```

Update the host port in `docker-compose.yaml` if needed.

#### Consumer Not Processing Messages

Check consumer logs:

```bash
docker compose logs consumer-service
```

Look for `Connected to RabbitMQ` and `Connected to MongoDB`. If missing, check
that RabbitMQ and MongoDB health checks pass.

#### API Returns 500 Errors

Check API logs:

```bash
docker compose logs api-service
```

Verify MongoDB is healthy and the `MONGO_URI` is correct.

#### MongoDB Authentication Fails

If you changed credentials in `.env`, the MongoDB volume may still have old
credentials. Reset the volume:

```bash
docker compose down -v
docker compose up -d
```
