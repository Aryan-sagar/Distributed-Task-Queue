# Task Queue Broker

A lightweight, Redis-backed distributed task queue built with Python, FastAPI, and multithreaded workers.

The project demonstrates the core mechanics behind background job processing systems:

- Task submission through a REST API
- FIFO queueing with Redis
- Persistent task state
- Handler registration through a task-type registry
- Concurrent worker execution
- Task success/failure tracking
- Worker survival after handler failures
- Queue depth monitoring

---

## Architecture

```text
                    ┌─────────────────────┐
                    │      FastAPI        │
                    │                     │
                    │  POST /tasks        │
                    │  GET  /tasks/{id}   │
                    │  GET  /queue/depth  │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      TaskStore      │
                    │                     │
                    │   Redis FIFO Queue  │
                    │   Task Records      │
                    └──────────┬──────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
                    ▼                     ▼
              ┌──────────┐          ┌──────────┐
              │ Worker 0 │          │ Worker 1 │
              └────┬─────┘          └────┬─────┘
                   │                     │
                   ▼                     ▼
              ┌──────────┐          ┌──────────┐
              │ Handler  │          │ Handler  │
              │ Registry │          │ Registry │
              └──────────┘          └──────────┘
                   
                    + Worker 2
                    + Worker 3
````

Workers share the same Redis-backed queue and independently dequeue and execute tasks.

---

## Tech Stack

| Component          | Technology     |
| ------------------ | -------------- |
| API                | FastAPI        |
| Language           | Python         |
| Queue / Storage    | Redis          |
| Worker Concurrency | Python Threads |
| Validation         | Pydantic       |
| Testing            | Pytest         |
| Redis Testing      | Fakeredis      |

---

## Project Structure

```text
task-queue-broker/
│
├── broker/
│   ├── schema.py
│   ├── storage.py
│   ├── api.py
│   ├── registry.py
│   ├── handlers.py
│   ├── worker.py
│   └── pool.py
│
├── tests/
│   ├── test_storage.py
│   ├── test_api.py
│   └── test_worker.py
│
├── main.py
├── worker_main.py
├── requirements.txt
├── requirements-dev.txt
├── .gitignore
└── README.md
```

---

## Core Components

### TaskStore

Responsible for:

* Enqueuing tasks
* Dequeuing tasks
* Persisting task records
* Updating task state
* Reading task status
* Reporting queue depth

Redis provides the FIFO queue using list operations.

### Handler Registry

Handlers are registered against a task type:

```python
@register("my_task")
def handle_my_task(payload):
    ...
```

Workers dynamically resolve the appropriate handler when processing a task.

### Worker

A worker continuously:

1. Dequeues a task
2. Resolves its handler
3. Marks it as running
4. Executes the handler
5. Persists the result
6. Marks the task as successful or failed

A handler exception is isolated to the task and does not terminate the worker.

### WorkerPool

The worker pool creates multiple workers sharing the same `TaskStore`.

```text
             Redis Queue
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
     Worker 0  Worker 1  Worker 2
        │         │         │
        └─────────┼─────────┘
                  ▼
             Task Handler
```

---

## Task Lifecycle

```text
             ┌─────────┐
             │ PENDING │
             └────┬────┘
                  │
                  ▼
             ┌─────────┐
             │ RUNNING │
             └────┬────┘
                  │
            ┌─────┴─────┐
            ▼           ▼
       ┌─────────┐ ┌─────────┐
       │ SUCCESS │ │ FAILED  │
       └─────────┘ └─────────┘
```

Unknown task types are immediately marked as `FAILED`.

---

## Running Locally

### 1. Create the virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```powershell
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 3. Start Redis

Using Docker:

```powershell
docker run -d --name task-queue-redis -p 6379:6379 redis:latest
```

Verify:

```powershell
docker exec -it task-queue-redis redis-cli ping
```

Expected:

```text
PONG
```

### 4. Start the API

```powershell
python main.py
```

The API will be available at:

```text
http://127.0.0.1:8000
```

Interactive API documentation:

```text
http://127.0.0.1:8000/docs
```

### 5. Start the workers

In another terminal:

```powershell
python worker_main.py
```

Expected:

```text
Worker pool running with 4 workers. Ctrl+C to stop.
```

---

## API

### Submit a task

```http
POST /tasks
```

Example:

```json
{
  "task_type": "noop",
  "payload": {
    "message": "hello broker"
  }
}
```

Example response:

```json
{
  "id": "3a86a311-8013-444c-9aee-e765b4e4d5f2",
  "task_type": "noop",
  "payload": {
    "message": "hello broker"
  },
  "status": "pending",
  "retry_count": 0
}
```

---

### Get task status

```http
GET /tasks/{task_id}
```

Example successful task:

```json
{
  "id": "...",
  "task_type": "noop",
  "status": "success",
  "result": null,
  "error": null
}
```

---

### Queue depth

```http
GET /queue/depth
```

Example:

```json
{
  "depth": 4
}
```

---

## Failure Handling

If a registered handler raises an exception, the worker:

1. Catches the exception
2. Marks the task as `FAILED`
3. Stores the error
4. Continues processing other tasks

Example:

```python
@register("boom")
def handle_boom(payload):
    raise ValueError("kaboom")
```

The worker remains alive and can process subsequent tasks.

Unknown task types are also safely rejected:

```text
No handler registered for task_type='unknown'
```

---

## Testing

Run the complete test suite:

```powershell
python -m pytest -v
```

The test suite covers:

* Task storage
* FIFO queue behavior
* API endpoints
* Task submission
* Handler execution
* Successful tasks
* Handler exceptions
* Unknown handlers
* Worker pools
* Multiple task processing
* Concurrent worker execution

---

## Design Decisions

### Why Redis?

Redis provides:

* Fast queue operations
* Atomic list operations
* Simple deployment
* Shared state between worker processes
* A natural foundation for distributed workers

### Why a Handler Registry?

The registry separates task routing from worker logic.

Workers don't need to know how individual tasks work. They only need to:

```text
task_type → handler → execute
```

This makes new task types easy to add without modifying the worker implementation.

### Why Threads?

The current worker pool uses threads because the primary workload is assumed to be I/O-oriented background work.

The architecture keeps task execution isolated enough that the execution model can later be replaced with processes or distributed workers.

---

## Current Limitations

This is intentionally a foundational implementation.

Planned improvements include:

* Retry policies
* Exponential backoff
* Dead-letter queues
* Task timeouts
* Idempotency
* Graceful worker shutdown
* Worker heartbeats
* Distributed worker processes
* Docker Compose deployment
* Structured logging
* Metrics and observability
* Redis integration tests
* Authentication and API rate limiting

---

## Roadmap

### Phase 1 — Core Broker

* [x] Task schema
* [x] Redis-backed FIFO queue
* [x] Task persistence
* [x] FastAPI API

### Phase 2 — Task Execution

* [x] Handler registry
* [x] Example handlers
* [x] Worker
* [x] Worker pool

### Phase 3 — Reliability

* [ ] Retries
* [ ] Backoff
* [ ] Dead-letter queue
* [ ] Task timeout
* [ ] Idempotency

### Phase 4 — Infrastructure

* [ ] Dockerfile
* [ ] Docker Compose
* [ ] Separate API / worker services
* [ ] Production configuration

### Phase 5 — Observability

* [ ] Structured logging
* [ ] Metrics
* [ ] Worker health
* [ ] Queue monitoring

---

## Why This Project?

This project was built to understand the engineering fundamentals behind asynchronous job-processing systems rather than simply using an existing task queue abstraction.

It focuses on:

* Distributed systems fundamentals
* Queue semantics
* Concurrent execution
* Failure isolation
* State management
* API design
* Testing
* Production-oriented architecture

````

