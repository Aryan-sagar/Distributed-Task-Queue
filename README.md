# Task Queue Broker

A distributed task queue broker built from scratch (not a wrapper around
Celery/RQ) to understand what one actually has to solve: durable task
storage, worker coordination, priority scheduling, failure handling, and
observability.

## Status: Phase 5 of 8 — Dead-letter queue

What exists right now:
- `POST /tasks` — submit a task (`task_type`, JSON `payload`, optional
  `priority` and `max_retries`, defaulting to `0` and `3`).
- `GET /tasks/{id}` — fetch a task's current record.
- `GET /queue/depth` — number of tasks currently waiting.
- `GET /dlq` — list tasks currently in the dead-letter queue (optional
  `limit` query param, default 100).
- `GET /dlq/count` — DLQ size.
- `POST /dlq/{id}/replay` — reset a DLQ'd task (retry_count → 0, error
  cleared) and put it back on the pending queue at its normal priority.
  404 if the id isn't in the DLQ.
- The pending queue is a Redis sorted set scored by priority, with FIFO
  tie-break via a monotonic sequence encoded in the member string.
  `broker/heap.py` has the standalone, unit-tested `MinHeap` demonstrating
  the same ordering logic in isolation.
- A delayed ZSET holds tasks with retries remaining, scored by
  `next_retry_at`; `broker/scheduler.py`'s `RetryScheduler` polls it and
  promotes due tasks back onto the pending queue. Backoff is full-jitter
  exponential (`broker/backoff.py`).
- A DLQ set (`broker/storage.py`'s `move_to_dlq`/`list_dlq`/`replay`)
  indexes any task that becomes terminally `FAILED` — whether from
  exhausting its retries or from an unrecoverable error like a missing
  handler. Either way, a human needs to look at it, which is what the DLQ
  is for.
- `broker/worker.py` routes both terminal-failure paths through
  `move_to_dlq` instead of a plain status update.
- `worker_main.py` runs the worker pool and retry scheduler side by side
  as one process.

What's deliberately *not* here yet: no idempotency enforcement, no
dashboard. Those are the last two phases.

## Roadmap

1. ~~Core broker + submission API~~
2. ~~Worker pool & execution~~
3. ~~Priority scheduling~~
4. ~~Retries with exponential backoff + jitter~~
5. ~~Dead-letter queue for permanently-failed tasks~~ (this phase)
6. Idempotency enforcement
4. Retries with exponential backoff + jitter
5. Dead-letter queue for permanently-failed tasks
6. Idempotency enforcement — dedup on an idempotency key
7. Live WebSocket dashboard — queue depth + worker status in real time
8. Hardening — Docker Compose, full test suite, benchmarks, and
   (stretch) consistent hashing across multiple broker nodes

## Running it

```bash
pip install -r requirements-dev.txt
redis-server &                # or point REDIS_URL at an existing instance
uvicorn main:app --reload &   # API process
python worker_main.py         # worker pool process
```

## Testing

Tests run against `fakeredis`, so no live Redis instance is required:

```bash
pytest
```
