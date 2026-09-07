# Task Queue Broker

A distributed task queue broker built from scratch (not a wrapper around
Celery/RQ) to understand what one actually has to solve: durable task
storage, worker coordination, priority scheduling, failure handling, and
observability.

## Status: Phase 7 of 8 — Live WebSocket dashboard

What exists right now:
- `POST /tasks` — submit a task (`task_type`, JSON `payload`, optional
  `priority`, `max_retries`, `idempotency_key`).
- `GET /tasks/{id}` — fetch a task's current record.
- `GET /queue/depth` — number of tasks currently waiting.
- `GET /dlq`, `GET /dlq/count`, `POST /dlq/{id}/replay` — dead-letter
  queue inspection and replay.
- `GET /dashboard` — a single self-contained HTML page (no build step)
  showing pending/retrying/dead-lettered counts and live worker status.
- `GET /ws/dashboard` — the WebSocket the dashboard page connects to.
  Pushes a JSON snapshot once per second. Runs the (synchronous,
  blocking) Redis calls via `asyncio.to_thread` rather than calling them
  directly from the async route, so one slow Redis call can't stall the
  event loop for every other connection.
- Worker status is a **heartbeat published into Redis**, not a live
  read of an in-process object — the API and worker pool are separate
  processes (since Phase 2), so Redis is the only channel between them.
  `WorkerPool.start()` registers every worker id up front; each `Worker`
  publishes its status (with a TTL) on every idle/busy transition, so a
  worker that stops heartbeating — crashed, or a task running longer
  than the TTL without another transition — ages out to `"unknown"`
  rather than showing stale data forever. That "running longer than the
  TTL" case is a known simplification: a proper fix would heartbeat on a
  fixed timer independent of task duration, not just on transitions.
- The pending queue is a Redis sorted set scored by priority
  (`broker/storage.py`), with `broker/heap.py`'s standalone `MinHeap`
  demonstrating the ordering logic in isolation.
- Retries use full-jitter exponential backoff via a delayed ZSET and
  `broker/scheduler.py`'s `RetryScheduler`.
- Terminal failures (retry exhaustion or missing handler) move to a DLQ.
- Every task carries an idempotency key; the worker checks a cached
  result before invoking a handler, so a task processed twice can't run
  its side effect twice.
- `worker_main.py` runs the worker pool and retry scheduler together.

What's deliberately *not* here yet: Docker Compose, a full end-to-end
test run, benchmarks, and (stretch) consistent hashing across multiple
broker nodes. That's the last phase.

## Roadmap

1. ~~Core broker + submission API~~
2. ~~Worker pool & execution~~
3. ~~Priority scheduling~~
4. ~~Retries with exponential backoff + jitter~~
5. ~~Dead-letter queue for permanently-failed tasks~~
6. ~~Idempotency enforcement~~
7. ~~Live WebSocket dashboard~~ (this phase)
8. Hardening — Docker Compose, benchmarks, and (stretch) consistent
   hashing across multiple broker nodes
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
