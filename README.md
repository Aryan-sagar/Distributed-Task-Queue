# Task Queue Broker

A distributed task queue broker built from scratch (not a wrapper around
Celery/RQ) to understand what one actually has to solve: durable task
storage, worker coordination, priority scheduling, failure handling, and
observability.

## Status: Phase 7 of 8 — Live WebSocket dashboard, plus lease-based crash recovery

What exists right now:
- `POST /tasks` — submit a task (`task_type`, JSON `payload`, optional
  `priority`, `max_retries`, `lease_seconds`, `idempotency_key`).
- `GET /tasks/{id}` — fetch a task's current record.
- `GET /queue/depth` — number of tasks currently waiting.
- `GET /dlq`, `GET /dlq/count`, `POST /dlq/{id}/replay` — dead-letter
  queue inspection and replay.
- `GET /dashboard` — a single self-contained HTML page (no build step)
  showing pending/retrying/dead-lettered counts and live worker status.
- `GET /ws/dashboard` — pushes a JSON snapshot once per second, via
  `asyncio.to_thread` so the (synchronous) Redis calls can't stall the
  event loop for other connections.
- **Crash recovery via leases.** A worker acquires a time-limited lease
  (`task.lease_seconds`, default 30s) when it starts executing a task,
  and actively renews it on a background thread for as long as the
  handler actually runs — a static timeout doesn't work for handlers of
  variable duration, so the lease is kept alive by renewal, not just set
  once up front. `broker/reaper.py`'s `StalledTaskReaper` polls for tasks
  that are still marked in-flight but whose lease has expired — meaning
  the worker that held it crashed (or otherwise stopped renewing)
  without ever reaching a terminal state — and recovers them through
  `broker/failure.py`'s `handle_task_failure`, the exact same
  retry-vs-DLQ decision a normal handler exception goes through. This
  closes a gap flagged (and left open) since Phase 4: at-least-once
  delivery previously only covered *handler exceptions*, not a worker
  process dying mid-handler.
- Worker status is a heartbeat published into Redis with a TTL — the API
  and worker pool are separate processes and share nothing but Redis, so
  a worker that stops heartbeating ages out to `"unknown"` on the
  dashboard.
- The pending queue is a Redis sorted set scored by priority
  (`broker/storage.py`), with `broker/heap.py`'s standalone `MinHeap`
  demonstrating the ordering logic in isolation.
- Retries use full-jitter exponential backoff via a delayed ZSET and
  `broker/scheduler.py`'s `RetryScheduler`.
- Every task carries an idempotency key; the worker checks a cached
  result before invoking a handler, so a task processed twice — whether
  via DLQ replay or crash recovery — can't run its side effect twice.
- `worker_main.py` runs the worker pool, retry scheduler, and stalled-task
  reaper together as one process.

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
7. ~~Live WebSocket dashboard~~, ~~lease-based crash recovery~~ (this phase)
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
