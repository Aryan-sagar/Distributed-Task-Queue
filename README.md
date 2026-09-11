# Task Queue Broker

A distributed task queue broker built from scratch (not a wrapper around
Celery/RQ) to understand what one actually has to solve: durable task
storage, worker coordination, priority scheduling, failure handling, and
observability.

## Status: Phase 7 of 8 — Live WebSocket dashboard + lease-based crash recovery

What exists right now:

- `POST /tasks` — submit a task (`task_type`, JSON `payload`, optional
  `priority`, `max_retries`, `lease_seconds`, `idempotency_key`).
- `GET /tasks/{id}` — fetch a task's current record.
- `GET /queue/depth` — number of tasks currently waiting.
- `GET /dlq`, `GET /dlq/count`, `POST /dlq/{id}/replay` — dead-letter
  queue inspection and replay.
- `GET /dashboard` — a self-contained HTML dashboard showing pending,
  retrying, dead-lettered counts and live worker status.
- `GET /ws/dashboard` — pushes a JSON snapshot once per second. Blocking
  Redis calls run through `asyncio.to_thread` so they don't stall the
  async event loop.
- **Crash recovery via leases.** A worker acquires a time-limited lease
  (`task.lease_seconds`, default 30s) while executing a task and actively
  renews it while the handler runs. `broker/reaper.py`'s
  `StalledTaskReaper` detects tasks whose leases have expired and recovers
  them through the same retry-vs-DLQ path used for normal failures.
- Worker status is a **heartbeat published into Redis** with a TTL.
  Because the API and worker pool are separate processes, Redis is the
  shared source of worker state. Workers publish their status while
  transitioning between idle and busy, and stale workers eventually age
  out to `"unknown"`.
- The pending queue is a Redis sorted set scored by priority
  (`broker/storage.py`), with `broker/heap.py`'s standalone `MinHeap`
  demonstrating the ordering logic in isolation.
- Retries use full-jitter exponential backoff via a delayed ZSET and
  `broker/scheduler.py`'s `RetryScheduler`.
- Terminal failures (retry exhaustion or missing handler) move to a DLQ.
- Every task carries an idempotency key; the worker checks a cached
  result before invoking a handler, preventing repeated execution after
  successful prior processing.
- `worker_main.py` runs the worker pool, retry scheduler, and stalled-task
  reaper together as one process.

What's deliberately *not* here yet: Docker Compose, benchmarks, and
(stretch) consistent hashing across multiple broker nodes. That's the
last phase.

## Roadmap

1. ~~Core broker + submission API~~
2. ~~Worker pool & execution~~
3. ~~Priority scheduling~~
4. ~~Retries with exponential backoff + jitter~~
5. ~~Dead-letter queue for permanently-failed tasks~~
6. ~~Idempotency enforcement~~
7. ~~Live WebSocket dashboard + lease-based crash recovery~~
8. Hardening — Docker Compose, benchmarks, and (stretch) consistent
   hashing across multiple broker nodes

## Running it

```bash
pip install -r requirements-dev.txt
redis-server &                # or point REDIS_URL at an existing instance
uvicorn main:app --reload &   # API process
python worker_main.py         # worker pool process