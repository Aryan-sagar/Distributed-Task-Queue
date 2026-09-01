"""Entrypoint to run the worker pool as a standalone process, separate
from the API process — this mirrors how a real broker deploys (API and
workers scale independently).

Run with: python worker_main.py
Requires REDIS_URL pointing at the same Redis instance the API uses.
"""
from __future__ import annotations

import os
import signal
import threading
import time

import redis

from broker import handlers  # noqa: F401 — import registers the example handlers
from broker.pool import WorkerPool
from broker.storage import TaskStore

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "4"))


def main() -> None:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    store = TaskStore(redis_client)
    pool = WorkerPool(store, num_workers=NUM_WORKERS)
    pool.start()

    stop_event = threading.Event()

    def handle_signal(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print(f"Worker pool running with {NUM_WORKERS} workers. Ctrl+C to stop.")
    while not stop_event.is_set():
        time.sleep(1)

    pool.stop()


if __name__ == "__main__":
    main()
