from __future__ import annotations

import os
import signal
import threading
import time

import redis

from broker import handlers  # noqa: F401 — import registers the example handlers
from broker.pool import WorkerPool
from broker.reaper import StalledTaskReaper
from broker.scheduler import RetryScheduler
from broker.storage import TaskStore

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "4"))


def main() -> None:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    store = TaskStore(redis_client)

    pool = WorkerPool(store, num_workers=NUM_WORKERS)
    pool.start()

    scheduler = RetryScheduler(store)
    scheduler_thread = threading.Thread(target=scheduler.run, daemon=True, name="retry-scheduler")
    scheduler_thread.start()

    reaper = StalledTaskReaper(store)
    reaper_thread = threading.Thread(target=reaper.run, daemon=True, name="stalled-task-reaper")
    reaper_thread.start()

    stop_event = threading.Event()

    def handle_signal(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print(
        f"Worker pool running with {NUM_WORKERS} workers, "
        "retry scheduler and stalled-task reaper active. Ctrl+C to stop."
    )
    while not stop_event.is_set():
        time.sleep(1)

    pool.stop()
    scheduler.stop()
    scheduler_thread.join(timeout=5)
    reaper.stop()
    reaper_thread.join(timeout=5)


if __name__ == "__main__":
    main()
