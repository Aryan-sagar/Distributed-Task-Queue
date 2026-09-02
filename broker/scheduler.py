"""Promotes delayed (retry-scheduled) tasks back onto the pending queue
once their retry time has arrived.

Runs as its own background thread, separate from the worker pool — moving
a due task from the delayed set to the pending set is queue maintenance,
not task execution, so it doesn't belong inside Worker.
"""
from __future__ import annotations

import threading
import time

from .storage import TaskStore

POLL_INTERVAL_SECONDS = 0.5


class RetryScheduler:
    def __init__(self, store: TaskStore):
        self.store = store
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            self.store.promote_due_tasks()
            time.sleep(POLL_INTERVAL_SECONDS)
