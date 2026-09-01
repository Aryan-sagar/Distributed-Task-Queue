from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from .registry import get_handler
from .schema import Task, TaskStatus
from .storage import TaskStore

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 0.5


class Worker:
    def __init__(self, worker_id: str, store: TaskStore):
        self.worker_id = worker_id
        self.store = store
        self._stop_event = threading.Event()
        self.status = "idle"  # idle | busy | stopped
        self.current_task_id: Optional[str] = None

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        """Main loop — call this on its own thread. Blocks until stop()."""
        while not self._stop_event.is_set():
            task = self.store.dequeue()
            if task is None:
                self.status = "idle"
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            self._execute(task)
        self.status = "stopped"

    def _execute(self, task: Task) -> None:
        """Run a single task to completion and persist the result.

        Split out from run() so tests can drive one task through a worker
        synchronously without spinning up a thread.
        """
        self.status = "busy"
        self.current_task_id = task.id

        handler = get_handler(task.task_type)
        if handler is None:
            task.status = TaskStatus.FAILED
            task.error = f"No handler registered for task_type={task.task_type!r}"
            task.updated_at = datetime.now(timezone.utc)
            self.store.update(task)
            self.current_task_id = None
            return

        task.status = TaskStatus.RUNNING
        task.updated_at = datetime.now(timezone.utc)
        self.store.update(task)

        try:
            result = handler(task.payload)
        except Exception as exc:  # noqa: BLE001 — any handler failure lands here
            task.status = TaskStatus.FAILED
            task.error = str(exc)
            logger.exception("Task %s failed", task.id)
        else:
            task.status = TaskStatus.SUCCESS
            task.result = result

        task.updated_at = datetime.now(timezone.utc)
        self.store.update(task)
        self.current_task_id = None
