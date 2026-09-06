from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from .backoff import compute_backoff_seconds
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
            # Not retryable — a missing handler is a config problem that
            # won't fix itself on the next attempt. Still goes to the DLQ
            # so an operator can see it and replay once the handler exists.
            task.status = TaskStatus.FAILED
            task.error = f"No handler registered for task_type={task.task_type!r}"
            task.updated_at = datetime.now(timezone.utc)
            self.store.move_to_dlq(task)
            self.current_task_id = None
            return

        # Idempotency check: if this exact unit of work already completed
        # successfully — e.g. this task is being processed a second time
        # because it was replayed from the DLQ after it had actually
        # already succeeded — don't run the handler's side effect again.
        # Just report the cached outcome.
        cached_result = self.store.get_idempotent_result(task.idempotency_key)
        if cached_result is not None:
            task.status = TaskStatus.SUCCESS
            task.result = cached_result
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
            logger.exception("Task %s raised during execution", task.id)
            self._handle_failure(task, exc)
        else:
            task.status = TaskStatus.SUCCESS
            task.result = result
            task.updated_at = datetime.now(timezone.utc)
            self.store.update(task)
            self.store.record_idempotent_result(task.idempotency_key, result)

        self.current_task_id = None

    def _handle_failure(self, task: Task, exc: Exception) -> None:
        task.error = str(exc)
        task.updated_at = datetime.now(timezone.utc)

        if task.retry_count < task.max_retries:
            task.retry_count += 1
            delay = compute_backoff_seconds(task.retry_count)
            task.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
            task.status = TaskStatus.RETRYING
            self.store.schedule_retry(task)
            logger.warning(
                "Task %s failed (attempt %s/%s), retrying in %.1fs",
                task.id,
                task.retry_count,
                task.max_retries,
                delay,
            )
        else:
            task.status = TaskStatus.FAILED
            self.store.move_to_dlq(task)
            logger.error(
                "Task %s permanently failed after %s retries, moved to DLQ",
                task.id,
                task.retry_count,
            )

