from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from .failure import handle_task_failure
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
                self.store.publish_worker_status(self.worker_id, self.status, None)
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            self._execute(task)
        self.status = "stopped"
        self.store.publish_worker_status(self.worker_id, self.status, None)

    def _execute(self, task: Task) -> None:
        """Run a single task to completion and persist the result.

        Split out from run() so tests can drive one task through a worker
        synchronously without spinning up a thread.
        """
        self.status = "busy"
        self.current_task_id = task.id
        self.store.publish_worker_status(self.worker_id, self.status, task.id)

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
        self.store.mark_inflight(task.id, self.worker_id, task.lease_seconds)

        lease_stop = threading.Event()
        renewal_thread = threading.Thread(
            target=self._renew_lease_periodically,
            args=(task.id, task.lease_seconds, lease_stop),
            daemon=True,
        )
        renewal_thread.start()

        try:
            result = handler(task.payload)
        except Exception as exc:  # noqa: BLE001 — any handler failure lands here
            logger.exception("Task %s raised during execution", task.id)
            handle_task_failure(self.store, task, str(exc))
        else:
            task.status = TaskStatus.SUCCESS
            task.result = result
            task.updated_at = datetime.now(timezone.utc)
            self.store.update(task)
            self.store.record_idempotent_result(task.idempotency_key, result)
        finally:
            lease_stop.set()
            renewal_thread.join(timeout=1)
            self.store.clear_inflight(task.id)

        self.current_task_id = None

    def _renew_lease_periodically(
        self, task_id: str, lease_seconds: int, stop_event: threading.Event
    ) -> None:
        """Runs on its own thread for the duration of one handler call.
        Renews at half the lease interval, so a single missed renewal
        (a slow Redis round-trip, say) doesn't cost the lease outright."""
        interval = max(1.0, lease_seconds / 2)
        while not stop_event.wait(interval):
            renewed = self.store.renew_lease(task_id, self.worker_id, lease_seconds)
            if not renewed:
                # Something else (a reaper) already reclaimed this task —
                # nothing more for this thread to do.
                return
