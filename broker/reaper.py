from __future__ import annotations

import logging
import threading
import time

from .failure import handle_task_failure
from .storage import TaskStore

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SECONDS = 5.0
STALL_ERROR_MESSAGE = "Worker crashed or stalled during execution (lease expired)"


class StalledTaskReaper:
    def __init__(
        self, store: TaskStore, poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS
    ):
        self.store = store
        self.poll_interval_seconds = poll_interval_seconds
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            self._reap_once()
            time.sleep(self.poll_interval_seconds)

    def _reap_once(self) -> int:
        """One pass: recover every currently-stalled task. Returns how
        many were recovered. Split out from run() so tests can drive a
        single pass synchronously without a background thread."""
        recovered = 0
        for task_id in self.store.list_stalled_task_ids():
            if not self.store.claim_stalled_task(task_id):
                # Lost the race — another reaper claimed it first, or the
                # original worker actually finished in the gap between
                # the lease expiring and this pass running.
                continue
            task = self.store.get(task_id)
            if task is None:
                continue
            logger.warning("Recovering stalled task %s", task_id)
            handle_task_failure(self.store, task, STALL_ERROR_MESSAGE)
            recovered += 1
        return recovered
