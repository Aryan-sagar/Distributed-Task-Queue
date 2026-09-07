from __future__ import annotations

import threading
from typing import List

from .storage import TaskStore
from .worker import Worker


class WorkerPool:
    def __init__(self, store: TaskStore, num_workers: int = 4):
        self.store = store
        self.workers: List[Worker] = [
            Worker(worker_id=f"worker-{i}", store=store) for i in range(num_workers)
        ]
        self._threads: List[threading.Thread] = []

    def start(self) -> None:
        for worker in self.workers:
            # Registered up front (not just heartbeated) so the dashboard
            # can list every worker immediately, even in the gap before
            # its first status heartbeat lands.
            self.store.register_worker(worker.worker_id)
            thread = threading.Thread(target=worker.run, daemon=True, name=worker.worker_id)
            thread.start()
            self._threads.append(thread)

    def stop(self, wait: bool = True) -> None:
        for worker in self.workers:
            worker.stop()
        if wait:
            for thread in self._threads:
                thread.join(timeout=5)

    def status(self) -> List[dict]:
        return [
            {
                "worker_id": w.worker_id,
                "status": w.status,
                "current_task_id": w.current_task_id,
            }
            for w in self.workers
        ]
