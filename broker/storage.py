from __future__ import annotations

from typing import Optional

import redis

from .schema import Task

QUEUE_KEY = "broker:queue:pending"
TASK_KEY_PREFIX = "broker:task:"


class TaskStore:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    def _task_key(self, task_id: str) -> str:
        return f"{TASK_KEY_PREFIX}{task_id}"

    def enqueue(self, task: Task) -> None:
        self.redis.set(self._task_key(task.id), task.model_dump_json())
        self.redis.rpush(QUEUE_KEY, task.id)

    def dequeue(self) -> Optional[Task]:
        task_id = self.redis.lpop(QUEUE_KEY)
        if task_id is None:
            return None
        return self.get(task_id)

    def get(self, task_id: str) -> Optional[Task]:
        raw = self.redis.get(self._task_key(task_id))
        if raw is None:
            return None
        return Task.model_validate_json(raw)

    def update(self, task: Task) -> None:
        self.redis.set(self._task_key(task.id), task.model_dump_json())

    def queue_depth(self) -> int:
        return self.redis.llen(QUEUE_KEY)
