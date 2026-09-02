from __future__ import annotations

import time
from typing import List, Optional

import redis

from .schema import Task, TaskStatus

QUEUE_KEY = "broker:queue:pending"
DELAYED_KEY = "broker:queue:delayed"
DLQ_KEY = "broker:queue:dlq"
SEQUENCE_KEY = "broker:queue:sequence"
TASK_KEY_PREFIX = "broker:task:"

# Zero-padded wide enough that lexicographic order matches numeric order
# for any realistic task volume.
SEQUENCE_WIDTH = 20


class TaskStore:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    def _task_key(self, task_id: str) -> str:
        return f"{TASK_KEY_PREFIX}{task_id}"

    @staticmethod
    def _member(sequence: int, task_id: str) -> str:
        return f"{sequence:0{SEQUENCE_WIDTH}d}:{task_id}"

    @staticmethod
    def _task_id_from_member(member: str) -> str:
        # task ids (uuid4) never contain ':', so split once from the left.
        return member.split(":", 1)[1]

    def enqueue(self, task: Task) -> None:
        self.redis.set(self._task_key(task.id), task.model_dump_json())
        sequence = self.redis.incr(SEQUENCE_KEY)
        member = self._member(sequence, task.id)
        self.redis.zadd(QUEUE_KEY, {member: -task.priority})

    def dequeue(self) -> Optional[Task]:
        popped = self.redis.zpopmin(QUEUE_KEY, count=1)
        if not popped:
            return None
        member, _score = popped[0]
        task_id = self._task_id_from_member(member)
        return self.get(task_id)

    def get(self, task_id: str) -> Optional[Task]:
        raw = self.redis.get(self._task_key(task_id))
        if raw is None:
            return None
        return Task.model_validate_json(raw)

    def update(self, task: Task) -> None:
        self.redis.set(self._task_key(task.id), task.model_dump_json())

    def queue_depth(self) -> int:
        return self.redis.zcard(QUEUE_KEY)

    def schedule_retry(self, task: Task) -> None:
        """Persist a task that failed but has retries remaining, and park
        it in the delayed set until task.next_retry_at — this is what
        actually delays the retry, rather than requeuing it instantly."""
        if task.next_retry_at is None:
            raise ValueError("task.next_retry_at must be set before scheduling a retry")
        self.update(task)
        self.redis.zadd(DELAYED_KEY, {task.id: task.next_retry_at.timestamp()})

    def promote_due_tasks(self, now: Optional[float] = None) -> int:
        """Move delayed tasks whose retry time has passed back onto the
        pending queue. Returns how many were promoted. Safe to call from
        multiple processes: ZREM's return value acts as a claim, so two
        callers racing on the same due task can't both promote it."""
        now = now if now is not None else time.time()
        due_ids = self.redis.zrangebyscore(DELAYED_KEY, min="-inf", max=now)
        promoted = 0
        for task_id in due_ids:
            if not self.redis.zrem(DELAYED_KEY, task_id):
                continue  # another promoter already claimed this one
            task = self.get(task_id)
            if task is None:
                continue
            task.status = TaskStatus.PENDING
            self.enqueue(task)
            promoted += 1
        return promoted

    def delayed_count(self) -> int:
        return self.redis.zcard(DELAYED_KEY)

    def move_to_dlq(self, task: Task) -> None:
        """Mark a task terminally failed and index it in the dead-letter
        set for operator visibility and replay. Called both when retries
        are exhausted and when a task fails in a way retrying can't fix
        (e.g. no handler registered) — either way, a human needs to look
        at it, which is what the DLQ is for."""
        task.status = TaskStatus.FAILED
        self.update(task)
        self.redis.sadd(DLQ_KEY, task.id)

    def list_dlq(self, limit: Optional[int] = None) -> List[Task]:
        task_ids = list(self.redis.smembers(DLQ_KEY))
        if limit is not None:
            task_ids = task_ids[:limit]
        tasks = [self.get(task_id) for task_id in task_ids]
        return [t for t in tasks if t is not None]

    def dlq_count(self) -> int:
        return self.redis.scard(DLQ_KEY)

    def replay(self, task_id: str) -> Optional[Task]:
        """Move a task out of the DLQ and back onto the pending queue as a
        fresh attempt (retry_count reset). Returns the replayed task, or
        None if it wasn't in the DLQ. SREM's return value is the claim, so
        this is safe if two callers race to replay the same task."""
        removed = self.redis.srem(DLQ_KEY, task_id)
        if not removed:
            return None
        task = self.get(task_id)
        if task is None:
            return None
        task.status = TaskStatus.PENDING
        task.retry_count = 0
        task.error = None
        task.next_retry_at = None
        self.enqueue(task)
        return task

