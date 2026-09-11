from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from .backoff import compute_backoff_seconds
from .schema import Task, TaskStatus
from .storage import TaskStore

logger = logging.getLogger(__name__)


def handle_task_failure(store: TaskStore, task: Task, error_message: str) -> None:
    task.error = error_message
    task.updated_at = datetime.now(timezone.utc)

    if task.retry_count < task.max_retries:
        task.retry_count += 1
        delay = compute_backoff_seconds(task.retry_count)
        task.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
        task.status = TaskStatus.RETRYING
        store.schedule_retry(task)
        logger.warning(
            "Task %s failed (attempt %s/%s), retrying in %.1fs: %s",
            task.id,
            task.retry_count,
            task.max_retries,
            delay,
            error_message,
        )
    else:
        task.status = TaskStatus.FAILED
        store.move_to_dlq(task)
        logger.error(
            "Task %s permanently failed after %s retries, moved to DLQ: %s",
            task.id,
            task.retry_count,
            error_message,
        )
