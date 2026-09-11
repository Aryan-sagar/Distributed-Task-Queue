from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCESS = "success"
    FAILED = "failed"


class TaskSubmission(BaseModel):
    """What a client sends when submitting a task."""

    task_type: str = Field(..., description="Name of the registered handler to run")
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, description="Higher runs first; ties broken FIFO")
    max_retries: int = Field(default=3, description="Retries allowed before FAILED is terminal")
    lease_seconds: int = Field(
        default=30,
        description="Max expected handler duration; a worker that stops "
        "renewing this task's lease within this window is presumed crashed",
    )
    idempotency_key: Optional[str] = Field(
        default=None,
        description="Optional client-supplied key; resubmitting the same key "
        "returns the existing task instead of creating a duplicate",
    )


class Task(BaseModel):
    """Full task record as stored in the broker."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = 0
    max_retries: int = 3
    lease_seconds: int = 30
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count: int = 0
    next_retry_at: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    # Every task has one, client-supplied or auto-generated. It's the unit
    # of dedup for both submission (see TaskStore.enqueue) and execution
    # (see TaskStore.get_idempotent_result / worker._execute).
    idempotency_key: str = Field(default_factory=lambda: str(uuid.uuid4()))

    @classmethod
    def from_submission(cls, submission: TaskSubmission) -> "Task":
        kwargs: dict[str, Any] = dict(
            task_type=submission.task_type,
            payload=submission.payload,
            priority=submission.priority,
            max_retries=submission.max_retries,
            lease_seconds=submission.lease_seconds,
        )
        if submission.idempotency_key:
            kwargs["idempotency_key"] = submission.idempotency_key
        return cls(**kwargs)
