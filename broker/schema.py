from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class TaskSubmission(BaseModel):
    """What a client sends when submitting a task."""

    task_type: str = Field(..., description="Name of the registered handler to run")
    payload: dict[str, Any] = Field(default_factory=dict)


class Task(BaseModel):
    """Full task record as stored in the broker."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count: int = 0
    result: Optional[Any] = None
    error: Optional[str] = None

    @classmethod
    def from_submission(cls, submission: TaskSubmission) -> "Task":
        return cls(task_type=submission.task_type, payload=submission.payload)
