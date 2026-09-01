from __future__ import annotations

import os

import redis
from fastapi import FastAPI, HTTPException

from .schema import Task, TaskSubmission
from .storage import TaskStore

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

app = FastAPI(title="Task Queue Broker", version="0.1.0")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)
store = TaskStore(redis_client)


@app.post("/tasks", response_model=Task)
def submit_task(submission: TaskSubmission) -> Task:
    task = Task.from_submission(submission)
    store.enqueue(task)
    return task


@app.get("/tasks/{task_id}", response_model=Task)
def get_task(task_id: str) -> Task:
    task = store.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@app.get("/queue/depth")
def queue_depth() -> dict[str, int]:
    return {"depth": store.queue_depth()}
