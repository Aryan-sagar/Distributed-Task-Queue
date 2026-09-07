from __future__ import annotations

import asyncio
import os

import redis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from .dashboard import DASHBOARD_HTML
from .schema import Task, TaskSubmission
from .storage import TaskStore

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
DASHBOARD_PUSH_INTERVAL_SECONDS = 1.0

app = FastAPI(title="Task Queue Broker", version="0.1.0")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)
store = TaskStore(redis_client)


@app.post("/tasks", response_model=Task)
def submit_task(submission: TaskSubmission) -> Task:
    task = Task.from_submission(submission)
    # enqueue() may return an existing task instead of this one, if
    # submission.idempotency_key was already claimed by a prior submission.
    return store.enqueue(task)


@app.get("/tasks/{task_id}", response_model=Task)
def get_task(task_id: str) -> Task:
    task = store.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@app.get("/queue/depth")
def queue_depth() -> dict[str, int]:
    return {"depth": store.queue_depth()}


@app.get("/dlq", response_model=list[Task])
def list_dlq(limit: int = 100) -> list[Task]:
    return store.list_dlq(limit=limit)


@app.get("/dlq/count")
def dlq_count() -> dict[str, int]:
    return {"count": store.dlq_count()}


@app.post("/dlq/{task_id}/replay", response_model=Task)
def replay_task(task_id: str) -> Task:
    replayed = store.replay(task_id)
    if replayed is None:
        raise HTTPException(status_code=404, detail="Task not found in dead-letter queue")
    return replayed


def _build_dashboard_snapshot() -> dict:
    """Assemble one dashboard frame. Plain sync function (not a method on
    an async route) so it can be offloaded to a thread — see below."""
    return {
        "queue_depth": store.queue_depth(),
        "delayed_count": store.delayed_count(),
        "dlq_count": store.dlq_count(),
        "workers": store.list_worker_statuses(),
    }


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page() -> str:
    return DASHBOARD_HTML


@app.websocket("/ws/dashboard")
async def dashboard_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            # redis-py's client is synchronous/blocking. Running it via
            # to_thread instead of calling it directly keeps this from
            # blocking the event loop that every other connection (API
            # requests, other dashboard viewers) also depends on.
            snapshot = await asyncio.to_thread(_build_dashboard_snapshot)
            await websocket.send_json(snapshot)
            await asyncio.sleep(DASHBOARD_PUSH_INTERVAL_SECONDS)
    except WebSocketDisconnect:
        pass
