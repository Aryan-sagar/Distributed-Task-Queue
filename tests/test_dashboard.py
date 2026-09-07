import fakeredis
from fastapi.testclient import TestClient

from broker import api
from broker.schema import Task, TaskSubmission
from broker.storage import TaskStore


def make_client() -> TestClient:
    api.store = TaskStore(fakeredis.FakeStrictRedis(decode_responses=True))
    return TestClient(api.app)


def test_dashboard_page_serves_html():
    client = make_client()
    resp = client.get("/dashboard")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "ws/dashboard" in resp.text


def test_websocket_dashboard_streams_snapshot_with_current_counts():
    client = make_client()
    api.store.enqueue(Task.from_submission(TaskSubmission(task_type="noop")))
    api.store.register_worker("worker-0")
    api.store.publish_worker_status("worker-0", "idle", None)

    with client.websocket_connect("/ws/dashboard") as ws:
        snapshot = ws.receive_json()

    assert snapshot["queue_depth"] == 1
    assert snapshot["delayed_count"] == 0
    assert snapshot["dlq_count"] == 0
    assert snapshot["workers"] == [
        {"worker_id": "worker-0", "status": "idle", "current_task_id": None}
    ]
