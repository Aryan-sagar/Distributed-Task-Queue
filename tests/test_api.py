import fakeredis
from fastapi.testclient import TestClient

from broker import api
from broker.storage import TaskStore


def make_client() -> TestClient:
    api.store = TaskStore(fakeredis.FakeStrictRedis(decode_responses=True))
    return TestClient(api.app)


def test_submit_and_fetch_task():
    client = make_client()

    resp = client.post("/tasks", json={"task_type": "send_email", "payload": {"to": "a@b.com"}})
    assert resp.status_code == 200
    task_id = resp.json()["id"]

    resp = client.get(f"/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending"


def test_get_unknown_task_returns_404():
    client = make_client()
    resp = client.get("/tasks/does-not-exist")
    assert resp.status_code == 404


def test_queue_depth_reflects_submissions():
    client = make_client()
    client.post("/tasks", json={"task_type": "noop", "payload": {}})
    client.post("/tasks", json={"task_type": "noop", "payload": {}})

    resp = client.get("/queue/depth")
    assert resp.json()["depth"] == 2
