import fakeredis
from fastapi.testclient import TestClient

from broker import api
from broker.schema import Task, TaskSubmission
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


def test_dlq_list_count_and_replay():
    client = make_client()
    task = Task.from_submission(TaskSubmission(task_type="broken"))
    task.error = "boom"
    api.store.move_to_dlq(task)

    resp = client.get("/dlq")
    assert resp.status_code == 200
    assert [t["id"] for t in resp.json()] == [task.id]

    resp = client.get("/dlq/count")
    assert resp.json()["count"] == 1

    resp = client.post(f"/dlq/{task.id}/replay")
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending"

    resp = client.get("/dlq/count")
    assert resp.json()["count"] == 0

    resp = client.get("/queue/depth")
    assert resp.json()["depth"] == 1


def test_replay_unknown_task_returns_404():
    client = make_client()
    resp = client.post("/dlq/does-not-exist/replay")
    assert resp.status_code == 404


def test_resubmitting_same_idempotency_key_returns_same_task():
    client = make_client()

    resp1 = client.post(
        "/tasks",
        json={"task_type": "send_email", "payload": {"to": "a"}, "idempotency_key": "req-1"},
    )
    resp2 = client.post(
        "/tasks",
        json={"task_type": "send_email", "payload": {"to": "a"}, "idempotency_key": "req-1"},
    )

    assert resp1.json()["id"] == resp2.json()["id"]

    depth = client.get("/queue/depth").json()["depth"]
    assert depth == 1
