import fakeredis

from broker.schema import Task, TaskStatus, TaskSubmission
from broker.storage import TaskStore


def make_store() -> TaskStore:
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    return TaskStore(client)


def test_enqueue_dequeue_is_fifo():
    store = make_store()
    t1 = Task.from_submission(TaskSubmission(task_type="send_email", payload={"to": "a"}))
    t2 = Task.from_submission(TaskSubmission(task_type="send_email", payload={"to": "b"}))
    store.enqueue(t1)
    store.enqueue(t2)

    first = store.dequeue()
    second = store.dequeue()

    assert first.id == t1.id
    assert second.id == t2.id


def test_dequeue_empty_queue_returns_none():
    store = make_store()
    assert store.dequeue() is None


def test_get_missing_task_returns_none():
    store = make_store()
    assert store.get("does-not-exist") is None


def test_update_persists_status_change():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="noop"))
    store.enqueue(task)

    task.status = TaskStatus.RUNNING
    store.update(task)

    fetched = store.get(task.id)
    assert fetched.status == TaskStatus.RUNNING
