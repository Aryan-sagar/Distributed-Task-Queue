import threading
import time

import fakeredis
import pytest

from broker.pool import WorkerPool
from broker.registry import clear_registry, register
from broker.schema import Task, TaskStatus, TaskSubmission
from broker.storage import TaskStore
from broker.worker import Worker


def make_store() -> TaskStore:
    return TaskStore(fakeredis.FakeStrictRedis(decode_responses=True))


@pytest.fixture(autouse=True)
def _clear_registry_between_tests():
    yield
    clear_registry()


def test_worker_executes_registered_handler_and_marks_success():
    calls = []

    @register("greet")
    def handle_greet(payload):
        calls.append(payload)
        return f"hello {payload['name']}"

    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="greet", payload={"name": "Aryan"}))
    store.enqueue(task)

    worker = Worker(worker_id="w1", store=store)
    worker._execute(store.dequeue())

    updated = store.get(task.id)
    assert updated.status == TaskStatus.SUCCESS
    assert updated.result == "hello Aryan"
    assert calls == [{"name": "Aryan"}]


def test_worker_marks_failed_on_handler_exception():
    @register("boom")
    def handle_boom(payload):
        raise ValueError("kaboom")

    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="boom"))
    store.enqueue(task)

    worker = Worker(worker_id="w1", store=store)
    worker._execute(store.dequeue())

    updated = store.get(task.id)
    assert updated.status == TaskStatus.FAILED
    assert "kaboom" in updated.error


def test_worker_marks_failed_when_no_handler_registered():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="does_not_exist"))
    store.enqueue(task)

    worker = Worker(worker_id="w1", store=store)
    worker._execute(store.dequeue())

    updated = store.get(task.id)
    assert updated.status == TaskStatus.FAILED
    assert "No handler registered" in updated.error


def test_pool_processes_multiple_tasks_concurrently():
    processed = []
    lock = threading.Lock()

    @register("record")
    def handle_record(payload):
        with lock:
            processed.append(payload["n"])

    store = make_store()
    for i in range(10):
        store.enqueue(Task.from_submission(TaskSubmission(task_type="record", payload={"n": i})))

    pool = WorkerPool(store, num_workers=3)
    pool.start()

    deadline = time.time() + 5
    while len(processed) < 10 and time.time() < deadline:
        time.sleep(0.1)

    pool.stop()

    assert sorted(processed) == list(range(10))
