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


def test_worker_marks_failed_on_handler_exception_when_no_retries_allowed():
    @register("boom")
    def handle_boom(payload):
        raise ValueError("kaboom")

    store = make_store()
    # max_retries=0: this exception has nowhere to retry to, so it's a
    # direct assertion of the terminal-failure path (distinct from
    # test_worker_schedules_retry_on_failure_within_max_retries below,
    # which covers the same exception when retries ARE available).
    task = Task.from_submission(TaskSubmission(task_type="boom", max_retries=0))
    store.enqueue(task)

    worker = Worker(worker_id="w1", store=store)
    worker._execute(store.dequeue())

    updated = store.get(task.id)
    assert updated.status == TaskStatus.FAILED
    assert "kaboom" in updated.error
    assert store.dlq_count() == 1


def test_worker_marks_failed_when_no_handler_registered():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="does_not_exist"))
    store.enqueue(task)

    worker = Worker(worker_id="w1", store=store)
    worker._execute(store.dequeue())

    updated = store.get(task.id)
    assert updated.status == TaskStatus.FAILED
    assert "No handler registered" in updated.error
    assert store.dlq_count() == 1


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


def test_pool_registers_all_workers_on_start():
    store = make_store()
    pool = WorkerPool(store, num_workers=3)
    pool.start()
    pool.stop()

    statuses = store.list_worker_statuses()
    assert {s["worker_id"] for s in statuses} == {"worker-0", "worker-1", "worker-2"}


def test_worker_publishes_busy_status_during_execution():
    published = []

    @register("record_status")
    def handle_record_status(payload):
        # Snapshot what the store says about this worker mid-handler.
        published.append(store.list_worker_statuses())
        return "ok"

    store = make_store()
    store.register_worker("w1")
    task = Task.from_submission(TaskSubmission(task_type="record_status"))
    store.enqueue(task)

    worker = Worker(worker_id="w1", store=store)
    worker._execute(store.dequeue())

    assert published[0] == [{"worker_id": "w1", "status": "busy", "current_task_id": task.id}]


def test_worker_schedules_retry_on_failure_within_max_retries():
    @register("flaky")
    def handle_flaky(payload):
        raise RuntimeError("transient error")

    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="flaky", max_retries=3))
    store.enqueue(task)

    worker = Worker(worker_id="w1", store=store)
    worker._execute(store.dequeue())

    updated = store.get(task.id)
    assert updated.status == TaskStatus.RETRYING
    assert updated.retry_count == 1
    assert updated.next_retry_at is not None
    assert "transient error" in updated.error

    # Not immediately requeued — it's parked in the delayed set.
    assert store.queue_depth() == 0
    assert store.delayed_count() == 1


def test_worker_marks_permanently_failed_after_max_retries_exhausted():
    @register("always_fails")
    def handle_always_fails(payload):
        raise RuntimeError("still broken")

    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="always_fails", max_retries=1))
    task.retry_count = 1  # already used its one retry
    store.enqueue(task)

    worker = Worker(worker_id="w1", store=store)
    worker._execute(store.dequeue())

    updated = store.get(task.id)
    assert updated.status == TaskStatus.FAILED
    assert store.delayed_count() == 0  # not scheduled again
    assert store.dlq_count() == 1


def test_replay_after_worker_exhausts_retries():
    attempts = []

    @register("fails_then_recorded")
    def handle_fails_then_recorded(payload):
        attempts.append(1)
        raise RuntimeError("nope")

    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="fails_then_recorded", max_retries=0))
    store.enqueue(task)

    worker = Worker(worker_id="w1", store=store)
    worker._execute(store.dequeue())

    assert store.get(task.id).status == TaskStatus.FAILED
    assert store.dlq_count() == 1

    replayed = store.replay(task.id)
    assert replayed.status == TaskStatus.PENDING
    assert store.dlq_count() == 0
    assert store.queue_depth() == 1


def test_retried_task_eventually_becomes_dequeueable_again():
    attempts = []

    @register("eventually_succeeds")
    def handle_eventually_succeeds(payload):
        attempts.append(1)
        if len(attempts) < 2:
            raise RuntimeError("not yet")
        return "ok"

    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="eventually_succeeds", max_retries=3))
    store.enqueue(task)

    worker = Worker(worker_id="w1", store=store)
    worker._execute(store.dequeue())  # first attempt fails, gets scheduled

    assert store.get(task.id).status == TaskStatus.RETRYING

    # Force it to be "due" and promote it back onto the pending queue.
    due_task = store.get(task.id)
    due_task.next_retry_at = due_task.updated_at  # already in the past by now
    store.schedule_retry(due_task)
    promoted = store.promote_due_tasks()
    assert promoted == 1

    worker._execute(store.dequeue())  # second attempt succeeds
    assert store.get(task.id).status == TaskStatus.SUCCESS
