import fakeredis
import pytest

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


def test_duplicate_submission_with_same_idempotency_key_returns_existing_task():
    store = make_store()
    submission = TaskSubmission(task_type="charge_card", idempotency_key="order-123")
    # Two separate Task objects, as if the client retried the HTTP call
    # after a timeout and the server built a fresh Task both times — the
    # idempotency_key is what ties them together.
    first_attempt = Task.from_submission(submission)
    second_attempt = Task.from_submission(submission)

    result1 = store.enqueue(first_attempt)
    result2 = store.enqueue(second_attempt)

    assert result1.id == result2.id
    assert store.queue_depth() == 1


def test_different_idempotency_keys_create_separate_tasks():
    store = make_store()
    t1 = Task.from_submission(TaskSubmission(task_type="charge_card", idempotency_key="order-1"))
    t2 = Task.from_submission(TaskSubmission(task_type="charge_card", idempotency_key="order-2"))

    store.enqueue(t1)
    store.enqueue(t2)

    assert store.queue_depth() == 2


def test_auto_generated_keys_never_collide_across_unrelated_submissions():
    store = make_store()
    t1 = Task.from_submission(TaskSubmission(task_type="noop"))
    t2 = Task.from_submission(TaskSubmission(task_type="noop"))

    store.enqueue(t1)
    store.enqueue(t2)

    assert store.queue_depth() == 2
    assert t1.idempotency_key != t2.idempotency_key


def test_idempotent_result_round_trip():
    store = make_store()
    assert store.get_idempotent_result("order-123") is None

    store.record_idempotent_result("order-123", {"charged": True, "amount": 500})

    assert store.get_idempotent_result("order-123") == {"charged": True, "amount": 500}


def test_worker_does_not_rerun_handler_for_already_completed_idempotency_key():
    call_count = {"n": 0}

    @register("charge_card")
    def handle_charge_card(payload):
        call_count["n"] += 1
        return {"charged": True}

    store = make_store()
    task = Task.from_submission(
        TaskSubmission(task_type="charge_card", idempotency_key="order-999")
    )
    store.enqueue(task)

    worker = Worker(worker_id="w1", store=store)
    worker._execute(store.dequeue())

    assert call_count["n"] == 1
    assert store.get(task.id).status == TaskStatus.SUCCESS

    # Simulate the same logical task being delivered to a worker a second
    # time (e.g. replayed from the DLQ after it had actually already
    # succeeded). The handler must NOT run again.
    store.enqueue(task)
    worker._execute(store.dequeue())

    assert call_count["n"] == 1  # still 1, not 2
    updated = store.get(task.id)
    assert updated.status == TaskStatus.SUCCESS
    assert updated.result == {"charged": True}


def test_worker_runs_handler_normally_when_no_cached_result_exists():
    call_count = {"n": 0}

    @register("first_time")
    def handle_first_time(payload):
        call_count["n"] += 1
        return "ok"

    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="first_time"))
    store.enqueue(task)

    worker = Worker(worker_id="w1", store=store)
    worker._execute(store.dequeue())

    assert call_count["n"] == 1
    assert store.get(task.id).result == "ok"
