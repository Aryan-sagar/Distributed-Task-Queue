from datetime import datetime, timedelta, timezone

import fakeredis
import pytest

from broker.schema import Task, TaskStatus, TaskSubmission
from broker.storage import TaskStore


def make_store() -> TaskStore:
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    return TaskStore(client)


def test_fifo_among_equal_priority():
    store = make_store()
    t1 = Task.from_submission(TaskSubmission(task_type="send_email", payload={"to": "a"}))
    t2 = Task.from_submission(TaskSubmission(task_type="send_email", payload={"to": "b"}))
    store.enqueue(t1)
    store.enqueue(t2)

    first = store.dequeue()
    second = store.dequeue()

    assert first.id == t1.id
    assert second.id == t2.id


def test_higher_priority_dequeued_first_even_if_submitted_later():
    store = make_store()
    low = Task.from_submission(TaskSubmission(task_type="report", priority=0))
    high = Task.from_submission(TaskSubmission(task_type="payment_retry", priority=10))
    store.enqueue(low)
    store.enqueue(high)  # submitted second, but higher priority

    first = store.dequeue()
    assert first.id == high.id

    second = store.dequeue()
    assert second.id == low.id


def test_multiple_priority_levels_interleaved():
    store = make_store()
    tasks = [
        Task.from_submission(TaskSubmission(task_type="t", priority=p))
        for p in [1, 5, 1, 10, 5, 0]
    ]
    for t in tasks:
        store.enqueue(t)

    order = [store.dequeue().id for _ in tasks]
    expected_order = [
        tasks[3].id,  # priority 10
        tasks[1].id,  # priority 5, submitted before tasks[4]
        tasks[4].id,  # priority 5
        tasks[0].id,  # priority 1, submitted before tasks[2]
        tasks[2].id,  # priority 1
        tasks[5].id,  # priority 0
    ]
    assert order == expected_order


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


def test_queue_depth_reflects_pending_count():
    store = make_store()
    store.enqueue(Task.from_submission(TaskSubmission(task_type="a")))
    store.enqueue(Task.from_submission(TaskSubmission(task_type="b")))
    assert store.queue_depth() == 2

    store.dequeue()
    assert store.queue_depth() == 1


def test_schedule_retry_parks_task_in_delayed_set_not_pending():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="flaky"))
    task.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=30)
    store.schedule_retry(task)

    assert store.queue_depth() == 0
    assert store.delayed_count() == 1
    assert store.dequeue() is None


def test_promote_due_tasks_moves_ready_task_to_pending():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="flaky"))
    task.next_retry_at = datetime.now(timezone.utc) - timedelta(seconds=1)  # already due
    store.schedule_retry(task)

    promoted = store.promote_due_tasks()

    assert promoted == 1
    assert store.delayed_count() == 0
    assert store.queue_depth() == 1

    dequeued = store.dequeue()
    assert dequeued.id == task.id
    assert dequeued.status == TaskStatus.PENDING


def test_promote_due_tasks_leaves_not_yet_due_task_alone():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="flaky"))
    task.next_retry_at = datetime.now(timezone.utc) + timedelta(hours=1)  # not due
    store.schedule_retry(task)

    promoted = store.promote_due_tasks()

    assert promoted == 0
    assert store.delayed_count() == 1
    assert store.queue_depth() == 0


def test_schedule_retry_without_next_retry_at_raises():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="flaky"))
    with pytest.raises(ValueError):
        store.schedule_retry(task)


def test_move_to_dlq_marks_failed_and_indexes_task():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="broken"))
    task.error = "boom"
    store.move_to_dlq(task)

    assert store.dlq_count() == 1
    listed = store.list_dlq()
    assert [t.id for t in listed] == [task.id]
    assert listed[0].status == TaskStatus.FAILED
    assert listed[0].error == "boom"


def test_list_dlq_respects_limit():
    store = make_store()
    for i in range(5):
        t = Task.from_submission(TaskSubmission(task_type="broken", payload={"n": i}))
        store.move_to_dlq(t)

    assert store.dlq_count() == 5
    assert len(store.list_dlq(limit=2)) == 2
    assert len(store.list_dlq()) == 5


def test_replay_moves_task_from_dlq_to_pending_reset():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="broken"))
    task.retry_count = 3
    task.error = "boom"
    store.move_to_dlq(task)

    replayed = store.replay(task.id)

    assert replayed is not None
    assert replayed.status == TaskStatus.PENDING
    assert replayed.retry_count == 0
    assert replayed.error is None
    assert store.dlq_count() == 0
    assert store.queue_depth() == 1

    dequeued = store.dequeue()
    assert dequeued.id == task.id


def test_replay_unknown_task_returns_none():
    store = make_store()
    assert store.replay("does-not-exist") is None


def test_list_worker_statuses_reports_unknown_before_registration():
    store = make_store()
    assert store.list_worker_statuses() == []


def test_registered_worker_reports_unknown_before_first_heartbeat():
    store = make_store()
    store.register_worker("worker-0")

    statuses = store.list_worker_statuses()
    assert statuses == [{"worker_id": "worker-0", "status": "unknown", "current_task_id": None}]


def test_published_status_is_reflected_and_ordered_by_worker_id():
    store = make_store()
    store.register_worker("worker-1")
    store.register_worker("worker-0")
    store.publish_worker_status("worker-0", "busy", "task-abc")
    store.publish_worker_status("worker-1", "idle", None)

    statuses = store.list_worker_statuses()
    assert statuses == [
        {"worker_id": "worker-0", "status": "busy", "current_task_id": "task-abc"},
        {"worker_id": "worker-1", "status": "idle", "current_task_id": None},
    ]
