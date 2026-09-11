import threading
import time

import fakeredis

from broker.reaper import StalledTaskReaper
from broker.schema import Task, TaskStatus, TaskSubmission
from broker.storage import TaskStore


def make_store() -> TaskStore:
    return TaskStore(fakeredis.FakeStrictRedis(decode_responses=True))


def _simulate_crash_mid_execution(store: TaskStore, task: Task, worker_id: str = "worker-0") -> None:
    """Puts a task into exactly the state a crashed worker would leave it
    in: marked RUNNING, marked in-flight, but with its lease already gone
    (as if the TTL had expired) and never cleared."""
    store.mark_inflight(task.id, worker_id, lease_seconds=30)
    task.status = TaskStatus.RUNNING
    store.update(task)
    store.redis.delete(f"broker:lease:{task.id}")


def test_reaper_recovers_stalled_task_with_retries_remaining():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="whatever", max_retries=3))
    store.enqueue(task)
    dequeued = store.dequeue()
    _simulate_crash_mid_execution(store, dequeued)

    reaper = StalledTaskReaper(store, poll_interval_seconds=0.1)
    recovered_count = reaper._reap_once()

    assert recovered_count == 1
    recovered = store.get(dequeued.id)
    assert recovered.status == TaskStatus.RETRYING
    assert recovered.retry_count == 1
    assert "crashed or stalled" in recovered.error
    assert store.delayed_count() == 1
    assert store.queue_depth() == 0


def test_reaper_moves_stalled_task_to_dlq_when_retries_exhausted():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="whatever", max_retries=0))
    store.enqueue(task)
    dequeued = store.dequeue()
    _simulate_crash_mid_execution(store, dequeued)

    reaper = StalledTaskReaper(store, poll_interval_seconds=0.1)
    reaper._reap_once()

    recovered = store.get(dequeued.id)
    assert recovered.status == TaskStatus.FAILED
    assert store.dlq_count() == 1


def test_reaper_leaves_actively_leased_task_alone():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="whatever"))
    store.enqueue(task)
    dequeued = store.dequeue()
    dequeued.status = TaskStatus.RUNNING
    store.update(dequeued)
    store.mark_inflight(dequeued.id, "worker-0", lease_seconds=30)  # lease still valid

    reaper = StalledTaskReaper(store, poll_interval_seconds=0.1)
    recovered_count = reaper._reap_once()

    assert recovered_count == 0
    # The important assertion: it was never touched by the reaper.
    assert store.get(dequeued.id).status == TaskStatus.RUNNING
    assert store.dlq_count() == 0
    assert store.delayed_count() == 0


def test_claim_prevents_double_recovery_of_the_same_task():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="whatever", max_retries=3))
    store.enqueue(task)
    dequeued = store.dequeue()
    _simulate_crash_mid_execution(store, dequeued)

    reaper_a = StalledTaskReaper(store, poll_interval_seconds=0.1)
    reaper_b = StalledTaskReaper(store, poll_interval_seconds=0.1)

    stalled_ids = store.list_stalled_task_ids()
    assert reaper_a.store.claim_stalled_task(stalled_ids[0]) is True
    assert reaper_b.store.claim_stalled_task(stalled_ids[0]) is False


def test_reaper_thread_runs_continuously_and_recovers_in_the_background():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="whatever", max_retries=0))
    store.enqueue(task)
    dequeued = store.dequeue()
    _simulate_crash_mid_execution(store, dequeued)

    reaper = StalledTaskReaper(store, poll_interval_seconds=0.1)
    thread = threading.Thread(target=reaper.run, daemon=True)
    thread.start()

    deadline = time.time() + 3
    while store.dlq_count() == 0 and time.time() < deadline:
        time.sleep(0.05)

    reaper.stop()
    thread.join(timeout=2)

    assert store.dlq_count() == 1
