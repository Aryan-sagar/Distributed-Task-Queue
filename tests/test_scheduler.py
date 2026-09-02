import threading
import time
from datetime import datetime, timedelta, timezone

import fakeredis

from broker.schema import Task, TaskSubmission
from broker.scheduler import RetryScheduler
from broker.storage import TaskStore


def make_store() -> TaskStore:
    return TaskStore(fakeredis.FakeStrictRedis(decode_responses=True))


def test_scheduler_promotes_due_task_automatically():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="flaky"))
    task.next_retry_at = datetime.now(timezone.utc) - timedelta(seconds=1)  # already due
    store.schedule_retry(task)

    scheduler = RetryScheduler(store)
    thread = threading.Thread(target=scheduler.run, daemon=True)
    thread.start()

    deadline = time.time() + 3
    while store.queue_depth() == 0 and time.time() < deadline:
        time.sleep(0.05)

    scheduler.stop()
    thread.join(timeout=2)

    assert store.queue_depth() == 1
    assert store.delayed_count() == 0


def test_scheduler_leaves_not_yet_due_task_untouched():
    store = make_store()
    task = Task.from_submission(TaskSubmission(task_type="flaky"))
    task.next_retry_at = datetime.now(timezone.utc) + timedelta(hours=1)
    store.schedule_retry(task)

    scheduler = RetryScheduler(store)
    thread = threading.Thread(target=scheduler.run, daemon=True)
    thread.start()

    time.sleep(1)  # give it a couple of poll cycles

    scheduler.stop()
    thread.join(timeout=2)

    assert store.queue_depth() == 0
    assert store.delayed_count() == 1
