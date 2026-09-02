import pytest

from broker.heap import MinHeap


def test_pops_highest_priority_first():
    heap = MinHeap()
    heap.push(priority=1, sequence=0, task_id="low")
    heap.push(priority=10, sequence=1, task_id="high")
    heap.push(priority=5, sequence=2, task_id="mid")

    assert heap.pop() == "high"
    assert heap.pop() == "mid"
    assert heap.pop() == "low"


def test_fifo_among_equal_priority():
    heap = MinHeap()
    heap.push(priority=1, sequence=0, task_id="first")
    heap.push(priority=1, sequence=1, task_id="second")
    heap.push(priority=1, sequence=2, task_id="third")

    assert [heap.pop(), heap.pop(), heap.pop()] == ["first", "second", "third"]


def test_len_and_peek():
    heap = MinHeap()
    assert len(heap) == 0

    heap.push(priority=1, sequence=0, task_id="a")
    heap.push(priority=2, sequence=1, task_id="b")

    assert len(heap) == 2
    assert heap.peek() == "b"
    assert len(heap) == 2  # peek doesn't remove


def test_pop_empty_raises():
    heap = MinHeap()
    with pytest.raises(IndexError):
        heap.pop()


def test_peek_empty_raises():
    heap = MinHeap()
    with pytest.raises(IndexError):
        heap.peek()
