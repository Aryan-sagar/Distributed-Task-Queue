"""A plain binary min-heap over (priority_key, sequence, task_id) tuples.

This is deliberately decoupled from Redis/TaskStore: it exists to isolate
and unit-test the ordering logic itself (higher priority first, FIFO among
equal priorities) before that same comparator gets applied to the
Redis-backed queue in storage.py. An in-process heap like this can't be
the *actual* production queue once workers run as a separate process from
the API — but the comparator logic it proves out is exactly what the
Redis sorted-set implementation reproduces.
"""
from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import List, Tuple


@dataclass(order=True)
class HeapEntry:
    # Negative priority so heapq's min-heap pops the *highest* priority first.
    sort_key: Tuple[int, int] = field(compare=True)
    task_id: str = field(compare=False)

    @classmethod
    def create(cls, priority: int, sequence: int, task_id: str) -> "HeapEntry":
        return cls(sort_key=(-priority, sequence), task_id=task_id)


class MinHeap:
    """Min-heap ordered by (-priority, sequence): highest priority first,
    FIFO among tasks of equal priority."""

    def __init__(self) -> None:
        self._heap: List[HeapEntry] = []

    def push(self, priority: int, sequence: int, task_id: str) -> None:
        heapq.heappush(self._heap, HeapEntry.create(priority, sequence, task_id))

    def pop(self) -> str:
        if not self._heap:
            raise IndexError("pop from an empty heap")
        return heapq.heappop(self._heap).task_id

    def peek(self) -> str:
        if not self._heap:
            raise IndexError("peek from an empty heap")
        return self._heap[0].task_id

    def __len__(self) -> int:
        return len(self._heap)
