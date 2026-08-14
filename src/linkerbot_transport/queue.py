"""Closable iterable queue with bounded producer and consumer waits."""

import math
import queue
import threading
import time
from collections import deque
from collections.abc import Iterator
from typing import Generic, TypeVar

from .errors import StateError

T = TypeVar("T")


class IterableQueue(Generic[T]):
    """Thread-safe queue that stops iteration after close and drain."""

    def __init__(self, maxsize: int = 0) -> None:
        if not isinstance(maxsize, int) or isinstance(maxsize, bool) or maxsize < 0:
            raise ValueError("maxsize must be a non-negative int")
        self._items: deque[T] = deque()
        self._maxsize = maxsize
        self._closed = False
        self._condition = threading.Condition()

    def put(self, item: T, block: bool = True, timeout: float | None = None) -> None:
        """Add an item, optionally waiting for bounded capacity."""

        _validate_timeout(block=block, timeout=timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            if self._closed:
                raise StateError("Cannot put to a closed queue")
            while self._is_full():
                if not block:
                    raise queue.Full
                remaining = _remaining_time(deadline)
                if remaining == 0:
                    raise queue.Full
                self._condition.wait(remaining)
                if self._closed:
                    raise StateError("Cannot put to a closed queue")
            self._items.append(item)
            self._condition.notify_all()

    def put_nowait(self, item: T) -> None:
        """Add an item without waiting for capacity."""

        self.put(item, block=False)

    def get(self, block: bool = True, timeout: float | None = None) -> T:
        """Remove an item, optionally waiting for one to arrive."""

        _validate_timeout(block=block, timeout=timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while not self._items:
                if self._closed:
                    raise StopIteration
                if not block:
                    raise queue.Empty
                remaining = _remaining_time(deadline)
                if remaining == 0:
                    raise queue.Empty
                self._condition.wait(remaining)
            item = self._items.popleft()
            self._condition.notify_all()
            return item

    def get_nowait(self) -> T:
        """Remove an item without waiting."""

        return self.get(block=False)

    def empty(self) -> bool:
        """Return whether the queue currently contains no items."""

        with self._condition:
            return not self._items

    def close(self) -> None:
        """Reject producers and wake all blocked callers."""

        with self._condition:
            self._closed = True
            self._condition.notify_all()

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        return self.get(block=True)

    def _is_full(self) -> bool:
        return self._maxsize > 0 and len(self._items) >= self._maxsize


def _validate_timeout(*, block: bool, timeout: float | None) -> None:
    if not block and timeout is not None:
        raise ValueError("timeout must not be specified when block is False")
    if timeout is None:
        return
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
        raise ValueError("timeout must be a finite, non-negative number")
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout must be a finite, non-negative number")


def _remaining_time(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())


__all__ = ["IterableQueue"]
