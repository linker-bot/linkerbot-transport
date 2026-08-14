"""Thread-safe latest-value cache, waiter and event sink."""

import math
import threading
from collections.abc import Callable
from typing import Generic, TypeVar, cast

from .errors import TimeoutError, ValidationError

T = TypeVar("T")
_SENTINEL = object()


class DataRelay(Generic[T]):
    """Cache the latest value and notify waiters and an optional sink."""

    def __init__(self) -> None:
        self._latest: T | None = None
        self._waiters: list[tuple[threading.Event, dict[str, object]]] = []
        self._lock = threading.Lock()
        self._sink: Callable[[T], None] | None = None

    def snapshot(self) -> T | None:
        """Return the most recently pushed value without blocking."""

        with self._lock:
            return self._latest

    def wait(self, timeout_s: float) -> T:
        """Wait for the next pushed value until a finite timeout expires."""

        if (
            not isinstance(timeout_s, (int, float))
            or isinstance(timeout_s, bool)
            or not math.isfinite(timeout_s)
            or timeout_s < 0
        ):
            raise ValidationError("timeout_s must be a finite, non-negative number")
        event = threading.Event()
        result: dict[str, object] = {"data": _SENTINEL}
        waiter = (event, result)
        with self._lock:
            self._waiters.append(waiter)
        if event.wait(timeout_s):
            return cast(T, result["data"])
        with self._lock:
            if waiter in self._waiters:
                self._waiters.remove(waiter)
        raise TimeoutError(f"No data received within {timeout_s * 1000:.0f}ms")

    def push(self, data: T) -> None:
        """Update the cache, wake current waiters and notify the sink."""

        with self._lock:
            self._latest = data
            waiters = self._waiters
            self._waiters = []
            sink = self._sink
            for event, result in waiters:
                result["data"] = data
                event.set()
        if sink is not None:
            sink(data)

    def set_sink(self, sink: Callable[[T], None] | None) -> None:
        """Replace or clear the callback invoked for each pushed value."""

        if sink is not None and not callable(sink):
            raise ValidationError("sink must be callable or None")
        with self._lock:
            self._sink = sink


__all__ = ["DataRelay"]
