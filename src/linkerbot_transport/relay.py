"""Thread-safe latest-value cache, waiter and event sink."""

import math
import threading
from collections.abc import Callable
from typing import Generic, TypeVar, cast

from .errors import ClosedError, TimeoutError, ValidationError

T = TypeVar("T")
_SENTINEL = object()


class DataRelay(Generic[T]):
    """Cache the latest value and notify waiters and an optional sink."""

    def __init__(self) -> None:
        self._latest: T | None = None
        self._waiters: list[tuple[threading.Event, dict[str, object]]] = []
        self._lock = threading.Lock()
        self._request_lock = threading.Lock()
        self._sink: Callable[[T], None] | None = None
        self._closed = False

    def snapshot(self) -> T | None:
        """Return the most recently pushed value without blocking."""

        with self._lock:
            return self._latest

    def close(self) -> None:
        """Close the relay and wake every current waiter with ``ClosedError``."""

        with self._lock:
            if self._closed:
                return
            self._closed = True
            waiters = self._waiters
            self._waiters = []
            self._sink = None
            for event, result in waiters:
                result["error"] = ClosedError("Data relay is closed")
                event.set()

    def wait(self, timeout_s: float) -> T:
        """Wait for the next pushed value until a finite timeout expires."""

        self._validate_timeout(timeout_s)
        return self._wait_for(self._register_waiter(), timeout_s)

    def request(self, send: Callable[[], object], timeout_s: float) -> T:
        """Register a waiter, send one request, then wait for its response.

        Requests on the same relay are serialized so that one response cannot wake
        multiple concurrent request calls. The waiter is always registered before
        ``send`` runs, including when a transport pushes a response synchronously.
        """

        if not callable(send):
            raise ValidationError("send must be callable")
        self._validate_timeout(timeout_s)
        with self._request_lock:
            waiter = self._register_waiter()
            send_completed = False
            try:
                send()
                send_completed = True
            finally:
                if not send_completed:
                    self._discard_waiter(waiter)
            return self._wait_for(waiter, timeout_s)

    @staticmethod
    def _validate_timeout(timeout_s: float) -> None:
        if (
            not isinstance(timeout_s, (int, float))
            or isinstance(timeout_s, bool)
            or not math.isfinite(timeout_s)
            or timeout_s < 0
        ):
            raise ValidationError("timeout_s must be a finite, non-negative number")

    def _register_waiter(self) -> tuple[threading.Event, dict[str, object]]:
        event = threading.Event()
        result: dict[str, object] = {"data": _SENTINEL}
        waiter = (event, result)
        with self._lock:
            if self._closed:
                raise ClosedError("Data relay is closed")
            self._waiters.append(waiter)
        return waiter

    def _wait_for(
        self,
        waiter: tuple[threading.Event, dict[str, object]],
        timeout_s: float,
    ) -> T:
        event, result = waiter
        if event.wait(timeout_s):
            error = result.get("error", _SENTINEL)
            if error is not _SENTINEL:
                raise cast(BaseException, error)
            return cast(T, result["data"])
        self._discard_waiter(waiter)
        raise TimeoutError(f"No data received within {timeout_s * 1000:.0f}ms")

    def _discard_waiter(
        self, waiter: tuple[threading.Event, dict[str, object]]
    ) -> None:
        with self._lock:
            if waiter in self._waiters:
                self._waiters.remove(waiter)

    def push(self, data: T) -> None:
        """Update the cache, wake current waiters and notify the sink."""

        with self._lock:
            if self._closed:
                return
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
