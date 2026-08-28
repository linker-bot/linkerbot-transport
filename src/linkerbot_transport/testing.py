"""In-memory transport implementation for device SDK tests."""

from __future__ import annotations

import time
from collections.abc import Callable, Hashable
from typing import Generic, TypeVar

from ._managed import ManagedMessageTransport
from .errors import ClosedError, TransportError

MessageT = TypeVar("MessageT")


class FakeDispatcher(Generic[MessageT]):
    """Synchronous dispatcher with controllable send, close and bus failures."""

    def __init__(self, on_bus_error: Callable[[Exception], None]) -> None:
        self.sent: list[MessageT] = []
        self._subscribers: list[Callable[[MessageT], None]] = []
        self._on_bus_error = on_bus_error
        self._stopped = False
        self.send_error: Exception | None = None
        self.stop_error: Exception | None = None
        self.stop_count = 0

    def send_now(self, message: MessageT) -> None:
        """Record one message or raise the configured send error."""

        if self._stopped:
            raise ClosedError("fake dispatcher is stopped")
        if self.send_error is not None:
            error = self.send_error
            self.send_error = None
            raise error
        self.sent.append(message)

    def subscribe(self, callback: Callable[[MessageT], None]) -> None:
        """Register a receive callback."""

        if callback not in self._subscribers:
            self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[MessageT], None]) -> None:
        """Remove a receive callback."""

        if callback in self._subscribers:
            self._subscribers.remove(callback)

    def stop(self) -> None:
        """Stop idempotently or raise the configured close error once."""

        self.stop_count += 1
        if self.stop_error is not None:
            error = self.stop_error
            self.stop_error = None
            raise error
        self._stopped = True
        self._subscribers.clear()

    def emit(self, message: MessageT) -> None:
        """Deliver a message to the current subscriber snapshot."""

        if self._stopped:
            return
        for callback in tuple(self._subscribers):
            callback(message)

    def disconnect(self, error: Exception | None = None) -> None:
        """Report a fatal backend error to the managed transport."""

        self._on_bus_error(error or TransportError("fake transport disconnected"))


class FakeTransport(ManagedMessageTransport[MessageT]):
    """Explicit-lifecycle fake implementing the public message contract."""

    def __init__(
        self,
        *,
        route_key_of: Callable[[MessageT], Hashable] | None = None,
        data_size_of: Callable[[MessageT], int] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.dispatchers: list[FakeDispatcher[MessageT]] = []

        def create(
            on_bus_error: Callable[[Exception], None],
        ) -> FakeDispatcher[MessageT]:
            dispatcher = FakeDispatcher[MessageT](on_bus_error)
            self.dispatchers.append(dispatcher)
            return dispatcher

        if route_key_of is None:
            route_key_of = _identity_route_key
        super().__init__(
            create,
            route_key_of,
            data_size_of=data_size_of,
            monotonic=monotonic,
        )

    @property
    def current_dispatcher(self) -> FakeDispatcher[MessageT]:
        """Return the most recently created fake dispatcher."""

        if not self.dispatchers:
            raise ClosedError("fake transport has not been opened")
        return self.dispatchers[-1]

    @property
    def sent(self) -> tuple[MessageT, ...]:
        """Return all messages sent across every connection generation."""

        return tuple(
            message for dispatcher in self.dispatchers for message in dispatcher.sent
        )

    def emit(self, message: MessageT) -> None:
        """Inject one received message into the active generation."""

        self.current_dispatcher.emit(message)


def _identity_route_key(message: MessageT) -> Hashable:
    if not isinstance(message, Hashable):
        raise TypeError("fake message is not hashable; inject route_key_of")
    return message


__all__ = ["FakeDispatcher", "FakeTransport"]
