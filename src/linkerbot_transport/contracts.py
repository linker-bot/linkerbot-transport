"""Transport-neutral lifecycle and message transport contracts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, TypeVar, runtime_checkable

from .errors import ErrorCode

MessageT = TypeVar("MessageT")


class TransportState(str, Enum):
    """Lifecycle state of one physical transport connection."""

    CLOSED = "closed"
    OPENING = "opening"
    OPEN = "open"
    CLOSING = "closing"
    FAULTED = "faulted"


@dataclass(frozen=True, slots=True)
class TransportHealth:
    """Immutable health snapshot for one transport instance."""

    state: TransportState
    generation: int
    last_successful_tx_s: float | None
    last_successful_rx_s: float | None
    tx_age_s: float | None
    rx_age_s: float | None
    consecutive_errors: int
    last_error: str | None
    last_error_code: ErrorCode | None
    reconnect_count: int
    pending_tx: int
    subscriber_count: int

    @property
    def connected(self) -> bool:
        """Return whether the transport is currently open."""

        return self.state is TransportState.OPEN


@runtime_checkable
class TransportLifecycle(Protocol):
    """Lifecycle shared by message, request-response and cyclic transports."""

    def open(self) -> None:
        """Open the physical transport idempotently."""
        ...

    def close(self) -> None:
        """Close the physical transport idempotently."""
        ...

    def recover(self) -> None:
        """Replace a failed connection without replaying prior traffic."""
        ...

    def get_health(self) -> TransportHealth:
        """Return a non-blocking cached health snapshot."""
        ...


@runtime_checkable
class MessageTransport(TransportLifecycle, Protocol[MessageT]):
    """Callback-based transport for discrete protocol messages."""

    def send(self, message: MessageT) -> None:
        """Synchronously hand one message to the physical backend."""
        ...

    def subscribe(self, callback: Callable[[MessageT], None]) -> None:
        """Register a callback for received messages."""
        ...

    def unsubscribe(self, callback: Callable[[MessageT], None]) -> None:
        """Remove a previously registered receive callback."""
        ...


__all__ = [
    "MessageTransport",
    "TransportHealth",
    "TransportLifecycle",
    "TransportState",
]
