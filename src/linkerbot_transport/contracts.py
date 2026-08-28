"""Transport-neutral lifecycle and message transport contracts."""

from __future__ import annotations

from collections.abc import Hashable, Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, TypeVar, runtime_checkable

from .errors import ErrorCode
from .receipts import SendOutcome, SendReceipt, SendResult
from .routing import RouteSubscription

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
    """Immutable health snapshot for one physical transport instance.

    Traffic counters are cumulative across connection generations. Byte
    counters measure the CAN/CAN FD data field selected by DLC; they exclude
    arbitration, CRC, bit stuffing and inter-frame spacing.
    """

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
    route_count: int
    pending_rx: int
    max_pending_rx: int
    rx_frames: int
    routed_rx_frames: int
    route_misses: int
    rx_overflows: int
    stale_rx_frames: int
    route_key_errors: int
    route_registration_rejections: int
    dropped_rx_on_close: int
    tx_frames: int = 0
    tx_bytes: int = 0
    rx_bytes: int = 0

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
    """Route-based transport for discrete protocol messages."""

    def send(self, message: MessageT) -> SendReceipt:
        """Accept one message and return its physical-send receipt."""
        ...

    def subscribe_route(
        self,
        route_key: Hashable,
        *,
        queue_capacity: int | None = None,
    ) -> RouteSubscription[MessageT, Hashable]:
        """Open one exclusive bounded receive route."""
        ...

    def subscribe_routes(
        self,
        route_keys: Iterable[Hashable],
        *,
        queue_capacity: int | None = None,
    ) -> RouteSubscription[MessageT, Hashable]:
        """Open one bounded inbox shared by several exact receive routes."""
        ...


__all__ = [
    "MessageTransport",
    "SendOutcome",
    "SendReceipt",
    "SendResult",
    "TransportHealth",
    "TransportLifecycle",
    "TransportState",
]
