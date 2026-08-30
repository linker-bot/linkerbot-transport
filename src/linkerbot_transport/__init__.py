"""Physical transports and shared runtime primitives for Linkerbot SDKs."""

from .contracts import (
    MessageTransport,
    TransportHealth,
    TransportLifecycle,
    TransportState,
)
from .errors import (
    ClosedError,
    DeviceFaultError,
    DeviceNotReadyError,
    DeviceStateError,
    ErrorCode,
    LinkerbotError,
    QueueFullError,
    TimeoutError,
    TransportError,
    UnsupportedOperationError,
    ValidationError,
    normalize_error,
)
from .pool import SharedTransportPool, TransportLease
from .receipts import SendOutcome, SendReceipt, SendResult
from .routing import (
    CANRouteKey,
    ReceiveRouter,
    ReceiveRouterHealth,
    RoutedMessage,
    RouteHealth,
    RouteSubscription,
)
from .transmit import (
    SendOptions,
    SendPolicy,
    TransmitArbiter,
    TransmitHealth,
    TransmitMode,
)

__all__ = [
    "CANRouteKey",
    "ClosedError",
    "DeviceFaultError",
    "DeviceNotReadyError",
    "DeviceStateError",
    "ErrorCode",
    "LinkerbotError",
    "MessageTransport",
    "QueueFullError",
    "ReceiveRouter",
    "ReceiveRouterHealth",
    "RouteHealth",
    "RouteSubscription",
    "RoutedMessage",
    "SendOptions",
    "SendOutcome",
    "SendPolicy",
    "SendReceipt",
    "SendResult",
    "SharedTransportPool",
    "TimeoutError",
    "TransmitArbiter",
    "TransmitHealth",
    "TransmitMode",
    "TransportError",
    "TransportHealth",
    "TransportLease",
    "TransportLifecycle",
    "TransportState",
    "UnsupportedOperationError",
    "ValidationError",
    "normalize_error",
]
