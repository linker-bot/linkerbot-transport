"""Physical transports and shared runtime primitives for Linkerbot SDKs."""

from .contracts import (
    MessageTransport,
    TransportHealth,
    TransportLifecycle,
    TransportState,
)
from .errors import (
    CANError,
    ClosedError,
    DeviceFaultError,
    DeviceNotReadyError,
    DeviceStateError,
    ErrorCode,
    HandSDKError,
    LinkerbotError,
    QueueFullError,
    SDKError,
    StateError,
    TimeoutError,
    TransportError,
    UnsupportedOperationError,
    ValidationError,
    normalize_error,
)
from .pool import SharedTransportPool, TransportLease
from .queue import IterableQueue
from .relay import DataRelay

__all__ = [
    "CANError",
    "ClosedError",
    "DataRelay",
    "DeviceFaultError",
    "DeviceNotReadyError",
    "DeviceStateError",
    "ErrorCode",
    "HandSDKError",
    "IterableQueue",
    "LinkerbotError",
    "MessageTransport",
    "QueueFullError",
    "SDKError",
    "StateError",
    "SharedTransportPool",
    "TimeoutError",
    "TransportError",
    "TransportHealth",
    "TransportLease",
    "TransportLifecycle",
    "TransportState",
    "UnsupportedOperationError",
    "ValidationError",
    "normalize_error",
]
