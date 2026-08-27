"""Physical transports and shared runtime primitives for Linkerbot SDKs."""

from ._managed import ManagedMessageTransport
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
from .receipts import SendOutcome, SendReceipt, SendResult
from .relay import DataRelay
from .transmit import SendOptions, SendPolicy, TransmitArbiter, TransmitHealth

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
    "ManagedMessageTransport",
    "MessageTransport",
    "QueueFullError",
    "SDKError",
    "SendOptions",
    "SendOutcome",
    "SendPolicy",
    "SendReceipt",
    "SendResult",
    "SharedTransportPool",
    "StateError",
    "TimeoutError",
    "TransmitArbiter",
    "TransmitHealth",
    "TransportError",
    "TransportHealth",
    "TransportLease",
    "TransportLifecycle",
    "TransportState",
    "UnsupportedOperationError",
    "ValidationError",
    "normalize_error",
]
