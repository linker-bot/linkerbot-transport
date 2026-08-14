"""Physical transports and shared runtime primitives for Linkerbot SDKs."""

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
    "QueueFullError",
    "SDKError",
    "StateError",
    "TimeoutError",
    "TransportError",
    "UnsupportedOperationError",
    "ValidationError",
    "normalize_error",
]
