"""Compatibility aliases for ``linkerbot_transport.errors``."""

from linkerbot_transport.errors import (
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

__all__ = [
    "CANError",
    "ClosedError",
    "DeviceFaultError",
    "DeviceNotReadyError",
    "DeviceStateError",
    "ErrorCode",
    "HandSDKError",
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
