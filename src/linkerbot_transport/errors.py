"""Stable error categories shared by Linkerbot SDK distributions."""

from __future__ import annotations

import builtins
from enum import Enum


class ErrorCode(str, Enum):
    """Machine-readable error codes for integrations such as ROS."""

    VALIDATION = "validation"
    UNSUPPORTED_OPERATION = "unsupported_operation"
    DEVICE_NOT_READY = "device_not_ready"
    TRANSPORT = "transport"
    TIMEOUT = "timeout"
    DEVICE_FAULT = "device_fault"
    QUEUE_FULL = "queue_full"
    CLOSED = "closed"
    INTERNAL = "internal"


class LinkerbotError(Exception):
    """Base class for stable SDK errors that callers may map by type or code."""

    code = ErrorCode.INTERNAL

    def __init__(
        self, message: str = "", *, cause: BaseException | None = None
    ) -> None:
        super().__init__(message)
        self.cause = cause


class ValidationError(LinkerbotError, ValueError):
    """Raised when public input fails validation."""

    code = ErrorCode.VALIDATION


class UnsupportedOperationError(LinkerbotError):
    """Raised when a device or transport does not support an operation."""

    code = ErrorCode.UNSUPPORTED_OPERATION


class DeviceStateError(LinkerbotError):
    """Base class for operations rejected by the current lifecycle state."""

    code = ErrorCode.DEVICE_NOT_READY


class DeviceNotReadyError(DeviceStateError):
    """Raised when an operation requires a ready device."""

    code = ErrorCode.DEVICE_NOT_READY


class TransportError(LinkerbotError, OSError):
    """Raised for physical channel and protocol I/O failures."""

    code = ErrorCode.TRANSPORT


class TimeoutError(LinkerbotError, builtins.TimeoutError):
    """Raised when a bounded SDK operation expires."""

    code = ErrorCode.TIMEOUT


class DeviceFaultError(LinkerbotError):
    """Raised when a device explicitly reports a fault."""

    code = ErrorCode.DEVICE_FAULT


class QueueFullError(LinkerbotError):
    """Raised when a bounded queue cannot accept an operation."""

    code = ErrorCode.QUEUE_FULL


class ClosedError(DeviceStateError):
    """Raised when an operation targets a closed resource."""

    code = ErrorCode.CLOSED


def normalize_error(error: BaseException) -> LinkerbotError:
    """Map backend exceptions into the public, stable hierarchy."""

    if isinstance(error, LinkerbotError):
        return error
    if isinstance(error, builtins.TimeoutError):
        return TimeoutError(str(error), cause=error)
    return TransportError(str(error), cause=error)


# These exact aliases keep old and new catch sites interoperable during the
# package split. Device packages can migrate from HandSDKError to SDKError.
SDKError = LinkerbotError
HandSDKError = LinkerbotError
CANError = TransportError
StateError = DeviceStateError


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
