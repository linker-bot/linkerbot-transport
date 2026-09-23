"""Configuration snapshots and process-local ownership for python-can buses."""

from __future__ import annotations

import logging
import math
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .errors import TransportError, ValidationError

if TYPE_CHECKING:
    import can

_LOGGER = logging.getLogger(__name__)

BusOption = str | int | float | bool | None
_RESERVED = {
    "channel",
    "interface",
    "bustype",
    "bitrate",
    "data_bitrate",
    "fd",
    "receive_own_messages",
    "timing",
    "ignore_config",
    "config_context",
    # device_id can override PCAN's channel, bypassing channel ownership.
    "device_id",
}


def freeze_bus_options(
    options: Mapping[str, BusOption] | None,
) -> tuple[tuple[str, BusOption], ...]:
    """Copy scalar backend options into a stable, hashable snapshot."""
    if options is None:
        return ()
    if not isinstance(options, Mapping):
        raise ValidationError("bus_options must be a mapping")
    result = []
    for key, value in options.items():
        if not isinstance(key, str) or not key or key in _RESERVED:
            raise ValidationError(f"reserved or invalid bus_options key: {key!r}")
        if type(value) not in (str, int, float, bool, type(None)):
            raise ValidationError(f"bus_options[{key!r}] must be a scalar value")
        if isinstance(value, float) and not math.isfinite(value):
            raise ValidationError(f"bus_options[{key!r}] must be finite")
        result.append((key, value))
    return tuple(sorted(result))


def validate_channel(channel: str, interface: str) -> None:
    if not isinstance(channel, str) or not channel.strip():
        raise ValidationError("channel must be a non-empty string")
    if not isinstance(interface, str) or not interface.strip():
        raise ValidationError("interface must be a non-empty string")


def positive_int(value: int, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValidationError(f"{name} must be a positive int")


@dataclass
class _ChannelUse:
    settings: object
    count: int = 1


_CHANNEL_LOCK = threading.Lock()
_CHANNELS: dict[tuple[str, str], _ChannelUse] = {}


class ChannelClaim:
    """Reject conflicting settings before opening a second handle.

    Identical handles remain allowed, as on SocketCAN; applications use the
    shared pool to reuse one receiver. This is not a cross-process lock.
    Release only after successful shutdown, including startup rollback.
    """

    def __init__(self, interface: str, channel: str, settings: object) -> None:
        if interface in ("pcan", "slcan") and not channel.startswith("/"):
            channel = channel.upper()
        self._key = (interface, channel)
        self._released = False
        with _CHANNEL_LOCK:
            current = _CHANNELS.get(self._key)
            if current is not None:
                if current.settings != settings:
                    raise TransportError(
                        f"{interface} channel {channel!r} is already open with "
                        "different settings; close its users before reconfiguring"
                    )
                current.count += 1
            else:
                _CHANNELS[self._key] = _ChannelUse(settings)

    def release(self) -> None:
        with _CHANNEL_LOCK:
            if self._released:
                return
            current = _CHANNELS[self._key]
            current.count -= 1
            if current.count == 0:
                del _CHANNELS[self._key]
            self._released = True


def open_bus(*, channel: str, interface: str, **kwargs: Any) -> can.BusABC:
    """Open explicit settings with local lifecycle fixes for affected drivers.

    Import hardware backends only at open time. Direct backend construction
    bypasses python-can's ambient configuration, just like ignore_config=True.
    No global python-can registry or driver class is patched.
    """
    if interface == "slcan":
        from ._slcan import ManagedSlcanBus

        return ManagedSlcanBus(channel=channel, **kwargs)
    if interface == "pcan":
        from ._pcan import ManagedPcanBus

        return ManagedPcanBus(channel=channel, **kwargs)
    import can

    return can.Bus(channel=channel, interface=interface, ignore_config=True, **kwargs)


def rollback_bus_open(error: BaseException, claim: ChannelClaim) -> None:
    """Retain partially constructed buses until their resources are released."""
    bus = getattr(error, "_failed_bus", None)
    if bus is None:
        claim.release()
        return
    owner = _FailedBusCleanup(bus, claim)
    try:
        owner.stop()
    except BaseException as cleanup_error:
        _LOGGER.warning("python-can initialization cleanup failed: %s", cleanup_error)
        # The existing managed transport/pool retry this owner before reopening.
        setattr(error, "_failed_dispatcher", owner)
        if isinstance(error, Exception) and not isinstance(cleanup_error, Exception):
            setattr(cleanup_error, "_failed_dispatcher", owner)
            raise cleanup_error from error


class _FailedBusCleanup:
    """Cleanup-only dispatcher for a bus that failed before workers existed."""

    def __init__(self, bus: can.BusABC, claim: ChannelClaim) -> None:
        self._bus = bus
        self._claim = claim
        self._lock = threading.Lock()
        self._closed = False

    def stop(self) -> None:
        with self._lock:
            if not self._closed:
                self._bus.shutdown()
                self._claim.release()
                self._closed = True

    def unsubscribe(self, callback: object) -> None:
        # No worker or subscription was created during the failed open.
        pass
