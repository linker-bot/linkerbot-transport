"""Configuration snapshots and process-local ownership for python-can buses."""

from __future__ import annotations

import logging
import math
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
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


def normalize_channel_options(
    channel: str, interface: str, bus_options: Mapping[str, BusOption] | None
) -> tuple[str, tuple[tuple[str, BusOption], ...]]:
    """Resolve the SLCAN port@baud spelling before keys, claims or driver I/O."""
    validate_channel(channel, interface)
    options = dict(freeze_bus_options(bus_options))
    if interface == "slcan":
        if "@" in channel:
            channel, baud = channel.rsplit("@", 1)
            try:
                serial_baud = int(baud)
            except ValueError as error:
                raise ValidationError("invalid SLCAN channel@baudrate") from error
            if "@" in channel:
                raise ValidationError("invalid SLCAN channel@baudrate")
            if "tty_baudrate" in options and options["tty_baudrate"] != serial_baud:
                raise ValidationError("conflicting SLCAN serial baudrates")
            options["tty_baudrate"] = serial_baud
        validate_channel(channel, interface)
        positive_int(options.setdefault("tty_baudrate", 115_200), "tty_baudrate")
    return channel, freeze_bus_options(options)


@dataclass
class _ChannelUse:
    settings: object
    owners: set[object] = field(default_factory=set)

    @property
    def count(self) -> int:
        return len(self.owners)


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
        self._token = object()
        with _CHANNEL_LOCK:
            current = _CHANNELS.get(self._key)
            if current is not None:
                if current.settings != settings:
                    raise TransportError(
                        f"{interface} channel {channel!r} is already open with "
                        "different settings; close its users before reconfiguring"
                    )
            else:
                current = _ChannelUse(settings)
                _CHANNELS[self._key] = current
            self._use = current
            current.owners.add(self._token)

    @property
    def _released(self) -> bool:
        return self._token not in self._use.owners

    def release(self) -> None:
        with _CHANNEL_LOCK:
            # One owner-specific idempotent mutation replaces count/flag writes.
            # Retrying after an interrupt cannot consume another handle's claim.
            self._use.owners.discard(self._token)
            if not self._use.owners and _CHANNELS.get(self._key) is self._use:
                _CHANNELS.pop(self._key, None)


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

    def __init__(self, bus: can.BusABC | None, claim: ChannelClaim) -> None:
        self._bus = bus
        self._claim = claim
        self._lock = threading.Lock()
        self._closed = False

    def stop(self) -> None:
        with self._lock:
            if not self._closed:
                if self._bus is not None:
                    self._bus.shutdown()
                self._closed = True
            self._claim.release()

    def unsubscribe(self, callback: object) -> None:
        # No worker or subscription was created during the failed open.
        pass
