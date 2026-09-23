"""Configuration snapshots and process-local ownership for python-can buses."""

from __future__ import annotations

import math
import threading
from collections.abc import Mapping
from dataclasses import dataclass

from .errors import TransportError, ValidationError

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
