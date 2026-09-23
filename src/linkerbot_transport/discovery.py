"""Frame-free PCAN and SLCAN adapter enumeration with explicit diagnostics.

Optional drivers are imported only when their enumeration function is called.
No function opens a CAN channel or serial port, or changes device configuration.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Sequence
from dataclasses import dataclass

from .errors import TransportError, ValidationError

_CANABLE_20 = (0x16D0, 0x117E)


@dataclass(frozen=True, slots=True)
class PythonCANChannel:
    """Native python-can channel and enumeration evidence, not a hand identity.

    ``supports_fd=None`` means unknown firmware capability (SLCAN candidates).
    A USB identifier never proves support for CANable's FD serial extensions.
    Only available PCAN channels are returned; occupied ones produce failures.
    """

    interface: str
    channel: str
    supports_fd: bool | None
    description: str | None = None
    serial_number: str | None = None
    device_id: int | None = None
    controller_number: int | None = None
    vid: int | None = None
    pid: int | None = None

    def __post_init__(self) -> None:
        if self.interface not in {"pcan", "slcan"}:
            raise ValidationError("interface must be pcan or slcan")
        if not isinstance(self.channel, str) or not self.channel.strip():
            raise ValidationError("channel must be a non-empty string")
        if self.supports_fd is not None and type(self.supports_fd) is not bool:
            raise ValidationError("supports_fd must be bool or None")


@dataclass(frozen=True, slots=True)
class PythonCANEnumerationFailure:
    """One backend or channel could not be enumerated or is unavailable."""

    interface: str
    stage: str
    error: TransportError
    channel: str | None = None


@dataclass(frozen=True, slots=True)
class PythonCANEnumerationResult:
    """Available candidates and failures; an empty list alone is inconclusive."""

    channels: tuple[PythonCANChannel, ...] = ()
    failures: tuple[PythonCANEnumerationFailure, ...] = ()


def enumerate_pcan_channels() -> PythonCANEnumerationResult:
    """Query attached channels using python-can's PCANBasic driver wrapper.

    Unlike ``detect_available_configs``, preserve driver-load/query failures.
    Read the attached-channel table once, without trying to initialize handles
    or guessing PCAN_USBBUS1..N. Windows and Linux PCANBasic are supported when
    their driver implements PCAN_ATTACHED_CHANNELS.
    """
    try:
        basic = importlib.import_module("can.interfaces.pcan.basic")
    except Exception as error:
        return _failed("pcan", "dependency", error)
    try:
        driver = basic.PCANBasic()
    except Exception as error:
        return _failed("pcan", "driver", error)
    try:
        status, attached = driver.GetValue(
            basic.PCAN_NONEBUS, basic.PCAN_ATTACHED_CHANNELS
        )
        if status != basic.PCAN_ERROR_OK:
            raise TransportError(f"PCAN_ATTACHED_CHANNELS failed with status {status}")
        attached = tuple(attached)
    except Exception as error:
        return _failed("pcan", "scan", error)

    names = {handle.value: name for name, handle in basic.PCAN_CHANNEL_NAMES.items()}
    channels: dict[str, PythonCANChannel] = {}
    rejected: set[str] = set()
    failures: list[PythonCANEnumerationFailure] = []
    for item in attached:
        name = names.get(getattr(item, "channel_handle", None))
        try:
            if name is None:
                raise TransportError(
                    f"unknown PCAN channel handle {item.channel_handle}"
                )
            condition = item.channel_condition
            if condition & basic.PCAN_CHANNEL_OCCUPIED:
                channels.pop(name, None)
                rejected.add(name)
                failures.append(
                    _failure("pcan", "busy", "PCAN channel is occupied", name)
                )
                continue
            if not condition & basic.PCAN_CHANNEL_AVAILABLE:
                channels.pop(name, None)
                rejected.add(name)
                failures.append(
                    _failure("pcan", "unavailable", "PCAN channel is unavailable", name)
                )
                continue
            channel = PythonCANChannel(
                interface="pcan",
                channel=name,
                supports_fd=bool(item.device_features & basic.FEATURE_FD_CAPABLE),
                description=item.device_name.decode("latin-1"),
                device_id=item.device_id,
                controller_number=item.controller_number,
            )
            if name in channels and channels[name] != channel:
                raise TransportError("conflicting PCAN enumeration entries")
            if name not in rejected:
                channels[name] = channel
        except Exception as error:
            channels.pop(name, None)
            if name is not None:
                rejected.add(name)
            failures.append(_failure("pcan", "channel", error, name))
    return PythonCANEnumerationResult(tuple(channels.values()), tuple(failures))


def enumerate_slcan_channels(
    *, ports: Sequence[str] | None = None
) -> PythonCANEnumerationResult:
    """List known CANable 2.0 candidates, or explicitly selected serial ports.

    Automatic selection uses USB VID/PID 16D0:117E. Other adapters require
    ``ports``; explicit ports must appear in the OS serial enumeration. Port
    existence does not prove availability or firmware capability. Serial open
    errors and protocol confirmation belong to the caller's connection stage.
    """
    selected = _selected_ports(ports)
    try:
        list_ports = importlib.import_module("serial.tools.list_ports")
    except Exception as error:
        return _failed("slcan", "dependency", error)
    try:
        available = list(list_ports.comports())
    except Exception as error:
        return _failed("slcan", "scan", error)
    channels: dict[str, PythonCANChannel] = {}
    failures: list[PythonCANEnumerationFailure] = []
    found: set[str] = set()
    for item in available:
        name = getattr(item, "device", None)
        try:
            if not isinstance(name, str) or not name.strip():
                raise TransportError("serial enumeration returned an invalid port name")
            key = _port_key(name)
            if selected is not None:
                if key not in selected:
                    continue
            elif (item.vid, item.pid) != _CANABLE_20:
                continue
            found.add(key)
            channel = PythonCANChannel(
                interface="slcan",
                channel=name,
                supports_fd=None,
                description=item.description,
                serial_number=item.serial_number,
                vid=item.vid,
                pid=item.pid,
            )
            # Keep the first native name for aliases of the same OS port.
            channels.setdefault(key, channel)
        except Exception as error:
            failures.append(_failure("slcan", "channel", error, name))
    if selected is not None:
        for key, name in selected.items():
            if key not in found:
                failures.append(
                    _failure(
                        "slcan",
                        "unavailable",
                        "requested serial port was not enumerated",
                        name,
                    )
                )
    return PythonCANEnumerationResult(tuple(channels.values()), tuple(failures))


def _selected_ports(ports: Sequence[str] | None) -> dict[str, str] | None:
    if ports is None:
        return None
    if isinstance(ports, (str, bytes)) or not isinstance(ports, Sequence):
        raise ValidationError("ports must be a sequence of port names or None")
    result = {}
    for name in ports:
        if not isinstance(name, str) or not name.strip():
            raise ValidationError("ports must contain non-empty strings")
        result.setdefault(_port_key(name), name)
    return result


def _port_key(name: str) -> str:
    return name.casefold() if os.name == "nt" else os.path.realpath(name)


def _failure(
    interface: str, stage: str, error: Exception | str, channel: str | None = None
) -> PythonCANEnumerationFailure:
    classified = (
        error
        if isinstance(error, TransportError)
        else TransportError(
            str(error), cause=error if isinstance(error, Exception) else None
        )
    )
    return PythonCANEnumerationFailure(interface, stage, classified, channel)


def _failed(interface: str, stage: str, error: Exception) -> PythonCANEnumerationResult:
    return PythonCANEnumerationResult(failures=(_failure(interface, stage, error),))


__all__ = [
    "PythonCANChannel",
    "PythonCANEnumerationFailure",
    "PythonCANEnumerationResult",
    "enumerate_pcan_channels",
    "enumerate_slcan_channels",
]
