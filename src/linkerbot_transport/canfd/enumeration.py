"""Explicit, frame-free MiniCANFD adapter enumeration.

The vendor Linux ABI reports devices, but does not expose their channel count.
Only caller-confirmed channel layouts are inspected on that platform. Windows
reports channel numbers directly. Neither path opens or initializes a CAN bus.
"""

from __future__ import annotations

import ctypes
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from linkerbot_transport.errors import TransportError, ValidationError

from . import ctypes_backend as backend


@dataclass(frozen=True)
class MiniCANFDChannel:
    """One FD-capable native channel accepted by ``CANFDTransport``.

    Windows ``device_index`` is the DLL's devNum; ``channel_index`` is zero.
    Linux keeps the device index and caller-confirmed channel index separate.
    ``library_path`` preserves the loaded library selection, including an
    environment-provided path, for later transport construction.
    """

    device_index: int
    channel_index: int
    library_path: str


@dataclass(frozen=True)
class MiniCANFDEnumerationFailure:
    """A backend or native-channel failure; indices are absent when unknown."""

    stage: Literal["library", "busy", "lifecycle", "scan", "layout", "type", "cleanup"]
    error: TransportError
    device_index: int | None = None
    channel_index: int | None = None


@dataclass(frozen=True)
class MiniCANFDEnumerationResult:
    """FD channels plus diagnostics; empty channels alone do not imply success."""

    channels: tuple[MiniCANFDChannel, ...] = ()
    failures: tuple[MiniCANFDEnumerationFailure, ...] = ()


def enumerate_minicanfd_channels(
    library_path: str | Path | None = None,
    *,
    linux_channels: Mapping[int, Sequence[int]] | None = None,
) -> MiniCANFDEnumerationResult:
    """Scan once without opening channels or sending CAN messages.

    On Linux, ``linux_channels={0: (0, 1)}`` explicitly declares that scanned
    device 0 has channels 0 and 1 according to its confirmed specification.
    Unspecified devices produce layout diagnostics, not guessed channels.
    Empty channel sequences intentionally skip a device. Type queries filter
    out classic CAN and LIN channels. Windows rejects ``linux_channels`` and
    checks each DLL-enumerated devNum instead.

    Invalid configuration raises ``ValidationError`` before loading a library.
    Library, driver and cleanup failures are retained in ``result.failures``;
    failure of one type query does not discard other identified channels.
    Close this process's connections to the same library before scanning:
    an active or pending-close channel produces a busy diagnostic. External
    processes are governed by the vendor driver, not this process-local guard.
    """
    platform_name = backend.platform.system()
    if platform_name not in ("Linux", "Windows"):
        raise ValidationError("MiniCANFD enumeration supports Linux and Windows")
    windows = platform_name == "Windows"
    if windows and linux_channels is not None:
        raise ValidationError("linux_channels is only valid on Linux")
    layouts = _normalize_layouts(linux_channels)
    try:
        lib = backend._load_library(library_path)
        backend._bind_signatures(lib, windows=windows)
        resolved_path = _connection_library_path(lib, windows)
    except Exception as error:
        return _failed("library", error)

    channels: list[MiniCANFDChannel] = []
    failures: list[MiniCANFDEnumerationFailure] = []
    with backend._enumeration_lock:
        if backend._open_channel_counts.get(backend._library_key(lib), 0):
            return _failed(
                "busy",
                TransportError(
                    "MiniCANFD enumeration requires all channels using this library "
                    "to be closed; an open or pending-close channel still owns it"
                ),
            )
        lifecycle_ref = None
        if not windows:
            try:
                lifecycle_ref = backend._lifecycle_acquire(lib)
            except Exception as error:
                return _failed("lifecycle", error)
        try:
            try:
                count = backend._scan_device_count(lib)
            except Exception as error:
                failures.append(MiniCANFDEnumerationFailure("scan", _as_error(error)))
            else:
                _inspect_channels(
                    lib, count, windows, layouts, resolved_path, channels, failures
                )
        finally:
            cleanup_error = backend._lifecycle_release(lifecycle_ref)
            if cleanup_error is not None:
                failures.append(MiniCANFDEnumerationFailure("cleanup", cleanup_error))
    return MiniCANFDEnumerationResult(tuple(channels), tuple(failures))


def _normalize_layouts(
    layouts: Mapping[int, Sequence[int]] | None,
) -> dict[int, tuple[int, ...]]:
    if layouts is None:
        return {}
    if not isinstance(layouts, Mapping):
        raise ValidationError(
            "linux_channels must map device indices to channel indices"
        )
    result = {}
    for device_index, channel_indices in layouts.items():
        _validate_index(device_index, "device_index")
        if not isinstance(channel_indices, Sequence) or isinstance(
            channel_indices, (str, bytes, bytearray)
        ):
            raise ValidationError("channel indices must be a sequence of integers")
        copied = tuple(channel_indices)
        for channel_index in copied:
            _validate_index(channel_index, "channel_index")
        result[device_index] = tuple(dict.fromkeys(copied))
    return result


def _validate_index(value: int, name: str) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 0xFFFFFFFF
    ):
        raise ValidationError(f"{name} must be an unsigned 32-bit integer")


def _inspect_channels(
    lib: Any,
    count: int,
    windows: bool,
    layouts: dict[int, tuple[int, ...]],
    library_path: str,
    channels: list[MiniCANFDChannel],
    failures: list[MiniCANFDEnumerationFailure],
) -> None:
    candidates = []
    for device_index in range(count):
        if windows:
            candidates.append((device_index, 0))
        elif device_index not in layouts:
            failures.append(
                MiniCANFDEnumerationFailure(
                    "layout",
                    TransportError(
                        "Linux CAN_ScanDevice reports device count, not channel count; "
                        f"supply confirmed linux_channels for device {device_index}"
                    ),
                    device_index,
                )
            )
        else:
            candidates.extend(
                (device_index, channel) for channel in layouts[device_index]
            )
    for device_index in layouts:
        if device_index >= count:
            failures.append(
                MiniCANFDEnumerationFailure(
                    "layout",
                    TransportError(
                        f"Configured device_index {device_index} was not scanned; "
                        f"found {count} device(s)"
                    ),
                    device_index,
                )
            )
    if not candidates:
        return
    try:
        get_type, function_name = _bind_channel_type(lib, windows)
    except Exception as error:
        failures.append(MiniCANFDEnumerationFailure("type", _as_error(error)))
        return
    for device_index, channel_index in candidates:
        device_level = function_name != "CAN_GetCHType"
        if not windows and device_level and channel_index != 0:
            failures.append(
                MiniCANFDEnumerationFailure(
                    "layout",
                    TransportError(
                        "legacy Linux MiniCANFD supports only confirmed channel 0; "
                        "CAN_GetDevType cannot verify other channels"
                    ),
                    device_index,
                    channel_index,
                )
            )
            continue
        args = (device_index,) if device_level else (device_index, channel_index)
        try:
            channel_type = get_type(*args)
            if channel_type not in (0, 1, 2):
                raise TransportError(
                    f"{function_name}{args} returned unexpected type/status {channel_type}"
                )
        except Exception as error:
            failures.append(
                MiniCANFDEnumerationFailure(
                    "type", _as_error(error), device_index, channel_index
                )
            )
            continue
        if channel_type == 1:
            channels.append(MiniCANFDChannel(device_index, channel_index, library_path))


def _bind_channel_type(lib: Any, windows: bool) -> tuple[Any, str]:
    # V1.3 documents GetDevType. Distributed Windows DLLs also export the
    # spelling GeDevType, with the same one-devNum ABI. The verified legacy
    # Linux export set also has a one-device query, limited to channel 0 here.
    # Never substitute it for a missing GetCHType in a lifecycle-aware library.
    device_level = windows or backend._linux_uses_legacy_api(lib)
    names = (
        ("CAN_GetDevType", "CAN_GeDevType")
        if windows
        else (("CAN_GetDevType",) if device_level else ("CAN_GetCHType",))
    )
    for name in names:
        function = getattr(lib, name, None)
        if function is not None:
            function.restype = ctypes.c_int
            function.argtypes = [ctypes.c_uint] * (1 if device_level else 2)
            return function, name
    raise TransportError(
        "MiniCANFD enumeration cannot verify CAN FD channels: vendor library "
        f"is missing {' or '.join(names)}"
    )


def _connection_library_path(lib: Any, windows: bool) -> str:
    name = os.fspath(lib._name)
    if os.path.isabs(name) or os.sep in name or (os.altsep and os.altsep in name):
        return os.path.realpath(os.path.abspath(os.path.expanduser(name)))
    # A loader-resolved soname is not a cwd-relative file. CANFDTransport fixes
    # explicit paths at construction, so resolve the already loaded module
    # before returning parameters that can recreate the same connection.
    return _native_library_path(lib, windows)


class _DlInfo(ctypes.Structure):
    _fields_ = [
        ("filename", ctypes.c_char_p),
        ("base", ctypes.c_void_p),
        ("symbol", ctypes.c_char_p),
        ("address", ctypes.c_void_p),
    ]


def _native_library_path(lib: Any, windows: bool) -> str:
    if windows:
        kernel = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
        get_path = kernel.GetModuleFileNameW
        get_path.restype = ctypes.c_uint
        get_path.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p, ctypes.c_uint]
        buffer = ctypes.create_unicode_buffer(32768)
        length = get_path(lib._handle, buffer, len(buffer))
        path = buffer.value if 0 < length < len(buffer) else ""
    else:
        get_info = ctypes.CDLL(None).dladdr
        get_info.restype = ctypes.c_int
        get_info.argtypes = [ctypes.c_void_p, ctypes.POINTER(_DlInfo)]
        info = _DlInfo()
        found = get_info(
            ctypes.cast(lib.CAN_ScanDevice, ctypes.c_void_p), ctypes.byref(info)
        )
        path = os.fsdecode(info.filename) if found and info.filename else ""
    if not path:
        raise TransportError(
            "Cannot resolve the loaded MiniCANFD library path; "
            "pass an explicit absolute library_path"
        )
    return os.path.realpath(os.path.abspath(path))


def _as_error(error: Exception) -> TransportError:
    if isinstance(error, TransportError):
        return error
    return TransportError(str(error), cause=error)


def _failed(
    stage: Literal["library", "busy", "lifecycle"], error: Exception
) -> MiniCANFDEnumerationResult:
    return MiniCANFDEnumerationResult(
        failures=(MiniCANFDEnumerationFailure(stage, _as_error(error)),)
    )
