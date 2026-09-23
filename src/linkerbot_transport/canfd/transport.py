"""Explicit-lifecycle transport for CAN FD channels."""

from __future__ import annotations

import os
import platform
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

import can

from linkerbot_transport._managed import DispatcherFactory, ManagedMessageTransport
from linkerbot_transport._python_can import BusOption, positive_int
from linkerbot_transport.errors import ValidationError
from linkerbot_transport.routing import CANRouteKey

from .dispatcher import DEFAULT_MAX_CONSECUTIVE_ERRORS, CANFDMessageDispatcher
from .python_can_backend import (
    PythonCANFDBackend,
    resolve_fd_options,
    restore_fd_timing,
)
from .socketcan_backend import (
    DEFAULT_DATA_BITRATE,
    DEFAULT_NOMINAL_BITRATE,
    SocketCANFDBackend,
)
from .types import CANFDBackend, CANFDConfigOptions, CANFDMessage, dlc_to_length


@dataclass(frozen=True, slots=True)
class CANFDTransportKey:
    """Pool key identifying one vendor CAN FD adapter channel."""

    device_index: int
    channel_index: int
    library_path: str | None
    config: CANFDConfigOptions


class CANFDTransport(ManagedMessageTransport[CANFDMessage]):
    """Vendor CAN FD transport whose constructor performs no device I/O.

    On Windows, ``device_index`` is HCanbus.dll's enumerated channel number
    (devNum), and ``channel_index`` must be zero. Linux keeps separate device
    and channel indices. Explicit library paths are fixed at construction;
    otherwise the loader reads LINKERBOT_CANFD_LIB when opening.
    """

    def __init__(
        self,
        device_index: int = 0,
        channel_index: int = 0,
        library_path: str | Path | None = None,
        config: CANFDConfigOptions | None = None,
        *,
        max_consecutive_errors: int = DEFAULT_MAX_CONSECUTIVE_ERRORS,
        dispatcher_factory: DispatcherFactory[CANFDMessage] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not isinstance(device_index, int) or isinstance(device_index, bool):
            raise ValidationError("device_index must be int")
        if not isinstance(channel_index, int) or isinstance(channel_index, bool):
            raise ValidationError("channel_index must be int")
        if device_index < 0:
            raise ValidationError("device_index must be non-negative")
        if channel_index < 0:
            raise ValidationError("channel_index must be non-negative")
        if device_index > 0xFFFFFFFF or channel_index > 0xFFFFFFFF:
            raise ValidationError("device_index and channel_index must fit uint32")
        if platform.system() == "Windows" and channel_index != 0:
            raise ValidationError(
                "Windows uses device_index as devNum; channel_index must be 0"
            )
        if (
            not isinstance(max_consecutive_errors, int)
            or isinstance(max_consecutive_errors, bool)
            or max_consecutive_errors <= 0
        ):
            raise ValidationError("max_consecutive_errors must be a positive int")
        normalized_path = (
            None
            if library_path is None
            else os.path.abspath(os.path.expanduser(library_path))
        )
        normalized_config = config or CANFDConfigOptions()
        self._key = CANFDTransportKey(
            device_index,
            channel_index,
            normalized_path,
            normalized_config,
        )

        if dispatcher_factory is None:

            def create(
                on_bus_error: Callable[[Exception], None],
            ) -> CANFDMessageDispatcher:
                try:
                    return CANFDMessageDispatcher(
                        device_index=device_index,
                        channel_index=channel_index,
                        library_path=normalized_path,
                        config=normalized_config,
                        on_bus_error=on_bus_error,
                        max_consecutive_errors=max_consecutive_errors,
                    )
                except BaseException as error:
                    failed_interface = getattr(error, "_failed_interface", None)
                    if failed_interface is not None:
                        setattr(
                            error,
                            "_failed_dispatcher",
                            _FailedInterfaceCleanup(failed_interface),
                        )
                    raise

            resolved_factory: DispatcherFactory[CANFDMessage] = create
        else:
            resolved_factory = dispatcher_factory
        super().__init__(
            resolved_factory,
            _canfd_route_key,
            data_size_of=_canfd_data_size,
            monotonic=monotonic,
        )

    @property
    def pool_key(self) -> CANFDTransportKey:
        """Return the default key used to share this physical channel."""

        return self._key


class _FailedInterfaceCleanup:
    """Keep an unreturned vendor handle reachable through managed close/recover."""

    def __init__(self, interface: CANFDBackend) -> None:
        self._interface = interface

    def stop(self) -> None:
        self._interface.close()

    def unsubscribe(self, callback: Callable[[CANFDMessage], None]) -> None:
        # Opening failed before a dispatcher or any subscription existed.
        pass


@dataclass(frozen=True, slots=True)
class SocketCANFDTransportKey:
    """Pool key identifying one Linux SocketCAN FD channel."""

    channel: str
    bitrate: int
    data_bitrate: int
    fd: bool
    auto_reconfigure: bool
    receive_own_messages: bool


@dataclass(frozen=True, slots=True)
class PythonCANFDTransportKey:
    """Immutable python-can channel settings, including serial and bit timing."""

    channel: str
    interface: str
    bitrate: int
    data_bitrate: int
    fd: bool
    receive_own_messages: bool
    bus_options: tuple[tuple[str, BusOption], ...]
    timing: tuple[tuple[str, int], ...]


class PythonCANFDTransport(ManagedMessageTransport[CANFDMessage]):
    """Explicit-lifecycle CAN FD via python-can (SLCAN, SocketCAN, PCAN, etc.).

    SLCAN requires CANable 2.0 FD extensions and the serial dependency. Its
    nominal/data bitrates are translated into BitTimingFd for python-can.
    ``bus_options`` accepts immutable scalar driver settings such as
    ``tty_baudrate``. Explicit timing is copied; it must match both bitrates.
    """

    def __init__(
        self,
        channel: str,
        interface: str,
        *,
        bitrate: int = DEFAULT_NOMINAL_BITRATE,
        data_bitrate: int = DEFAULT_DATA_BITRATE,
        fd: bool = True,
        receive_own_messages: bool = False,
        bus_options: Mapping[str, BusOption] | None = None,
        timing: can.BitTimingFd | None = None,
        max_consecutive_errors: int = DEFAULT_MAX_CONSECUTIVE_ERRORS,
        dispatcher_factory: DispatcherFactory[CANFDMessage] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        positive_int(max_consecutive_errors, "max_consecutive_errors")
        channel, options, timing_values = resolve_fd_options(
            channel,
            interface,
            bitrate,
            data_bitrate,
            fd,
            receive_own_messages,
            bus_options,
            timing,
        )
        self._key = PythonCANFDTransportKey(
            channel,
            interface,
            bitrate,
            data_bitrate,
            fd,
            receive_own_messages,
            options,
            timing_values,
        )
        if dispatcher_factory is None:

            def create(
                on_bus_error: Callable[[Exception], None],
            ) -> CANFDMessageDispatcher:
                resolved_timing = restore_fd_timing(timing_values)
                if interface == "socketcan":
                    backend = SocketCANFDBackend(
                        channel,
                        bitrate=bitrate,
                        data_bitrate=data_bitrate,
                        fd=fd,
                        receive_own_messages=receive_own_messages,
                        bus_options=dict(options),
                        timing=resolved_timing,
                    )
                else:
                    backend = PythonCANFDBackend(
                        channel,
                        interface,
                        bitrate=bitrate,
                        data_bitrate=data_bitrate,
                        fd=fd,
                        receive_own_messages=receive_own_messages,
                        bus_options=dict(options),
                        timing=resolved_timing,
                    )
                return CANFDMessageDispatcher(
                    interface=backend,
                    on_bus_error=on_bus_error,
                    max_consecutive_errors=max_consecutive_errors,
                )

            resolved_factory: DispatcherFactory[CANFDMessage] = create
        else:
            resolved_factory = dispatcher_factory
        super().__init__(
            resolved_factory,
            _canfd_route_key,
            data_size_of=_canfd_data_size,
            monotonic=monotonic,
        )

    @property
    def pool_key(self) -> PythonCANFDTransportKey:
        return self._key


class SocketCANFDTransport(ManagedMessageTransport[CANFDMessage]):
    """SocketCAN FD transport whose constructor performs no link or bus I/O."""

    def __init__(
        self,
        channel: str,
        *,
        bitrate: int = DEFAULT_NOMINAL_BITRATE,
        data_bitrate: int = DEFAULT_DATA_BITRATE,
        fd: bool = True,
        auto_reconfigure: bool = False,
        receive_own_messages: bool = False,
        max_consecutive_errors: int = DEFAULT_MAX_CONSECUTIVE_ERRORS,
        dispatcher_factory: DispatcherFactory[CANFDMessage] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not isinstance(channel, str) or not channel:
            raise ValidationError("channel must be a non-empty string")
        for value, name in (
            (bitrate, "bitrate"),
            (data_bitrate, "data_bitrate"),
            (max_consecutive_errors, "max_consecutive_errors"),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValidationError(f"{name} must be a positive int")
        for value, name in (
            (fd, "fd"),
            (auto_reconfigure, "auto_reconfigure"),
            (receive_own_messages, "receive_own_messages"),
        ):
            if not isinstance(value, bool):
                raise ValidationError(f"{name} must be bool")
        self._key = SocketCANFDTransportKey(
            channel,
            bitrate,
            data_bitrate,
            fd,
            auto_reconfigure,
            receive_own_messages,
        )

        if dispatcher_factory is None:

            def create(
                on_bus_error: Callable[[Exception], None],
            ) -> CANFDMessageDispatcher:
                backend = SocketCANFDBackend(
                    channel,
                    bitrate=bitrate,
                    data_bitrate=data_bitrate,
                    fd=fd,
                    auto_reconfigure=auto_reconfigure,
                    receive_own_messages=receive_own_messages,
                )
                return CANFDMessageDispatcher(
                    interface=backend,
                    on_bus_error=on_bus_error,
                    max_consecutive_errors=max_consecutive_errors,
                )

            resolved_factory: DispatcherFactory[CANFDMessage] = create
        else:
            resolved_factory = dispatcher_factory
        super().__init__(
            resolved_factory,
            _canfd_route_key,
            data_size_of=_canfd_data_size,
            monotonic=monotonic,
        )

    @property
    def pool_key(self) -> SocketCANFDTransportKey:
        """Return the default key used to share this physical channel."""

        return self._key


def _canfd_route_key(message: CANFDMessage) -> CANRouteKey:
    return CANRouteKey(message.arbitration_id, message.is_extended_id)


def _canfd_data_size(message: CANFDMessage) -> int:
    if message.dlc is None:
        return len(message.data)
    return dlc_to_length(message.dlc)


__all__ = [
    "CANFDTransport",
    "CANFDTransportKey",
    "PythonCANFDTransport",
    "PythonCANFDTransportKey",
    "SocketCANFDTransport",
    "SocketCANFDTransportKey",
]
