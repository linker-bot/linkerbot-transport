"""Explicit-lifecycle transport for CAN FD channels."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from linkerbot_transport._managed import DispatcherFactory, ManagedMessageTransport
from linkerbot_transport.errors import ValidationError

from .dispatcher import DEFAULT_MAX_CONSECUTIVE_ERRORS, CANFDMessageDispatcher
from .socketcan_backend import (
    DEFAULT_DATA_BITRATE,
    DEFAULT_NOMINAL_BITRATE,
    SocketCANFDBackend,
)
from .types import CANFDConfigOptions, CANFDMessage


@dataclass(frozen=True, slots=True)
class CANFDTransportKey:
    """Pool key identifying one vendor CAN FD adapter channel."""

    device_index: int
    channel_index: int
    library_path: str | None
    config: CANFDConfigOptions


class CANFDTransport(ManagedMessageTransport[CANFDMessage]):
    """CAN FD transport whose constructor performs no device I/O."""

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
        if (
            not isinstance(max_consecutive_errors, int)
            or isinstance(max_consecutive_errors, bool)
            or max_consecutive_errors <= 0
        ):
            raise ValidationError("max_consecutive_errors must be a positive int")
        normalized_path = None if library_path is None else str(library_path)
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
                return CANFDMessageDispatcher(
                    device_index=device_index,
                    channel_index=channel_index,
                    library_path=library_path,
                    config=normalized_config,
                    on_bus_error=on_bus_error,
                    max_consecutive_errors=max_consecutive_errors,
                )

            resolved_factory: DispatcherFactory[CANFDMessage] = create
        else:
            resolved_factory = dispatcher_factory
        super().__init__(resolved_factory, monotonic=monotonic)

    @property
    def pool_key(self) -> CANFDTransportKey:
        """Return the default key used to share this physical channel."""

        return self._key


@dataclass(frozen=True, slots=True)
class SocketCANFDTransportKey:
    """Pool key identifying one Linux SocketCAN FD channel."""

    channel: str
    bitrate: int
    data_bitrate: int
    fd: bool
    auto_reconfigure: bool
    receive_own_messages: bool


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
                try:
                    return CANFDMessageDispatcher(
                        interface=backend,
                        on_bus_error=on_bus_error,
                        max_consecutive_errors=max_consecutive_errors,
                    )
                except Exception:
                    backend.close()
                    raise

            resolved_factory: DispatcherFactory[CANFDMessage] = create
        else:
            resolved_factory = dispatcher_factory
        super().__init__(resolved_factory, monotonic=monotonic)

    @property
    def pool_key(self) -> SocketCANFDTransportKey:
        """Return the default key used to share this physical channel."""

        return self._key


__all__ = [
    "CANFDTransport",
    "CANFDTransportKey",
    "SocketCANFDTransport",
    "SocketCANFDTransportKey",
]
