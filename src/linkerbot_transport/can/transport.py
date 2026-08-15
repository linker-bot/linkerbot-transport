"""Explicit-lifecycle transport for classic CAN channels."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

import can

from linkerbot_transport._managed import DispatcherFactory, ManagedMessageTransport
from linkerbot_transport.errors import ValidationError

from .can import DEFAULT_MAX_CONSECUTIVE_ERRORS, CANMessageDispatcher
from .types import CanInterface


@dataclass(frozen=True, slots=True)
class CANTransportKey:
    """Pool key identifying one classic CAN channel."""

    interface_name: str
    interface_type: str = "socketcan"


class CANTransport(ManagedMessageTransport[can.Message]):
    """Classic CAN transport whose constructor performs no device I/O."""

    def __init__(
        self,
        interface_name: str,
        interface_type: str = "socketcan",
        *,
        max_consecutive_errors: int = DEFAULT_MAX_CONSECUTIVE_ERRORS,
        dispatcher_factory: DispatcherFactory[can.Message] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not isinstance(interface_name, str) or not interface_name:
            raise ValidationError("interface_name must be a non-empty string")
        if not isinstance(interface_type, str) or not interface_type:
            raise ValidationError("interface_type must be a non-empty string")
        if (
            not isinstance(max_consecutive_errors, int)
            or isinstance(max_consecutive_errors, bool)
            or max_consecutive_errors <= 0
        ):
            raise ValidationError("max_consecutive_errors must be a positive int")
        self._can_interface = CanInterface(interface_name, interface_type)
        self._key = CANTransportKey(interface_name, interface_type)

        if dispatcher_factory is None:

            def create(
                on_bus_error: Callable[[Exception], None],
            ) -> CANMessageDispatcher:
                return CANMessageDispatcher(
                    interface_name=interface_name,
                    interface_type=interface_type,
                    on_bus_error=on_bus_error,
                    max_consecutive_errors=max_consecutive_errors,
                )

            resolved_factory: DispatcherFactory[can.Message] = create
        else:
            resolved_factory = dispatcher_factory
        super().__init__(resolved_factory, monotonic=monotonic)

    @property
    def can_interface(self) -> CanInterface:
        """Return the configured CAN interface without opening it."""

        return self._can_interface

    @property
    def pool_key(self) -> CANTransportKey:
        """Return the default key used to share this physical channel."""

        return self._key


__all__ = ["CANTransport", "CANTransportKey"]
