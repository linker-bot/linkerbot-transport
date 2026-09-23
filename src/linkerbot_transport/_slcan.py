"""Lifecycle fixes around python-can's SLCAN parser and serial transport."""

from __future__ import annotations

import logging
import threading
from typing import Any

import can
from can.interfaces.slcan import slcanBus

_LOGGER = logging.getLogger(__name__)


class ManagedSlcanBus(slcanBus):
    """Always release the local serial handle, even when the adapter is gone."""

    def __init__(self, channel: str, **kwargs: Any) -> None:
        self._cleanup_lock = threading.Lock()
        self._serial_closed = False
        try:
            super().__init__(channel=channel, **kwargs)
        except BaseException as error:
            # BusABC.__init__ runs last upstream, so its destructor cannot
            # release a serial port acquired before initialization failed.
            setattr(error, "_failed_bus", self)
            raise

    def shutdown(self) -> None:
        with self._cleanup_lock:
            if self._serial_closed:
                return
            can.BusABC.shutdown(self)
            port = getattr(self, "serialPortOrig", None)
            if port is None:
                self._serial_closed = True
                return
            command_error: BaseException | None = None
            try:
                self.close()  # SLCAN C command, not the local serial close.
            except BaseException as error:
                command_error = error
            # Always attempt local release, preserving an original interrupt
            # even if release itself also fails. Failed release remains retryable.
            try:
                port.close()
            except BaseException as error:
                if command_error is not None and not isinstance(
                    command_error, Exception
                ):
                    raise command_error from error
                raise
            self._serial_closed = True
            if isinstance(command_error, (can.CanError, OSError)):
                _LOGGER.warning("SLCAN close command failed: %s", command_error)
            elif command_error is not None:
                raise command_error
