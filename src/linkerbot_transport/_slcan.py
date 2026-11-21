"""Lifecycle fixes around python-can's SLCAN parser and serial transport."""

from __future__ import annotations

import logging
import threading
from typing import Any

import can
import serial
from can.interfaces.slcan import slcanBus

_LOGGER = logging.getLogger(__name__)


class ManagedSlcanBus(slcanBus):
    """Always release the local serial handle, even when the adapter is gone."""

    # Extended FD header + 64 data bytes + optional four-digit timestamp + CR.
    _MAX_LINE_BYTES = 143
    _READ_BYTE_BUDGET = 4096

    def __init__(self, channel: str, **kwargs: Any) -> None:
        self._cleanup_lock = threading.Lock()
        self._serial_closed = False
        self._discard_line = False
        try:
            super().__init__(channel=channel, **kwargs)
        except BaseException as error:
            # BusABC.__init__ runs last upstream, so its destructor cannot
            # release a serial port acquired before initialization failed.
            setattr(error, "_failed_bus", self)
            raise

    def _read(self, timeout: float | None) -> str | None:
        """Bound partial lines and resynchronize after an overlong/bad line."""
        deadline = serial.Timeout(timeout)
        consumed = 0
        try:
            while consumed < self._READ_BYTE_BUDGET:
                waiting = self.serialPortOrig.in_waiting
                count = min(max(1, waiting), self._READ_BYTE_BUDGET - consumed)
                for _ in range(count):
                    byte = self.serialPortOrig.read(1)
                    if not byte:
                        break
                    consumed += 1
                    delimiter = byte in (self._OK, self._ERROR)
                    if self._discard_line:
                        if delimiter:
                            self._discard_line = False
                            return None
                    else:
                        self._buffer.extend(byte)
                        if len(self._buffer) > self._MAX_LINE_BYTES:
                            self._buffer.clear()
                            self._discard_line = not delimiter
                            raise can.CanOperationError(
                                "SLCAN line exceeds protocol limit"
                            )
                        if delimiter:
                            # Consumed bytes must leave the buffer even when
                            # decoding fails, so the next valid line can recover.
                            raw = bytes(self._buffer)
                            self._buffer.clear()
                            return raw.decode("ascii")
                    if timeout and deadline.expired():
                        return None
                if deadline.expired():
                    return None
            return None
        except (OSError, UnicodeError) as error:
            raise can.CanOperationError(
                f"Could not read SLCAN line: {error}"
            ) from error

    def flush(self) -> None:
        super().flush()
        self._discard_line = False

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
