"""Local ownership tracking around python-can's PCAN implementation.

The instance proxy observes successful native initialization without replacing
python-can's process-global PCANBasic factory or duplicating its initialization,
framing or receive loop. Keep driver-version assumptions tested at this boundary.
"""

from __future__ import annotations

import importlib
import logging
import threading
from typing import Any

import can
from can.interfaces.pcan import pcan

_LOGGER = logging.getLogger(__name__)


class _OwnedPCANDriver:
    def __init__(self, driver: Any) -> None:
        self._driver = driver
        self.channel: Any = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._driver, name)

    def Initialize(self, channel: Any, *args: Any, **kwargs: Any) -> Any:
        result = self._driver.Initialize(channel, *args, **kwargs)
        if result == pcan.PCAN_ERROR_OK:
            self.channel = channel
        return result

    def InitializeFD(self, channel: Any, *args: Any, **kwargs: Any) -> Any:
        result = self._driver.InitializeFD(channel, *args, **kwargs)
        if result == pcan.PCAN_ERROR_OK:
            self.channel = channel
        return result

    def release(self) -> None:
        if self.channel is None:
            return
        status = self._driver.Uninitialize(self.channel)
        if status != pcan.PCAN_ERROR_OK:
            raise can.CanOperationError(
                f"PCAN Uninitialize failed with status {status}"
            )
        self.channel = None


def _close_windows_event(handle: Any) -> None:
    # python-can creates a raw HANDLE with _overlapped.CreateEvent on Windows.
    # Import lazily so PCAN on Linux and no-driver installations still import.
    winapi: Any = importlib.import_module("_winapi")
    winapi.CloseHandle(handle)


class ManagedPcanBus(pcan.PcanBus):
    """Rollback only channels acquired by this instance, including partial init."""

    def __init__(self, channel: str, **kwargs: Any) -> None:
        self._driver: _OwnedPCANDriver | None = None
        self._cleanup_lock = threading.Lock()
        self._cleanup_complete = False
        self._windows_events = pcan.HAS_EVENTS and pcan.IS_WINDOWS
        self._linux_events = pcan.HAS_EVENTS and pcan.IS_LINUX
        try:
            super().__init__(channel=channel, **kwargs)
        except BaseException as error:
            setattr(error, "_failed_bus", self)
            raise

    @property
    def m_objPCANBasic(self) -> Any:
        if self._driver is None:
            raise AttributeError("PCAN driver has not been loaded")
        return self._driver

    @m_objPCANBasic.setter
    def m_objPCANBasic(self, driver: Any) -> None:
        # PcanBus creates and assigns its own PCANBasic instance before the
        # first native call. Wrap only that instance to track Init/InitFD.
        self._driver = _OwnedPCANDriver(driver)

    def shutdown(self) -> None:
        with self._cleanup_lock:
            if self._cleanup_complete:
                return
            can.BusABC.shutdown(self)
            event = getattr(self, "_recv_event", None)
            if self._driver is not None:
                if (
                    self._driver.channel is not None
                    and self._linux_events
                    and event is not None
                ):
                    try:
                        status = self._driver.SetValue(
                            self._driver.channel, pcan.PCAN_RECEIVE_EVENT, 0
                        )
                        if status != pcan.PCAN_ERROR_OK:
                            _LOGGER.warning("PCAN event detach returned %s", status)
                    except Exception as error:
                        _LOGGER.warning("PCAN event detach failed: %s", error)
                # Upstream shutdown ignores this return value. Only successful
                # Uninitialize commits release, so failed cleanup is retryable.
                self._driver.release()
            if self._windows_events and event is not None:
                # Stop the native producer before freeing its notification
                # handle. A failure here retries just CloseHandle, not Init/Exit.
                _close_windows_event(event)
                self._recv_event = None
            self._cleanup_complete = True
