"""Lifecycle owner for legacy dispatcher-backed message transports."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Generic, Protocol, TypeVar

from .contracts import TransportHealth, TransportState
from .errors import (
    ClosedError,
    DeviceNotReadyError,
    ErrorCode,
    LinkerbotError,
    ValidationError,
    normalize_error,
)
from .receipts import SendReceipt

MessageT = TypeVar("MessageT")


class DispatcherLike(Protocol[MessageT]):
    """Dispatcher surface required by ``ManagedMessageTransport``."""

    def send_now(self, message: MessageT, /) -> None:
        """Synchronously transmit a message."""
        ...

    def subscribe(self, callback: Callable[[MessageT], None]) -> None:
        """Register a receive callback."""
        ...

    def unsubscribe(self, callback: Callable[[MessageT], None]) -> None:
        """Remove a receive callback."""
        ...

    def stop(self) -> None:
        """Stop worker threads and close the physical channel."""
        ...


DispatcherFactory = Callable[[Callable[[Exception], None]], DispatcherLike[MessageT]]


class ManagedMessageTransport(Generic[MessageT]):
    """Explicit lifecycle facade around an auto-starting legacy dispatcher.

    Construction stores only the dispatcher factory. The first ``open()``
    call creates the dispatcher and therefore owns all physical I/O and worker
    thread startup. Lifecycle operations use one operation lock so concurrent
    callers cannot create duplicate channels or interleave recovery.

    Args:
        dispatcher_factory: Callable that creates one dispatcher when opened.
            It receives the callback used to report fatal backend errors.
        monotonic: Injectable monotonic clock used by health snapshots.
    """

    def __init__(
        self,
        dispatcher_factory: DispatcherFactory[MessageT],
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not callable(dispatcher_factory):
            raise ValidationError("dispatcher_factory must be callable")
        if not callable(monotonic):
            raise ValidationError("monotonic must be callable")
        self._dispatcher_factory = dispatcher_factory
        self._monotonic = monotonic
        self._operation_lock = threading.RLock()
        self._state_lock = threading.RLock()
        self._state = TransportState.CLOSED
        self._dispatcher: DispatcherLike[MessageT] | None = None
        self._active_token: object | None = None
        self._receive_callback: Callable[[MessageT], None] | None = None
        self._subscribers: list[Callable[[MessageT], None]] = []
        self._generation = 0
        self._last_successful_tx_s: float | None = None
        self._last_successful_rx_s: float | None = None
        self._consecutive_errors = 0
        self._last_error: str | None = None
        self._last_error_code: ErrorCode | None = None
        self._opening_error: LinkerbotError | None = None
        self._reconnect_count = 0
        self._logger = logging.getLogger(f"{__name__}.{type(self).__name__}")

    def open(self) -> None:
        """Create the dispatcher once and mark the connection open."""

        with self._operation_lock:
            with self._state_lock:
                if self._state is TransportState.OPEN:
                    return
                if self._dispatcher is not None:
                    raise DeviceNotReadyError(
                        "transport owns a failed dispatcher; call recover() or close()"
                    )
                self._state = TransportState.OPENING
                token = object()
                self._active_token = token

            def on_bus_error(error: Exception) -> None:
                self._on_dispatcher_error(token, error)

            def on_message(message: MessageT) -> None:
                self._on_message(token, message)

            dispatcher: DispatcherLike[MessageT] | None = None
            try:
                dispatcher = self._dispatcher_factory(on_bus_error)
                dispatcher.subscribe(on_message)
                with self._state_lock:
                    if self._state is TransportState.FAULTED:
                        if self._opening_error is not None:
                            raise self._opening_error
                        raise DeviceNotReadyError("dispatcher failed while opening")
            except Exception as error:
                if dispatcher is not None:
                    try:
                        dispatcher.stop()
                    except Exception as cleanup_error:
                        self._logger.warning(
                            "dispatcher cleanup after open failure failed: %s",
                            cleanup_error,
                        )
                        with self._state_lock:
                            self._dispatcher = dispatcher
                            self._receive_callback = on_message
                        self._record_error(cleanup_error, faulted=True)
                with self._state_lock:
                    self._active_token = None
                classified = self._record_error(error, faulted=True)
                with self._state_lock:
                    self._opening_error = None
                _raise_classified(classified, error)

            with self._state_lock:
                self._dispatcher = dispatcher
                self._receive_callback = on_message
                self._generation += 1
                self._consecutive_errors = 0
                self._opening_error = None
                self._state = TransportState.OPEN

    def close(self) -> None:
        """Stop the owned dispatcher, retaining it when shutdown must retry."""

        with self._operation_lock:
            with self._state_lock:
                if self._state is TransportState.CLOSED:
                    return
                dispatcher = self._dispatcher
                receive_callback = self._receive_callback
                if dispatcher is None:
                    self._active_token = None
                    self._receive_callback = None
                    self._state = TransportState.CLOSED
                    return
                self._state = TransportState.CLOSING
                self._active_token = None

            if receive_callback is not None:
                try:
                    dispatcher.unsubscribe(receive_callback)
                except Exception as error:
                    self._logger.warning(
                        "dispatcher unsubscribe during close failed: %s", error
                    )

            try:
                dispatcher.stop()
            except Exception as error:
                classified = self._record_error(error, faulted=False)
                with self._state_lock:
                    self._state = TransportState.FAULTED
                _raise_classified(classified, error)

            with self._state_lock:
                self._dispatcher = None
                self._receive_callback = None
                self._state = TransportState.CLOSED

    def recover(self) -> None:
        """Close and replace the connection without replaying sent messages."""

        with self._operation_lock:
            with self._state_lock:
                had_connection = self._generation > 0
            self.close()
            self.open()
            if had_connection:
                with self._state_lock:
                    self._reconnect_count += 1

    def send(self, message: MessageT) -> SendReceipt:
        """Send through the dispatcher and return an observable result.

        Lifecycle admission failures still raise before a send is accepted.
        Once accepted, physical backend failures complete the returned receipt
        with a stable transport exception.
        """

        with self._state_lock:
            dispatcher = self._require_open_locked()
        try:
            dispatcher.send_now(message)
        except Exception as error:
            classified = self._record_error(error, faulted=False)
            return SendReceipt.failed(classified)
        with self._state_lock:
            self._last_successful_tx_s = self._monotonic()
            self._consecutive_errors = 0
        return SendReceipt.completed()

    def subscribe(self, callback: Callable[[MessageT], None]) -> None:
        """Register a callback without opening the transport."""

        if not callable(callback):
            raise ValidationError("callback must be callable")
        with self._state_lock:
            if callback not in self._subscribers:
                self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[MessageT], None]) -> None:
        """Remove a callback idempotently."""

        with self._state_lock:
            if callback in self._subscribers:
                self._subscribers.remove(callback)

    def get_health(self) -> TransportHealth:
        """Return cached health data without device I/O."""

        now = self._monotonic()
        with self._state_lock:
            return TransportHealth(
                state=self._state,
                generation=self._generation,
                last_successful_tx_s=self._last_successful_tx_s,
                last_successful_rx_s=self._last_successful_rx_s,
                tx_age_s=_age(now, self._last_successful_tx_s),
                rx_age_s=_age(now, self._last_successful_rx_s),
                consecutive_errors=self._consecutive_errors,
                last_error=self._last_error,
                last_error_code=self._last_error_code,
                reconnect_count=self._reconnect_count,
                pending_tx=0,
                subscriber_count=len(self._subscribers),
            )

    def _on_message(self, token: object, message: MessageT) -> None:
        with self._state_lock:
            if (
                token is not self._active_token
                or self._state is not TransportState.OPEN
            ):
                return
            self._last_successful_rx_s = self._monotonic()
            self._consecutive_errors = 0
            subscribers = tuple(self._subscribers)
        for callback in subscribers:
            with self._state_lock:
                if (
                    token is not self._active_token
                    or self._state is not TransportState.OPEN
                ):
                    return
            try:
                callback(message)
            except Exception as error:
                self._logger.error("transport subscriber failed: %s", error)

    def _on_dispatcher_error(self, token: object, error: Exception) -> None:
        with self._state_lock:
            if token is not self._active_token:
                return
            self._record_error(error, faulted=True)

    def _record_error(self, error: BaseException, *, faulted: bool) -> LinkerbotError:
        classified = normalize_error(error)
        with self._state_lock:
            self._consecutive_errors += 1
            self._last_error = str(classified)
            self._last_error_code = classified.code
            if faulted and self._state is TransportState.OPENING:
                self._opening_error = classified.with_traceback(None)
            if faulted and self._state is not TransportState.CLOSING:
                self._state = TransportState.FAULTED
        return classified

    def _require_open_locked(self) -> DispatcherLike[MessageT]:
        if self._state is TransportState.CLOSED:
            raise ClosedError("transport is closed")
        if self._state is not TransportState.OPEN or self._dispatcher is None:
            raise DeviceNotReadyError(f"transport is {self._state.value}")
        return self._dispatcher


def _age(now: float, timestamp: float | None) -> float | None:
    if timestamp is None:
        return None
    return max(0.0, now - timestamp)


def _raise_classified(classified: LinkerbotError, original: BaseException) -> None:
    if classified is original:
        raise classified
    raise classified from original


__all__ = ["DispatcherFactory", "DispatcherLike", "ManagedMessageTransport"]
