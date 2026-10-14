"""Lifecycle owner for worker-backed message transports."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Hashable, Iterable
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
from .routing import ReceiveRouter, RouteSubscription

MessageT = TypeVar("MessageT")
_DataSizeOf = Callable[[MessageT], int]


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
    """Explicit lifecycle facade around an internal channel worker.

    Construction stores only the dispatcher factory. The first ``open()``
    call creates the dispatcher and therefore owns all physical I/O and worker
    thread startup. Lifecycle operations use one operation lock so concurrent
    callers cannot create duplicate channels or interleave recovery.

    Args:
        dispatcher_factory: Callable that creates one dispatcher when opened.
            It receives the callback used to report fatal backend errors.
        route_key_of: Constant-time key extraction for one received datagram.
        data_size_of: Optional zero-I/O callback returning the datagram data
            field length used by traffic counters.
        monotonic: Injectable monotonic clock used by health snapshots.
    """

    def __init__(
        self,
        dispatcher_factory: DispatcherFactory[MessageT],
        route_key_of: Callable[[MessageT], Hashable],
        *,
        data_size_of: _DataSizeOf[MessageT] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not callable(dispatcher_factory):
            raise ValidationError("dispatcher_factory must be callable")
        if not callable(route_key_of):
            raise ValidationError("route_key_of must be callable")
        if not callable(monotonic):
            raise ValidationError("monotonic must be callable")
        if data_size_of is not None and not callable(data_size_of):
            raise ValidationError("data_size_of must be callable")
        self._dispatcher_factory = dispatcher_factory
        self._data_size_of = data_size_of or _zero_data_size
        self._monotonic = monotonic
        self._receive_router = ReceiveRouter[MessageT, Hashable](
            route_key_of,
            monotonic=monotonic,
        )
        self._operation_lock = threading.RLock()
        self._state_lock = threading.RLock()
        self._state = TransportState.CLOSED
        self._dispatcher: DispatcherLike[MessageT] | None = None
        self._active_token: object | None = None
        self._receive_callback: Callable[[MessageT], None] | None = None
        self._generation = 0
        self._last_successful_tx_s: float | None = None
        self._last_successful_rx_s: float | None = None
        self._tx_frames = 0
        self._tx_bytes = 0
        self._rx_bytes = 0
        self._data_size_warning_logged = False
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
                self._generation += 1
                generation = self._generation
                token = object()
                self._active_token = token

            def on_bus_error(error: Exception) -> None:
                self._on_dispatcher_error(token, error)

            def on_message(message: MessageT) -> None:
                self._on_message(token, generation, message)

            dispatcher: DispatcherLike[MessageT] | None = None
            try:
                self._receive_router.open(generation=generation)
                dispatcher = self._dispatcher_factory(on_bus_error)
                dispatcher.subscribe(on_message)
                with self._state_lock:
                    if self._state is TransportState.FAULTED:
                        if self._opening_error is not None:
                            raise self._opening_error
                        raise DeviceNotReadyError("dispatcher failed while opening")
                    # Commit under the same lock as the fault check. A worker
                    # fault after this point must remain FAULTED, not be
                    # overwritten by the successful open transition.
                    self._dispatcher = dispatcher
                    self._receive_callback = on_message
                    self._consecutive_errors = 0
                    self._opening_error = None
                    self._state = TransportState.OPEN
            except BaseException as error:
                # A CAN constructor can fail after opening its bus. If its own
                # rollback also failed, take ownership for an explicit retry.
                if dispatcher is None:
                    dispatcher = getattr(error, "_failed_dispatcher", None)
                with self._state_lock:
                    self._active_token = None
                self._receive_router.close()
                if dispatcher is not None:
                    try:
                        dispatcher.stop()
                    except BaseException as cleanup_error:
                        self._logger.warning(
                            "dispatcher cleanup after open failure failed: %s",
                            cleanup_error,
                        )
                        with self._state_lock:
                            self._dispatcher = dispatcher
                            self._receive_callback = on_message
                        self._record_error(cleanup_error, faulted=True)
                classified = self._record_error(error, faulted=True)
                with self._state_lock:
                    self._opening_error = None
                if not isinstance(error, Exception):
                    raise
                _raise_classified(classified, error)

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
                self._receive_router.close()

            try:
                if receive_callback is not None:
                    try:
                        dispatcher.unsubscribe(receive_callback)
                    except Exception as error:
                        self._logger.warning(
                            "dispatcher unsubscribe during close failed: %s", error
                        )
                dispatcher.stop()
            except BaseException as error:
                classified = self._record_error(error, faulted=False)
                with self._state_lock:
                    self._state = TransportState.FAULTED
                if not isinstance(error, Exception):
                    raise
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
            generation = self._generation
        data_size = self._measure_data_size(message)
        receipt = SendReceipt(generation=generation, monotonic=self._monotonic)
        receipt._claim()
        try:
            dispatcher.send_now(message)
        except Exception as error:
            classified = self._record_error(error, faulted=False)
            receipt._fail(classified)
            return receipt
        with self._state_lock:
            self._last_successful_tx_s = self._monotonic()
            self._tx_frames += 1
            self._tx_bytes += data_size
            self._consecutive_errors = 0
        receipt._dispatch()
        return receipt

    def subscribe_route(
        self,
        route_key: Hashable,
        *,
        queue_capacity: int | None = None,
        on_available: Callable[[], None] | None = None,
    ) -> RouteSubscription[MessageT, Hashable]:
        """Open one exclusive bounded inbox on the current connection."""

        with self._state_lock:
            self._require_open_locked()
            return self._receive_router.subscribe(
                route_key,
                queue_capacity=queue_capacity,
                on_available=on_available,
            )

    def subscribe_routes(
        self,
        route_keys: Iterable[Hashable],
        *,
        queue_capacity: int | None = None,
        on_available: Callable[[], None] | None = None,
    ) -> RouteSubscription[MessageT, Hashable]:
        """Open one shared bounded inbox for several exact receive routes."""

        with self._state_lock:
            self._require_open_locked()
            return self._receive_router.subscribe_routes(
                route_keys,
                queue_capacity=queue_capacity,
                on_available=on_available,
            )

    def get_health(self) -> TransportHealth:
        """Return cached health data without device I/O."""

        now = self._monotonic()
        with self._state_lock:
            receive = self._receive_router.get_health()
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
                route_count=receive.route_count,
                pending_rx=receive.queue_depth,
                max_pending_rx=receive.max_queue_depth,
                rx_frames=receive.received,
                routed_rx_frames=receive.routed,
                route_misses=receive.route_misses,
                rx_overflows=receive.overflows,
                stale_rx_frames=receive.stale_frames,
                route_key_errors=receive.key_errors,
                route_registration_rejections=receive.registration_rejections,
                dropped_rx_on_close=receive.dropped_on_close,
                tx_frames=self._tx_frames,
                tx_bytes=self._tx_bytes,
                rx_bytes=self._rx_bytes,
            )

    def _on_message(self, token: object, generation: int, message: MessageT) -> None:
        data_size = self._measure_data_size(message)
        with self._state_lock:
            self._rx_bytes += data_size
            is_current = (
                token is self._active_token
                and self._state is TransportState.OPEN
                and generation == self._generation
            )
            if is_current:
                self._last_successful_rx_s = self._monotonic()
                self._consecutive_errors = 0
        if not is_current:
            self._receive_router.record_stale()
            return
        self._receive_router.route(message, generation=generation)

    def _measure_data_size(self, message: MessageT) -> int:
        try:
            size = self._data_size_of(message)
        except Exception as error:  # noqa: BLE001 - metrics must not break I/O
            self._warn_data_size_once("message data-size measurement failed: %s", error)
            return 0
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            self._warn_data_size_once(
                "message data-size measurement returned invalid value: %r",
                size,
            )
            return 0
        return size

    def _warn_data_size_once(self, message: str, value: object) -> None:
        with self._state_lock:
            if self._data_size_warning_logged:
                return
            self._data_size_warning_logged = True
        self._logger.warning(message, value)

    def _on_dispatcher_error(self, token: object, error: Exception) -> None:
        with self._state_lock:
            if token is not self._active_token:
                return
            classified = self._record_error(error, faulted=True)
        self._receive_router.abort(classified)

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


def _zero_data_size(message: object) -> int:
    del message
    return 0


def _raise_classified(classified: LinkerbotError, original: BaseException) -> None:
    if classified is original:
        raise classified
    raise classified from original


__all__ = [
    "DispatcherFactory",
    "DispatcherLike",
    "ManagedMessageTransport",
]
