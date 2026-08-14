"""Threaded CANFD message dispatcher."""

import logging
import queue
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

from linkerbot_transport.errors import CANError, HandSDKError, ValidationError

from .ctypes_backend import CANFDInterface
from .types import CANFDBackend, CANFDConfigOptions, CANFDMessage

DEFAULT_MAX_CONSECUTIVE_ERRORS = 10
RECEIVE_BATCH_SIZE = 64
RECEIVE_TIMEOUT_MS = 10
SEND_QUEUE_GET_TIMEOUT_S = 0.01
THREAD_JOIN_TIMEOUT_S = 1.0
# Tight error backoff: a transient USB hiccup should recover in milliseconds,
# not pause the whole bus for seconds. Previously 0.1/1.0 made a single USB
# stutter cascade into ~5.5 s of bus silence before the dispatcher gave up.
ERROR_BACKOFF_BASE_S = 0.005
ERROR_BACKOFF_MAX_S = 0.05


class _ImmediateDispatchState:
    __slots__ = ("claim_dispatch", "claim_result", "mark_dispatched", "marked")

    def __init__(
        self,
        claim_dispatch: Callable[[], bool],
        mark_dispatched: Callable[[], None],
    ) -> None:
        self.claim_dispatch = claim_dispatch
        self.claim_result: bool | None = None
        self.mark_dispatched = mark_dispatched
        self.marked = False


class CANFDMessageDispatcher:
    """Thread-safe CANFD dispatcher with publish-subscribe message routing."""

    SEND_QUEUE_SIZE = 2000
    # Default off: the vendor USB stack already paces transmits, and any
    # positive interval ends up sleeping the send thread. Subclasses or callers
    # that want explicit pacing can override this class attribute. The legacy
    # 300 us CPU busy-wait has been removed entirely.
    SEND_INTERVAL_S = 0.0
    # Maximum frames coalesced into a single vendor call when the backend
    # exposes ``send_batch``. 32 leaves substantial headroom under the
    # vendor's 100-frame ceiling while keeping per-batch latency bounded.
    # Override on a subclass if a workload needs different latency/throughput
    # tradeoffs.
    SEND_BATCH_MAX = 32

    def __init__(
        self,
        device_index: int = 0,
        channel_index: int = 0,
        library_path: str | Path | None = None,
        config: CANFDConfigOptions | None = None,
        on_bus_error: Callable[[Exception], None] | None = None,
        max_consecutive_errors: int = DEFAULT_MAX_CONSECUTIVE_ERRORS,
        interface: CANFDBackend | None = None,
    ) -> None:
        """Initialize dispatcher threads for one CANFD interface.

        ``interface`` accepts any object satisfying ``CANFDBackend`` (the
        ctypes ``CANFDInterface`` and the new SocketCAN FD backend both do).
        When omitted, the dispatcher constructs a vendor ``CANFDInterface``
        using ``device_index`` / ``channel_index`` / ``library_path`` / ``config``,
        preserving the original behaviour.
        """
        if (
            not isinstance(max_consecutive_errors, int)
            or isinstance(max_consecutive_errors, bool)
            or max_consecutive_errors <= 0
        ):
            raise ValidationError("max_consecutive_errors must be a positive int")

        self._interface: CANFDBackend = (
            interface
            if interface is not None
            else CANFDInterface(
                device_index=device_index,
                channel_index=channel_index,
                library_path=library_path,
                config=config,
            )
        )
        self._subscribers: list[Callable[[CANFDMessage], None]] = []
        # subscribe_filter targets are only invoked when their predicate
        # returns True. Predicates are still scanned linearly, but unrelated
        # clients avoid the more expensive frame parsing and callback path.
        self._filtered_subscribers: list[
            tuple[
                Callable[[CANFDMessage], bool],
                Callable[[CANFDMessage], None],
            ]
        ] = []
        self._subscribers_lock = threading.Lock()
        self._running = True
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._send_queue: queue.Queue[CANFDMessage] = queue.Queue(
            maxsize=self.SEND_QUEUE_SIZE
        )
        self._on_bus_error = on_bus_error
        self._max_consecutive_errors = max_consecutive_errors
        self._bus_error: Exception | None = None
        self._error_reported = False
        self._error_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._interface_closed = False
        self._transmit_lock = threading.Lock()
        self._immediate_dispatch = threading.local()
        self._recv_thread = threading.Thread(
            target=self._recv_loop, daemon=True, name="CANFDMessageDispatcher.recv_loop"
        )
        self._send_thread = threading.Thread(
            target=self._send_loop, daemon=True, name="CANFDMessageDispatcher.send_loop"
        )
        self._recv_thread.start()
        self._send_thread.start()

    @property
    def interface(self) -> CANFDBackend:
        """CANFD backend this dispatcher is bound to."""
        return self._interface

    def _handle_bus_error(self, error: Exception) -> None:
        with self._error_lock:
            if self._error_reported:
                return
            self._error_reported = True
            self._running = False
            self._bus_error = error
        self._logger.error(f"CANFD bus fatal error, stopping dispatcher: {error}")
        if self._on_bus_error is not None:
            try:
                self._on_bus_error(error)
            except Exception as callback_error:
                self._logger.error(f"Error in on_bus_error callback: {callback_error}")

    def _recv_loop(self) -> None:
        consecutive_errors = 0
        while self._is_running():
            try:
                messages = self._interface.receive(
                    max_frames=RECEIVE_BATCH_SIZE,
                    timeout_ms=RECEIVE_TIMEOUT_MS,
                )
                consecutive_errors = 0
                for message in messages:
                    if not self._is_running():
                        return
                    self._dispatch(message)
            except Exception as error:
                if not self._is_running():
                    return
                consecutive_errors += 1
                if consecutive_errors >= self._max_consecutive_errors:
                    self._logger.error(
                        "CANFD receive failed after %d consecutive errors: %s",
                        consecutive_errors,
                        error,
                    )
                    self._handle_bus_error(error)
                    return
                self._logger.warning(
                    "CANFD receive error (%d/%d): %s",
                    consecutive_errors,
                    self._max_consecutive_errors,
                    error,
                )
                time.sleep(_error_backoff_s(consecutive_errors))

    def _dispatch(self, message: CANFDMessage) -> None:
        with self._subscribers_lock:
            subscribers = self._subscribers[:]
            filtered = self._filtered_subscribers[:]
        for callback in subscribers:
            if not self._is_running():
                return
            try:
                callback(message)
            except Exception as error:
                self._logger.error("Error in callback: %s", error)
        for predicate, callback in filtered:
            if not self._is_running():
                return
            try:
                if predicate(message) and self._is_running():
                    callback(message)
            except Exception as error:
                self._logger.error("Error in filtered callback: %s", error)

    def subscribe(self, callback: Callable[[CANFDMessage], None]) -> None:
        """Register a callback to receive every CANFD message.

        Equivalent to ``subscribe_filter(lambda _: True, callback)`` but
        skips the per-message predicate call.
        """
        with self._subscribers_lock:
            if callback not in self._subscribers:
                self._subscribers.append(callback)

    def subscribe_filter(
        self,
        predicate: Callable[[CANFDMessage], bool],
        callback: Callable[[CANFDMessage], None],
    ) -> None:
        """Register a callback gated by a per-message predicate.

        ``callback`` is invoked only when ``predicate(message)`` returns
        ``True``. Predicates are evaluated by the receive thread, so they
        must be cheap and non-blocking. The L30 client uses this to filter
        by ``(dst_id, src_id)`` before doing the full frame parse.
        """
        with self._subscribers_lock:
            entry = (predicate, callback)
            if entry not in self._filtered_subscribers:
                self._filtered_subscribers.append(entry)

    def unsubscribe(self, callback: Callable[[CANFDMessage], None]) -> None:
        """Unregister a CANFD message callback (either plain or filtered)."""
        with self._subscribers_lock:
            if callback in self._subscribers:
                self._subscribers.remove(callback)
            self._filtered_subscribers = [
                (predicate, existing)
                for predicate, existing in self._filtered_subscribers
                if existing != callback
            ]

    def _send_loop(self) -> None:
        consecutive_errors = 0
        # Detect the optional batch path once; backends that don't implement
        # send_batch fall back to per-frame send transparently.
        send_batch = getattr(self._interface, "send_batch", None)
        while self._is_running():
            try:
                first = self._send_queue.get(timeout=SEND_QUEUE_GET_TIMEOUT_S)
            except queue.Empty:
                continue
            # Opportunistic batching: drain whatever frames are already queued
            # up to SEND_BATCH_MAX, but never block — if there's only one
            # frame ready, send it immediately rather than introducing latency
            # by waiting for more.
            batch: list[CANFDMessage] = [first]
            if send_batch is not None and self.SEND_BATCH_MAX > 1:
                while len(batch) < self.SEND_BATCH_MAX:
                    try:
                        batch.append(self._send_queue.get_nowait())
                    except queue.Empty:
                        break
            try:
                if send_batch is not None and len(batch) > 1:
                    with self._transmit_lock:
                        with self._error_lock:
                            if not self._running:
                                return
                        send_batch(batch)
                else:
                    # Either backend lacks batch support, or only one frame
                    # was ready — both paths fall through to per-frame send.
                    with self._transmit_lock:
                        with self._error_lock:
                            if not self._running:
                                return
                        for message in batch:
                            self._interface.send(message)
                consecutive_errors = 0
                # Optional pacing for backward compatibility; default 0 means
                # rely entirely on the vendor USB stack. A previous version
                # spun `while time.monotonic() < deadline: pass` here which
                # pegged one CPU core and capped throughput at ~3300 frames/s.
                if self.SEND_INTERVAL_S > 0:
                    time.sleep(self.SEND_INTERVAL_S)
            except Exception as error:
                if not self._is_running():
                    return
                consecutive_errors += 1
                if consecutive_errors >= self._max_consecutive_errors:
                    self._logger.error(
                        "CANFD send failed after %d consecutive errors: %s",
                        consecutive_errors,
                        error,
                    )
                    self._handle_bus_error(error)
                    return
                self._logger.warning(
                    "CANFD send error (%d/%d): %s",
                    consecutive_errors,
                    self._max_consecutive_errors,
                    error,
                )
                time.sleep(_error_backoff_s(consecutive_errors))

    def send(self, message: CANFDMessage) -> None:
        """Enqueue a CANFD message for rate-limited transmission."""
        state: _ImmediateDispatchState | None = getattr(
            self._immediate_dispatch, "state", None
        )
        if state is not None:
            if state.claim_result is None:
                state.claim_result = state.claim_dispatch()
            if not state.claim_result:
                return
            self.send_now(message)
            if not state.marked:
                state.mark_dispatched()
                state.marked = True
            return
        with self._error_lock:
            self._raise_if_cannot_send_locked()
            self._send_queue.put_nowait(message)

    def send_now(self, message: CANFDMessage) -> None:
        """Synchronously send a frame for lifecycle-aware command scheduling."""
        with self._transmit_lock:
            with self._error_lock:
                self._raise_if_cannot_send_locked()
            try:
                self._interface.send(message)
            except HandSDKError:
                raise
            except Exception as error:
                raise CANError(f"CANFD send failed: {error}", cause=error) from error

    @contextmanager
    def immediate_dispatch(
        self,
        claim_dispatch: Callable[[], bool],
        mark_dispatched: Callable[[], None],
    ) -> Iterator[None]:
        """Make sends synchronous for the current thread and command scope."""
        if not callable(claim_dispatch):
            raise ValidationError("claim_dispatch must be callable")
        if not callable(mark_dispatched):
            raise ValidationError("mark_dispatched must be callable")
        previous = getattr(self._immediate_dispatch, "state", None)
        self._immediate_dispatch.state = _ImmediateDispatchState(
            claim_dispatch, mark_dispatched
        )
        try:
            yield
        finally:
            if previous is None:
                del self._immediate_dispatch.state
            else:
                self._immediate_dispatch.state = previous

    def stop(self) -> None:
        """Stop dispatcher threads and close the CANFD interface.

        Shutdown ordering honours the interface's close contract while keeping
        worker waits bounded:

        1. Reject new sends and clear subscribers.
        2. Give both workers a bounded opportunity to exit normally.
        3. Boundedly drain synchronous transmit activity. If it becomes idle,
           close while holding the transmit lock; otherwise close the interface
           without that lock so backend shutdown can interrupt pending I/O.
        4. Perform a second bounded worker join and drain synchronous sends.

        If ``stop()`` is invoked from inside a dispatcher-managed callback,
        the calling thread is skipped to avoid join-self deadlock.

        Raises:
            RuntimeError: If worker or transmit activity survives interface close.
            HandSDKError: If the backend reports a classified close failure.
        """
        with self._error_lock:
            self._error_reported = True
            self._running = False
        with self._subscribers_lock:
            self._subscribers.clear()
            self._filtered_subscribers.clear()
        current = threading.current_thread()
        running_threads = self._join_worker_threads(current)
        if running_threads:
            self._logger.warning(
                "CANFD worker threads did not stop before interface close: %s",
                ", ".join(thread.name for thread in running_threads),
            )

        close_error = self._close_interface_after_transmit_drain()
        running_threads = self._join_worker_threads(current)
        transmit_idle = self._wait_for_transmit_idle()
        if running_threads:
            names = ", ".join(thread.name for thread in running_threads)
            raise RuntimeError(
                f"CANFD worker threads did not stop after interface close: {names}"
            )
        if not transmit_idle:
            raise RuntimeError("CANFD transmit did not stop after interface close")
        if close_error is not None:
            raise close_error

    def _is_running(self) -> bool:
        with self._error_lock:
            return self._running

    def _raise_if_cannot_send_locked(self) -> None:
        if self._bus_error is not None:
            raise CANError(f"CANFD bus unavailable: {self._bus_error}")
        if not self._running:
            raise RuntimeError("Cannot send on a stopped CANFDMessageDispatcher")

    def _join_worker_threads(self, current: threading.Thread) -> list[threading.Thread]:
        running_threads: list[threading.Thread] = []
        for thread in (self._send_thread, self._recv_thread):
            if thread is current or not thread.is_alive():
                continue
            thread.join(timeout=THREAD_JOIN_TIMEOUT_S)
            if thread.is_alive():
                running_threads.append(thread)
        return running_threads

    def _close_interface(self) -> HandSDKError | None:
        with self._close_lock:
            if self._interface_closed:
                return None
            try:
                self._interface.close()
            except HandSDKError as error:
                return error
            except Exception as error:
                return CANError(f"CANFD interface close failed: {error}", cause=error)
            self._interface_closed = True
            return None

    def _close_interface_after_transmit_drain(self) -> HandSDKError | None:
        transmit_idle = self._transmit_lock.acquire(timeout=THREAD_JOIN_TIMEOUT_S)
        if not transmit_idle:
            self._logger.warning(
                "CANFD transmit did not stop before interface close; "
                "closing the interface to interrupt it"
            )
            return self._close_interface()
        try:
            return self._close_interface()
        finally:
            self._transmit_lock.release()

    def _wait_for_transmit_idle(self) -> bool:
        acquired = self._transmit_lock.acquire(timeout=THREAD_JOIN_TIMEOUT_S)
        if acquired:
            self._transmit_lock.release()
        return acquired

    def __enter__(self) -> "CANFDMessageDispatcher":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager and stop the dispatcher."""
        self.stop()


def _error_backoff_s(consecutive_errors: int) -> float:
    return min(ERROR_BACKOFF_BASE_S * consecutive_errors, ERROR_BACKOFF_MAX_S)
