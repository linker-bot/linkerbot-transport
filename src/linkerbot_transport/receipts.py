"""Observable completion of one accepted transport send."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
from enum import Enum

from ._event_order import next_event_order


class SendOutcome(str, Enum):
    """Terminal outcome of an accepted transport send."""

    DISPATCHED = "dispatched"
    SUPERSEDED = "superseded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class SendResult:
    """Immutable timing and generation metadata for a completed send."""

    outcome: SendOutcome
    generation: int
    accepted_at_s: float
    claimed_at_s: float | None
    completed_at_s: float
    claimed_order: int | None = None

    @property
    def queue_delay_s(self) -> float | None:
        """Return time from acceptance to atomic transport claim."""

        if self.claimed_at_s is None:
            return None
        return max(0.0, self.claimed_at_s - self.accepted_at_s)


class SendReceipt(Future[SendResult]):
    """Result completed only after the physical backend accepts one message.

    A receipt represents an already accepted send, so callers cannot cancel it.
    Replacement completes with ``SUPERSEDED``; lifecycle and transport failures
    complete with the corresponding stable exception.
    """

    def __init__(
        self,
        *,
        generation: int = 0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not isinstance(generation, int) or isinstance(generation, bool):
            raise TypeError("generation must be an int")
        if generation < 0:
            raise ValueError("generation must be non-negative")
        if not callable(monotonic):
            raise TypeError("monotonic must be callable")
        super().__init__()
        self._monotonic = monotonic
        self._generation = generation
        self._accepted_at_s = monotonic()
        self._claimed_at_s: float | None = None
        self._claimed_order: int | None = None
        self._outcome: SendOutcome | None = None
        self._transition_lock = threading.RLock()
        self.set_running_or_notify_cancel()

    @property
    def generation(self) -> int:
        """Connection generation in which this send was accepted."""

        return self._generation

    @property
    def accepted_at_s(self) -> float:
        """Monotonic acceptance timestamp."""

        return self._accepted_at_s

    @property
    def claimed_at_s(self) -> float | None:
        """Monotonic atomic-claim timestamp, if the send was claimed."""

        with self._transition_lock:
            return self._claimed_at_s

    @property
    def outcome(self) -> SendOutcome | None:
        """Return the terminal outcome without raising a send failure."""

        with self._transition_lock:
            return self._outcome

    @property
    def claimed_order(self) -> int | None:
        """Process-local event order at claim, independent of clock precision."""

        with self._transition_lock:
            return self._claimed_order

    @classmethod
    def completed(
        cls,
        *,
        generation: int = 0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> SendReceipt:
        """Create a receipt for a message already sent successfully."""

        receipt = cls(generation=generation, monotonic=monotonic)
        receipt._claim()
        receipt._dispatch()
        return receipt

    @classmethod
    def failed(
        cls,
        error: BaseException,
        *,
        generation: int = 0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> SendReceipt:
        """Create a receipt completed with ``error``."""

        if not isinstance(error, BaseException):
            raise TypeError("error must be a BaseException")
        receipt = cls(generation=generation, monotonic=monotonic)
        receipt._fail(error)
        return receipt

    def _claim(self) -> bool:
        """Atomically record that replacement is no longer allowed."""

        with self._transition_lock:
            if self.done():
                return False
            if self._claimed_at_s is None:
                self._claimed_order = next_event_order()
                self._claimed_at_s = self._monotonic()
            return True

    def _dispatch(self) -> bool:
        with self._transition_lock:
            if self.done():
                return False
            now = self._monotonic()
            if self._claimed_at_s is None:
                self._claimed_order = next_event_order()
                self._claimed_at_s = now
            self._outcome = SendOutcome.DISPATCHED
            self.set_result(
                SendResult(
                    outcome=SendOutcome.DISPATCHED,
                    generation=self._generation,
                    accepted_at_s=self._accepted_at_s,
                    claimed_at_s=self._claimed_at_s,
                    completed_at_s=now,
                    claimed_order=self._claimed_order,
                )
            )
            return True

    def _supersede(self) -> bool:
        with self._transition_lock:
            if self.done() or self._claimed_at_s is not None:
                return False
            now = self._monotonic()
            self._outcome = SendOutcome.SUPERSEDED
            self.set_result(
                SendResult(
                    outcome=SendOutcome.SUPERSEDED,
                    generation=self._generation,
                    accepted_at_s=self._accepted_at_s,
                    claimed_at_s=None,
                    completed_at_s=now,
                )
            )
            return True

    def _fail(self, error: BaseException) -> bool:
        if not isinstance(error, BaseException):
            raise TypeError("error must be a BaseException")
        with self._transition_lock:
            if self.done():
                return False
            self._outcome = SendOutcome.FAILED
            self.set_exception(error)
            return True


class BatchSendReceipt(SendReceipt):
    """One atomically admitted FIFO batch, with observable physical progress.

    ``sent_frames`` includes only successful backend sends. A failure may leave
    a physically sent prefix; admission is atomic, hardware execution is not.
    """

    def __init__(self, *, frame_count: int, **kwargs) -> None:
        if type(frame_count) is not int or frame_count <= 0:
            raise ValueError("frame_count must be a positive int")
        super().__init__(**kwargs)
        self._frame_count = frame_count
        self._sent_frames = 0

    @property
    def frame_count(self) -> int:
        """Total number of frames atomically admitted in this batch."""
        return self._frame_count

    @property
    def sent_frames(self) -> int:
        with self._transition_lock:
            return self._sent_frames

    def _frame_dispatched(self) -> None:
        with self._transition_lock:
            self._sent_frames += 1

    def _set_sent_frames(self, count: int) -> None:
        """Publish a confirmed prefix, including progress before interruption."""

        with self._transition_lock:
            if (
                type(count) is not int
                or not self._sent_frames <= count <= self._frame_count
            ):
                raise ValueError("confirmed frame count must be monotonic and in range")
            self._sent_frames = count


_UNRESOLVED = object()
_SUPERSEDED = object()


class _ReceiptTracker:
    """Thread-safe ownership of receipts not yet completed by a send loop."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # None means confirmed physical success. Keep resolutions reachable
        # until publication finishes; one entry binds ownership and result.
        self._pending: dict[SendReceipt, object] = {}
        self._publishers: dict[SendReceipt, object] = {}
        self._failure_publisher: object | None = None
        # A conservative hint, not another owner/result index. Registration
        # sets it before mutation so an interrupt can only cause an extra scan.
        self._needs_failure_resolution = False

    def register(self, receipt: SendReceipt) -> None:
        with self._lock:
            self._needs_failure_resolution = True
            self._pending.setdefault(receipt, _UNRESOLVED)

    def register_failed(self, receipt: SendReceipt, error: BaseException) -> None:
        """Take cleanup ownership and bind its cause before queue removal."""

        with self._lock:
            if self._pending.get(receipt, _UNRESOLVED) is _UNRESOLVED:
                self._pending[receipt] = error

    def register_succeeded(self, receipt: SendReceipt) -> None:
        """Retain confirmed success when interrupted publication must retry."""

        with self._lock:
            if self._pending.get(receipt, _UNRESOLVED) is _UNRESOLVED:
                self._pending[receipt] = None

    def register_superseded(self, receipt: SendReceipt) -> None:
        """Retain replacement as an immutable fact before queue ownership moves."""
        with self._lock:
            if self._pending.get(receipt, _UNRESOLVED) is _UNRESOLVED:
                self._pending[receipt] = _SUPERSEDED

    def supersede(self, receipt: SendReceipt) -> None:
        self.register_superseded(receipt)
        self._publish_many((receipt,))

    def is_superseded(self, receipt: SendReceipt) -> bool:
        with self._lock:
            return self._pending.get(receipt) is _SUPERSEDED

    def has_pending(self) -> bool:
        with self._lock:
            return bool(self._pending)

    def discard(self, receipt: SendReceipt) -> None:
        with self._lock:
            self._pending.pop(receipt, None)

    def succeed(self, receipt: SendReceipt) -> None:
        self.succeed_many((receipt,))

    def succeed_many(self, receipts: tuple[SendReceipt, ...]) -> None:
        """Commit a physical batch before any synchronous completion callback."""

        with self._lock:
            self._resolve_locked(receipts, None)
        self._publish_many(receipts)

    def fail(self, receipt: SendReceipt, error: BaseException) -> None:
        with self._lock:
            self._resolve_locked((receipt,), error)
        self._publish_many((receipt,))

    def fail_all(self, error: BaseException) -> None:
        publisher = object()
        try:
            with self._lock:
                if (
                    self._failure_publisher is not None
                    and not self._needs_failure_resolution
                ):
                    return
                receipts = tuple(self._pending)
                self._resolve_locked(receipts, error)
                self._needs_failure_resolution = False
                if self._failure_publisher is not None:
                    return
                # Reentrant stop callbacks leave publication to this owner,
                # rather than recursively walking the entire remaining queue.
                self._failure_publisher = publisher
            attempted: set[SendReceipt] = set()
            interrupted: BaseException | None = None
            while receipts:
                attempted.update(receipts)
                try:
                    self._publish_many(receipts)
                except BaseException as publication_error:
                    if interrupted is None:
                        interrupted = publication_error
                with self._lock:
                    # A cancellation callback can detach another endpoint into
                    # this owner. Drain new work iteratively, but never spin on
                    # a receipt whose publication already failed this attempt.
                    receipts = tuple(
                        receipt
                        for receipt, result in self._pending.items()
                        if result is not _UNRESOLVED
                        and receipt not in attempted
                        and receipt not in self._publishers
                    )
                    if not receipts:
                        # Release in the same critical section as the final
                        # scan, so a new cancellation cannot hand off to an
                        # owner that has already decided to exit.
                        self._failure_publisher = None
            if interrupted is not None:
                raise interrupted
        finally:
            with self._lock:
                if self._failure_publisher is publisher:
                    self._failure_publisher = None

    def _resolve_locked(
        self, receipts: tuple[SendReceipt, ...], error: BaseException | None
    ) -> None:
        # Build first, then commit with one dict update. No user callbacks run
        # under the tracker lock, and already confirmed facts are immutable.
        resolutions = {
            receipt: error
            for receipt in receipts
            if self._pending.get(receipt) is _UNRESOLVED
        }
        self._pending.update(resolutions)

    def _publish_many(self, receipts: tuple[SendReceipt, ...]) -> None:
        interrupted: BaseException | None = None
        for receipt in receipts:
            try:
                self._publish(receipt)
            except BaseException as error:
                # Finish the suffix even if publication/callback is interrupted.
                # An unfinished receipt retains its resolution for a later stop.
                if interrupted is None:
                    interrupted = error
        if interrupted is not None:
            raise interrupted

    def _publish(self, receipt: SendReceipt) -> None:
        publisher = object()
        try:
            with self._lock:
                error = self._pending.get(receipt, _UNRESOLVED)
                if error is _UNRESOLVED or receipt in self._publishers:
                    return
                self._publishers[receipt] = publisher
            if not receipt.done():
                if error is _SUPERSEDED:
                    receipt._supersede()
                elif error is None:
                    receipt._dispatch()
                else:
                    assert isinstance(error, BaseException)
                    receipt._fail(error)
        finally:
            with self._lock:
                if self._publishers.get(receipt) is publisher:
                    self._publishers.pop(receipt)
                    if receipt.done():
                        self._pending.pop(receipt, None)


__all__ = ["BatchSendReceipt", "SendOutcome", "SendReceipt", "SendResult"]
