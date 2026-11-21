"""Bounded, fair arbitration for messages sharing one physical transmitter."""

from __future__ import annotations

import math
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable, Hashable, Iterable
from dataclasses import dataclass
from enum import Enum
from itertools import chain, islice
from typing import Generic, Protocol, TypeVar

from ._worker import WorkerThread
from .contracts import _MAX_SEND_BATCH_FRAMES
from .errors import (
    ClosedError,
    DeviceNotReadyError,
    QueueFullError,
    TimeoutError,
    TransportError,
    ValidationError,
    normalize_error,
)
from .receipts import BatchSendReceipt, SendOutcome, SendReceipt, _ReceiptTracker

MessageT = TypeVar("MessageT")


class _BatchDispatch(Protocol[MessageT]):
    def __call__(
        self,
        messages: tuple[MessageT, ...],
        *,
        expected_generation: int,
        on_progress: Callable[[int], None],
    ) -> BatchSendReceipt: ...


class SendPolicy(str, Enum):
    """Queueing policy for one transport message."""

    LATEST = "latest"
    FIFO = "fifo"


class TransmitMode(str, Enum):
    """Execution owner used to drain accepted transmit work."""

    WORKER = "worker"
    CALLER = "caller"


@dataclass(frozen=True, slots=True)
class SendOptions:
    """Scheduling metadata independent of any device protocol."""

    endpoint_id: Hashable
    policy: SendPolicy
    command_family: Hashable | None = None
    priority: int = 0
    deadline_at_s: float | None = None

    def __post_init__(self) -> None:
        _validate_hashable(self.endpoint_id, "endpoint_id")
        if not isinstance(self.policy, SendPolicy):
            raise ValidationError("policy must be a SendPolicy")
        if not isinstance(self.priority, int) or isinstance(self.priority, bool):
            raise ValidationError("priority must be an int")
        if self.deadline_at_s is not None and (
            not isinstance(self.deadline_at_s, (int, float))
            or isinstance(self.deadline_at_s, bool)
            or not math.isfinite(self.deadline_at_s)
            or self.deadline_at_s < 0
        ):
            raise ValidationError("deadline_at_s must be finite and non-negative")
        if self.policy is SendPolicy.LATEST:
            if self.command_family is None:
                raise ValidationError("latest sends require command_family")
            _validate_hashable(self.command_family, "command_family")
        elif self.command_family is not None:
            raise ValidationError("FIFO sends must not define command_family")


@dataclass(frozen=True, slots=True)
class TransmitHealth:
    """Immutable, zero-I/O snapshot of arbiter state and counters.

    Pending/in-flight sizes count frames (in_flight_frames is the entire
    claimed batch). Outcome/rejection counters count submissions, so one
    batch is one accepted/dispatched/failed job. Oldest age includes in-flight
    work: a blocked physical send is part of transmit latency too.
    """

    running: bool
    generation: int
    pending_latest: int
    pending_fifo: int
    in_flight: bool
    accepted: int
    dispatched: int
    superseded: int
    failed: int
    rejected_full: int
    expired: int = 0
    rejected_stale: int = 0
    oldest_pending_age_s: float | None = None
    in_flight_frames: int = 0

    @property
    def pending_total(self) -> int:
        """Return all accepted sends not yet claimed."""

        return self.pending_latest + self.pending_fifo


@dataclass(frozen=True, slots=True)
class _PendingSend(Generic[MessageT]):
    message: MessageT
    options: SendOptions
    receipt: SendReceipt
    sequence: int
    batch: tuple[MessageT, ...] | None = None

    @property
    def frame_count(self) -> int:
        return 1 if self.batch is None else len(self.batch)


class TransmitArbiter(Generic[MessageT]):
    """Serialize a shared physical transmitter with bounded scheduling.

    Latest jobs are replaced by ``(endpoint_id, command_family)`` until the
    dispatcher atomically claims them. Discrete jobs retain FIFO order per
    endpoint and share one bounded capacity. Priority and deadline ordering are
    combined with aging and least-recently-served endpoint selection to prevent
    a busy device from permanently starving another device.

    ``TransmitMode.WORKER`` preserves immediate-return submission with one
    dedicated worker. ``TransmitMode.CALLER`` is intended for runtimes that
    already own I/O actors: submission remains non-blocking, and one actor calls
    :meth:`drain` to cooperatively empty the shared queue. Concurrent actors can
    still enqueue and replace unclaimed latest work while a drain is active.

    ``dispatch_batch`` explicitly enables blocks of at most 32 frames for FIFO
    batches. Use a managed transport's ``send_batch`` and open the arbiter with
    that transport's generation. A custom callback accepts ``messages``,
    ``expected_generation`` and ``on_progress`` keywords, and returns a
    ``BatchSendReceipt``. It reports cumulative confirmed progress within that
    block before publishing the receipt; process-interrupt retries may repeat
    the last count. Single messages keep the ordinary ``dispatch`` path.
    """

    def __init__(
        self,
        dispatch: Callable[[MessageT], SendReceipt],
        *,
        dispatch_batch: _BatchDispatch[MessageT] | None = None,
        latest_capacity: int = 64,
        fifo_capacity: int = 64,
        aging_quantum_s: float = 0.05,
        join_timeout_s: float = 1.0,
        monotonic: Callable[[], float] = time.monotonic,
        mode: TransmitMode = TransmitMode.WORKER,
    ) -> None:
        if not callable(dispatch):
            raise ValidationError("dispatch must be callable")
        if dispatch_batch is not None and not callable(dispatch_batch):
            raise ValidationError("dispatch_batch must be callable")
        if (
            not isinstance(latest_capacity, int)
            or isinstance(latest_capacity, bool)
            or latest_capacity <= 0
        ):
            raise ValidationError("latest_capacity must be a positive int")
        if (
            not isinstance(fifo_capacity, int)
            or isinstance(fifo_capacity, bool)
            or fifo_capacity <= 0
        ):
            raise ValidationError("fifo_capacity must be a positive int")
        _validate_positive_float(aging_quantum_s, "aging_quantum_s")
        _validate_positive_float(join_timeout_s, "join_timeout_s")
        if not callable(monotonic):
            raise ValidationError("monotonic must be callable")
        if not isinstance(mode, TransmitMode):
            raise ValidationError("mode must be a TransmitMode")

        self._dispatch = dispatch
        self._dispatch_batch = dispatch_batch
        self._latest_capacity = latest_capacity
        self._fifo_capacity = fifo_capacity
        self._aging_quantum_s = float(aging_quantum_s)
        self._join_timeout_s = float(join_timeout_s)
        self._monotonic = monotonic
        self._mode = mode
        self._condition = threading.Condition(threading.RLock())
        self._cleanup_receipts = _ReceiptTracker()
        self._latest: dict[tuple[Hashable, Hashable], _PendingSend[MessageT]] = {}
        # Derived membership cache only. Invalidate before changing queue
        # membership, then publish the updated cache after both mutations.
        # An interrupt leaves None, so the next lookup rebuilds from _latest.
        self._latest_endpoint_counts: dict[Hashable, int] | None = {}
        self._fifo: dict[Hashable, deque[_PendingSend[MessageT]]] = defaultdict(deque)
        self._fifo_size = 0
        self._in_flight: _PendingSend[MessageT] | None = None
        self._worker: WorkerThread | None = None
        self._draining_thread_id: int | None = None
        self._accepting = False
        self._generation = 0
        self._next_sequence = 0
        self._service_sequence = 0
        self._last_served: dict[Hashable, int] = {}
        self._accepted = 0
        self._superseded = 0
        # Dispatched, failed and the last accounted job sequence are committed
        # together, so a process interrupt cannot separate counting from its
        # cleanup marker. Keep only the sequence, never an idle endpoint owner.
        self._completion_counts = (0, 0, 0)
        self._rejected_full = 0
        self._expired = 0
        self._rejected_stale = 0

    def open(self, *, generation: int) -> None:
        """Open one worker-backed or caller-drained generation idempotently."""

        _validate_generation(generation)
        with self._condition:
            if self._accepting:
                if self._generation == generation:
                    return
                raise DeviceNotReadyError(
                    "transmit arbiter is already open for another generation"
                )
            if self._worker is not None and not self._worker.exited.is_set():
                raise DeviceNotReadyError("transmit arbiter is still closing")
            if self._draining_thread_id is not None:
                raise DeviceNotReadyError("transmit arbiter is still draining")
            if self._cleanup_receipts.has_pending():
                raise DeviceNotReadyError(
                    "transmit arbiter has interrupted receipt cleanup; retry close()"
                )
            self._generation = generation
            self._service_sequence = 0
            self._last_served.clear()
            self._accepting = True
            if self._mode is TransmitMode.CALLER:
                return
            worker = WorkerThread(
                target=self._run,
                daemon=True,
                name="LinkerbotTransmitArbiter.tx",
            )
            self._worker = worker
            try:
                worker.start()
            except BaseException:
                # start() can raise after OS creation, even before ident is
                # visible. Keep the exit responsibility and forbid reopening.
                self._accepting = False
                self._condition.notify_all()
                raise

    def close(self) -> None:
        """Reject new work, fail every unclaimed send and boundedly stop."""

        self._stop(ClosedError("transmit arbiter closed before dispatch"))

    def abort(self, error: BaseException) -> None:
        """Stop after a connection failure without replaying pending work."""

        if not isinstance(error, BaseException):
            raise ValidationError("error must be a BaseException")
        self._stop(normalize_error(error))

    def cancel_endpoint(
        self,
        endpoint_id: Hashable,
        error: BaseException,
        *,
        timeout_s: float | None = None,
    ) -> int:
        """Cancel pending work for one endpoint and drain its claimed send.

        The caller must close its endpoint admission gate before calling this
        method. Jobs that have not been claimed are failed with ``error``.
        A job already claimed by the worker cannot be cancelled honestly, so
        this method waits boundedly for it to finish. Work owned by every other
        endpoint remains queued and dispatchable.

        Args:
            endpoint_id: Endpoint whose pending work must be removed.
            error: Stable failure stored in each cancelled send receipt.
            timeout_s: Maximum time to wait for a claimed send. Defaults to the
                arbiter's configured worker join timeout.

        Returns:
            The number of unclaimed send receipts cancelled.

        Raises:
            TimeoutError: If this endpoint's claimed send does not finish in
                time.
            ValidationError: If an argument is invalid.
        """

        _validate_hashable(endpoint_id, "endpoint_id")
        if not isinstance(error, BaseException):
            raise ValidationError("error must be a BaseException")
        if timeout_s is None:
            timeout_s = self._join_timeout_s
        else:
            _validate_positive_float(timeout_s, "timeout_s")
            timeout_s = float(timeout_s)
        failure = normalize_error(error)

        with self._condition:
            pending = self._take_endpoint_pending_locked(endpoint_id, failure)
            self._add_failed_locked(len(pending))
            self._condition.notify_all()

        self._cleanup_receipts.fail_all(failure)

        deadline = self._monotonic() + timeout_s
        with self._condition:
            while (
                self._in_flight is not None
                and self._in_flight.options.endpoint_id == endpoint_id
                and not self._in_flight.receipt.done()
            ):
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "endpoint send did not drain; transmit owner did not stop in time"
                    )
                self._condition.wait(timeout=remaining)
        return len(pending)

    def submit(self, message: MessageT, options: SendOptions) -> SendReceipt:
        """Accept one bounded send and return immediately with its receipt."""

        return self._submit(message, options)

    def submit_fifo_batch(
        self,
        messages: Iterable[MessageT],
        *,
        endpoint_id: Hashable,
        priority: int = 0,
        deadline_at_s: float | None = None,
        max_pending_age_s: float | None = None,
    ) -> BatchSendReceipt:
        """Admit all frames or none, then dispatch them without interleaving.

        Capacity counts frames. The deadline is checked before the first frame
        only; an already-started batch is never truncated by its deadline.
        A backend failure stops the batch and the receipt exposes its sent prefix.
        Optional admission age includes the oldest queued or in-flight work.
        """
        batch = tuple(islice(messages, self._fifo_capacity + 1))
        if not batch:
            raise ValidationError("a FIFO batch must contain at least one frame")
        if max_pending_age_s is not None:
            _validate_positive_float(max_pending_age_s, "max_pending_age_s")
        options = SendOptions(
            endpoint_id=endpoint_id,
            policy=SendPolicy.FIFO,
            priority=priority,
            deadline_at_s=deadline_at_s,
        )
        receipt = self._submit(
            batch[0], options, batch=batch, max_pending_age_s=max_pending_age_s
        )
        assert isinstance(receipt, BatchSendReceipt)
        return receipt

    def _submit(
        self,
        message: MessageT,
        options: SendOptions,
        *,
        batch: tuple[MessageT, ...] | None = None,
        max_pending_age_s: float | None = None,
    ) -> SendReceipt:

        if not isinstance(options, SendOptions):
            raise ValidationError("options must be SendOptions")

        superseded: SendReceipt | None = None
        with self._condition:
            if not self._accepting:
                raise ClosedError("transmit arbiter is closed")
            now = self._monotonic()
            if options.deadline_at_s is not None and options.deadline_at_s <= now:
                raise TimeoutError("send deadline has already expired")
            oldest = (
                self._oldest_accepted_locked()
                if max_pending_age_s is not None
                else None
            )
            if (
                max_pending_age_s is not None
                and oldest is not None
                and now - oldest >= max_pending_age_s
            ):
                self._rejected_stale += 1
                raise TimeoutError("pending transmit work exceeded the age budget")
            frame_count = 1 if batch is None else len(batch)
            latest_key = (options.endpoint_id, options.command_family)
            if (
                options.policy is SendPolicy.LATEST
                and latest_key not in self._latest
                and len(self._latest) >= self._latest_capacity
            ):
                self._rejected_full += 1
                raise QueueFullError("transmit latest slots are full")
            if (
                options.policy is SendPolicy.FIFO
                and self._fifo_size + frame_count > self._fifo_capacity
            ):
                self._rejected_full += 1
                raise QueueFullError("transmit FIFO is full")

            receipt = (
                SendReceipt(generation=self._generation, monotonic=self._monotonic)
                if batch is None
                else BatchSendReceipt(
                    frame_count=frame_count,
                    generation=self._generation,
                    monotonic=self._monotonic,
                )
            )
            self._next_sequence += 1
            pending = _PendingSend(
                message=message,
                options=options,
                receipt=receipt,
                sequence=self._next_sequence,
                batch=batch,
            )
            if options.policy is SendPolicy.LATEST:
                previous = self._latest.get(latest_key)
                if previous is None:
                    counts = self._latest_endpoint_counts
                    self._latest_endpoint_counts = None
                    self._latest[latest_key] = pending
                    if counts is not None:
                        endpoint = options.endpoint_id
                        counts[endpoint] = counts.get(endpoint, 0) + 1
                    self._latest_endpoint_counts = counts
                else:
                    self._cleanup_receipts.register_superseded(previous.receipt)
                    self._latest[latest_key] = pending
                    superseded = previous.receipt
                    self._superseded += 1
            else:
                self._fifo[options.endpoint_id].append(pending)
                self._fifo_size += frame_count
            self._accepted += 1
            self._condition.notify()

        if superseded is not None:
            self._cleanup_receipts.supersede(superseded)
        return receipt

    def drain(self) -> None:
        """Cooperatively dispatch queued work in caller mode.

        The first caller becomes the temporary drain owner. Concurrent calls
        return immediately because that owner also observes work admitted while
        it is active. Worker mode already has a permanent owner, so calling this
        method there is a configuration error.

        Process interrupts stop admission, fail the unsent work and propagate
        to the caller. Already dispatched batch frames are never replayed.
        """

        if self._mode is not TransmitMode.CALLER:
            raise DeviceNotReadyError("drain is only available in caller mode")
        current_thread_id = threading.get_ident()
        with self._condition:
            if not self._accepting:
                raise ClosedError("transmit arbiter is closed")
            if self._draining_thread_id is not None or not self._has_pending_locked():
                return
            self._draining_thread_id = current_thread_id
        self._drain_on_caller(current_thread_id)

    def submit_latest(
        self,
        message: MessageT,
        *,
        endpoint_id: Hashable,
        command_family: Hashable,
        priority: int = 0,
        deadline_at_s: float | None = None,
    ) -> SendReceipt:
        """Submit a replaceable continuous target."""

        return self.submit(
            message,
            SendOptions(
                endpoint_id=endpoint_id,
                policy=SendPolicy.LATEST,
                command_family=command_family,
                priority=priority,
                deadline_at_s=deadline_at_s,
            ),
        )

    def replace_latest_if_pending(
        self,
        message: MessageT,
        *,
        endpoint_id: Hashable,
        command_family: Hashable,
        priority: int = 0,
        deadline_at_s: float | None = None,
    ) -> SendReceipt | None:
        """Atomically replace an existing unclaimed keyed-latest send.

        Unlike :meth:`submit_latest`, this method never creates a second pending
        job after the prior job has been claimed. This lets a protocol owner
        update a queued target without allowing a later request to overtake an
        in-flight acknowledgement transaction.

        Returns:
            The replacement receipt when the keyed job was still pending, or
            ``None`` when no replaceable job exists.
        """

        options = SendOptions(
            endpoint_id=endpoint_id,
            policy=SendPolicy.LATEST,
            command_family=command_family,
            priority=priority,
            deadline_at_s=deadline_at_s,
        )
        superseded: SendReceipt
        with self._condition:
            if not self._accepting:
                raise ClosedError("transmit arbiter is closed")
            now = self._monotonic()
            if options.deadline_at_s is not None and options.deadline_at_s <= now:
                raise TimeoutError("send deadline has already expired")
            latest_key = (options.endpoint_id, options.command_family)
            previous = self._latest.get(latest_key)
            if previous is None:
                return None

            receipt = SendReceipt(
                generation=self._generation,
                monotonic=self._monotonic,
            )
            self._next_sequence += 1
            self._cleanup_receipts.register_superseded(previous.receipt)
            self._latest[latest_key] = _PendingSend(
                message=message,
                options=options,
                receipt=receipt,
                sequence=self._next_sequence,
            )
            superseded = previous.receipt
            self._accepted += 1
            self._superseded += 1
            self._condition.notify()

        self._cleanup_receipts.supersede(superseded)
        return receipt

    def submit_fifo(
        self,
        message: MessageT,
        *,
        endpoint_id: Hashable,
        priority: int = 0,
        deadline_at_s: float | None = None,
    ) -> SendReceipt:
        """Submit a non-replaceable discrete operation."""

        return self.submit(
            message,
            SendOptions(
                endpoint_id=endpoint_id,
                policy=SendPolicy.FIFO,
                priority=priority,
                deadline_at_s=deadline_at_s,
            ),
        )

    def get_health(self) -> TransmitHealth:
        """Return current queue state without transport I/O."""

        with self._condition:
            oldest = self._oldest_accepted_locked()
            worker = self._worker
            in_flight = self._in_flight
            if in_flight is not None and in_flight.receipt.done():
                in_flight = None
            running = self._accepting and (
                self._mode is TransmitMode.CALLER
                or (worker is not None and not worker.exited.is_set())
            )
            return TransmitHealth(
                running=running,
                generation=self._generation,
                pending_latest=len(self._latest),
                pending_fifo=self._fifo_size,
                in_flight=in_flight is not None,
                accepted=self._accepted,
                dispatched=self._completion_counts[0],
                superseded=self._superseded,
                failed=self._completion_counts[1],
                rejected_full=self._rejected_full,
                expired=self._expired,
                rejected_stale=self._rejected_stale,
                oldest_pending_age_s=(
                    None if oldest is None else max(0.0, self._monotonic() - oldest)
                ),
                in_flight_frames=(0 if in_flight is None else in_flight.frame_count),
            )

    def _stop(self, error: BaseException) -> None:
        with self._condition:
            worker = self._worker
            draining_thread_id = self._draining_thread_id
            if (
                not self._accepting
                and (worker is None or worker.exited.is_set())
                and draining_thread_id is None
                and not self._has_pending_locked()
                and not self._cleanup_receipts.has_pending()
            ):
                self._worker = None
                return
            self._accepting = False
            pending = self._take_all_pending_locked(error)
            self._add_failed_locked(len(pending))
            self._condition.notify_all()

        self._cleanup_receipts.fail_all(error)

        if self._mode is TransmitMode.CALLER:
            if draining_thread_id == threading.get_ident():
                return
            deadline = self._monotonic() + self._join_timeout_s
            with self._condition:
                while self._draining_thread_id is not None:
                    remaining = deadline - self._monotonic()
                    if remaining <= 0:
                        raise TimeoutError(
                            "transmit arbiter caller did not stop draining in time"
                        )
                    self._condition.wait(timeout=remaining)
            return

        if worker is None or worker is threading.current_thread():
            return
        if not worker.exited.wait(timeout=self._join_timeout_s):
            raise TimeoutError("transmit arbiter worker did not stop in time")
        with self._condition:
            if self._worker is worker:
                self._worker = None

    def _run(self) -> None:
        unexpected: BaseException | None = None
        pending: tuple[_PendingSend[MessageT], ...] = ()
        orphaned: _PendingSend[MessageT] | None = None
        try:
            while True:
                with self._condition:
                    while self._accepting and not self._has_pending_locked():
                        self._condition.wait()
                    if not self._accepting:
                        return
                    job = self._claim_next_locked()

                self._dispatch_claimed(job)
        except BaseException as error:  # noqa: BLE001 - worker boundary must clean up
            unexpected = error
        finally:
            if unexpected is not None:
                failure = TransportError(
                    f"transmit arbiter worker failed: {unexpected}", cause=unexpected
                )
            with self._condition:
                if unexpected is not None:
                    self._accepting = False
                    pending = self._take_all_pending_locked(failure)
                    orphaned = self._in_flight
                    if orphaned is not None:
                        self._cleanup_receipts.register_failed(
                            orphaned.receipt, failure
                        )
                    self._in_flight = None
                    failed_count = len(pending)
                    if (
                        orphaned is not None
                        and not orphaned.receipt.done()
                        and orphaned.sequence != self._completion_counts[2]
                    ):
                        failed_count += 1
                    self._add_failed_locked(failed_count)
                self._condition.notify_all()
            if unexpected is not None:
                # The worker failure is already observable in these receipts.
                # A cleanup callback cannot replace it or orphan the suffix.
                self._fail_detached(failure)

    def _drain_on_caller(self, current_thread_id: int) -> None:
        """Drain cooperatively without adding another persistent I/O thread."""

        try:
            while True:
                with self._condition:
                    if not self._accepting or not self._has_pending_locked():
                        if self._draining_thread_id == current_thread_id:
                            self._draining_thread_id = None
                        self._condition.notify_all()
                        return
                    job = self._claim_next_locked()
                self._dispatch_claimed(job)
        except BaseException as error:  # noqa: BLE001 - scheduler boundary cleanup
            failure = TransportError(
                f"transmit arbiter caller failed: {error}", cause=error
            )
            with self._condition:
                self._accepting = False
                pending = self._take_all_pending_locked(failure)
                orphaned = self._in_flight
                if orphaned is not None:
                    self._cleanup_receipts.register_failed(orphaned.receipt, failure)
                self._in_flight = None
                failed_count = len(pending)
                if (
                    orphaned is not None
                    and not orphaned.receipt.done()
                    and orphaned.sequence != self._completion_counts[2]
                ):
                    failed_count += 1
                self._add_failed_locked(failed_count)
                if self._draining_thread_id == current_thread_id:
                    self._draining_thread_id = None
                self._condition.notify_all()
            callback_error = self._fail_detached(failure)
            if not isinstance(error, Exception):
                raise
            if callback_error is not None:
                raise callback_error from error

    def _fail_detached(self, error: BaseException) -> BaseException | None:
        try:
            self._cleanup_receipts.fail_all(error)
        except BaseException as publication_error:
            # The tracker retains any receipt interrupted before it became
            # terminal; close() can retry without replacing its recorded cause.
            return publication_error
        return None

    def _dispatch_claimed(self, job: _PendingSend[MessageT]) -> None:
        # Replacement may have committed its outcome just before a caller
        # interrupt prevented the queue-slot update. Such a job must never
        # become physical work when a worker later encounters the old slot.
        if (
            job.options.policy is SendPolicy.LATEST
            and self._cleanup_receipts.is_superseded(job.receipt)
        ):
            try:
                self._cleanup_receipts.supersede(job.receipt)
            finally:
                with self._condition:
                    if job.receipt.done() and self._in_flight is job:
                        self._in_flight = None
                    self._condition.notify_all()
            return
        failure: BaseException | None = None
        interrupted: BaseException | None = None
        dispatched = False
        try:
            try:
                deadline = job.options.deadline_at_s
                if deadline is not None and deadline <= self._monotonic():
                    with self._condition:
                        self._expired += 1
                    raise TimeoutError("send deadline expired before dispatch")
                messages = (job.message,) if job.batch is None else job.batch
                batch_dispatch = self._dispatch_batch if len(messages) > 1 else None
                block_size = 1 if batch_dispatch is None else _MAX_SEND_BATCH_FRAMES
                for index in range(0, len(messages), block_size):
                    # Close can cancel the unsent suffix; it must not replay on open.
                    if index:
                        with self._condition:
                            if not self._accepting:
                                raise ClosedError(
                                    "transmit closed during batch dispatch"
                                )
                    if batch_dispatch is None:
                        downstream = self._dispatch(messages[index])
                    else:
                        assert isinstance(job.receipt, BatchSendReceipt)
                        block = messages[index : index + block_size]

                        def record_progress(
                            count: int, *, offset: int = index, size: int = len(block)
                        ) -> None:
                            if type(count) is not int or not 0 <= count <= size:
                                raise ValidationError("invalid batch dispatch progress")
                            job.receipt._set_sent_frames(offset + count)
                            if count < size:
                                with self._condition:
                                    if not self._accepting:
                                        raise ClosedError(
                                            "transmit closed during batch dispatch"
                                        )

                        downstream = batch_dispatch(
                            block,
                            expected_generation=job.receipt.generation,
                            on_progress=record_progress,
                        )
                    if not isinstance(downstream, SendReceipt):
                        raise TransportError("dispatch must return SendReceipt")
                    if batch_dispatch is not None and (
                        not isinstance(downstream, BatchSendReceipt)
                        or downstream.frame_count != len(block)
                        or downstream.generation != job.receipt.generation
                    ):
                        raise TransportError(
                            "batch dispatch returned a mismatched receipt"
                        )
                    try:
                        result = downstream.result()
                    finally:
                        if batch_dispatch is not None:
                            job.receipt._set_sent_frames(index + downstream.sent_frames)
                    if result.outcome is not SendOutcome.DISPATCHED:
                        raise TransportError(
                            f"downstream send ended as {result.outcome.value}"
                        )
                    if batch_dispatch is not None:
                        if job.receipt.sent_frames != index + len(block):
                            raise TransportError(
                                "batch dispatch did not confirm all frames"
                            )
                    elif isinstance(job.receipt, BatchSendReceipt):
                        job.receipt._frame_dispatched()
                dispatched = True
            except BaseException as error:
                if isinstance(error, Exception):
                    # Full progress cannot override a failed or invalid receipt.
                    failure = normalize_error(error)
                elif (
                    isinstance(job.receipt, BatchSendReceipt)
                    and job.receipt.sent_frames == job.frame_count
                ):
                    # A process interrupt after final confirmation must preserve
                    # success, propagate unchanged and stop later queued work.
                    dispatched = True
                    interrupted = error
                else:
                    raise

            # Record physical completion before synchronous receipt callbacks.
            # Keep the claimed job until publication is terminal, so interrupted
            # completion accounting and publication can both retain its result.
            with self._condition:
                self._record_completion_locked(job, dispatched=dispatched)

            if dispatched:
                job.receipt._dispatch()
            else:
                if failure is None:
                    failure = TransportError("send failed without an error")
                job.receipt._fail(failure)
        except BaseException:
            if dispatched and isinstance(job.receipt, BatchSendReceipt):
                with self._condition:
                    # Counting is idempotent if the first commit was interrupted.
                    # Cleanup must publish success even if publication is
                    # interrupted again and close() has to retry it later.
                    self._record_completion_locked(job, dispatched=True)
                    self._cleanup_receipts.register_succeeded(job.receipt)
            if interrupted is not None:
                raise interrupted
            raise
        finally:
            # If publication itself was interrupted before becoming terminal,
            # leave ownership to _run() / _drain_on_caller() cleanup.
            with self._condition:
                if job.receipt.done():
                    if self._in_flight is job:
                        self._in_flight = None
                    self._forget_idle_endpoint_locked(job.options.endpoint_id)
                self._condition.notify_all()

        if interrupted is not None:
            raise interrupted

    def _record_completion_locked(
        self, job: _PendingSend[MessageT], *, dispatched: bool
    ) -> None:
        sent_count, failed_count, sequence = self._completion_counts
        if sequence != job.sequence:
            self._completion_counts = (
                sent_count + int(dispatched),
                failed_count + int(not dispatched),
                job.sequence,
            )
        self._condition.notify_all()

    def _add_failed_locked(self, count: int) -> None:
        sent_count, failed_count, sequence = self._completion_counts
        self._completion_counts = (sent_count, failed_count + count, sequence)

    def _claim_next_locked(self) -> _PendingSend[MessageT]:
        now = self._monotonic()
        candidates = chain(
            self._latest.values(), (queue[0] for queue in self._fifo.values() if queue)
        )
        job = next(candidates)
        best_rank = self._rank(job, now)
        for candidate in candidates:
            rank = self._rank(candidate, now)
            if rank < best_rank:
                job, best_rank = candidate, rank

        # Transfer cleanup ownership before removing the job from its queue.
        # An interrupt during removal must leave the receipt reachable.
        self._in_flight = job
        options = job.options
        if options.policy is SendPolicy.LATEST:
            key = (options.endpoint_id, options.command_family)
            counts = self._latest_endpoint_counts
            self._latest_endpoint_counts = None
            del self._latest[key]
            if counts is not None:
                remaining = counts[options.endpoint_id] - 1
                if remaining:
                    counts[options.endpoint_id] = remaining
                else:
                    del counts[options.endpoint_id]
            self._latest_endpoint_counts = counts
        else:
            queue = self._fifo[options.endpoint_id]
            claimed = queue.popleft()
            if claimed is not job:
                raise RuntimeError("transmit FIFO order was corrupted")
            self._fifo_size -= job.frame_count
            if not queue:
                del self._fifo[options.endpoint_id]

        if options.policy is SendPolicy.LATEST and self._cleanup_receipts.is_superseded(
            job.receipt
        ):
            return job
        if not job.receipt._claim():
            raise RuntimeError("pending send completed before transport claim")
        self._service_sequence += 1
        self._last_served[options.endpoint_id] = self._service_sequence
        return job

    def _rank(self, job: _PendingSend[MessageT], now: float) -> tuple[float, ...]:
        age_s = max(0.0, now - job.receipt.accepted_at_s)
        aged_priority = job.options.priority + int(age_s / self._aging_quantum_s)
        deadline = (
            math.inf if job.options.deadline_at_s is None else job.options.deadline_at_s
        )
        last_served = self._last_served.get(job.options.endpoint_id, -1)
        return (-aged_priority, deadline, last_served, job.sequence)

    def _has_pending_locked(self) -> bool:
        return bool(self._latest) or self._fifo_size > 0

    def _oldest_accepted_locked(self) -> float | None:
        heads = list(self._latest.values())
        heads.extend(queue[0] for queue in self._fifo.values() if queue)
        if self._in_flight is not None and not self._in_flight.receipt.done():
            heads.append(self._in_flight)
        return min((job.receipt.accepted_at_s for job in heads), default=None)

    def _take_all_pending_locked(
        self, error: BaseException
    ) -> tuple[_PendingSend[MessageT], ...]:
        pending = [job for job in self._latest.values() if job is not self._in_flight]
        for queue in self._fifo.values():
            pending.extend(job for job in queue if job is not self._in_flight)
        for job in pending:
            self._cleanup_receipts.register_failed(job.receipt, error)
        self._latest_endpoint_counts = None
        self._latest.clear()
        self._latest_endpoint_counts = {}
        self._fifo.clear()
        self._fifo_size = 0
        self._last_served.clear()
        return tuple(pending)

    def _take_endpoint_pending_locked(
        self, endpoint_id: Hashable, error: BaseException
    ) -> tuple[_PendingSend[MessageT], ...]:
        latest_keys = tuple(key for key in self._latest if key[0] == endpoint_id)
        pending = [self._latest[key] for key in latest_keys]
        fifo = self._fifo.get(endpoint_id)
        if fifo is not None:
            pending.extend(fifo)
        for job in pending:
            self._cleanup_receipts.register_failed(job.receipt, error)
        counts = self._latest_endpoint_counts
        self._latest_endpoint_counts = None
        for key in latest_keys:
            del self._latest[key]
            # Only removed jobs change membership; a non-reflexive endpoint
            # can match no latest keys despite being present in the cache.
            if counts is not None:
                endpoint = key[0]
                remaining = counts[endpoint] - 1
                if remaining:
                    counts[endpoint] = remaining
                else:
                    del counts[endpoint]
        self._latest_endpoint_counts = counts
        if fifo is not None:
            self._fifo.pop(endpoint_id)
            self._fifo_size -= sum(job.frame_count for job in fifo)
        self._forget_idle_endpoint_locked(endpoint_id)
        return tuple(pending)

    def _forget_idle_endpoint_locked(self, endpoint_id: Hashable) -> None:
        if endpoint_id in self._fifo:
            return
        counts = self._latest_endpoint_counts
        if counts is None:
            counts = {}
            for endpoint, _ in self._latest:
                counts[endpoint] = counts.get(endpoint, 0) + 1
            self._latest_endpoint_counts = counts
        if endpoint_id in counts:
            return
        if (
            self._in_flight is not None
            and self._in_flight.options.endpoint_id == endpoint_id
        ):
            return
        self._last_served.pop(endpoint_id, None)


def _validate_hashable(value: Hashable, name: str) -> None:
    if value is None:
        raise ValidationError(f"{name} must not be None")
    try:
        hash(value)
    except TypeError as error:
        raise ValidationError(f"{name} must be hashable") from error


def _validate_positive_float(value: float, name: str) -> None:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValidationError(f"{name} must be finite and positive")


def _validate_generation(generation: int) -> None:
    if not isinstance(generation, int) or isinstance(generation, bool):
        raise ValidationError("generation must be an int")
    if generation < 0:
        raise ValidationError("generation must be non-negative")


__all__ = [
    "SendOptions",
    "SendPolicy",
    "TransmitArbiter",
    "TransmitHealth",
    "TransmitMode",
]
