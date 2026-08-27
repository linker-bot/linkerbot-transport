"""Bounded, fair arbitration for messages sharing one physical transmitter."""

from __future__ import annotations

import math
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

from .errors import (
    ClosedError,
    DeviceNotReadyError,
    QueueFullError,
    TimeoutError,
    TransportError,
    ValidationError,
    normalize_error,
)
from .receipts import SendOutcome, SendReceipt

MessageT = TypeVar("MessageT")


class SendPolicy(str, Enum):
    """Queueing policy for one transport message."""

    LATEST = "latest"
    FIFO = "fifo"


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
    """Immutable, zero-I/O snapshot of arbiter state and counters."""

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


class TransmitArbiter(Generic[MessageT]):
    """Own one worker that serializes a shared physical transmitter.

    Latest jobs are replaced by ``(endpoint_id, command_family)`` until the
    worker atomically claims them. Discrete jobs retain FIFO order per endpoint
    and share one bounded capacity. Priority and deadline ordering are combined
    with aging and least-recently-served endpoint selection to prevent a busy
    device from permanently starving another device.
    """

    def __init__(
        self,
        dispatch: Callable[[MessageT], SendReceipt],
        *,
        latest_capacity: int = 64,
        fifo_capacity: int = 64,
        aging_quantum_s: float = 0.05,
        join_timeout_s: float = 1.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not callable(dispatch):
            raise ValidationError("dispatch must be callable")
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

        self._dispatch = dispatch
        self._latest_capacity = latest_capacity
        self._fifo_capacity = fifo_capacity
        self._aging_quantum_s = float(aging_quantum_s)
        self._join_timeout_s = float(join_timeout_s)
        self._monotonic = monotonic
        self._condition = threading.Condition(threading.RLock())
        self._latest: dict[tuple[Hashable, Hashable], _PendingSend[MessageT]] = {}
        self._fifo: dict[Hashable, deque[_PendingSend[MessageT]]] = defaultdict(deque)
        self._fifo_size = 0
        self._in_flight: _PendingSend[MessageT] | None = None
        self._worker: threading.Thread | None = None
        self._accepting = False
        self._generation = 0
        self._next_sequence = 0
        self._service_sequence = 0
        self._last_served: dict[Hashable, int] = {}
        self._accepted = 0
        self._dispatched = 0
        self._superseded = 0
        self._failed = 0
        self._rejected_full = 0

    def open(self, *, generation: int) -> None:
        """Start the single TX worker idempotently for ``generation``."""

        _validate_generation(generation)
        with self._condition:
            if self._accepting:
                if self._generation == generation:
                    return
                raise DeviceNotReadyError(
                    "transmit arbiter is already open for another generation"
                )
            if self._worker is not None and self._worker.is_alive():
                raise DeviceNotReadyError("transmit arbiter is still closing")
            self._generation = generation
            self._service_sequence = 0
            self._last_served.clear()
            self._accepting = True
            worker = threading.Thread(
                target=self._run,
                daemon=True,
                name="LinkerbotTransmitArbiter.tx",
            )
            self._worker = worker
            try:
                worker.start()
            except BaseException:
                self._worker = None
                self._accepting = False
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
            pending = self._take_endpoint_pending_locked(endpoint_id)
            self._failed += len(pending)
            self._condition.notify_all()

        for job in pending:
            job.receipt._fail(failure)

        deadline = self._monotonic() + timeout_s
        with self._condition:
            while (
                self._in_flight is not None
                and self._in_flight.options.endpoint_id == endpoint_id
            ):
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "endpoint send did not drain before cancellation timeout"
                    )
                self._condition.wait(timeout=remaining)
        return len(pending)

    def submit(self, message: MessageT, options: SendOptions) -> SendReceipt:
        """Accept one bounded send and return immediately with its receipt."""

        if not isinstance(options, SendOptions):
            raise ValidationError("options must be SendOptions")

        superseded: SendReceipt | None = None
        with self._condition:
            if not self._accepting:
                raise ClosedError("transmit arbiter is closed")
            now = self._monotonic()
            if options.deadline_at_s is not None and options.deadline_at_s <= now:
                raise TimeoutError("send deadline has already expired")
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
                and self._fifo_size >= self._fifo_capacity
            ):
                self._rejected_full += 1
                raise QueueFullError("transmit FIFO is full")

            receipt = SendReceipt(
                generation=self._generation,
                monotonic=self._monotonic,
            )
            self._next_sequence += 1
            pending = _PendingSend(
                message=message,
                options=options,
                receipt=receipt,
                sequence=self._next_sequence,
            )
            if options.policy is SendPolicy.LATEST:
                previous = self._latest.get(latest_key)
                self._latest[latest_key] = pending
                if previous is not None:
                    superseded = previous.receipt
                    self._superseded += 1
            else:
                self._fifo[options.endpoint_id].append(pending)
                self._fifo_size += 1
            self._accepted += 1
            self._condition.notify()

        if superseded is not None:
            superseded._supersede()
        return receipt

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

        if not superseded._supersede():
            raise RuntimeError("pending latest send was claimed outside arbiter lock")
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
            worker = self._worker
            return TransmitHealth(
                running=self._accepting and worker is not None and worker.is_alive(),
                generation=self._generation,
                pending_latest=len(self._latest),
                pending_fifo=self._fifo_size,
                in_flight=self._in_flight is not None,
                accepted=self._accepted,
                dispatched=self._dispatched,
                superseded=self._superseded,
                failed=self._failed,
                rejected_full=self._rejected_full,
            )

    def _stop(self, error: BaseException) -> None:
        with self._condition:
            worker = self._worker
            if not self._accepting and (worker is None or not worker.is_alive()):
                self._worker = None
                return
            self._accepting = False
            pending = self._take_all_pending_locked()
            self._failed += len(pending)
            self._condition.notify_all()

        for job in pending:
            job.receipt._fail(error)

        if worker is None or worker is threading.current_thread():
            return
        worker.join(timeout=self._join_timeout_s)
        if worker.is_alive():
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
            with self._condition:
                if unexpected is not None:
                    self._accepting = False
                    pending = self._take_all_pending_locked()
                    orphaned = self._in_flight
                    self._in_flight = None
                    failed_count = len(pending)
                    if orphaned is not None and not orphaned.receipt.done():
                        failed_count += 1
                    self._failed += failed_count
                self._condition.notify_all()
            if unexpected is not None:
                failure = TransportError(
                    f"transmit arbiter worker failed: {unexpected}", cause=unexpected
                )
                for job in pending:
                    job.receipt._fail(failure)
                if orphaned is not None:
                    orphaned.receipt._fail(failure)

    def _dispatch_claimed(self, job: _PendingSend[MessageT]) -> None:
        failure: BaseException | None = None
        dispatched = False
        try:
            deadline = job.options.deadline_at_s
            if deadline is not None and deadline <= self._monotonic():
                raise TimeoutError("send deadline expired before dispatch")
            downstream = self._dispatch(job.message)
            if not isinstance(downstream, SendReceipt):
                raise TransportError("dispatch must return SendReceipt")
            result = downstream.result()
            if result.outcome is not SendOutcome.DISPATCHED:
                raise TransportError(f"downstream send ended as {result.outcome.value}")
            dispatched = True
        except BaseException as error:  # noqa: BLE001 - receipt owns worker errors
            failure = normalize_error(error)

        if dispatched:
            job.receipt._dispatch()
        else:
            if failure is None:
                failure = TransportError("send failed without an error")
            job.receipt._fail(failure)

        with self._condition:
            if self._in_flight is job:
                self._in_flight = None
            if dispatched:
                self._dispatched += 1
            else:
                self._failed += 1
            self._condition.notify_all()

    def _claim_next_locked(self) -> _PendingSend[MessageT]:
        now = self._monotonic()
        candidates = list(self._latest.values())
        candidates.extend(queue[0] for queue in self._fifo.values() if queue)
        job = min(candidates, key=lambda candidate: self._rank(candidate, now))

        options = job.options
        if options.policy is SendPolicy.LATEST:
            key = (options.endpoint_id, options.command_family)
            del self._latest[key]
        else:
            queue = self._fifo[options.endpoint_id]
            claimed = queue.popleft()
            if claimed is not job:
                raise RuntimeError("transmit FIFO order was corrupted")
            self._fifo_size -= 1
            if not queue:
                del self._fifo[options.endpoint_id]

        if not job.receipt._claim():
            raise RuntimeError("pending send completed before transport claim")
        self._service_sequence += 1
        self._last_served[options.endpoint_id] = self._service_sequence
        self._in_flight = job
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

    def _take_all_pending_locked(self) -> tuple[_PendingSend[MessageT], ...]:
        pending = list(self._latest.values())
        self._latest.clear()
        for queue in self._fifo.values():
            pending.extend(queue)
        self._fifo.clear()
        self._fifo_size = 0
        return tuple(pending)

    def _take_endpoint_pending_locked(
        self, endpoint_id: Hashable
    ) -> tuple[_PendingSend[MessageT], ...]:
        latest_keys = tuple(key for key in self._latest if key[0] == endpoint_id)
        pending = [self._latest.pop(key) for key in latest_keys]
        fifo = self._fifo.pop(endpoint_id, None)
        if fifo is not None:
            pending.extend(fifo)
            self._fifo_size -= len(fifo)
        return tuple(pending)


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
]
