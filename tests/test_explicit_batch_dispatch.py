"""Explicit batches preserve physical progress, generation and shutdown bounds."""

import inspect
import sys
import threading
from types import SimpleNamespace

import pytest

from linkerbot_transport import (
    BatchSendReceipt,
    ClosedError,
    DeviceNotReadyError,
    SendOutcome,
    SendReceipt,
    TimeoutError,
    TransmitArbiter,
    TransmitMode,
    TransportError,
    TransportState,
    ValidationError,
)
from linkerbot_transport._managed import ManagedMessageTransport
from linkerbot_transport.canfd import CANFDMessage, CANFDTransport
from linkerbot_transport.canfd.dispatcher import CANFDMessageDispatcher
from linkerbot_transport.testing import FakeTransport


class Channel:
    def __init__(self):
        self.calls = []
        self.sent = []
        self.errors = {}
        self.after_send = None
        self.closed = threading.Event()
        self.release = threading.Event()

    def _send(self, messages):
        self.calls.append([message.arbitration_id for message in messages])
        error = self.errors.get(len(self.calls))
        if error is not None:
            raise error
        self.sent.extend(messages)
        if self.after_send is not None:
            self.after_send(messages)

    def send(self, message):
        self._send([message])

    def receive(self, *, max_frames, timeout_ms):
        self.closed.wait(timeout_ms / 1000)
        return []

    def close(self):
        self.release.set()
        self.closed.set()


class NativeChannel(Channel):
    def send_batch(self, messages):
        self._send(messages)


@pytest.fixture
def runtime_factory():
    runtimes = []

    def create(*, native=True, mode=TransmitMode.CALLER, clock=None):
        channels, dispatchers = [], []

        def factory(on_error):
            channel = NativeChannel() if native else Channel()
            channels.append(channel)
            dispatcher = CANFDMessageDispatcher(
                interface=channel,
                on_bus_error=on_error,
                max_consecutive_errors=2,
                start_send_worker=False,
            )
            dispatchers.append(dispatcher)
            return dispatcher

        options = {} if clock is None else {"monotonic": clock}
        transport = CANFDTransport(dispatcher_factory=factory, **options)
        transport.open()
        arbiter = TransmitArbiter(
            transport.send,
            dispatch_batch=transport.send_batch,
            fifo_capacity=128,
            mode=mode,
            **options,
        )
        arbiter.open(generation=1)
        runtime = SimpleNamespace(
            transport=transport,
            arbiter=arbiter,
            channel=channels[0],
            channels=channels,
            dispatchers=dispatchers,
        )
        runtimes.append(runtime)
        return runtime

    yield create
    for runtime in runtimes:
        for channel in runtime.channels:
            channel.release.set()
        runtime.arbiter.close()
        runtime.transport.close()
        for dispatcher in runtime.dispatchers:
            assert not dispatcher._recv_thread.is_alive()
            assert not dispatcher._send_thread.is_alive()


def frames(count):
    return [CANFDMessage(index + 1, b"abcd") for index in range(count)]


@pytest.mark.parametrize("mode", [TransmitMode.CALLER, TransmitMode.WORKER])
@pytest.mark.parametrize("count", [1, 3, 17, 32, 33, 51, 64, 65])
def test_native_batches_bound_calls_preserve_order_and_health(
    runtime_factory, mode, count
):
    runtime = runtime_factory(mode=mode)
    messages = frames(count)
    receipt = runtime.arbiter.submit_fifo_batch(messages, endpoint_id="arm")
    if mode is TransmitMode.CALLER:
        runtime.arbiter.drain()
    assert receipt.result(2).outcome is SendOutcome.DISPATCHED
    assert receipt.sent_frames == count
    assert runtime.channel.sent == messages
    assert [len(call) for call in runtime.channel.calls] == [
        min(32, count - offset) for offset in range(0, count, 32)
    ]
    health = runtime.transport.get_health()
    assert health.tx_frames == count
    assert health.tx_bytes == count * 4
    assert health.consecutive_errors == 0
    assert runtime.arbiter.get_health().dispatched == 1
    assert all(
        dispatcher._send_thread.ident is None for dispatcher in runtime.dispatchers
    )


@pytest.mark.parametrize("native", [False, True])
def test_failure_keeps_only_confirmed_prefix_and_never_replays(runtime_factory, native):
    runtime = runtime_factory(native=native)
    failure = TransportError("short send; physical result of this call is unknown")
    runtime.channel.errors[2 if native else 35] = failure
    receipt = runtime.arbiter.submit_fifo_batch(frames(51), endpoint_id="arm")
    runtime.arbiter.drain()
    assert receipt.exception(0) is failure
    assert receipt.sent_frames == (32 if native else 34)
    assert len(runtime.channel.calls) == (2 if native else 35)
    assert runtime.transport.get_health().tx_frames == receipt.sent_frames
    assert runtime.transport.get_health().tx_bytes == receipt.sent_frames * 4
    assert runtime.transport.get_health().state is TransportState.OPEN
    assert runtime.transport.get_health().consecutive_errors == 1
    following = runtime.arbiter.submit_fifo_batch(frames(3), endpoint_id="other")
    runtime.arbiter.drain()
    assert following.result(0).outcome is SendOutcome.DISPATCHED
    assert following.sent_frames == 3
    assert runtime.transport.get_health().consecutive_errors == 0


@pytest.mark.parametrize("native", [False, True])
def test_stop_during_physical_send_preserves_current_unit_and_cancels_suffix(
    runtime_factory, native
):
    runtime = runtime_factory(native=native)
    runtime.channel.after_send = lambda _: runtime.arbiter.close()
    receipt = runtime.arbiter.submit_fifo_batch(frames(51), endpoint_id="arm")
    runtime.arbiter.drain()
    assert isinstance(receipt.exception(0), ClosedError)
    assert receipt.sent_frames == (32 if native else 1)
    assert len(runtime.channel.calls) == 1
    assert len(runtime.channel.sent) == receipt.sent_frames
    assert runtime.arbiter.get_health().failed == 1
    assert runtime.transport.get_health().tx_frames == receipt.sent_frames
    runtime.arbiter.open(generation=2)
    assert runtime.arbiter.get_health().pending_total == 0


def test_concurrent_close_waits_for_current_native_block_and_stops_before_next(
    runtime_factory,
):
    runtime = runtime_factory(mode=TransmitMode.WORKER)
    entered, closing = threading.Event(), threading.Event()
    errors = []

    def block(_):
        entered.set()
        assert runtime.channel.release.wait(2)

    runtime.channel.after_send = block
    receipt = runtime.arbiter.submit_fifo_batch(frames(51), endpoint_id="arm")
    assert entered.wait(1)

    def close():
        try:
            closing.set()
            runtime.arbiter.close()
        except BaseException as error:
            errors.append(error)

    closer = threading.Thread(target=close)
    closer.start()
    try:
        assert closing.wait(1)
        # Wait on the state condition, not a guessed scheduling delay.
        with runtime.arbiter._condition:
            assert runtime.arbiter._condition.wait_for(
                lambda: not runtime.arbiter._accepting, timeout=1
            )
        assert not receipt.done()
        runtime.channel.release.set()
        closer.join(2)
        assert not closer.is_alive()
        assert errors == []
        assert isinstance(receipt.exception(0), ClosedError)
        assert receipt.sent_frames == 32
        assert [len(call) for call in runtime.channel.calls] == [32]
    finally:
        runtime.channel.release.set()
        closer.join(2)


def test_recovery_between_blocks_rejects_old_generation_without_sending_to_new_one(
    runtime_factory, monkeypatch
):
    runtime = runtime_factory()
    dispatcher = runtime.dispatchers[0]
    send_batch_now = dispatcher.send_batch_now

    def recover_before_commit(messages, *, on_progress):
        def commit(count):
            # Recover after physical completion and transmit lock release,
            # before the old-generation result reaches managed accounting.
            assert dispatcher._transmit_lock.acquire(blocking=False)
            dispatcher._transmit_lock.release()
            runtime.transport.recover()
            on_progress(count)

        send_batch_now(messages, on_progress=commit)

    monkeypatch.setattr(dispatcher, "send_batch_now", recover_before_commit)
    receipt = runtime.arbiter.submit_fifo_batch(frames(51), endpoint_id="arm")
    runtime.arbiter.drain()
    assert isinstance(receipt.exception(0), DeviceNotReadyError)
    assert receipt.sent_frames == 32
    assert len(runtime.channels) == 2
    assert runtime.channels[1].sent == []
    health = runtime.transport.get_health()
    assert health.generation == 2
    assert health.consecutive_errors == 0
    assert health.tx_frames == 32
    assert health.last_successful_tx_s is None


def test_failed_batch_shares_threshold_with_single_send_and_recovery_is_fresh(
    runtime_factory,
):
    runtime = runtime_factory()
    failure = TransportError("physical write failed")
    runtime.channel.errors[1] = runtime.channel.errors[2] = failure
    batch = runtime.arbiter.submit_fifo_batch(frames(3), endpoint_id="arm")
    runtime.arbiter.drain()
    assert batch.exception(0) is failure
    assert runtime.transport.get_health().state is TransportState.OPEN
    single = runtime.transport.send(frames(1)[0])
    assert single.exception(0) is failure
    assert runtime.transport.get_health().state is TransportState.FAULTED
    assert runtime.transport.get_health().consecutive_errors == 2
    runtime.transport.recover()
    assert runtime.transport.get_health().consecutive_errors == 0
    assert runtime.channels[1].sent == []


def test_started_batch_does_not_recheck_deadline_at_block_boundaries(runtime_factory):
    now = [1.0]
    runtime = runtime_factory(clock=lambda: now[0])
    runtime.channel.after_send = lambda _: now.__setitem__(0, 10.0)
    receipt = runtime.arbiter.submit_fifo_batch(
        frames(51), endpoint_id="arm", deadline_at_s=2.0
    )
    runtime.arbiter.drain()
    assert receipt.result(0).outcome is SendOutcome.DISPATCHED
    assert receipt.sent_frames == 51


def test_expired_batch_makes_no_physical_calls(runtime_factory):
    now = [1.0]
    runtime = runtime_factory(clock=lambda: now[0])
    receipt = runtime.arbiter.submit_fifo_batch(
        frames(51), endpoint_id="arm", deadline_at_s=2.0
    )
    now[0] = 3.0
    runtime.arbiter.drain()
    assert isinstance(receipt.exception(0), TimeoutError)
    assert receipt.sent_frames == 0
    assert runtime.channel.calls == []


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_process_interrupt_keeps_confirmed_prefix_and_terminal_cleanup(
    runtime_factory, native, interrupt_type
):
    runtime = runtime_factory(native=native)
    interrupted = interrupt_type("physical send interrupted")
    runtime.channel.errors[2 if native else 35] = interrupted
    receipt = runtime.arbiter.submit_fifo_batch(frames(51), endpoint_id="arm")
    queued = runtime.arbiter.submit_fifo(frames(1)[0], endpoint_id="other")
    with pytest.raises(interrupt_type) as caught:
        runtime.arbiter.drain()
    assert caught.value is interrupted
    assert receipt.sent_frames == (32 if native else 34)
    assert receipt.done() and queued.done()
    assert not runtime.arbiter.get_health().running
    runtime.arbiter.open(generation=2)
    assert runtime.arbiter.get_health().pending_total == 0


def test_generic_dispatcher_fallback_stays_on_captured_generation():
    transport = FakeTransport[int]()
    transport.open()
    first = transport.current_dispatcher
    try:

        def progress(count):
            if count == 1:
                transport.recover()

        receipt = transport.send_batch([1, 2, 3], on_progress=progress)
        assert isinstance(receipt.exception(0), ClosedError)
        assert receipt.sent_frames == 1
        assert first.sent == [1]
        assert transport.current_dispatcher.sent == []
        assert transport.get_health().consecutive_errors == 0
    finally:
        transport.close()


def test_direct_batch_input_is_bounded_before_io(runtime_factory):
    runtime = runtime_factory()
    consumed = []

    def infinite():
        while True:
            consumed.append(1)
            yield frames(1)[0]

    with pytest.raises(ValidationError):
        runtime.transport.send_batch(infinite())
    assert len(consumed) == 33
    with pytest.raises(ValidationError):
        runtime.transport.send_batch([])
    with pytest.raises(DeviceNotReadyError):
        runtime.transport.send_batch(frames(3), expected_generation=2)
    assert runtime.channel.calls == []


def test_opt_in_callback_is_validated_and_not_used_for_single_frame():
    with pytest.raises(ValidationError):
        TransmitArbiter(lambda _: SendReceipt.completed(), dispatch_batch=3)

    def unexpected(*args, **kwargs):
        raise AssertionError("single-frame batch must use the single-send fast path")

    arbiter = TransmitArbiter(
        lambda _: SendReceipt.completed(),
        dispatch_batch=unexpected,
        mode=TransmitMode.CALLER,
    )
    arbiter.open(generation=1)
    try:
        receipt = arbiter.submit_fifo_batch([1], endpoint_id="arm")
        arbiter.drain()
        assert receipt.result(0).outcome is SendOutcome.DISPATCHED
        assert receipt.sent_frames == 1
    finally:
        arbiter.close()


@pytest.mark.parametrize("count", [17, 51])
@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_interrupted_progress_keeps_whole_confirmed_success(
    runtime_factory, monkeypatch, count, interrupt_type
):
    runtime = runtime_factory()
    interrupted = interrupt_type("interrupted after confirmed progress")
    send_batch = runtime.transport.send_batch

    def dispatch_batch(messages, *, expected_generation, on_progress):
        def progress(confirmed):
            on_progress(confirmed)
            raise interrupted

        return send_batch(
            messages, expected_generation=expected_generation, on_progress=progress
        )

    monkeypatch.setattr(runtime.arbiter, "_dispatch_batch", dispatch_batch)
    receipt = runtime.arbiter.submit_fifo_batch(frames(count), endpoint_id="arm")
    queued = runtime.arbiter.submit_fifo(frames(1)[0], endpoint_id="other")
    with pytest.raises(interrupt_type) as caught:
        runtime.arbiter.drain()
    assert caught.value is interrupted
    assert receipt.sent_frames == min(32, count)
    if count <= 32:
        assert receipt.result(0).outcome is SendOutcome.DISPATCHED
        assert runtime.arbiter.get_health().dispatched == 1
    else:
        assert isinstance(receipt.exception(0), TransportError)
    assert queued.done()
    assert runtime.transport.get_health().tx_frames == min(32, count)
    assert len(runtime.channel.calls) == 1


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_interruption_before_progress_callback_keeps_committed_prefix(
    runtime_factory, monkeypatch, interrupt_type
):
    runtime = runtime_factory()
    receipt = runtime.arbiter.submit_fifo_batch(frames(51), endpoint_id="arm")
    original = BatchSendReceipt._set_sent_frames
    interrupted = interrupt_type("between managed commit and progress report")
    fired = False

    def commit(current, count):
        nonlocal fired
        original(current, count)
        if current is not receipt and not fired:
            fired = True
            raise interrupted

    monkeypatch.setattr(BatchSendReceipt, "_set_sent_frames", commit)
    with pytest.raises(interrupt_type) as caught:
        runtime.arbiter.drain()
    assert caught.value is interrupted
    assert receipt.done()
    assert receipt.sent_frames == 32
    assert runtime.transport.get_health().tx_frames == 32
    assert runtime.transport.get_health().tx_bytes == 128
    assert len(runtime.channel.calls) == 1


def test_native_batching_requires_explicit_arbiter_opt_in(runtime_factory):
    runtime = runtime_factory()
    arbiter = TransmitArbiter(runtime.transport.send, mode=TransmitMode.CALLER)
    arbiter.open(generation=1)
    try:
        receipt = arbiter.submit_fifo_batch(frames(3), endpoint_id="arm")
        arbiter.drain()
        assert receipt.result(0).outcome is SendOutcome.DISPATCHED
        assert [len(call) for call in runtime.channel.calls] == [1, 1, 1]
    finally:
        arbiter.close()


def test_invalid_batch_does_not_reset_or_advance_physical_failure_streak(
    runtime_factory,
):
    runtime = runtime_factory()
    physical = TransportError("physical failure")
    runtime.channel.errors = {
        1: physical,
        2: ValidationError("invalid frame"),
        3: physical,
    }
    assert runtime.transport.send(frames(1)[0]).exception(0) is physical
    invalid = runtime.transport.send_batch(frames(3))
    assert isinstance(invalid.exception(0), ValidationError)
    assert runtime.transport.get_health().state is TransportState.OPEN
    terminal = runtime.transport.send_batch(frames(3))
    assert terminal.exception(0) is physical
    assert runtime.transport.get_health().state is TransportState.FAULTED


def test_completed_batch_commits_health_before_reentrant_close(runtime_factory):
    runtime = runtime_factory()
    observed = []
    receipt = runtime.arbiter.submit_fifo_batch(frames(51), endpoint_id="arm")

    def complete(done):
        observed.append(
            (
                done.sent_frames,
                runtime.transport.get_health().tx_frames,
                runtime.arbiter.get_health().dispatched,
            )
        )
        runtime.arbiter.close()

    receipt.add_done_callback(complete)
    runtime.arbiter.drain()
    assert receipt.result(0).outcome is SendOutcome.DISPATCHED
    assert observed == [(51, 51, 1)]


def test_completion_callback_does_not_replace_original_process_interrupt():
    transport = FakeTransport[int]()
    transport.open()
    original = KeyboardInterrupt("original progress interrupt")
    secondary = SystemExit("completion callback interrupted")

    def dispatch_batch(messages, *, expected_generation, on_progress):
        def progress(count):
            on_progress(count)
            if count == len(messages):
                raise original

        return transport.send_batch(
            messages, expected_generation=expected_generation, on_progress=progress
        )

    arbiter = TransmitArbiter(
        transport.send, dispatch_batch=dispatch_batch, mode=TransmitMode.CALLER
    )
    arbiter.open(generation=1)

    def interrupt_completion(_):
        raise secondary

    try:
        receipt = arbiter.submit_fifo_batch([1, 2], endpoint_id="arm")
        receipt.add_done_callback(interrupt_completion)
        queued = arbiter.submit_fifo(3, endpoint_id="other")
        with pytest.raises(KeyboardInterrupt) as caught:
            arbiter.drain()
        assert caught.value is original
        assert receipt.result(0).outcome is SendOutcome.DISPATCHED
        assert receipt.sent_frames == 2
        assert queued.exception(0).cause is original
        assert transport.sent == (1, 2)
    finally:
        arbiter.close()
        transport.close()


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_interrupted_progress_accounting_commits_once_before_transfer(interrupt_type):
    transport = FakeTransport[int](data_size_of=lambda _: 4)
    transport.open()
    arbiter = TransmitArbiter(
        transport.send,
        dispatch_batch=transport.send_batch,
        mode=TransmitMode.CALLER,
    )
    arbiter.open(generation=1)
    interrupted = interrupt_type("between confirmation and accounting")
    filename = inspect.getsourcefile(ManagedMessageTransport.send_batch)
    fired = False

    def trace(frame, event, arg):
        nonlocal fired
        if (
            event == "line"
            and frame.f_code.co_filename == filename
            and frame.f_code.co_name == "account_confirmed"
            and frame.f_locals.get("confirmed") == 2
            and not fired
        ):
            fired = True
            raise interrupted
        return trace

    receipt = arbiter.submit_fifo_batch([1, 2], endpoint_id="arm")
    previous_trace = sys.gettrace()
    try:
        sys.settrace(trace)
        with pytest.raises(interrupt_type) as caught:
            arbiter.drain()
        assert caught.value is interrupted
    finally:
        sys.settrace(previous_trace)
        arbiter.close()
        transport.close()
    assert fired
    assert receipt.result(0).outcome is SendOutcome.DISPATCHED
    assert receipt.sent_frames == 2
    assert transport.sent == (1, 2)
    assert transport.get_health().tx_frames == 2
    assert transport.get_health().tx_bytes == 8


@pytest.mark.parametrize("mode", list(TransmitMode))
@pytest.mark.parametrize("stage", ["completion_count", "receipt_publication"])
@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_confirmed_batch_survives_interrupted_completion_handoff(
    runtime_factory, monkeypatch, mode, stage, interrupt_type
):
    runtime = runtime_factory(mode=mode)
    runtime.channel.after_send = lambda _: runtime.channel.release.wait(2)
    arbiter = runtime.arbiter
    dispatch = arbiter._dispatch_claimed
    source, first = inspect.getsourcelines(dispatch)
    if stage == "completion_count":
        marker = next(
            i for i, line in enumerate(source) if "Record physical completion" in line
        )
        target = first + next(
            i
            for i, line in enumerate(source)
            if i > marker and line.strip() == "with self._condition:"
        )
    else:
        target = first + next(
            i
            for i, line in enumerate(source)
            if line.strip() == "job.receipt._dispatch()"
        )
    interrupted = interrupt_type("confirmed batch completion interrupted")
    fired = False

    def traced_dispatch(job):
        def trace(frame, event, arg):
            nonlocal fired
            if (
                event == "line"
                and frame.f_code is dispatch.__func__.__code__
                and frame.f_lineno == target
                and not fired
            ):
                fired = True
                raise interrupted
            return trace

        previous = sys.gettrace()
        sys.settrace(trace)
        try:
            dispatch(job)
        finally:
            sys.settrace(previous)

    monkeypatch.setattr(arbiter, "_dispatch_claimed", traced_dispatch)
    receipt = arbiter.submit_fifo_batch(frames(51), endpoint_id="arm")
    queued = arbiter.submit_fifo(CANFDMessage(100, b"late"), endpoint_id="other")
    observed = []
    receipt.add_done_callback(
        lambda _: observed.append(
            (arbiter.get_health().dispatched, runtime.transport.get_health().tx_frames)
        )
    )
    runtime.channel.release.set()
    if mode is TransmitMode.CALLER:
        with pytest.raises(interrupt_type) as caught:
            arbiter.drain()
        assert caught.value is interrupted
    else:
        assert queued.exception(2).cause is interrupted
    arbiter.close()
    assert fired
    assert receipt.result(0).outcome is SendOutcome.DISPATCHED
    assert receipt.sent_frames == 51
    assert queued.exception(0).cause is interrupted
    assert [len(call) for call in runtime.channel.calls] == [32, 19]
    assert runtime.channel.sent == frames(51)
    assert runtime.transport.get_health().tx_frames == 51
    assert runtime.transport.get_health().tx_bytes == 204
    assert observed == [(1, 51)]
    health = arbiter.get_health()
    assert (health.accepted, health.dispatched, health.failed) == (2, 1, 1)


@pytest.mark.parametrize("mode", list(TransmitMode))
def test_confirmed_batch_retains_success_when_cleanup_publication_needs_retry(
    runtime_factory, monkeypatch, mode
):
    runtime = runtime_factory(mode=mode)
    runtime.channel.after_send = lambda _: runtime.channel.release.wait(2)
    arbiter = runtime.arbiter
    dispatch = BatchSendReceipt._dispatch
    interrupted = KeyboardInterrupt("success publication interrupted twice")
    attempts = 0
    cleanup_attempted = threading.Event()

    def interrupt_twice(receipt):
        nonlocal attempts
        if receipt.frame_count == 51:
            attempts += 1
            if attempts <= 2:
                if attempts == 2:
                    cleanup_attempted.set()
                raise interrupted
        return dispatch(receipt)

    monkeypatch.setattr(BatchSendReceipt, "_dispatch", interrupt_twice)
    receipt = arbiter.submit_fifo_batch(frames(51), endpoint_id="arm")
    queued = arbiter.submit_fifo(CANFDMessage(100, b"late"), endpoint_id="other")
    runtime.channel.release.set()
    if mode is TransmitMode.CALLER:
        with pytest.raises(KeyboardInterrupt) as caught:
            arbiter.drain()
        assert caught.value is interrupted
    assert cleanup_attempted.wait(2)
    if mode is TransmitMode.WORKER:
        arbiter._worker.join(2)
        assert not arbiter._worker.is_alive()
    assert not receipt.done()
    assert receipt.sent_frames == 51
    assert queued.exception(0).cause is interrupted
    with pytest.raises(DeviceNotReadyError):
        arbiter.open(generation=2)
    arbiter.close()
    assert receipt.result(0).outcome is SendOutcome.DISPATCHED
    assert [len(call) for call in runtime.channel.calls] == [32, 19]
    assert runtime.transport.get_health().tx_frames == 51
    health = arbiter.get_health()
    assert (health.accepted, health.dispatched, health.failed) == (2, 1, 1)
    arbiter.open(generation=2)


@pytest.mark.parametrize("mode", list(TransmitMode))
@pytest.mark.parametrize("count", [3, 51])
@pytest.mark.parametrize(
    "fault",
    [
        "generation",
        "frame_count",
        "single_receipt",
        "non_receipt",
        "failed_receipt",
        "superseded_receipt",
        "callback_error",
    ],
)
def test_full_progress_does_not_hide_final_block_errors(
    runtime_factory, monkeypatch, mode, count, fault
):
    runtime = runtime_factory(mode=mode)
    failure = TransportError("batch callback failed after reporting progress")
    send_batch = runtime.transport.send_batch

    def dispatch_batch(messages, *, expected_generation, on_progress):
        downstream = send_batch(
            messages, expected_generation=expected_generation, on_progress=on_progress
        )
        if messages[-1].arbitration_id != count:
            return downstream
        if fault == "callback_error":
            raise failure
        if fault == "non_receipt":
            return None
        if fault == "single_receipt":
            return SendReceipt.completed(generation=expected_generation)
        receipt = BatchSendReceipt(
            frame_count=len(messages) + (fault == "frame_count"),
            generation=expected_generation + (fault == "generation"),
        )
        receipt._set_sent_frames(len(messages))
        if fault == "failed_receipt":
            receipt._fail(failure)
        elif fault == "superseded_receipt":
            receipt._supersede()
        else:
            receipt._dispatch()
        return receipt

    monkeypatch.setattr(runtime.arbiter, "_dispatch_batch", dispatch_batch)
    receipt = runtime.arbiter.submit_fifo_batch(frames(count), endpoint_id="arm")
    if mode is TransmitMode.CALLER:
        runtime.arbiter.drain()
    assert isinstance(receipt.exception(2), TransportError)
    if fault in ("callback_error", "failed_receipt"):
        assert receipt.exception(0) is failure
    assert receipt.outcome is SendOutcome.FAILED
    assert receipt.sent_frames == count
    assert runtime.channel.sent == frames(count)
    assert [len(call) for call in runtime.channel.calls] == (
        [3] if count == 3 else [32, 19]
    )
    health = runtime.arbiter.get_health()
    assert health.running
    assert (health.accepted, health.dispatched, health.failed) == (1, 0, 1)
