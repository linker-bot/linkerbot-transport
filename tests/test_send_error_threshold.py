"""Send fault policy through real dispatchers and managed transports, without I/O."""

import queue
import threading
from collections import deque
from types import SimpleNamespace

import can
import pytest

from linkerbot_transport import SendOutcome, TransportState
from linkerbot_transport.can import CANTransport
from linkerbot_transport.can import can as classic
from linkerbot_transport.canfd import (
    CANFDMessage,
    CANFDTransport,
    CANRouteKey,
    PythonCANFDTransport,
    python_can_backend,
)
from linkerbot_transport.canfd.dispatcher import CANFDMessageDispatcher
from linkerbot_transport.errors import (
    DeviceNotReadyError,
    TimeoutError,
    TransportError,
    UnsupportedOperationError,
    ValidationError,
)


class Channel:
    def __init__(self):
        self.errors = deque()
        self.incoming = queue.Queue()
        self.sent = []
        self.attempts = 0
        self.closed = False

    def send(self, message, **kwargs):
        self.attempts += 1
        if self.errors:
            error = self.errors.popleft()
            if error is not None:
                raise error
        self.sent.append(message)

    def recv(self, timeout):
        try:
            return self.incoming.get(timeout=timeout)
        except queue.Empty:
            return None

    def receive(self, max_frames=64, timeout_ms=10):
        message = self.recv(timeout_ms / 1000)
        return [] if message is None else [message]

    def close(self):
        self.closed = True

    shutdown = close


@pytest.fixture(params=["classic", "native_fd", "python_fd"])
def runtime(monkeypatch, request):
    channels = []
    faults = []
    faulted = threading.Event()

    def channel(**kwargs):
        result = Channel()
        channels.append(result)
        return result

    if request.param == "classic":
        monkeypatch.setattr(classic, "open_bus", channel)
        transport = CANTransport("send-threshold", "virtual", max_consecutive_errors=2)
        message = can.Message(arbitration_id=0x123, data=b"x", is_extended_id=False)
    elif request.param == "python_fd":
        monkeypatch.setattr(python_can_backend, "open_bus", channel)
        transport = PythonCANFDTransport(
            "send-threshold", "virtual", max_consecutive_errors=2
        )
        message = CANFDMessage(0x123, b"x", is_extended_id=False)
    else:

        def create(on_error):
            return CANFDMessageDispatcher(
                interface=channel(), on_bus_error=on_error, max_consecutive_errors=2
            )

        transport = CANFDTransport(dispatcher_factory=create)
        message = CANFDMessage(0x123, b"x", is_extended_id=False)
    transport.open()
    dispatcher = transport._dispatcher
    original = dispatcher._on_bus_error

    def report(error):
        original(error)
        faults.append(error)
        faulted.set()

    dispatcher._on_bus_error = report
    try:
        yield SimpleNamespace(
            kind=request.param,
            transport=transport,
            dispatcher=dispatcher,
            channel=channels[0],
            channels=channels,
            message=message,
            faults=faults,
            faulted=faulted,
        )
    finally:
        transport.close()
        dispatcher.stop()
        assert not dispatcher._recv_thread.is_alive()
        assert not dispatcher._send_thread.is_alive()


def fail_send(runtime, mode="sync", error=None):
    runtime.channel.errors.append(error or OSError("physical write failed"))
    receipt = (
        runtime.transport.send(runtime.message)
        if mode == "sync"
        else runtime.dispatcher.send(runtime.message)
    )
    with pytest.raises((TransportError, TimeoutError)):
        receipt.result(timeout=1)
    return receipt


@pytest.mark.parametrize(
    "modes", [("sync", "sync"), ("sync", "queue"), ("queue", "sync")]
)
def test_sync_and_queue_share_threshold_and_recovery_starts_fresh(runtime, modes):
    transport = runtime.transport
    route = transport.subscribe_route(CANRouteKey(0x123, False))
    fail_send(runtime, modes[0])
    assert transport.get_health().state is TransportState.OPEN
    fail_send(runtime, modes[1])
    assert runtime.faulted.wait(timeout=1)
    health = transport.get_health()
    assert health.state is TransportState.FAULTED
    assert not health.connected
    assert health.tx_frames == 0
    assert len(runtime.faults) == 1
    with pytest.raises(TransportError):
        route.receive(timeout_s=0)
    with pytest.raises(DeviceNotReadyError):
        transport.send(runtime.message)
    assert runtime.channel.attempts == 2
    transport.recover()
    assert len(runtime.channels) == 2
    assert runtime.channels[0].closed
    assert runtime.channels[1].sent == []
    assert transport.get_health().generation == health.generation + 1
    assert transport.get_health().consecutive_errors == 0
    result = transport.send(runtime.message).result(timeout=0)
    assert result.outcome is SendOutcome.DISPATCHED
    assert transport.get_health().state is TransportState.OPEN


@pytest.mark.parametrize("success_mode", ["sync", "queue"])
def test_only_successful_send_resets_shared_tx_streak(runtime, success_mode):
    fail_send(runtime)
    send = runtime.transport.send if success_mode == "sync" else runtime.dispatcher.send
    assert send(runtime.message).result(timeout=1).outcome is SendOutcome.DISPATCHED
    fail_send(runtime, "queue")
    assert not runtime.faults
    assert runtime.transport.get_health().state is TransportState.OPEN
    fail_send(runtime)
    assert runtime.faulted.wait(timeout=1)
    assert len(runtime.faults) == 1


def test_successful_receive_does_not_hide_persistent_send_failure(runtime):
    route = runtime.transport.subscribe_route(CANRouteKey(0x123, False))
    fail_send(runtime)
    incoming = (
        can.Message(arbitration_id=0x123, data=b"rx", is_extended_id=False, is_fd=True)
        if runtime.kind == "python_fd"
        else runtime.message
    )
    runtime.channel.incoming.put(incoming)
    assert route.receive(timeout_s=1).message.data in (b"x", b"rx")
    fail_send(runtime)
    assert runtime.faulted.wait(timeout=1)
    assert runtime.transport.get_health().state is TransportState.FAULTED


@pytest.mark.parametrize(
    "error_type", [ValidationError, ValueError, TypeError, UnsupportedOperationError]
)
@pytest.mark.parametrize("mode", ["sync", "queue"])
def test_invalid_sends_neither_advance_nor_reset_physical_failure_streak(
    runtime, error_type, mode
):
    fail_send(runtime)
    for _ in range(3):
        runtime.channel.errors.append(error_type("invalid frame"))
        send = runtime.transport.send if mode == "sync" else runtime.dispatcher.send
        receipt = send(runtime.message)
        expected = (
            UnsupportedOperationError
            if error_type is UnsupportedOperationError
            else ValidationError
        )
        with pytest.raises(expected):
            receipt.result(timeout=1)
        assert runtime.transport.get_health().state is TransportState.OPEN
        assert not runtime.faults
    fail_send(runtime)
    assert runtime.faulted.wait(timeout=1)


@pytest.mark.parametrize(
    "error_type", [TransportError, TimeoutError, can.CanOperationError]
)
def test_classified_physical_failures_count_and_terminal_error_is_not_double_counted(
    runtime, error_type
):
    fail_send(runtime, error=error_type("physical write failed"))
    assert runtime.transport.get_health().consecutive_errors == 1
    terminal = fail_send(runtime, error=error_type("physical write failed"))
    assert runtime.faulted.wait(timeout=1)
    assert runtime.transport.get_health().consecutive_errors == 2
    assert len(runtime.faults) == 1
    if error_type in (TransportError, TimeoutError):
        assert terminal.exception() is runtime.faults[0]


def test_fatal_send_notifies_outside_transmit_lock_and_callback_can_stop(runtime):
    original = runtime.dispatcher._on_bus_error
    idle_at_callback = []

    def stop_at_fault(error):
        acquired = runtime.dispatcher._transmit_lock.acquire(blocking=False)
        idle_at_callback.append(acquired)
        if acquired:
            runtime.dispatcher._transmit_lock.release()
        original(error)
        runtime.dispatcher.stop()

    runtime.dispatcher._on_bus_error = stop_at_fault
    fail_send(runtime)
    fail_send(runtime)
    assert idle_at_callback == [True]
    assert runtime.channel.closed
    assert runtime.transport.get_health().consecutive_errors == 2


def test_concurrent_senders_stop_at_threshold_and_report_once(runtime):
    count = 12
    barrier = threading.Barrier(count)
    errors = []
    runtime.channel.errors.extend(
        OSError("persistent write failure") for _ in range(count)
    )

    def send():
        barrier.wait(timeout=2)
        try:
            runtime.dispatcher.send_now(runtime.message)
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=send, daemon=True) for _ in range(count)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)
        assert all(not thread.is_alive() for thread in threads)
        assert len(errors) == count
        assert all(isinstance(error, TransportError) for error in errors)
        assert runtime.channel.attempts == 2
        assert runtime.faulted.wait(timeout=1)
        assert len(runtime.faults) == 1
    finally:
        barrier.abort()
        for thread in threads:
            if thread.ident is not None:
                thread.join(timeout=2)


def test_terminal_send_callback_recovery_does_not_record_old_error_in_new_generation(
    runtime,
):
    original = runtime.dispatcher._on_bus_error
    transport = runtime.transport

    def recover_at_fault(error):
        original(error)
        transport.recover()

    runtime.dispatcher._on_bus_error = recover_at_fault
    fail_send(runtime)
    generation = transport.get_health().generation
    receipt = fail_send(runtime)
    assert receipt.generation == generation
    health = transport.get_health()
    assert health.state is TransportState.OPEN
    assert health.generation == generation + 1
    assert health.consecutive_errors == 0
    assert runtime.channels[0].closed
    assert runtime.channels[1].sent == []


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit])
def test_process_interrupt_is_not_classified_as_a_send_failure(runtime, error_type):
    fail_send(runtime)
    interrupt = error_type("interrupted send")
    runtime.channel.errors.append(interrupt)
    with pytest.raises(error_type) as raised:
        runtime.transport.send(runtime.message)
    assert raised.value is interrupt
    assert not runtime.faults
    fail_send(runtime)
    assert runtime.faulted.wait(timeout=1)


@pytest.mark.parametrize("batch_fails", [True, False])
def test_native_batch_and_sync_sends_share_failure_and_success_accounting(batch_fails):
    entered = threading.Event()
    release = threading.Event()
    reported = threading.Event()

    class BatchChannel(Channel):
        def __init__(self):
            super().__init__()
            self.batches = []

        def send(self, message, **kwargs):
            entered.set()
            assert release.wait(timeout=2)
            super().send(message, **kwargs)

        def send_batch(self, messages):
            self.batches.append(len(messages))
            Channel.send(self, messages)

    channel = BatchChannel()
    channel.errors.extend(
        [
            OSError("first write failed"),
            OSError("batch failed") if batch_fails else None,
        ]
    )
    dispatcher = CANFDMessageDispatcher(
        interface=channel,
        max_consecutive_errors=2,
        on_bus_error=lambda error: reported.set(),
    )
    message = CANFDMessage(0x123, b"x")
    try:
        first = dispatcher.send(message)
        assert entered.wait(timeout=1)
        batch = [dispatcher.send(message), dispatcher.send(message)]
        release.set()
        with pytest.raises(TransportError):
            first.result(timeout=1)
        for receipt in batch:
            if batch_fails:
                with pytest.raises(TransportError):
                    receipt.result(timeout=1)
            else:
                assert receipt.result(timeout=1).outcome is SendOutcome.DISPATCHED
        assert channel.batches == [2]
        if not batch_fails:
            channel.errors.append(OSError("write failed after successful batch"))
            with pytest.raises(TransportError):
                dispatcher.send_now(message)
            assert not reported.is_set()
            channel.errors.append(OSError("second write failed"))
            with pytest.raises(TransportError):
                dispatcher.send_now(message)
        assert reported.wait(timeout=1)
    finally:
        release.set()
        dispatcher.stop()
        assert not dispatcher._recv_thread.is_alive()
        assert not dispatcher._send_thread.is_alive()
