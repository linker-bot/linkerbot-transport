import math
import queue
import threading
import time
from typing import Any

import can
import pytest

from linkerbot_transport import SendOutcome, SendReceipt
from linkerbot_transport.can.can import CANMessageDispatcher
from linkerbot_transport.errors import (
    ClosedError,
    QueueFullError,
    TransportError,
    ValidationError,
)


class FakeBus:
    def __init__(self) -> None:
        self.sent: list[can.Message] = []
        self.send_timeouts: list[float | None] = []
        self.send_event = threading.Event()
        self.shutdown_event = threading.Event()
        self.shutdown_calls = 0
        self.receive_error: Exception | None = None

    def recv(self, timeout: float) -> can.Message | None:
        if self.receive_error is not None:
            error = self.receive_error
            self.receive_error = None
            raise error
        time.sleep(min(timeout, 0.001))
        return None

    def send(self, message: can.Message, timeout: float | None = None) -> None:
        self.sent.append(message)
        self.send_timeouts.append(timeout)
        self.send_event.set()

    def shutdown(self) -> None:
        self.shutdown_calls += 1
        self.shutdown_event.set()


class BlockingPutQueue:
    def __init__(self) -> None:
        self.put_entered = threading.Event()
        self.release_put = threading.Event()
        self.items: list[can.Message] = []

    def get(self, timeout: float | None = None) -> can.Message:
        time.sleep(min(timeout or 0.001, 0.001))
        raise queue.Empty

    def put_nowait(self, message: can.Message) -> None:
        self.put_entered.set()
        self.release_put.wait()
        self.items.append(message)


def make_dispatcher(
    monkeypatch: pytest.MonkeyPatch,
    bus: FakeBus,
    **kwargs: Any,
) -> CANMessageDispatcher:
    monkeypatch.setattr("linkerbot_transport.can.can.can.Bus", lambda **options: bus)
    return CANMessageDispatcher(interface_name="vcan0", **kwargs)


def test_send_pacing_sleeps_instead_of_busy_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = FakeBus()
    dispatcher = make_dispatcher(monkeypatch, bus)
    sleep_called = threading.Event()
    pacing_sleeps: list[float] = []
    original_sleep = time.sleep
    clock = iter([10.0, 10.0001])

    def record_sleep(duration: float) -> None:
        if duration < dispatcher.SEND_INTERVAL_S:
            pacing_sleeps.append(duration)
            sleep_called.set()
        original_sleep(duration)

    monkeypatch.setattr(
        "linkerbot_transport.can.can.time.monotonic", lambda: next(clock)
    )
    monkeypatch.setattr("linkerbot_transport.can.can.time.sleep", record_sleep)
    try:
        dispatcher.send(can.Message(arbitration_id=1, data=b"x"))
        assert bus.send_event.wait(timeout=1.0)
        assert sleep_called.wait(timeout=1.0)
        assert pacing_sleeps == [pytest.approx(0.0002)]
    finally:
        dispatcher.stop()


def test_send_uses_finite_bus_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    bus = FakeBus()
    dispatcher = make_dispatcher(monkeypatch, bus)
    try:
        receipt = dispatcher.send(can.Message(arbitration_id=1, data=b"x"))

        assert bus.send_event.wait(timeout=1.0)
        assert isinstance(receipt, SendReceipt)
        assert receipt.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert bus.send_timeouts == [dispatcher.SEND_TIMEOUT_S]
        assert dispatcher.SEND_TIMEOUT_S > 0
        assert math.isfinite(dispatcher.SEND_TIMEOUT_S)
    finally:
        dispatcher.stop()


def test_send_receipt_preserves_background_sdk_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = ValidationError("invalid CAN frame")

    class InvalidSendBus(FakeBus):
        def send(self, message: can.Message, timeout: float | None = None) -> None:
            _ = message, timeout
            raise expected

    dispatcher = make_dispatcher(monkeypatch, InvalidSendBus())
    try:
        receipt = dispatcher.send(can.Message(arbitration_id=1, data=b"x"))
        with pytest.raises(ValidationError) as raised:
            receipt.result(timeout=1.0)
        assert raised.value is expected
    finally:
        dispatcher.stop()


def test_send_receipt_wraps_background_system_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = OSError("write failed")

    class FailingSendBus(FakeBus):
        def send(self, message: can.Message, timeout: float | None = None) -> None:
            _ = message, timeout
            raise expected

    dispatcher = make_dispatcher(monkeypatch, FailingSendBus())
    try:
        receipt = dispatcher.send(can.Message(arbitration_id=1, data=b"x"))
        with pytest.raises(TransportError, match="background send failed") as raised:
            receipt.result(timeout=1.0)
        assert raised.value.cause is expected
    finally:
        dispatcher.stop()


def test_fatal_send_failure_fails_current_and_queued_receipts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class GatedFailingBus(FakeBus):
        def __init__(self) -> None:
            super().__init__()
            self.send_started = threading.Event()
            self.release_send = threading.Event()

        def send(self, message: can.Message, timeout: float | None = None) -> None:
            _ = message, timeout
            self.send_started.set()
            assert self.release_send.wait(timeout=1.0)
            raise TransportError("physical send failed")

    bus = GatedFailingBus()
    dispatcher = make_dispatcher(monkeypatch, bus, max_consecutive_errors=1)
    first = dispatcher.send(can.Message(arbitration_id=1, data=b"a"))
    assert bus.send_started.wait(timeout=1.0)
    second = dispatcher.send(can.Message(arbitration_id=2, data=b"b"))
    bus.release_send.set()

    try:
        for receipt in (first, second):
            with pytest.raises(TransportError):
                receipt.result(timeout=1.0)
    finally:
        dispatcher.stop()


def test_send_queue_full_raises_stable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FullQueue:
        def get(self, timeout: float | None = None) -> object:
            time.sleep(min(timeout or 0.001, 0.001))
            raise queue.Empty

        def put_nowait(self, item: object) -> None:
            _ = item
            raise queue.Full

    dispatcher = make_dispatcher(monkeypatch, FakeBus())
    dispatcher._send_queue = FullQueue()  # ty: ignore[invalid-assignment]
    try:
        with pytest.raises(QueueFullError, match="send queue is full"):
            dispatcher.send(can.Message(arbitration_id=1, data=b"x"))
    finally:
        dispatcher.stop()


def test_stop_fails_in_flight_and_queued_receipts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("linkerbot_transport.can.can.THREAD_JOIN_TIMEOUT_S", 0.01)

    class CloseReleasesSendBus(FakeBus):
        def __init__(self) -> None:
            super().__init__()
            self.send_started = threading.Event()
            self.release_send = threading.Event()

        def send(self, message: can.Message, timeout: float | None = None) -> None:
            self.send_started.set()
            self.release_send.wait(timeout=1.0)
            super().send(message, timeout)

        def shutdown(self) -> None:
            self.release_send.set()
            super().shutdown()

    bus = CloseReleasesSendBus()
    dispatcher = make_dispatcher(monkeypatch, bus)
    first = dispatcher.send(can.Message(arbitration_id=1, data=b"a"))
    assert bus.send_started.wait(timeout=1.0)
    second = dispatcher.send(can.Message(arbitration_id=2, data=b"b"))

    dispatcher.stop()

    for receipt in (first, second):
        with pytest.raises(ClosedError, match="stopped before queued send completed"):
            receipt.result(timeout=0)


def test_transient_receive_error_uses_millisecond_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = FakeBus()
    bus.receive_error = TransportError("transient")
    sleep_durations: list[float] = []
    original_sleep = time.sleep

    def record_sleep(duration: float) -> None:
        sleep_durations.append(duration)
        original_sleep(min(duration, 0.001))

    monkeypatch.setattr("linkerbot_transport.can.can.time.sleep", record_sleep)
    dispatcher = make_dispatcher(monkeypatch, bus, max_consecutive_errors=2)
    try:
        deadline = time.monotonic() + 1.0
        while (
            not any(duration >= 0.004 for duration in sleep_durations)
            and time.monotonic() < deadline
        ):
            original_sleep(0.001)
        backoffs = [duration for duration in sleep_durations if duration >= 0.004]
        assert backoffs
        assert min(backoffs) <= 0.005
    finally:
        dispatcher.stop()


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_invalid_error_threshold_is_rejected_before_opening_bus(
    monkeypatch: pytest.MonkeyPatch,
    value: object,
) -> None:
    opened = False

    def open_bus(**options: Any) -> FakeBus:
        nonlocal opened
        opened = True
        return FakeBus()

    monkeypatch.setattr("linkerbot_transport.can.can.can.Bus", open_bus)

    with pytest.raises(ValidationError, match="positive int"):
        CANMessageDispatcher(
            "vcan0",
            max_consecutive_errors=value,  # ty: ignore[invalid-argument-type]
        )

    assert opened is False


def test_stop_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    bus = FakeBus()
    dispatcher = make_dispatcher(monkeypatch, bus)

    dispatcher.stop()
    dispatcher.stop()

    assert bus.shutdown_calls == 1


class ShutdownInterruptsSendBus(FakeBus):
    def __init__(self) -> None:
        super().__init__()
        self.send_started = threading.Event()
        self.shutdown_event = threading.Event()

    def send(self, message: can.Message, timeout: float | None = None) -> None:
        self.sent.append(message)
        self.send_timeouts.append(timeout)
        self.send_started.set()
        self.shutdown_event.wait()

    def shutdown(self) -> None:
        super().shutdown()
        self.shutdown_event.set()


def test_stop_shuts_down_bus_to_interrupt_blocked_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("linkerbot_transport.can.can.THREAD_JOIN_TIMEOUT_S", 0.01)
    bus = ShutdownInterruptsSendBus()
    dispatcher = make_dispatcher(monkeypatch, bus)
    dispatcher.send(can.Message(arbitration_id=1, data=b"x"))
    assert bus.send_started.wait(timeout=1.0)

    dispatcher.stop()

    assert bus.shutdown_calls == 1
    assert not dispatcher._recv_thread.is_alive()
    assert not dispatcher._send_thread.is_alive()


def test_concurrent_stop_shuts_down_bus_once_and_joins_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("linkerbot_transport.can.can.THREAD_JOIN_TIMEOUT_S", 0.01)
    bus = ShutdownInterruptsSendBus()
    dispatcher = make_dispatcher(monkeypatch, bus)
    dispatcher.send(can.Message(arbitration_id=1, data=b"x"))
    assert bus.send_started.wait(timeout=1.0)

    start = threading.Barrier(3)
    errors: list[Exception] = []

    def stop_dispatcher() -> None:
        start.wait()
        try:
            dispatcher.stop()
        except Exception as error:
            errors.append(error)

    callers = [threading.Thread(target=stop_dispatcher) for _ in range(2)]
    for caller in callers:
        caller.start()
    start.wait()
    for caller in callers:
        caller.join(timeout=1.0)

    assert errors == []
    assert all(not caller.is_alive() for caller in callers)
    assert bus.shutdown_calls == 1
    assert not dispatcher._recv_thread.is_alive()
    assert not dispatcher._send_thread.is_alive()


def test_stop_reports_worker_that_ignores_bus_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("linkerbot_transport.can.can.THREAD_JOIN_TIMEOUT_S", 0.01)
    release_send = threading.Event()

    class StuckSendBus(FakeBus):
        def send(self, message: can.Message, timeout: float | None = None) -> None:
            self.send_event.set()
            release_send.wait()

    bus = StuckSendBus()
    dispatcher = make_dispatcher(monkeypatch, bus)
    dispatcher.send(can.Message(arbitration_id=1, data=b"x"))
    assert bus.send_event.wait(timeout=1.0)

    try:
        with pytest.raises(RuntimeError, match="send_loop"):
            dispatcher.stop()
        assert bus.shutdown_calls == 1
    finally:
        release_send.set()
        dispatcher._send_thread.join(timeout=1.0)

    assert not dispatcher._send_thread.is_alive()


def test_send_and_stop_are_atomic_at_queue_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = FakeBus()
    dispatcher = make_dispatcher(monkeypatch, bus)
    blocking_queue = BlockingPutQueue()
    dispatcher._send_queue = blocking_queue  # ty: ignore[invalid-assignment]
    send_errors: list[Exception] = []
    stop_errors: list[Exception] = []
    stop_returned = threading.Event()

    def send() -> None:
        try:
            dispatcher.send(can.Message(arbitration_id=1, data=b"x"))
        except Exception as error:
            send_errors.append(error)

    def stop() -> None:
        try:
            dispatcher.stop()
        except Exception as error:
            stop_errors.append(error)
        finally:
            stop_returned.set()

    sender = threading.Thread(target=send)
    stopper = threading.Thread(target=stop)
    sender.start()
    assert blocking_queue.put_entered.wait(timeout=1.0)
    stopper.start()
    assert not stop_returned.wait(timeout=0.03)

    blocking_queue.release_put.set()
    sender.join(timeout=1.0)
    stopper.join(timeout=1.0)

    assert not sender.is_alive()
    assert not stopper.is_alive()
    assert send_errors == []
    assert stop_errors == []
    assert len(blocking_queue.items) == 1
    assert bus.shutdown_calls == 1


def test_send_now_waiting_for_transmit_lock_cannot_send_after_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("linkerbot_transport.can.can.THREAD_JOIN_TIMEOUT_S", 0.01)
    bus = FakeBus()
    dispatcher = make_dispatcher(monkeypatch, bus)
    message = can.Message(arbitration_id=1, data=b"x")
    sender_started = threading.Event()
    sender_errors: list[Exception] = []
    stop_errors: list[Exception] = []

    dispatcher._transmit_lock.acquire()

    def send_now() -> None:
        sender_started.set()
        try:
            dispatcher.send_now(message)
        except Exception as error:
            sender_errors.append(error)

    def stop() -> None:
        try:
            dispatcher.stop()
        except Exception as error:
            stop_errors.append(error)

    sender = threading.Thread(target=send_now)
    stopper = threading.Thread(target=stop)
    try:
        sender.start()
        assert sender_started.wait(timeout=1.0)
        stopper.start()
        assert bus.shutdown_event.wait(timeout=1.0)
    finally:
        dispatcher._transmit_lock.release()

    sender.join(timeout=1.0)
    stopper.join(timeout=1.0)

    assert not sender.is_alive()
    assert not stopper.is_alive()
    assert len(sender_errors) == 1
    assert isinstance(sender_errors[0], RuntimeError)
    assert stop_errors == []
    assert bus.sent == []


def test_stop_waits_for_in_flight_send_now_before_bus_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("linkerbot_transport.can.can.THREAD_JOIN_TIMEOUT_S", 0.2)

    class OrderedSendBus(FakeBus):
        def __init__(self) -> None:
            super().__init__()
            self.send_started = threading.Event()
            self.release_send = threading.Event()
            self.send_finished = threading.Event()
            self.shutdown_before_send_finished = False

        def send(self, message: can.Message, timeout: float | None = None) -> None:
            self.send_started.set()
            self.release_send.wait(timeout=1.0)
            super().send(message, timeout)
            self.send_finished.set()

        def shutdown(self) -> None:
            self.shutdown_before_send_finished = not self.send_finished.is_set()
            super().shutdown()

    bus = OrderedSendBus()
    dispatcher = make_dispatcher(monkeypatch, bus)
    message = can.Message(arbitration_id=1, data=b"x")
    send_errors: list[Exception] = []
    stop_errors: list[Exception] = []

    def send_now() -> None:
        try:
            dispatcher.send_now(message)
        except Exception as error:
            send_errors.append(error)

    def stop() -> None:
        try:
            dispatcher.stop()
        except Exception as error:
            stop_errors.append(error)

    sender = threading.Thread(target=send_now)
    stopper = threading.Thread(target=stop)
    sender.start()
    assert bus.send_started.wait(timeout=1.0)
    stopper.start()

    try:
        assert not bus.shutdown_event.wait(timeout=0.03)
    finally:
        bus.release_send.set()

    sender.join(timeout=1.0)
    stopper.join(timeout=1.0)

    assert not sender.is_alive()
    assert not stopper.is_alive()
    assert send_errors == []
    assert stop_errors == []
    assert bus.shutdown_calls == 1
    assert not bus.shutdown_before_send_finished


def test_stop_from_callback_prevents_later_copied_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class IncomingBus(FakeBus):
        def __init__(self) -> None:
            super().__init__()
            self.incoming: queue.Queue[can.Message] = queue.Queue()

        def recv(self, timeout: float) -> can.Message | None:
            try:
                return self.incoming.get(timeout=timeout)
            except queue.Empty:
                return None

    bus = IncomingBus()
    dispatcher = make_dispatcher(monkeypatch, bus)
    first_returned = threading.Event()
    second_called = threading.Event()

    def stop_from_callback(message: can.Message) -> None:
        _ = message
        dispatcher.stop()
        first_returned.set()

    dispatcher.subscribe(stop_from_callback)
    dispatcher.subscribe(lambda message: second_called.set())
    bus.incoming.put(can.Message(arbitration_id=1, data=b"x"))

    assert first_returned.wait(timeout=1.0)
    dispatcher._recv_thread.join(timeout=1.0)
    assert not dispatcher._recv_thread.is_alive()
    assert not second_called.is_set()


def test_send_now_preserves_existing_sdk_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = ValidationError("invalid CAN frame")

    class InvalidSendBus(FakeBus):
        def send(self, message: can.Message, timeout: float | None = None) -> None:
            _ = message, timeout
            raise expected

    dispatcher = make_dispatcher(monkeypatch, InvalidSendBus())
    try:
        with pytest.raises(ValidationError) as raised:
            dispatcher.send_now(can.Message(arbitration_id=1, data=b"x"))
        assert raised.value is expected
    finally:
        dispatcher.stop()


def test_shutdown_failure_is_preserved_and_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = ValidationError("shutdown failed")

    class FlakyShutdownBus(FakeBus):
        def shutdown(self) -> None:
            self.shutdown_calls += 1
            if self.shutdown_calls == 1:
                raise expected
            self.shutdown_event.set()

    bus = FlakyShutdownBus()
    dispatcher = make_dispatcher(monkeypatch, bus)

    with pytest.raises(ValidationError) as raised:
        dispatcher.stop()
    assert raised.value is expected
    assert dispatcher._shutdown_complete is False

    dispatcher.stop()
    assert dispatcher._shutdown_complete is True
    assert bus.shutdown_calls == 2
