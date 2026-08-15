"""Explicit transport lifecycle, health and recovery tests."""

import threading
from collections.abc import Callable

import can
import pytest

from linkerbot_transport.can import CANTransport
from linkerbot_transport.canfd import (
    CANFDMessage,
    CANFDTransport,
    SocketCANFDTransport,
)
from linkerbot_transport.contracts import MessageTransport, TransportState
from linkerbot_transport.errors import ErrorCode, TransportError
from linkerbot_transport.testing import FakeDispatcher, FakeTransport


class FakeClock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_concrete_transport_constructors_do_not_create_dispatchers() -> None:
    can_created: list[FakeDispatcher[can.Message]] = []
    canfd_created: list[FakeDispatcher[CANFDMessage]] = []

    def create_can(
        on_bus_error: Callable[[Exception], None],
    ) -> FakeDispatcher[can.Message]:
        dispatcher = FakeDispatcher[can.Message](on_bus_error)
        can_created.append(dispatcher)
        return dispatcher

    def create_canfd(
        on_bus_error: Callable[[Exception], None],
    ) -> FakeDispatcher[CANFDMessage]:
        dispatcher = FakeDispatcher[CANFDMessage](on_bus_error)
        canfd_created.append(dispatcher)
        return dispatcher

    classic = CANTransport("can0", dispatcher_factory=create_can)
    flexible = CANFDTransport(dispatcher_factory=create_canfd)

    assert can_created == []
    assert canfd_created == []
    assert classic.get_health().state is TransportState.CLOSED
    assert flexible.get_health().state is TransportState.CLOSED

    classic.open()
    flexible.open()
    assert len(can_created) == 1
    assert len(canfd_created) == 1
    classic.close()
    flexible.close()


def test_default_constructors_do_not_touch_physical_channels() -> None:
    classic = CANTransport("missing-can-interface")
    vendor_fd = CANFDTransport(device_index=999)
    socket_fd = SocketCANFDTransport("missing-can-interface")

    assert classic.get_health().state is TransportState.CLOSED
    assert vendor_fd.get_health().state is TransportState.CLOSED
    assert socket_fd.get_health().state is TransportState.CLOSED


def test_open_and_close_are_idempotent() -> None:
    transport = FakeTransport[int]()

    transport.open()
    transport.open()
    assert len(transport.dispatchers) == 1
    assert transport.get_health().generation == 1

    dispatcher = transport.current_dispatcher
    transport.close()
    transport.close()
    assert dispatcher.stop_count == 1
    assert transport.get_health().state is TransportState.CLOSED


def test_concurrent_open_creates_one_connection() -> None:
    transport = FakeTransport[int]()
    start = threading.Barrier(17)
    errors: list[BaseException] = []

    def open_transport() -> None:
        start.wait()
        try:
            transport.open()
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=open_transport) for _ in range(16)]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(timeout=1.0)

    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    assert len(transport.dispatchers) == 1
    transport.close()


def test_health_tracks_send_receive_and_ages() -> None:
    clock = FakeClock(10.0)
    transport = FakeTransport[int](monotonic=clock)
    received: list[int] = []
    transport.subscribe(received.append)
    transport.open()

    transport.send(1)
    clock.advance(0.25)
    transport.emit(2)
    clock.advance(0.75)

    health = transport.get_health()
    assert health.connected
    assert health.last_successful_tx_s == 10.0
    assert health.last_successful_rx_s == 10.25
    assert health.tx_age_s == 1.0
    assert health.rx_age_s == 0.75
    assert health.subscriber_count == 1
    assert health.pending_tx == 0
    assert received == [2]
    transport.close()


def test_send_failure_is_classified_without_fake_acknowledgement() -> None:
    transport = FakeTransport[int]()
    transport.open()
    transport.current_dispatcher.send_error = OSError("write failed")

    with pytest.raises(TransportError, match="write failed"):
        transport.send(1)

    health = transport.get_health()
    assert health.state is TransportState.OPEN
    assert health.consecutive_errors == 1
    assert health.last_error_code is ErrorCode.TRANSPORT
    assert transport.sent == ()
    transport.close()


def test_recover_replaces_connection_without_replaying_messages() -> None:
    transport = FakeTransport[int]()
    transport.open()
    transport.send(1)
    first = transport.current_dispatcher
    first.disconnect(OSError("unplugged"))
    assert transport.get_health().state is TransportState.FAULTED

    transport.recover()

    health = transport.get_health()
    assert health.state is TransportState.OPEN
    assert health.generation == 2
    assert health.reconnect_count == 1
    assert len(transport.dispatchers) == 2
    assert transport.current_dispatcher.sent == []
    assert transport.sent == (1,)
    transport.close()


def test_close_failure_is_retryable() -> None:
    transport = FakeTransport[int]()
    transport.open()
    dispatcher = transport.current_dispatcher
    dispatcher.stop_error = OSError("close failed")

    with pytest.raises(TransportError, match="close failed"):
        transport.close()

    assert transport.get_health().state is TransportState.FAULTED
    assert transport.current_dispatcher is dispatcher
    transport.close()
    assert dispatcher.stop_count == 2
    assert transport.get_health().state is TransportState.CLOSED


def test_dispatcher_fault_during_open_fails_the_open() -> None:
    dispatcher: FakeDispatcher[can.Message] | None = None

    def create(
        on_bus_error: Callable[[Exception], None],
    ) -> FakeDispatcher[can.Message]:
        nonlocal dispatcher
        dispatcher = FakeDispatcher[can.Message](on_bus_error)
        dispatcher.disconnect(OSError("failed while opening"))
        return dispatcher

    transport = CANTransport("can0", dispatcher_factory=create)

    with pytest.raises(TransportError, match="failed while opening"):
        transport.open()

    assert dispatcher is not None
    assert dispatcher.stop_count == 1
    assert transport.get_health().state is TransportState.FAULTED
    assert transport.get_health().generation == 0


def test_late_error_from_closed_generation_is_ignored() -> None:
    transport = FakeTransport[int]()
    transport.open()
    dispatcher = transport.current_dispatcher
    transport.close()

    dispatcher.disconnect(OSError("late error"))

    health = transport.get_health()
    assert health.state is TransportState.CLOSED
    assert health.consecutive_errors == 0


def test_failed_open_cleanup_retains_dispatcher_for_close_retry() -> None:
    class FailingSubscribeDispatcher(FakeDispatcher[can.Message]):
        def subscribe(self, callback: Callable[[can.Message], None]) -> None:
            raise OSError("subscribe failed")

    dispatcher: FailingSubscribeDispatcher | None = None

    def create(
        on_bus_error: Callable[[Exception], None],
    ) -> FailingSubscribeDispatcher:
        nonlocal dispatcher
        dispatcher = FailingSubscribeDispatcher(on_bus_error)
        dispatcher.stop_error = OSError("cleanup failed")
        return dispatcher

    transport = CANTransport("can0", dispatcher_factory=create)

    with pytest.raises(TransportError, match="subscribe failed"):
        transport.open()

    assert dispatcher is not None
    assert dispatcher.stop_count == 1
    assert transport.get_health().state is TransportState.FAULTED
    transport.close()
    assert dispatcher.stop_count == 2
    assert transport.get_health().state is TransportState.CLOSED


def test_callback_close_prevents_later_callbacks() -> None:
    transport = FakeTransport[int]()
    callbacks: list[str] = []

    def first(message: int) -> None:
        callbacks.append(f"first:{message}")
        transport.close()

    def second(message: int) -> None:
        callbacks.append(f"second:{message}")

    transport.subscribe(first)
    transport.subscribe(second)
    transport.open()
    dispatcher = transport.current_dispatcher

    dispatcher.emit(7)
    dispatcher.emit(8)

    assert callbacks == ["first:7"]
    assert transport.get_health().state is TransportState.CLOSED


def test_subscriber_failure_does_not_block_other_subscribers() -> None:
    transport = FakeTransport[int]()
    received: list[int] = []

    def fail(message: int) -> None:
        raise RuntimeError(f"cannot process {message}")

    transport.subscribe(fail)
    transport.subscribe(received.append)
    transport.open()
    transport.emit(3)

    assert received == [3]
    transport.close()


def test_fake_transport_satisfies_message_contract() -> None:
    transport = FakeTransport[int]()
    assert isinstance(transport, MessageTransport)


def test_repeated_lifecycle_does_not_reuse_closed_dispatcher() -> None:
    transport = FakeTransport[int]()

    for _ in range(100):
        transport.open()
        transport.close()

    assert len(transport.dispatchers) == 100
    assert all(dispatcher.stop_count == 1 for dispatcher in transport.dispatchers)
    assert transport.get_health().generation == 100
