"""Receive ordering and error delivery across python-can batch boundaries."""

import threading
from unittest.mock import Mock

import can
import pytest

from linkerbot_transport import TransportState
from linkerbot_transport.canfd import CANRouteKey, PythonCANFDTransport
from linkerbot_transport.canfd.dispatcher import CANFDMessageDispatcher
from linkerbot_transport.canfd.python_can_backend import PythonCANFDBackend
from linkerbot_transport.errors import (
    ClosedError,
    TimeoutError,
    TransportError,
    ValidationError,
)


def data_frame(data):
    return can.Message(
        arbitration_id=0x123, is_extended_id=False, is_fd=True, data=data, check=True
    )


@pytest.fixture
def virtual_pair():
    backend = PythonCANFDBackend("fd-receive", "virtual")
    try:
        with can.Bus(
            interface="virtual", channel="fd-receive", ignore_config=True
        ) as peer:
            yield backend, peer
    finally:
        backend.close()


@pytest.mark.parametrize("max_frames", [1, 2, 64])
def test_valid_frames_and_errors_keep_wire_order(virtual_pair, max_frames):
    backend, peer = virtual_pair
    for message in (
        data_frame(b"a"),
        can.Message(is_remote_frame=True, dlc=1),
        data_frame(b"b"),
        can.Message(is_error_frame=True),
        data_frame(b"c"),
        can.Message(is_error_frame=True),
    ):
        peer.send(message)

    events = []
    # Enough calls for one-frame batches, both errors, and the empty tail.
    for _ in range(8):
        try:
            frames = backend.receive(max_frames=max_frames, timeout_ms=0)
            assert len(frames) <= max_frames
            events.extend(frame.data for frame in frames)
        except TransportError as error:
            assert "error frame" in str(error)
            events.append("error")
    assert events == [b"a", b"b", "error", b"c", "error"]
    assert backend.receive(timeout_ms=0) == []


@pytest.mark.parametrize("error_type", [can.CanOperationError, OSError, ValueError])
@pytest.mark.parametrize("has_data", [False, True])
def test_driver_error_preserves_data_and_cause_without_reading_ahead(
    virtual_pair, monkeypatch, error_type, has_data
):
    backend, _ = virtual_pair
    error = error_type("receive failed")
    prefix = data_frame(b"a") if has_data else can.Message(is_remote_frame=True)
    recv = Mock(side_effect=[prefix, error, data_frame(b"b"), None, None])
    monkeypatch.setattr(backend._bus, "recv", recv)

    if has_data:
        assert [frame.data for frame in backend.receive()] == [b"a"]
        assert recv.call_count == 2
        # Invalid calls must not consume a pending driver error.
        with pytest.raises(ValidationError):
            backend.receive(max_frames=0)
    expected_type = ValueError if error_type is ValueError else TransportError
    with pytest.raises(expected_type, match="receive failed") as raised:
        backend.receive()
    assert recv.call_count == 2
    if expected_type is TransportError:
        assert raised.value.cause is error
        assert raised.value.__cause__ is error
    else:
        assert raised.value is error
    assert [frame.data for frame in backend.receive()] == [b"b"]
    assert backend.receive(timeout_ms=0) == []


@pytest.mark.parametrize(
    "malformed,error_type",
    [
        (can.Message(is_fd=True, data=b"x", dlc=2), TransportError),
        (can.Message(is_fd=True, data=bytes(9)), TransportError),
        (can.Message(is_fd=True, data=bytes(65)), ValidationError),
        (can.Message(is_extended_id=False, arbitration_id=0x800), ValidationError),
    ],
)
def test_conversion_error_preserves_preceding_valid_frames(
    virtual_pair, malformed, error_type
):
    backend, peer = virtual_pair
    for message in (data_frame(b"a"), malformed, data_frame(b"b")):
        peer.send(message)
    assert [frame.data for frame in backend.receive(timeout_ms=0)] == [b"a"]
    with pytest.raises(error_type):
        backend.receive(timeout_ms=0)
    assert [frame.data for frame in backend.receive(timeout_ms=0)] == [b"b"]
    assert backend.receive(timeout_ms=0) == []


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit])
def test_receive_interrupt_is_not_deferred(virtual_pair, monkeypatch, error_type):
    backend, _ = virtual_pair
    error = error_type("interrupted")
    monkeypatch.setattr(
        backend._bus, "recv", Mock(side_effect=[data_frame(b"a"), error, None])
    )
    with pytest.raises(error_type) as raised:
        backend.receive()
    assert raised.value is error
    assert backend.receive(timeout_ms=0) == []


def test_close_discards_pending_error_and_unread_old_data(virtual_pair):
    backend, peer = virtual_pair
    for message in (
        data_frame(b"a"),
        can.Message(is_error_frame=True),
        data_frame(b"b"),
    ):
        peer.send(message)
    assert [frame.data for frame in backend.receive(timeout_ms=0)] == [b"a"]
    backend.close()
    with pytest.raises(TransportError, match="closed"):
        backend.receive(timeout_ms=0)

    fresh = PythonCANFDBackend("fd-receive", "virtual")
    try:
        assert fresh.receive(timeout_ms=0) == []
        peer.send(data_frame(b"c"))
        assert [frame.data for frame in fresh.receive(timeout_ms=0)] == [b"c"]
    finally:
        fresh.close()


def test_dispatcher_delivers_data_before_consecutive_errors_fault(
    virtual_pair, monkeypatch, caplog
):
    backend, peer = virtual_pair
    ready = threading.Event()
    faulted = threading.Event()
    events = []
    original_recv = backend._bus.recv

    def receive(timeout):
        assert ready.wait(1)
        return original_recv(timeout)

    def on_error(error):
        events.append(str(error))
        faulted.set()

    monkeypatch.setattr(backend._bus, "recv", receive)
    dispatcher = CANFDMessageDispatcher(
        interface=backend, max_consecutive_errors=2, on_bus_error=on_error
    )
    try:
        dispatcher.subscribe(lambda frame: events.append(frame.data))
        for message in (
            data_frame(b"a"),
            can.Message(is_error_frame=True),
            can.Message(is_error_frame=True),
        ):
            peer.send(message)
        ready.set()
        assert faulted.wait(1)
        assert events == [b"a", "virtual reported a CAN bus error frame"]
        assert "CANFD receive error (1/2)" in caplog.text
        assert "CANFD receive failed after 2 consecutive errors" in caplog.text
    finally:
        ready.set()
        dispatcher.stop()


def test_public_transport_receives_around_error_and_recovers(monkeypatch, caplog):
    ready = threading.Event()

    def create(on_bus_error):
        backend = PythonCANFDBackend("fd-receive-recover", "virtual")
        original_recv = backend._bus.recv

        def receive(timeout):
            assert ready.wait(1)
            return original_recv(timeout)

        monkeypatch.setattr(backend._bus, "recv", receive)
        return CANFDMessageDispatcher(
            interface=backend, max_consecutive_errors=2, on_bus_error=on_bus_error
        )

    transport = PythonCANFDTransport(
        "fd-receive-recover", "virtual", dispatcher_factory=create
    )
    with can.Bus(
        interface="virtual", channel="fd-receive-recover", ignore_config=True
    ) as peer:
        transport.open()
        try:
            route = transport.subscribe_route(CANRouteKey(0x123, False))
            for message in (
                data_frame(b"a"),
                can.Message(is_error_frame=True),
                data_frame(b"b"),
            ):
                peer.send(message)
            ready.set()
            assert route.receive(timeout_s=1).message.data == b"a"
            assert route.receive(timeout_s=1).message.data == b"b"
            with pytest.raises(TimeoutError):
                route.receive(timeout_s=0)
            assert transport.get_health().state is TransportState.OPEN
            assert caplog.text.count("CANFD receive error (1/2)") == 1

            generation = transport.get_health().generation
            transport.recover()
            with pytest.raises(ClosedError):
                route.receive(timeout_s=0)
            fresh = transport.subscribe_route(CANRouteKey(0x123, False))
            peer.send(data_frame(b"c"))
            received = fresh.receive(timeout_s=1)
            assert received.message.data == b"c"
            assert received.generation == generation + 1
            with pytest.raises(TimeoutError):
                fresh.receive(timeout_s=0)
            assert peer.recv(timeout=0) is None  # No transmit or historical replay.
        finally:
            ready.set()
            transport.close()
