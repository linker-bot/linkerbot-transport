"""Classic CAN error frames must follow the receive fault policy."""

import queue
import threading

import can
import pytest

from linkerbot_transport import CANRouteKey, TransportState
from linkerbot_transport.can import CANTransport
from linkerbot_transport.can import can as classic
from linkerbot_transport.errors import TransportError


class DiagnosticBus:
    def __init__(self):
        self.incoming = queue.Queue()
        self.closed = False

    def recv(self, timeout):
        try:
            return self.incoming.get(timeout=timeout)
        except queue.Empty:
            return None

    def shutdown(self):
        self.closed = True


def test_classic_error_frame_faults_without_routing_or_recording_success(monkeypatch):
    bus = DiagnosticBus()
    monkeypatch.setattr(classic, "open_bus", lambda **_: bus)
    transport = CANTransport("audit-only", "virtual", max_consecutive_errors=1)
    transport.open()
    dispatcher = transport._dispatcher
    try:
        route = transport.subscribe_route(CANRouteKey(0x28, False))
        bus.incoming.put(
            can.Message(
                arbitration_id=0x28,
                is_extended_id=False,
                is_error_frame=True,
                data=b"bus",
            )
        )
        with pytest.raises(TransportError, match="error frame") as raised:
            route.receive(timeout_s=1)
        assert "ID=0x28" in str(raised.value)
        assert "is_error_frame=True" in str(raised.value)
        assert "data=627573" in str(raised.value)
        health = transport.get_health()
        assert health.state is TransportState.FAULTED
        assert health.rx_frames == 0 and health.rx_bytes == 0
        assert health.last_successful_rx_s is None
    finally:
        transport.close()
        assert not dispatcher._recv_thread.is_alive()
        assert bus.closed


def test_data_resets_classic_error_streak_and_recovery_discards_old_routes(monkeypatch):
    buses = []

    def open_bus(**_):
        bus = DiagnosticBus()
        buses.append(bus)
        return bus

    monkeypatch.setattr(classic, "open_bus", open_bus)
    transport = CANTransport("audit-only", "virtual", max_consecutive_errors=2)
    transport.open()
    raw = []
    delivered = threading.Event()

    def on_raw(message):
        raw.append(message)
        delivered.set()

    transport._dispatcher.subscribe(on_raw)
    try:
        route = transport.subscribe_route(CANRouteKey(0x28, False))
        error = can.Message(
            arbitration_id=0x28, is_extended_id=False, is_error_frame=True
        )
        data = can.Message(arbitration_id=0x28, is_extended_id=False, data=b"ok")
        buses[0].incoming.put(error)
        buses[0].incoming.put(data)
        assert route.receive(timeout_s=1).message.data == b"ok"
        assert delivered.wait(1)
        assert raw == [data]
        assert transport.get_health().state is TransportState.OPEN
        buses[0].incoming.put(error)
        buses[0].incoming.put(error)
        with pytest.raises(TransportError, match="error frame"):
            route.receive(timeout_s=1)
        assert transport.get_health().rx_frames == 1
        transport.recover()
        fresh = transport.subscribe_route(CANRouteKey(0x28, False))
        buses[1].incoming.put(data)
        assert fresh.receive(timeout_s=1).generation == 2
        assert not route.get_health().active
    finally:
        transport.close()
        assert all(bus.closed for bus in buses)
