"""Exercise the real python-can SLCAN lifecycle using a serial endpoint only."""

import threading

import can
import pytest
import serial

from linkerbot_transport import SharedTransportPool, TransportState
from linkerbot_transport.can import CANRouteKey, CANTransport
from linkerbot_transport.canfd import CANFDMessage, PythonCANFDTransport
from linkerbot_transport.errors import ClosedError, TransportError


class SerialPort:
    def __init__(self):
        self.writes = []
        self.closed = threading.Event()
        self.write_error = None
        self.close_error = None
        self.close_calls = 0
        self.write_timeout = None

    @property
    def in_waiting(self):
        return 0

    def write(self, data):
        if self.write_error is not None:
            raise self.write_error
        self.writes.append(data)
        return len(data)

    def flush(self):
        pass

    def read(self, count):
        self.closed.wait(0.001)
        return b""

    def close(self):
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error
        self.closed.set()


@pytest.fixture(params=[False, True], ids=["classic", "fd"])
def transport_factory(request):
    def create(bitrate=1_000_000):
        cls = PythonCANFDTransport if request.param else CANTransport
        return cls(
            "COM5", "slcan", bitrate=bitrate, bus_options={"sleep_after_open": 0}
        )

    return create


def test_unplugged_port_recovery_releases_handle_and_invalidates_routes(
    monkeypatch, transport_factory, caplog
):
    ports = []

    def open_serial(*args, **kwargs):
        port = SerialPort()
        ports.append(port)
        return port

    monkeypatch.setattr(serial, "serial_for_url", open_serial)
    transport = transport_factory()
    transport.open()
    try:
        old_route = transport.subscribe_route(CANRouteKey(0x123, False))
        message = (
            CANFDMessage(0x123, b"\x55", is_extended_id=False)
            if isinstance(transport, PythonCANFDTransport)
            else can.Message(arbitration_id=0x123, data=b"\x55", is_extended_id=False)
        )
        transport.send(message).result(timeout=1)
        ports[0].write_error = serial.SerialException("USB unplugged")
        generation = transport.get_health().generation
        transport.recover()
        assert ports[0].closed.is_set() and ports[0].close_calls == 1
        assert len(ports) == 2
        assert transport.get_health().generation == generation + 1
        assert transport.get_health().state is TransportState.OPEN
        with pytest.raises(ClosedError):
            old_route.receive(timeout_s=0)
        assert not any(
            frame.startswith((b"t", b"T", b"d", b"D")) for frame in ports[1].writes
        )
        assert "SLCAN close command failed" in caplog.text
    finally:
        transport.close()
    transport.close()
    assert ports[1].close_calls == 1


def test_serial_release_failure_keeps_claim_and_blocks_reopen(
    monkeypatch, transport_factory
):
    port = SerialPort()
    monkeypatch.setattr(serial, "serial_for_url", lambda *args, **kwargs: port)
    transport = transport_factory()
    other = transport_factory(bitrate=500_000)
    transport.open()
    port.write_error = serial.SerialException("USB unplugged")
    port.close_error = OSError("serial close failed")
    try:
        with pytest.raises(TransportError, match="serial close failed"):
            transport.close()
        with pytest.raises(TransportError, match="different settings"):
            other.open()
        assert transport.get_health().state is TransportState.FAULTED
        assert not port.closed.is_set()
    finally:
        port.close_error = None
        transport.close()
        other.close()
    assert port.closed.is_set()


@pytest.mark.parametrize("interrupt", [False, True])
def test_partial_open_failure_retained_by_pool_until_release_succeeds(
    monkeypatch, transport_factory, interrupt
):
    old = SerialPort()
    old.write_error = (
        KeyboardInterrupt("open interrupted")
        if interrupt
        else serial.SerialException("configure failed")
    )
    old.close_error = OSError("serial close failed")
    ports = [old]
    monkeypatch.setattr(serial, "serial_for_url", lambda *args, **kwargs: ports[-1])
    transport = transport_factory()
    pool = SharedTransportPool()
    error_type = KeyboardInterrupt if interrupt else TransportError
    error_match = "open interrupted" if interrupt else "Could not write"
    try:
        with pytest.raises(error_type, match=error_match):
            pool.acquire(transport.pool_key, lambda: transport)
        assert pool.active_keys() == (transport.pool_key,)
        assert transport.get_health().state is TransportState.FAULTED
        other = transport_factory(bitrate=500_000)
        with pytest.raises(TransportError, match="different settings"):
            other.open()
        other.close()
    finally:
        old.close_error = None
        old.write_error = serial.SerialException("old USB handle invalid")
        ports.append(SerialPort())
        lease = pool.acquire(transport.pool_key, lambda: pytest.fail("lost owner"))
        lease.close()
    assert old.closed.is_set() and ports[-1].closed.is_set()
    assert pool.active_keys() == ()


def test_partial_open_failure_with_successful_cleanup_releases_claim(
    monkeypatch, transport_factory
):
    port = SerialPort()
    port.write_error = serial.SerialException("configure failed")
    monkeypatch.setattr(serial, "serial_for_url", lambda *args, **kwargs: port)
    transport = transport_factory()
    with pytest.raises(TransportError, match="Could not write"):
        transport.open()
    transport.close()
    assert port.closed.is_set() and port.close_calls == 1
    port = SerialPort()
    other = transport_factory(bitrate=500_000)
    other.open()
    other.close()


def test_close_interrupt_releases_serial_but_preserves_interrupt(
    monkeypatch, transport_factory
):
    port = SerialPort()
    monkeypatch.setattr(serial, "serial_for_url", lambda *args, **kwargs: port)
    transport = transport_factory()
    transport.open()
    port.write_error = KeyboardInterrupt("close interrupted")
    with pytest.raises(KeyboardInterrupt, match="close interrupted"):
        transport.close()
    assert port.closed.is_set()
    transport.close()
    assert port.close_calls == 1


def test_close_interrupt_is_not_masked_by_serial_release_failure(
    monkeypatch, transport_factory
):
    port = SerialPort()
    monkeypatch.setattr(serial, "serial_for_url", lambda *args, **kwargs: port)
    transport = transport_factory()
    transport.open()
    port.write_error = KeyboardInterrupt("close interrupted")
    port.close_error = OSError("serial close failed")
    try:
        with pytest.raises(KeyboardInterrupt, match="close interrupted"):
            transport.close()
        assert not port.closed.is_set()
        assert transport.get_health().state is TransportState.FAULTED
    finally:
        port.write_error = None
        port.close_error = None
        transport.close()
    assert port.closed.is_set()
