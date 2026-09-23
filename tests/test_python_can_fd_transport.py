"""CAN FD integration using real python-can with virtual/serial test endpoints."""

import threading

import can
import pytest

from linkerbot_transport.can import CANTransport
from linkerbot_transport.canfd import (
    CANFDMessage,
    CANRouteKey,
    PythonCANFDTransport,
    dlc_to_length,
)
from linkerbot_transport.canfd.python_can_backend import PythonCANFDBackend
from linkerbot_transport.errors import TransportError, ValidationError


class SerialEndpoint:
    """Serial bytes only; parsing/framing is done by the installed python-can."""

    def __init__(self):
        self.writes = []
        self.incoming = bytearray()
        self.closed = False
        self.write_timeout = None

    @property
    def in_waiting(self):
        return len(self.incoming)

    def write(self, data):
        self.writes.append(data)
        return len(data)

    def read(self, size):
        data = bytes(self.incoming[:size])
        del self.incoming[:size]
        return data

    def flush(self):
        pass

    def close(self):
        self.closed = True


@pytest.mark.parametrize(
    "extended,brs,prefix",
    [
        (False, False, "d"),
        (True, False, "D"),
        (False, True, "b"),
        (True, True, "B"),
    ],
)
@pytest.mark.parametrize("dlc", [0, 8, 9, 13, 15])
def test_real_slcan_fd_config_and_wire_round_trip(
    monkeypatch, extended, brs, prefix, dlc
):
    import serial

    port = SerialEndpoint()
    calls = []

    def open_serial(channel, **kwargs):
        calls.append((channel, kwargs))
        return port

    monkeypatch.setattr(serial, "serial_for_url", open_serial)
    backend = PythonCANFDBackend(
        "COM5@1000000",
        "slcan",
        bus_options={"sleep_after_open": 0},
    )
    try:
        assert calls[0][0] == "COM5"
        assert calls[0][1]["baudrate"] == 1_000_000
        assert b"S8\r" in port.writes  # 1 Mbit/s nominal
        assert b"Y5\r" in port.writes  # 5 Mbit/s data, via BitTimingFd
        assert b"O\r" in port.writes
        size = dlc_to_length(dlc)
        data = bytes(range(size))
        frame = CANFDMessage(
            0x123, data, is_extended_id=extended, frame_type=0x0C if brs else 0x04
        )
        backend.send(frame, timeout_ms=20)
        can_id = "00000123" if extended else "123"
        expected = f"{prefix}{can_id}{dlc:X}{data.hex().upper()}\r".encode()
        assert port.writes[-1] == expected
        assert port.write_timeout == 0.02
        port.incoming.extend(expected)
        received = backend.receive(max_frames=1, timeout_ms=0)
        assert len(received) == 1
        assert received[0].arbitration_id == frame.arbitration_id
        assert received[0].data == data
        assert received[0].dlc == dlc
        assert received[0].is_extended_id == extended
        assert received[0].frame_type == frame.frame_type
        assert received[0].timestamp > 0
    finally:
        backend.close()
    assert port.closed
    assert port.writes[-1] == b"C\r"


def test_slcan_constructor_does_not_touch_serial_and_normalizes_baudrate():
    one = PythonCANFDTransport("COM5@1000000", "slcan")
    two = PythonCANFDTransport("COM5", "slcan", bus_options={"tty_baudrate": 1_000_000})
    assert one.pool_key == two.pool_key


@pytest.mark.parametrize(
    "kwargs",
    [
        {"data_bitrate": 4_000_000},
        {"bitrate": True},
        {"fd": "true"},
        {"bus_options": {"tty_baudrate": 0}},
        {"bus_options": {"fd": True}},
        {"timing": "not-a-timing-object"},
    ],
)
def test_invalid_fd_config_rejected_before_open(kwargs):
    with pytest.raises(ValidationError):
        PythonCANFDTransport("COM5", "slcan", **kwargs)


@pytest.mark.parametrize("dlc", range(16))
def test_virtual_fd_uses_encoded_dlc_and_defaults_brs_off(dlc):
    backend = PythonCANFDBackend("fd-wire", "virtual")
    try:
        with can.Bus(
            interface="virtual", channel="fd-wire", ignore_config=True
        ) as peer:
            backend.send(CANFDMessage(0x123, b"", dlc=dlc))
            message = peer.recv(timeout=1)
            assert message is not None
            assert message.is_fd and not message.bitrate_switch
            assert message.dlc == dlc_to_length(dlc)
            assert bytes(message.data) == bytes(dlc_to_length(dlc))
    finally:
        backend.close()


def test_fd_public_transport_routes_classic_and_fd_and_recovers():
    transport = PythonCANFDTransport("fd-lifecycle", "virtual")
    with can.Bus(
        interface="virtual", channel="fd-lifecycle", ignore_config=True
    ) as peer:
        transport.open()
        try:
            route = transport.subscribe_route(CANRouteKey(0x123, False))
            transport.send(CANFDMessage(0x123, b"abc", is_extended_id=False)).result(
                timeout=1
            )
            assert peer.recv(timeout=1).is_fd
            peer.send(
                can.Message(
                    arbitration_id=0x123, is_extended_id=False, data=b"x", check=True
                )
            )
            assert route.receive(timeout_s=1).message.frame_type == 0
            transport.recover()
            fresh = transport.subscribe_route(CANRouteKey(0x123, False))
            peer.send(
                can.Message(
                    arbitration_id=0x123,
                    is_extended_id=False,
                    data=bytes(64),
                    is_fd=True,
                    bitrate_switch=True,
                    check=True,
                )
            )
            received = fresh.receive(timeout_s=1).message
            assert received.frame_type == 0x0C and received.dlc == 15
            assert peer.recv(timeout=0.02) is None
        finally:
            transport.close()


def test_classic_and_fd_config_conflict_before_driver_reopen():
    classic = CANTransport("conflicting-modes", "virtual")
    fd = PythonCANFDTransport("conflicting-modes", "virtual")
    classic.open()
    try:
        with pytest.raises(TransportError, match="different settings"):
            fd.open()
    finally:
        classic.close()
        fd.close()
    fd.open()
    fd.close()


def test_error_and_remote_frames_cannot_become_device_replies():
    backend = PythonCANFDBackend("fd-errors", "virtual")
    try:
        with can.Bus(
            interface="virtual", channel="fd-errors", ignore_config=True
        ) as peer:
            peer.send(can.Message(arbitration_id=1, is_remote_frame=True, dlc=2))
            assert backend.receive(timeout_ms=0) == []
            peer.send(can.Message(is_error_frame=True))
            with pytest.raises(TransportError, match="error frame"):
                backend.receive(timeout_ms=0)
    finally:
        backend.close()


def test_startup_interruption_retains_failed_cleanup_for_retry(monkeypatch):
    original_start = threading.Thread.start
    original_close = PythonCANFDBackend.close
    created = []

    def fail_second_start(thread):
        if thread.name == "CANFDMessageDispatcher.send_loop":
            raise KeyboardInterrupt("interrupted startup")
        original_start(thread)

    def fail_close(backend):
        created.append(backend)
        raise OSError("USB shutdown failed")

    monkeypatch.setattr(threading.Thread, "start", fail_second_start)
    monkeypatch.setattr(PythonCANFDBackend, "close", fail_close)
    transport = PythonCANFDTransport("fd-startup", "virtual")
    with pytest.raises(KeyboardInterrupt, match="startup"):
        transport.open()
    assert created
    conflicting = PythonCANFDTransport("fd-startup", "virtual", bitrate=500_000)
    with pytest.raises(TransportError, match="different settings"):
        conflicting.open()
    monkeypatch.setattr(PythonCANFDBackend, "close", original_close)
    monkeypatch.setattr(threading.Thread, "start", original_start)
    transport.close()
    conflicting.close()
    conflicting.open()
    conflicting.close()
