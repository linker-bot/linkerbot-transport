"""Driver-independent Windows configuration and real python-can routing checks."""

import threading

import can
import pytest

from linkerbot_transport import SharedTransportPool
from linkerbot_transport.can import CANRouteKey, CANTransport
from linkerbot_transport.can import can as can_dispatcher
from linkerbot_transport.errors import TransportError, ValidationError


class StubBus:
    def __init__(self):
        self.closed = threading.Event()
        self.fail_shutdown = False

    def recv(self, timeout):
        self.closed.wait(timeout)
        return None

    def shutdown(self):
        if self.fail_shutdown:
            raise OSError("device still busy")
        self.closed.set()


def test_pcan_explicit_settings_are_snapshotted_and_open_is_lazy(monkeypatch):
    calls = []
    bus = StubBus()

    def create_bus(**kwargs):
        calls.append(kwargs)
        return bus

    monkeypatch.setattr(can_dispatcher, "open_bus", create_bus)
    options = {"auto_reset": True}
    transport = CANTransport(
        "PCAN_USBBUS1", "pcan", bitrate=500_000, bus_options=options
    )
    options["auto_reset"] = False
    assert calls == []
    transport.open()
    try:
        assert calls == [
            dict(
                channel="PCAN_USBBUS1",
                interface="pcan",
                bitrate=500_000,
                fd=False,
                receive_own_messages=False,
                auto_reset=True,
            )
        ]
    finally:
        transport.close()


@pytest.mark.parametrize(
    "options",
    [
        {"bitrate": 123},
        {"channel": "other"},
        {"fd": True},
        {"device_id": 123},
        {"ignore_config": False},
        {"timing": object()},
        {"mutable": []},
        {"timeout": float("nan")},
    ],
)
def test_invalid_backend_options_fail_without_device_io(options):
    with pytest.raises(ValidationError):
        CANTransport("PCAN_USBBUS1", "pcan", bus_options=options)


def test_pool_shares_matching_settings_but_rejects_live_bitrate_conflict(monkeypatch):
    calls = []

    def create_bus(**kwargs):
        calls.append(kwargs)
        return StubBus()

    monkeypatch.setattr(can_dispatcher, "open_bus", create_bus)
    pool = SharedTransportPool()
    first = CANTransport("PCAN_USBBUS1", "pcan", bitrate=1_000_000)
    same = CANTransport("PCAN_USBBUS1", "pcan", bitrate=1_000_000)
    other = CANTransport("PCAN_USBBUS1", "pcan", bitrate=500_000)
    lease1 = pool.acquire(first.pool_key, lambda: first)
    lease2 = pool.acquire(same.pool_key, lambda: same)
    try:
        assert lease1.transport is lease2.transport
        assert len(calls) == 1
        with pytest.raises(TransportError, match="different settings"):
            pool.acquire(other.pool_key, lambda: other)
        assert len(calls) == 1
    finally:
        lease2.close()
        lease1.close()
    lease3 = pool.acquire(other.pool_key, lambda: other)
    lease3.close()
    assert len(calls) == 2


def test_failed_shutdown_keeps_channel_claim_until_cleanup_succeeds(monkeypatch):
    bus = StubBus()
    monkeypatch.setattr(can_dispatcher, "open_bus", lambda **kwargs: bus)
    first = CANTransport("PCAN_USBBUS2", "pcan")
    other = CANTransport("PCAN_USBBUS2", "pcan", bitrate=500_000)
    first.open()
    bus.fail_shutdown = True
    with pytest.raises(TransportError, match="busy"):
        first.close()
    with pytest.raises(TransportError, match="different settings"):
        other.open()
    bus.fail_shutdown = False
    first.close()
    other.close()


def test_driver_open_failure_releases_claim(monkeypatch):
    def fail(**kwargs):
        raise can.CanInitializationError("driver missing")

    monkeypatch.setattr(can_dispatcher, "open_bus", fail)
    first = CANTransport("PCAN_USBBUS3", "pcan")
    with pytest.raises(TransportError, match="driver missing"):
        first.open()
    monkeypatch.setattr(can_dispatcher, "open_bus", lambda **kwargs: StubBus())
    other = CANTransport("PCAN_USBBUS3", "pcan", bitrate=500_000)
    other.open()
    other.close()
    first.close()


def test_real_virtual_bus_routes_data_and_recovers_without_replaying(monkeypatch):
    # Ambient python-can config must never change the immutable connection key.
    monkeypatch.setitem(can.rc, "receive_own_messages", True)
    transport = CANTransport("transport-pcan-contract", "virtual")
    with can.Bus(
        channel="transport-pcan-contract", interface="virtual", ignore_config=True
    ) as peer:
        transport.open()
        try:
            route = transport.subscribe_route(CANRouteKey(0x123, False))
            outgoing = can.Message(
                arbitration_id=0x123, data=[1, 2], is_extended_id=False
            )
            transport.send(outgoing).result(timeout=1)
            received = peer.recv(timeout=1)
            assert received is not None and received.data == outgoing.data
            assert route.get_health().queue_depth == 0
            peer.send(outgoing)
            assert route.receive(timeout_s=1).message.data == outgoing.data
            generation = transport.get_health().generation
            transport.recover()
            assert transport.get_health().generation == generation + 1
            assert peer.recv(timeout=0.02) is None
            fresh = transport.subscribe_route(CANRouteKey(0x123, False))
            peer.send(outgoing)
            assert fresh.receive(timeout_s=1).generation == generation + 1
        finally:
            transport.close()
