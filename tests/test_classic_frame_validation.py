"""Exercise frame boundaries through a real virtual bus and dispatcher."""

import threading

import can
import pytest

from linkerbot_transport.can import CANRouteKey, CANTransport
from linkerbot_transport.can.can import CANMessageDispatcher
from linkerbot_transport.errors import ValidationError


def invalid_frames():
    return [
        can.Message(arbitration_id=0x123, is_fd=True, data=bytes(12)),
        can.Message(arbitration_id=0x123, dlc=8, data=b"x"),
        can.Message(arbitration_id=0x123, data=bytes(9)),
        can.Message(arbitration_id=0x800, is_extended_id=False, data=b"x"),
        can.Message(arbitration_id=0x123, bitrate_switch=True),
    ]


@pytest.mark.parametrize("queued", [False, True])
@pytest.mark.parametrize("message", invalid_frames())
def test_invalid_tx_never_reaches_backend_or_io_fault_counter(
    monkeypatch, queued, message
):
    dispatcher = CANMessageDispatcher(
        "classic-invalid", "virtual", start_send_worker=False
    )
    sent = []
    monkeypatch.setattr(dispatcher._bus, "send", lambda frame, **kw: sent.append(frame))
    try:
        with pytest.raises(ValidationError):
            (dispatcher.send if queued else dispatcher.send_now)(message)
        assert sent == []
        assert dispatcher._consecutive_send_errors == 0
    finally:
        dispatcher.stop()


def test_queued_message_is_an_owned_snapshot(monkeypatch):
    dispatcher = CANMessageDispatcher(
        "classic-snapshot", "virtual", start_send_worker=False
    )
    entered = threading.Event()
    release = threading.Event()
    sent = []

    def blocked_send(frame, **kwargs):
        entered.set()
        assert release.wait(1)
        sent.append((frame.arbitration_id, bytes(frame.data)))

    monkeypatch.setattr(dispatcher._bus, "send", blocked_send)
    message = can.Message(arbitration_id=0x123, data=b"x")
    try:
        receipt = dispatcher.send(message)
        assert entered.wait(1)
        message.arbitration_id = 0x124
        message.data[:] = b"y"
        release.set()
        receipt.result(timeout=1)
        assert sent == [(0x123, b"x")]
    finally:
        release.set()
        dispatcher.stop()


def test_valid_classic_remote_tx_remains_supported():
    dispatcher = CANMessageDispatcher("classic-rtr", "virtual")
    with can.Bus(
        channel="classic-rtr", interface="virtual", ignore_config=True
    ) as peer:
        try:
            message = can.Message(
                arbitration_id=0x123, is_remote_frame=True, dlc=8, check=True
            )
            dispatcher.send(message).result(timeout=1)
            received = peer.recv(timeout=1)
            assert received.is_remote_frame and received.dlc == 8
        finally:
            dispatcher.stop()


@pytest.mark.parametrize(
    "message",
    invalid_frames() + [can.Message(arbitration_id=0x123, is_remote_frame=True, dlc=8)],
)
def test_rx_drops_invalid_or_remote_frames_before_routing_and_metrics(message, caplog):
    transport = CANTransport("classic-rx-validation", "virtual")
    # Use a custom route extractor to prove it receives only validated original data.
    with can.Bus(
        channel="classic-rx-validation", interface="virtual", ignore_config=True
    ) as peer:
        transport.open()
        route = transport.subscribe_route(CANRouteKey(0x123, False))
        try:
            message.is_extended_id = False
            peer.send(message)
            peer.send(
                can.Message(
                    arbitration_id=0x123, is_extended_id=False, data=b"ok", check=True
                )
            )
            received = route.receive(timeout_s=1)
            assert bytes(received.message.data) == b"ok"
            assert route.try_receive() is None
            health = transport.get_health()
            assert health.rx_frames == 1
            assert health.rx_bytes == 2
            if not message.is_remote_frame:
                assert "invalid classic CAN" in caplog.text
        finally:
            transport.close()
