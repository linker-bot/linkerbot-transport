"""A high-level CAN connection needs no idle legacy send worker."""

from linkerbot_transport.can import CANTransport
from linkerbot_transport.can.can import CANMessageDispatcher


def test_managed_transport_has_no_idle_send_worker_and_legacy_send_starts_it():
    import can

    transport = CANTransport("perf-no-idle-worker", "virtual")
    transport.open()
    try:
        dispatcher = transport._dispatcher
        assert not dispatcher._send_thread.is_alive()
        transport.send(can.Message(arbitration_id=1)).result(timeout=1)
        assert not dispatcher._send_thread.is_alive()
        dispatcher.send(can.Message(arbitration_id=2)).result(timeout=1)
        assert dispatcher._send_thread.is_alive()
    finally:
        transport.close()
    assert not dispatcher._send_thread.is_alive()
    transport.open()
    try:
        assert not transport._dispatcher._send_thread.is_alive()
    finally:
        transport.close()
    with CANMessageDispatcher("perf-legacy-worker", "virtual") as legacy:
        assert legacy._send_thread.is_alive()
