"""An uncertain thread start must never be retried on the same dispatcher."""

import threading

import can
import pytest

from linkerbot_transport.can import can as classic
from linkerbot_transport.errors import TransportError


@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt])
def test_lazy_classic_start_failure_stops_dispatcher_without_accepting_receipt(
    monkeypatch, error_type
):
    closed = threading.Event()

    class Backend:
        def recv(self, timeout):
            closed.wait(timeout)
            return None

        def shutdown(self):
            closed.set()

    monkeypatch.setattr(classic, "open_bus", lambda **kwargs: Backend())
    dispatcher = classic.CANMessageDispatcher(
        "uncertain-start", "virtual", start_send_worker=False
    )
    real_start = threading.Thread.start
    original = error_type("thread start interrupted")
    attempts = []

    def interrupt_start(thread):
        if thread is dispatcher._send_thread:
            attempts.append(thread)
            raise original
        return real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", interrupt_start)
    message = can.Message(arbitration_id=1, data=b"x", is_extended_id=False)
    try:
        with pytest.raises(error_type) as caught:
            dispatcher.send(message)
        assert caught.value is original
        with pytest.raises(TransportError, match="startup failed"):
            dispatcher.send(message)
        assert len(attempts) == 1
        assert dispatcher._send_queue.empty()
        assert not dispatcher._receipts._pending
    finally:
        dispatcher.stop()
        assert closed.is_set()
        assert not dispatcher._recv_thread.is_alive()
        assert not dispatcher._send_thread.is_alive()
