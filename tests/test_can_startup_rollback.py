"""Lifecycle failures using virtual CAN only, including interrupted startup."""

import threading

import can
import pytest

from linkerbot_transport import TransportState
from linkerbot_transport.can import CANTransport
from linkerbot_transport.can.can import CANMessageDispatcher
from linkerbot_transport.errors import DeviceNotReadyError, TransportError


@pytest.mark.parametrize("error_type", [OSError, KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_subscribe_failure_releases_or_retains_owned_dispatcher(
    monkeypatch, error_type, cleanup_fails
):
    created = []
    original = error_type("subscribe interrupted")

    def factory(on_error):
        dispatcher = CANMessageDispatcher("rollback-subscribe", "virtual", on_error)
        created.append(dispatcher)

        def subscribe(callback):
            raise original

        monkeypatch.setattr(dispatcher, "subscribe", subscribe)
        if cleanup_fails:
            monkeypatch.setattr(
                dispatcher._bus,
                "shutdown",
                lambda: (_ for _ in ()).throw(OSError("shutdown failed")),
            )
        return dispatcher

    transport = CANTransport("unused", dispatcher_factory=factory)
    try:
        with pytest.raises(
            TransportError if error_type is OSError else error_type
        ) as exc:
            transport.open()
        if error_type is not OSError:
            assert exc.value is original
        dispatcher = created[0]
        assert not dispatcher._recv_thread.is_alive()
        assert not dispatcher._send_thread.is_alive()
        assert transport.get_health().state is TransportState.FAULTED
        assert dispatcher._shutdown_complete is (not cleanup_fails)
        if cleanup_fails:
            with pytest.raises(DeviceNotReadyError):
                transport.open()
            monkeypatch.undo()
        transport.close()
        assert dispatcher._bus._is_shutdown
        assert transport.get_health().state is TransportState.CLOSED
    finally:
        monkeypatch.undo()
        transport.close()
        for dispatcher in created:
            dispatcher.stop()


@pytest.mark.parametrize("worker", ["recv_loop", "send_loop"])
@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt])
def test_partial_constructor_stops_already_started_workers(
    monkeypatch, worker, error_type
):
    bus = can.Bus("rollback-worker", interface="virtual")
    started = []
    start = threading.Thread.start
    original = error_type("thread startup failed")

    def start_worker(thread):
        if thread.name == "CANMessageDispatcher." + worker:
            raise original
        start(thread)
        started.append(thread)

    monkeypatch.setattr(can, "Bus", lambda **kwargs: bus)
    monkeypatch.setattr(threading.Thread, "start", start_worker)
    try:
        with pytest.raises(error_type) as exc:
            CANMessageDispatcher("unused", "virtual")
        assert exc.value is original
        assert bus._is_shutdown
        assert all(not thread.is_alive() for thread in started)
    finally:
        bus.shutdown()


def test_failed_constructor_cleanup_is_owned_by_transport_for_retry(monkeypatch):
    bus = can.Bus("rollback-constructor-cleanup", interface="virtual")
    shutdown = bus.shutdown
    calls = []

    def fail_shutdown():
        calls.append(True)
        raise OSError("shutdown must retry")

    monkeypatch.setattr(can, "Bus", lambda **kwargs: bus)
    monkeypatch.setattr(bus, "shutdown", fail_shutdown)
    monkeypatch.setattr(
        threading.Thread,
        "start",
        lambda self: (_ for _ in ()).throw(KeyboardInterrupt("start interrupted")),
    )
    transport = CANTransport("unused", "virtual")
    try:
        with pytest.raises(KeyboardInterrupt, match="start interrupted"):
            transport.open()
        assert transport.get_health().state is TransportState.FAULTED
        assert transport._dispatcher is not None
        assert not bus._is_shutdown
        monkeypatch.setattr(bus, "shutdown", shutdown)
        transport.close()
        assert bus._is_shutdown
        assert len(calls) >= 1
        assert transport.get_health().state is TransportState.CLOSED
    finally:
        monkeypatch.undo()
        transport.close()
        shutdown()


@pytest.mark.parametrize("stage", ["unsubscribe", "stop"])
def test_close_interruption_keeps_resource_retryable(monkeypatch, stage):
    transport = CANTransport("rollback-close", "virtual")
    transport.open()
    dispatcher = transport._dispatcher
    original = KeyboardInterrupt("close interrupted")

    def interrupt(*args):
        raise original

    monkeypatch.setattr(dispatcher, stage, interrupt)
    try:
        with pytest.raises(KeyboardInterrupt) as exc:
            transport.close()
        assert exc.value is original
        assert transport.get_health().state is TransportState.FAULTED
        assert transport._dispatcher is dispatcher
        monkeypatch.undo()
        transport.close()
        assert not dispatcher._recv_thread.is_alive()
        assert dispatcher._bus._is_shutdown
    finally:
        monkeypatch.undo()
        transport.close()
