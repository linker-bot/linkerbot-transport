"""Lifecycle callbacks cannot replace resources owned by an outer operation."""

import signal
import threading

import pytest

from linkerbot_transport import SharedTransportPool, TransportState
from linkerbot_transport.canfd import CANFDMessage, CANFDTransport
from linkerbot_transport.canfd.dispatcher import CANFDMessageDispatcher
from linkerbot_transport.errors import DeviceNotReadyError
from linkerbot_transport.testing import FakeDispatcher, FakeTransport


class GatedBackend:
    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()
        self.closed = threading.Event()

    def send(self, message):
        self.started.set()
        assert self.release.wait(2)

    def receive(self, *, max_frames, timeout_ms):
        self.closed.wait(timeout_ms / 1000)
        return []

    def close(self):
        self.release.set()
        self.closed.set()


@pytest.mark.parametrize("operation", ["open", "close", "recover", "acquire"])
def test_pending_completion_cannot_reenter_outer_close(operation):
    pool = SharedTransportPool()
    backends, dispatchers, callback_errors = [], [], []

    def create_dispatcher(on_bus_error):
        backend = GatedBackend()
        backends.append(backend)
        dispatcher = CANFDMessageDispatcher(
            interface=backend, on_bus_error=on_bus_error
        )
        dispatchers.append(dispatcher)
        return dispatcher

    def create():
        return CANFDTransport(dispatcher_factory=create_dispatcher)

    lease = pool.acquire("shared", create)
    transport = lease.transport
    try:
        first = dispatchers[0].send(CANFDMessage(1, b"a"))
        assert backends[0].started.wait(1)
        pending = dispatchers[0].send(CANFDMessage(2, b"b"))

        def completed(_):
            backends[0].release.set()
            try:
                if operation == "acquire":
                    pool.acquire("shared", create)
                else:
                    getattr(transport, operation)()
            except BaseException as error:
                callback_errors.append(error)

        pending.add_done_callback(completed)
        lease.close()
        assert len(callback_errors) == 1
        assert isinstance(callback_errors[0], DeviceNotReadyError)
        assert "already in progress" in str(callback_errors[0])
        assert transport.get_health().generation == 1
        assert transport.get_health().state is TransportState.CLOSED
        assert len(dispatchers) == 1
        assert pool.active_keys() == ()
        assert first.done() and pending.done()
        assert backends[0].closed.is_set()
        # Guard resets after the outer operation; normal recovery still works.
        transport.recover()
        assert transport.get_health().generation == 2
        assert transport.get_health().state is TransportState.OPEN
        transport.close()
    finally:
        for backend in backends:
            backend.release.set()
        transport.close()
        lease.close()
        for dispatcher in dispatchers:
            dispatcher.stop()
            assert not dispatcher._recv_thread.is_alive()
            assert not dispatcher._send_thread.is_alive()


@pytest.mark.parametrize("operation", ["open", "close", "recover"])
def test_open_factory_cannot_reenter_in_progress_transport(operation):
    errors = []

    def create(on_error):
        try:
            getattr(transport, operation)()
        except BaseException as error:
            errors.append(error)
        return FakeDispatcher(on_error)

    transport = CANFDTransport(dispatcher_factory=create)
    try:
        transport.open()
        assert len(errors) == 1 and isinstance(errors[0], DeviceNotReadyError)
        assert transport.get_health().state is TransportState.OPEN
        assert transport.get_health().generation == 1
    finally:
        transport.close()


def test_pool_factory_reentry_does_not_construct_duplicate_owner():
    pool = SharedTransportPool()
    errors = []

    def create():
        try:
            pool.acquire("same", lambda: pytest.fail("must not construct twice"))
        except BaseException as error:
            errors.append(error)
        return FakeTransport()

    lease = pool.acquire("same", create)
    try:
        assert len(errors) == 1 and isinstance(errors[0], DeviceNotReadyError)
        assert pool.lease_count("same") == 1
    finally:
        lease.close()


@pytest.mark.parametrize("interrupt_kind", ["SIGINT", "SystemExit"])
def test_open_rollback_propagates_first_cleanup_interrupt_and_retains_owner(
    interrupt_kind,
):
    original = ValueError("subscribe failed")

    class InterruptedDispatcher(FakeDispatcher):
        def subscribe(self, callback):
            raise original

        def stop(self):
            self.stop_count += 1
            if self.stop_count != 1:
                return
            if interrupt_kind == "SIGINT":
                signal.raise_signal(signal.SIGINT)
            raise SystemExit(23)

    dispatcher = InterruptedDispatcher(lambda error: None)
    transport = CANFDTransport(dispatcher_factory=lambda on_error: dispatcher)
    expected = KeyboardInterrupt if interrupt_kind == "SIGINT" else SystemExit
    try:
        with pytest.raises(expected) as caught:
            transport.open()
        assert caught.value.__cause__ is original
        assert transport.get_health().state is TransportState.FAULTED
        assert transport._dispatcher is dispatcher
        transport.close()
        assert dispatcher.stop_count == 2
        assert transport.get_health().state is TransportState.CLOSED
    finally:
        transport.close()
