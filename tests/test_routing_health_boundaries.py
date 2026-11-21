"""Terminal errors, bounded route inputs and connection-scoped health."""

import threading

import pytest

from linkerbot_transport import ReceiveRouter
from linkerbot_transport.errors import (
    ClosedError,
    QueueFullError,
    TimeoutError,
    TransportError,
)
from linkerbot_transport.testing import FakeTransport


@pytest.mark.parametrize("error_type", [TimeoutError, ClosedError, TransportError])
def test_polling_preserves_any_terminal_error_and_its_cause(error_type):
    router = ReceiveRouter(lambda value: value)
    router.open(generation=1)
    route = router.subscribe(1)
    cause = OSError("driver failure")
    error = error_type("terminal receive failure", cause=cause)
    try:
        assert route.try_receive() is None
        router.route(1, generation=1)
        assert route.try_receive().message == 1
        assert route.get_health().queue_depth == 0
        router.abort(error)
        for receive in (route.try_receive, lambda: route.receive(timeout_s=0)):
            with pytest.raises(error_type) as raised:
                receive()
            assert raised.value is error
            assert raised.value.cause is cause
    finally:
        router.close()


def test_polling_does_not_swallow_a_prior_wait_timeout_used_as_terminal_cause():
    router = ReceiveRouter(lambda value: value)
    router.open(generation=1)
    route = router.subscribe(1)
    try:
        with pytest.raises(TimeoutError) as timeout:
            route.receive(timeout_s=0)
        router.abort(timeout.value)
        with pytest.raises(TimeoutError) as terminal:
            route.try_receive()
        assert terminal.value is timeout.value
    finally:
        router.close()


def test_oversized_route_iterator_is_rejected_before_consuming_its_tail():
    router = ReceiveRouter(lambda value: value, max_routes=2)
    router.open(generation=1)
    consumed = []

    def keys():
        for key in range(10000):
            if key > 2:
                raise AssertionError("consumed beyond the capacity decision")
            consumed.append(key)
            yield key

    try:
        with pytest.raises(QueueFullError):
            router.subscribe_routes(keys())
        assert consumed == [0, 1, 2]
        health = router.get_health()
        assert health.route_count == 0
        assert health.registration_rejections == 1
        route = router.subscribe_routes([0, 1])
        assert route.route_keys == (0, 1)
    finally:
        router.close()


def test_old_physical_success_cannot_clear_replacement_connection_health(monkeypatch):
    transport = FakeTransport(data_size_of=lambda message: message)
    transport.open()
    old = transport.current_dispatcher
    original_send = old.send_now
    physical_done, release = threading.Event(), threading.Event()
    receipts, errors = [], []

    def delayed_return(message):
        original_send(message)
        physical_done.set()
        assert release.wait(2)

    def send():
        try:
            receipts.append(transport.send(3))
        except BaseException as error:
            errors.append(error)

    monkeypatch.setattr(old, "send_now", delayed_return)
    thread = threading.Thread(target=send)
    thread.start()
    try:
        assert physical_done.wait(1)
        transport.recover()
        error = TransportError("new connection failed")
        transport.current_dispatcher.send_error = error
        with pytest.raises(TransportError):
            transport.send(9).result(timeout=0)
        before = transport.get_health()
        assert before.consecutive_errors == 1
        release.set()
        thread.join(1)
        assert not thread.is_alive() and not errors
        after = transport.get_health()
        assert after.generation == 2 and receipts[0].generation == 1
        assert after.consecutive_errors == 1
        assert after.last_error is before.last_error
        assert after.last_successful_tx_s == before.last_successful_tx_s
        assert after.tx_frames == 1 and after.tx_bytes == 3
    finally:
        release.set()
        thread.join(2)
        transport.close()
