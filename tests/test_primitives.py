"""Concurrency primitive behavior tests."""

import queue
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from linkerbot_transport.errors import (
    ClosedError,
    StateError,
    TimeoutError,
    ValidationError,
)
from linkerbot_transport.queue import IterableQueue
from linkerbot_transport.relay import DataRelay


def test_iterable_queue_drains_after_close() -> None:
    items = IterableQueue[int](maxsize=2)
    items.put(1)
    items.put(2)
    items.close()

    assert list(items) == [1, 2]
    with pytest.raises(StateError):
        items.put(3)


def test_iterable_queue_wakes_blocked_producer_on_close() -> None:
    items = IterableQueue[int](maxsize=1)
    items.put(1)
    finished = threading.Event()
    errors: list[BaseException] = []

    def produce() -> None:
        try:
            items.put(2)
        except BaseException as error:
            errors.append(error)
        finally:
            finished.set()

    thread = threading.Thread(target=produce)
    thread.start()
    items.close()
    assert finished.wait(1.0)
    thread.join(timeout=1.0)
    assert len(errors) == 1
    assert isinstance(errors[0], StateError)


def test_iterable_queue_nonblocking_full() -> None:
    items = IterableQueue[int](maxsize=1)
    items.put_nowait(1)
    with pytest.raises(queue.Full):
        items.put_nowait(2)


def test_data_relay_snapshot_wait_and_clearable_sink() -> None:
    relay = DataRelay[int]()
    received: list[int] = []
    relay.set_sink(received.append)

    relay.push(1)
    assert relay.snapshot() == 1
    assert received == [1]

    relay.set_sink(None)
    relay.push(2)
    assert relay.snapshot() == 2
    assert received == [1]


def test_data_relay_waits_for_next_value() -> None:
    relay = DataRelay[int]()
    result: list[int] = []
    started = threading.Event()

    def wait() -> None:
        started.set()
        result.append(relay.wait(1.0))

    thread = threading.Thread(target=wait)
    thread.start()
    assert started.wait(1.0)
    relay.push(42)
    thread.join(timeout=1.0)
    assert result == [42]


@pytest.mark.parametrize("timeout", [-1.0, float("inf"), float("nan"), True])
def test_data_relay_rejects_invalid_timeout(timeout: float) -> None:
    relay = DataRelay[int]()
    with pytest.raises(ValidationError):
        relay.wait(timeout)


def test_data_relay_timeout_is_classified() -> None:
    relay = DataRelay[int]()
    with pytest.raises(TimeoutError):
        relay.wait(0.0)


def test_data_relay_request_captures_synchronous_response() -> None:
    relay = DataRelay[int]()

    assert relay.request(lambda: relay.push(42), 0.1) == 42


def test_data_relay_request_cleans_waiter_when_send_raises() -> None:
    relay = DataRelay[int]()
    error = RuntimeError("send failed")

    def fail() -> None:
        raise error

    with pytest.raises(RuntimeError) as raised:
        relay.request(fail, 0.1)
    assert raised.value is error

    relay.push(1)
    assert relay.request(lambda: relay.push(2), 0.1) == 2


def test_data_relay_request_does_not_reuse_response_after_timeout() -> None:
    relay = DataRelay[int]()

    with pytest.raises(TimeoutError):
        relay.request(lambda: None, 0.0)

    relay.push(1)
    assert relay.request(lambda: relay.push(2), 0.1) == 2


def test_data_relay_serializes_concurrent_requests() -> None:
    relay = DataRelay[int]()
    first_sent = threading.Event()
    release_first = threading.Event()
    second_sent = threading.Event()
    results: dict[str, int] = {}

    def first_send() -> None:
        first_sent.set()
        assert release_first.wait(1.0)
        relay.push(1)

    def first_request() -> None:
        results["first"] = relay.request(first_send, 1.0)

    def second_send() -> None:
        second_sent.set()
        relay.push(2)

    def second_request() -> None:
        results["second"] = relay.request(second_send, 1.0)

    first = threading.Thread(target=first_request)
    second = threading.Thread(target=second_request)
    first.start()
    assert first_sent.wait(1.0)
    second.start()
    assert not second_sent.wait(0.05)

    release_first.set()
    first.join(timeout=1.0)
    second.join(timeout=1.0)

    assert not first.is_alive()
    assert not second.is_alive()
    assert second_sent.is_set()
    assert results == {"first": 1, "second": 2}


def test_data_relay_close_cancels_and_cleans_request() -> None:
    relay = DataRelay[int]()
    sent = threading.Event()

    with ThreadPoolExecutor(max_workers=1) as executor:
        request = executor.submit(relay.request, sent.set, 1.0)
        assert sent.wait(1.0)
        relay.close()
        with pytest.raises(ClosedError):
            request.result(timeout=1.0)

    relay.push(1)
    assert relay.snapshot() is None
    with pytest.raises(ClosedError):
        relay.request(lambda: relay.push(2), 0.1)
