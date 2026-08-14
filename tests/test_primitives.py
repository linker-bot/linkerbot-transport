"""Concurrency primitive behavior tests."""

import queue
import threading

import pytest

from linkerbot_transport.errors import StateError, TimeoutError, ValidationError
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
