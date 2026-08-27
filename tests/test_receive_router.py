"""Constant-time receive routing, capacity and generation tests."""

from __future__ import annotations

import threading

import pytest

from linkerbot_transport.errors import (
    ClosedError,
    DeviceNotReadyError,
    QueueFullError,
    StateError,
    ValidationError,
)
from linkerbot_transport.routing import CANRouteKey, ReceiveRouter


class FakeClock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def test_routes_only_to_exact_key_with_generation_and_time_metadata() -> None:
    clock = FakeClock(12.5)
    key_calls: list[tuple[str, int]] = []

    def key_of(message: tuple[str, int]) -> str:
        key_calls.append(message)
        return message[0]

    router = ReceiveRouter[tuple[str, int], str](key_of, monotonic=clock)
    router.open(generation=4)
    left = router.subscribe("left")
    right = router.subscribe("right")

    assert router.route(("left", 7), generation=4)

    routed = left.receive(timeout_s=0)
    assert routed.message == ("left", 7)
    assert routed.route_key == "left"
    assert routed.generation == 4
    assert routed.sequence == 1
    assert routed.received_at_s == 12.5
    assert right.try_receive() is None
    assert key_calls == [("left", 7)]
    health = router.get_health()
    assert health.received == 1
    assert health.routed == 1
    assert health.queue_depth == 0

    router.close()


def test_route_miss_and_key_error_are_counted_without_delivery() -> None:
    def key_of(message: str) -> str:
        if message == "malformed":
            raise ValueError("missing route")
        return message

    router = ReceiveRouter[str, str](key_of)
    router.open(generation=1)
    route = router.subscribe("known")

    assert not router.route("unknown", generation=1)
    assert not router.route("malformed", generation=1)

    assert route.try_receive() is None
    health = router.get_health()
    assert health.received == 2
    assert health.route_misses == 2
    assert health.key_errors == 1
    router.close()


def test_route_queue_is_bounded_and_drops_newest_on_overflow() -> None:
    router = ReceiveRouter[int, int](lambda message: message, default_queue_capacity=1)
    router.open(generation=1)
    route = router.subscribe(3)

    assert router.route(3, generation=1)
    assert not router.route(3, generation=1)

    assert route.receive(timeout_s=0).message == 3
    route_health = route.get_health()
    assert route_health.delivered == 1
    assert route_health.overflows == 1
    health = router.get_health()
    assert health.routed == 1
    assert health.overflows == 1
    assert health.max_queue_depth == 1
    assert health.queue_depth == 0
    router.close()


def test_registry_is_bounded_and_route_keys_are_exclusive() -> None:
    router = ReceiveRouter[str, str](lambda message: message, max_routes=1)
    router.open(generation=1)
    first = router.subscribe("a")

    with pytest.raises(StateError, match="already registered"):
        router.subscribe("a")
    with pytest.raises(QueueFullError, match="registry is full"):
        router.subscribe("b")
    assert router.get_health().registration_rejections == 1

    first.close()
    second = router.subscribe("b", queue_capacity=2)
    assert second.route_key == "b"
    router.close()


def test_route_group_preserves_arrival_order_in_one_shared_inbox() -> None:
    router = ReceiveRouter[tuple[str, int], str](lambda message: message[0])
    router.open(generation=3)
    route = router.subscribe_routes(("identity", "angles", "faults"))

    assert route.route_key == "identity"
    assert route.route_keys == ("identity", "angles", "faults")
    assert route.get_health().route_keys == route.route_keys
    assert router.get_health().route_count == 3

    assert router.route(("faults", 1), generation=3)
    assert router.route(("angles", 2), generation=3)
    assert router.route(("identity", 3), generation=3)

    assert [route.receive(timeout_s=0).message for _ in range(3)] == [
        ("faults", 1),
        ("angles", 2),
        ("identity", 3),
    ]
    router.close()


def test_route_group_uses_one_combined_capacity_and_closes_atomically() -> None:
    router = ReceiveRouter[str, str](lambda message: message)
    router.open(generation=1)
    route = router.subscribe_routes(("a", "b"), queue_capacity=2)

    assert router.route("a", generation=1)
    assert router.route("b", generation=1)
    assert not router.route("a", generation=1)
    route.close()

    health = router.get_health()
    assert health.route_count == 0
    assert health.queue_depth == 0
    assert health.dropped_on_close == 2
    assert route.get_health().overflows == 1
    assert not router.route("b", generation=1)
    router.close()


def test_route_group_registration_failure_leaves_no_partial_routes() -> None:
    router = ReceiveRouter[str, str](lambda message: message, max_routes=3)
    router.open(generation=1)
    existing = router.subscribe("claimed")

    with pytest.raises(StateError, match="already registered"):
        router.subscribe_routes(("free", "claimed"))
    assert router.get_health().route_count == 1

    with pytest.raises(QueueFullError, match="registry is full"):
        router.subscribe_routes(("one", "two", "three"))
    assert router.get_health().route_count == 1
    assert router.get_health().registration_rejections == 1

    replacement = router.subscribe_routes(("free", "other"))
    assert router.get_health().route_count == 3
    replacement.close()
    existing.close()
    router.close()


@pytest.mark.parametrize("route_keys", [(), ("a", "a")])
def test_route_group_rejects_empty_or_duplicate_keys(
    route_keys: tuple[str, ...],
) -> None:
    router = ReceiveRouter[str, str](lambda message: message)
    router.open(generation=1)

    with pytest.raises(ValidationError):
        router.subscribe_routes(route_keys)

    assert router.get_health().route_count == 0
    router.close()


def test_subscription_close_discards_queue_and_wakes_blocked_receive() -> None:
    router = ReceiveRouter[int, int](lambda message: message)
    router.open(generation=1)
    queued = router.subscribe(1)
    assert router.route(1, generation=1)
    queued.close()

    queued_health = queued.get_health()
    assert not queued_health.active
    assert queued_health.queue_depth == 0
    assert queued_health.dropped_on_close == 1
    assert router.get_health().dropped_on_close == 1

    blocked = router.subscribe(2)
    started = threading.Event()
    errors: list[BaseException] = []

    def receive() -> None:
        started.set()
        try:
            blocked.receive()
        except BaseException as error:  # noqa: BLE001 - thread boundary
            errors.append(error)

    thread = threading.Thread(target=receive)
    thread.start()
    assert started.wait(timeout=1.0)
    blocked.close()
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], ClosedError)
    router.close()


def test_old_generation_never_reaches_reopened_route() -> None:
    router = ReceiveRouter[int, int](lambda message: message)
    router.open(generation=1)
    old = router.subscribe(5)
    router.close()

    with pytest.raises(ClosedError):
        old.receive(timeout_s=0)
    with pytest.raises(DeviceNotReadyError, match="generation must increase"):
        router.open(generation=1)

    router.open(generation=2)
    current = router.subscribe(5)
    assert not router.route(5, generation=1)
    assert current.try_receive() is None
    assert router.route(5, generation=2)
    assert current.receive(timeout_s=0).generation == 2
    assert router.get_health().stale_frames == 1
    router.close()


def test_concurrent_route_registration_and_close_is_consistent() -> None:
    router = ReceiveRouter[int, int](lambda message: message, max_routes=32)
    router.open(generation=1)
    start = threading.Barrier(17)
    errors: list[BaseException] = []

    def cycle(route_key: int) -> None:
        start.wait()
        try:
            route = router.subscribe(route_key)
            assert router.route(route_key, generation=1)
            assert route.receive(timeout_s=1.0).message == route_key
            route.close()
        except BaseException as error:  # noqa: BLE001 - thread boundary
            errors.append(error)

    threads = [threading.Thread(target=cycle, args=(index,)) for index in range(16)]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(timeout=1.0)

    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    assert router.get_health().route_count == 0
    assert router.get_health().queue_depth == 0
    router.close()


def test_repeated_lifecycle_does_not_retain_routes() -> None:
    router = ReceiveRouter[int, int](lambda message: message)

    for generation in range(1, 101):
        router.open(generation=generation)
        route = router.subscribe(generation)
        assert router.route(generation, generation=generation)
        router.close()
        assert not route.get_health().active
        assert router.get_health().route_count == 0
        assert router.get_health().queue_depth == 0

    assert router.get_health().dropped_on_close == 100


def test_repeated_group_lifecycle_does_not_retain_routes() -> None:
    router = ReceiveRouter[int, int](lambda message: message)

    for generation in range(1, 101):
        router.open(generation=generation)
        route = router.subscribe_routes((1, 2, 3))
        assert router.route(2, generation=generation)
        router.close()
        assert not route.get_health().active
        assert router.get_health().route_count == 0
        assert router.get_health().queue_depth == 0

    assert router.get_health().dropped_on_close == 100


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"arbitration_id": -1, "is_extended_id": False}, "standard"),
        ({"arbitration_id": 0x800, "is_extended_id": False}, "standard"),
        ({"arbitration_id": 0x20000000, "is_extended_id": True}, "extended"),
        ({"arbitration_id": True, "is_extended_id": False}, "must be an int"),
        ({"arbitration_id": 1, "is_extended_id": 1}, "must be a bool"),
    ],
)
def test_can_route_key_rejects_invalid_values(
    kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        CANRouteKey(**kwargs)  # ty: ignore[invalid-argument-type]


def test_receive_router_rejects_invalid_configuration() -> None:
    with pytest.raises(ValidationError):
        ReceiveRouter[int, int](lambda message: message, max_routes=0)
    with pytest.raises(ValidationError):
        ReceiveRouter[int, int](lambda message: message, default_queue_capacity=0)

    router = ReceiveRouter[int, int](lambda message: message)
    with pytest.raises(ClosedError):
        router.subscribe(1)
    router.open(generation=1)
    with pytest.raises(ValidationError):
        router.subscribe(1, queue_capacity=0)
    router.close()
