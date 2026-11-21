"""Resource handoff regressions, with real signals and bounded child processes."""

from __future__ import annotations

import inspect
import os
import signal
import subprocess
import sys
import textwrap
import threading
from contextlib import contextmanager

import pytest

from linkerbot_transport.contracts import TransportState
from linkerbot_transport.errors import ClosedError, DeviceNotReadyError, TransportError
from linkerbot_transport.pool import SharedTransportPool
from linkerbot_transport.routing import ReceiveRouter, _RouteState
from linkerbot_transport.testing import FakeTransport


@contextmanager
def interrupt_before(function, statement, *, occurrence=1):
    """Interrupt a real implementation boundary, without replacing its state."""

    function = inspect.unwrap(function)
    lines, start = inspect.getsourcelines(function)
    targets = {
        start + offset for offset, line in enumerate(lines) if statement(line.strip())
    }
    assert targets, function.__qualname__
    previous = sys.gettrace()
    hits = 0
    fired = False

    def trace(frame, event, _arg):
        nonlocal hits, fired
        if (
            frame.f_code is function.__code__
            and event == "line"
            and frame.f_lineno in targets
        ):
            hits += 1
            if hits == occurrence:
                fired = True
                sys.settrace(previous)
                signal.raise_signal(signal.SIGINT)
        return trace

    try:
        sys.settrace(trace)
        yield
    finally:
        sys.settrace(previous)
    assert fired, "the intended signal boundary was not reached"


def run_child(source):
    child = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        capture_output=True,
        text=True,
        timeout=8,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        check=False,
    )
    assert child.returncode == 0, child.stdout + child.stderr


def test_pool_queries_from_notifier_do_not_deadlock_final_close():
    run_child("""
        import threading
        from linkerbot_transport.pool import SharedTransportPool
        from linkerbot_transport.testing import FakeTransport

        callback_entered = threading.Event()
        close_entered = threading.Event()
        errors = []

        class Transport(FakeTransport):
            def close(self):
                close_entered.set()
                super().close()

        pool = SharedTransportPool()
        lease = pool.acquire('channel', Transport)
        transport = lease.transport

        def available():
            callback_entered.set()
            assert close_entered.wait(2)
            assert pool.lease_count('channel') == 0
            assert pool.active_keys() == ('channel',)

        def close():
            try:
                lease.close()
            except BaseException as error:
                errors.append(error)

        transport.subscribe_route(1, on_available=available)
        rx = threading.Thread(target=lambda: transport.emit(1), daemon=True)
        closer = threading.Thread(target=close, daemon=True)
        rx.start()
        assert callback_entered.wait(2)
        closer.start()
        closer.join(2)
        rx.join(2)
        assert not closer.is_alive() and not rx.is_alive(), 'pool/notifier lock cycle'
        assert not errors, errors
        assert pool.active_keys() == ()
        assert len(threading.enumerate()) == 1
    """)


def test_route_self_close_does_not_deadlock_another_closer():
    run_child("""
        import threading
        from linkerbot_transport.testing import FakeTransport

        callback_entered = threading.Event()
        unregister_entered = threading.Event()
        transport = FakeTransport()
        transport.open()
        errors = []

        def available():
            callback_entered.set()
            assert unregister_entered.wait(2)
            route.close()

        route = transport.subscribe_route(1, on_available=available)
        unregister = route._router._unregister

        def observe(state):
            unregister_entered.set()
            return unregister(state)

        route._router._unregister = observe

        def close():
            try:
                route.close()
            except BaseException as error:
                errors.append(error)

        rx = threading.Thread(target=lambda: transport.emit(1), daemon=True)
        closer = threading.Thread(target=close, daemon=True)
        rx.start()
        assert callback_entered.wait(2)
        closer.start()
        closer.join(2)
        rx.join(2)
        assert not closer.is_alive() and not rx.is_alive(), 'handle/notifier lock cycle'
        assert not errors, errors
        assert not route.get_health().active
        transport.close()
        assert len(threading.enumerate()) == 1
    """)


@pytest.mark.parametrize("close_router", [False, True])
def test_concurrent_notifiers_can_close_other_routes_without_lock_cycle(close_router):
    run_child(f"""
        import threading
        from linkerbot_transport.routing import ReceiveRouter

        router = ReceiveRouter(lambda message: message)
        router.open(generation=1)
        barrier = threading.Barrier(2)
        errors = []
        calls = {{1: 0, 2: 0}}

        def available(key):
            calls[key] += 1
            if calls[key] == 1:
                barrier.wait(2)
                if {close_router!r}:
                    router.close()
                else:
                    routes[3 - key].close()

        routes = {{key: router.subscribe(key, on_available=lambda key=key: available(key)) for key in (1, 2)}}

        def emit(key):
            try:
                router.route(key, generation=1)
            except BaseException as error:
                errors.append(error)

        threads = [threading.Thread(target=emit, args=(key,), daemon=True) for key in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(2)
        assert not any(thread.is_alive() for thread in threads), 'cross-route notification lock cycle'
        assert not errors, errors
        router.close()
        assert calls == {{1: 2, 2: 2}}
        assert router.get_health().route_count == 0
        assert router.get_health().dropped_on_close == 2
        assert len(threading.enumerate()) == 1
    """)


def test_unrelated_pool_key_and_metadata_progress_during_factory():
    pool = SharedTransportPool()
    entered = threading.Event()
    release = threading.Event()
    progressed = threading.Event()
    leases = []
    errors = []

    def factory():
        entered.set()
        assert release.wait(3)
        return FakeTransport()

    def first():
        try:
            leases.append(pool.acquire("first", factory))
        except BaseException as error:
            errors.append(error)

    def other():
        try:
            pool.active_keys()
            pool.lease_count("first")
            leases.append(pool.acquire("other", FakeTransport))
            progressed.set()
        except BaseException as error:
            errors.append(error)

    creator = threading.Thread(target=first)
    reader = threading.Thread(target=other)
    creator.start()
    try:
        assert entered.wait(2)
        reader.start()
        made_progress = progressed.wait(1)
    finally:
        release.set()
        creator.join(3)
        if reader.ident is not None:
            reader.join(3)
        for lease in leases:
            lease.close()
    assert not creator.is_alive() and not reader.is_alive()
    assert not errors
    assert made_progress
    assert pool.active_keys() == ()


def test_same_key_factory_reentry_is_rejected_without_leaking_reservation():
    pool = SharedTransportPool()

    def factory():
        with pytest.raises(DeviceNotReadyError):
            pool.acquire("key", FakeTransport)
        return FakeTransport()

    lease = pool.acquire("key", factory)
    assert pool.lease_count("key") == 1
    lease.close()
    assert pool.active_keys() == ()


def test_unreturned_lease_does_not_leave_an_active_reference():
    pool = SharedTransportPool()
    with interrupt_before(
        SharedTransportPool.acquire, lambda line: line.startswith("return ")
    ):
        with pytest.raises(KeyboardInterrupt):
            pool.acquire("key", FakeTransport)
    try:
        assert pool.lease_count("key") == 0
    finally:
        pool.close_idle("key")
    assert pool.active_keys() == ()


def test_interrupted_reservation_retirement_does_not_block_later_cleanup():
    pool = SharedTransportPool()
    with interrupt_before(
        SharedTransportPool._finish_key_operation,
        lambda line: line.startswith("del self._operations["),
    ):
        with pytest.raises(KeyboardInterrupt):
            pool.acquire("key", FakeTransport)
    assert pool._operations == {}
    assert pool.lease_count("key") == 0
    pool.close_idle("key")
    assert pool.active_keys() == ()


def test_interrupted_release_before_pool_lock_remains_retryable():
    pool = SharedTransportPool()
    lease = pool.acquire("key", FakeTransport)
    transport = lease.transport
    with interrupt_before(
        SharedTransportPool._key_operation,
        lambda line: line.startswith("with self._"),
    ):
        with pytest.raises(KeyboardInterrupt):
            lease.close()
    with pytest.raises(ClosedError):
        _ = lease.transport
    lease.close()
    lease.close()
    assert pool.lease_count("key") == 0
    assert pool.active_keys() == ()
    assert transport.get_health().state is TransportState.CLOSED


def test_interrupted_release_after_removal_does_not_consume_another_lease():
    pool = SharedTransportPool()
    first = pool.acquire("key", FakeTransport)
    second = pool.acquire("key", FakeTransport)
    with interrupt_before(
        SharedTransportPool._release, lambda line: line == "if entry.leases > 0:"
    ):
        with pytest.raises(KeyboardInterrupt):
            first.close()
    first.close()
    assert pool.lease_count("key") == 1
    assert second.transport.get_health().state is TransportState.OPEN
    second.close()
    assert pool.active_keys() == ()


def test_old_retry_does_not_close_new_entry_reusing_the_same_transport():
    pool = SharedTransportPool()
    old = pool.acquire("key", FakeTransport)
    transport = old.transport
    with interrupt_before(
        SharedTransportPool._release, lambda line: line.startswith("del self._entries[")
    ):
        with pytest.raises(KeyboardInterrupt):
            old.close()
    pool.close_idle("key")
    current = pool.acquire("key", lambda: transport)
    transport.current_dispatcher.stop_error = OSError("new entry cleanup failed")
    with pytest.raises(TransportError, match="new entry cleanup failed"):
        current.close()
    attempts = transport.current_dispatcher.stop_count
    old.close()
    assert transport.current_dispatcher.stop_count == attempts
    assert pool.active_keys() == ("key",)
    current.close()
    assert pool.active_keys() == ()


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_terminal_callback_failure_does_not_lose_other_routes(interrupt):
    router = ReceiveRouter(lambda message: message)
    router.open(generation=1)
    calls = []
    failure = interrupt("terminal interrupted")

    def first():
        calls.append(1)
        raise failure

    routes = [router.subscribe(1, on_available=first)]
    routes.extend(
        router.subscribe(key, on_available=lambda key=key: calls.append(key))
        for key in (2, 3)
    )
    with pytest.raises(interrupt) as captured:
        router.close()
    assert captured.value is failure
    assert calls == [1, 2, 3]
    router.close()
    for route in routes:
        route.close()
    assert calls == [1, 2, 3], "an attempted callback must not be replayed"


def test_interrupt_before_terminal_attempt_preserves_notification_for_retry():
    router = ReceiveRouter(lambda message: message)
    router.open(generation=1)
    calls = []
    routes = [
        router.subscribe(key, on_available=lambda key=key: calls.append(key))
        for key in (1, 2)
    ]
    with interrupt_before(
        _RouteState.notify_closed,
        lambda line: line == "with self._notification_lock:",
    ):
        with pytest.raises(KeyboardInterrupt):
            router.close()
    router.close()
    for route in routes:
        route.close()
    assert sorted(calls) == [1, 2]
    assert len(calls) == 2


def test_closed_subscription_retry_does_not_run_another_routes_callback():
    router = ReceiveRouter(lambda message: message)
    router.open(generation=1)
    calls = []
    first = router.subscribe(1, on_available=lambda: calls.append(1))
    second = router.subscribe(2, on_available=lambda: calls.append(2))
    first.close()
    with interrupt_before(
        _RouteState.notify_closed,
        lambda line: line.startswith("if getattr(_NOTIFICATION_CONTEXT"),
    ):
        with pytest.raises(KeyboardInterrupt):
            second.close()
    first.close()
    assert calls == [1]
    second.close()
    assert calls == [1, 2]
    router.close()


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_self_close_then_interrupt_still_attempts_terminal_edge(interrupt):
    router = ReceiveRouter(lambda message: message)
    router.open(generation=1)
    calls = []
    failure = interrupt("available interrupted after self-close")

    def available():
        calls.append("called")
        if len(calls) == 1:
            route.close()
            raise failure

    route = router.subscribe(1, on_available=available)
    with pytest.raises(interrupt) as captured:
        router.route(1, generation=1)
    assert captured.value is failure
    router.close()
    route.close()
    assert calls == ["called", "called"]


def test_nested_availability_preserves_outer_terminal_deferral():
    router = ReceiveRouter(lambda message: message)
    router.open(generation=1)
    calls = []

    def available():
        calls.append("edge")
        if len(calls) == 1:
            route.receive(timeout_s=0)
            assert router.route(1, generation=1)
            route.close()
            assert calls == ["edge", "edge"]
        elif len(calls) == 2:
            route.receive(timeout_s=0)

    route = router.subscribe(1, on_available=available)
    assert router.route(1, generation=1)
    assert calls == ["edge", "edge", "edge"]
    route.close()
    router.close()
    assert calls == ["edge", "edge", "edge"]


@pytest.mark.parametrize("boundary", ["partial_group", "return_handoff"])
def test_interrupted_group_registration_leaves_no_orphan_keys(boundary):
    router = ReceiveRouter(lambda message: message)
    router.open(generation=1)
    if boundary == "partial_group":

        def statement(line):
            return "routes[route_key] = state" in line

        occurrence = 2
    else:

        def statement(line):
            return line.startswith("return ")

        occurrence = 1
    with interrupt_before(ReceiveRouter._subscribe, statement, occurrence=occurrence):
        with pytest.raises(KeyboardInterrupt):
            router.subscribe_routes((1, 2, 3))
    assert router.get_health().route_count == 0
    assert not router.route(1, generation=1)
    with router.subscribe_routes((1, 2, 3)) as route:
        assert router.route(2, generation=1)
        assert route.receive(timeout_s=0).message == 2
    router.close()


def test_interrupted_unregister_can_retry_and_release_every_key():
    router = ReceiveRouter(lambda message: message)
    router.open(generation=1)
    route = router.subscribe_routes((1, 2))
    with interrupt_before(
        ReceiveRouter._unregister, lambda line: line == "with self._lock:"
    ):
        with pytest.raises(KeyboardInterrupt):
            route.close()
    route.close()
    assert not route.get_health().active
    assert router.get_health().route_count == 0
    router.subscribe_routes((1, 2)).close()
    router.close()


def test_interrupt_after_close_fact_preserves_drops_and_wakes_waiters():
    router = ReceiveRouter(lambda message: message)
    router.open(generation=1)
    route = router.subscribe_routes((1, 2))
    assert router.route(1, generation=1)
    assert router.route(2, generation=1)
    with interrupt_before(
        _RouteState.close, lambda line: line == "self._items.clear()"
    ):
        with pytest.raises(KeyboardInterrupt):
            route.close()
    route.close()
    router.close()
    assert route.get_health().queue_depth == 0
    assert route.get_health().dropped_on_close == 2
    assert router.get_health().queue_depth == 0
    assert router.get_health().dropped_on_close == 2
    with pytest.raises(ClosedError):
        route.receive(timeout_s=0)
