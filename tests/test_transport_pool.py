"""Reference-counted shared transport pool tests."""

import threading
from collections.abc import Callable

import pytest

from linkerbot_transport._managed import ManagedMessageTransport
from linkerbot_transport.contracts import TransportState
from linkerbot_transport.errors import ClosedError, TransportError
from linkerbot_transport.pool import SharedTransportPool
from linkerbot_transport.testing import FakeDispatcher, FakeTransport


class OpeningDispatcher(FakeDispatcher[int]):
    """A resource that can fail both subscription and subsequent cleanup."""

    def __init__(
        self,
        on_bus_error: Callable[[Exception], None],
        *,
        subscribe_error: BaseException | None = None,
        cleanup_error: BaseException | None = None,
    ) -> None:
        super().__init__(on_bus_error)
        self.subscribe_error = subscribe_error
        self.cleanup_error = cleanup_error
        self.live = True

    def subscribe(self, callback: Callable[[int], None]) -> None:
        super().subscribe(callback)
        if self.subscribe_error is not None:
            raise self.subscribe_error

    def stop(self) -> None:
        if self.cleanup_error is not None:
            self.stop_count += 1
            raise self.cleanup_error
        super().stop()
        self.live = False


def test_pool_shares_connection_until_last_lease_closes() -> None:
    pool = SharedTransportPool[FakeTransport[int]]()
    created: list[FakeTransport[int]] = []

    def create() -> FakeTransport[int]:
        transport = FakeTransport[int]()
        created.append(transport)
        return transport

    first = pool.acquire("can0", create)
    second = pool.acquire("can0", create)

    assert len(created) == 1
    assert first.transport is second.transport
    assert pool.lease_count("can0") == 2
    first.close()
    assert second.transport.get_health().state is TransportState.OPEN
    assert pool.lease_count("can0") == 1

    transport = second.transport
    second.close()
    assert transport.get_health().state is TransportState.CLOSED
    assert pool.lease_count("can0") == 0
    assert pool.active_keys() == ()


def test_lease_close_is_idempotent_and_access_after_close_fails() -> None:
    pool = SharedTransportPool[FakeTransport[int]]()
    lease = pool.acquire("can0", FakeTransport[int])

    lease.close()
    lease.close()

    with pytest.raises(ClosedError):
        _ = lease.transport


def test_context_manager_releases_final_reference() -> None:
    pool = SharedTransportPool[FakeTransport[int]]()

    with pool.acquire("can0", FakeTransport[int]) as transport:
        assert transport.get_health().state is TransportState.OPEN

    assert transport.get_health().state is TransportState.CLOSED
    assert pool.active_keys() == ()


def test_concurrent_acquire_uses_one_factory_and_connection() -> None:
    pool = SharedTransportPool[FakeTransport[int]]()
    start = threading.Barrier(17)
    factory_calls = 0
    factory_lock = threading.Lock()
    leases = []

    def create() -> FakeTransport[int]:
        nonlocal factory_calls
        with factory_lock:
            factory_calls += 1
        return FakeTransport[int]()

    def acquire() -> None:
        start.wait()
        leases.append(pool.acquire("can0", create))

    threads = [threading.Thread(target=acquire) for _ in range(16)]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(timeout=1.0)

    assert all(not thread.is_alive() for thread in threads)
    assert factory_calls == 1
    assert pool.lease_count("can0") == 16
    assert len({id(lease.transport) for lease in leases}) == 1
    transport = leases[0].transport
    for lease in leases:
        lease.close()
    assert transport.get_health().state is TransportState.CLOSED


def test_failed_final_close_retains_entry_for_recovery() -> None:
    pool = SharedTransportPool[FakeTransport[int]]()
    factory_calls = 0

    def create() -> FakeTransport[int]:
        nonlocal factory_calls
        factory_calls += 1
        return FakeTransport[int]()

    first = pool.acquire("can0", create)
    transport = first.transport
    transport.current_dispatcher.stop_error = OSError("close failed")

    with pytest.raises(TransportError, match="close failed"):
        first.close()

    assert pool.lease_count("can0") == 0
    assert pool.active_keys() == ("can0",)

    second = pool.acquire("can0", create)
    assert second.transport is transport
    assert factory_calls == 1
    assert transport.get_health().generation == 2
    assert transport.get_health().reconnect_count == 1
    second.close()


def test_failed_final_close_can_retry_without_reopening_or_double_releasing():
    pool = SharedTransportPool[FakeTransport[int]]()
    lease = pool.acquire("retry", FakeTransport[int])
    transport = lease.transport
    generation = transport.get_health().generation
    transport.current_dispatcher.stop_error = OSError("close failed")
    with pytest.raises(TransportError):
        lease.close()
    assert pool.lease_count("retry") == 0
    with pytest.raises(ClosedError):
        _ = lease.transport
    lease.close()
    lease.close()
    assert pool.active_keys() == ()
    assert transport.get_health().state is TransportState.CLOSED
    assert transport.get_health().generation == generation
    assert len(transport.dispatchers) == 1


def test_old_close_retry_does_not_close_a_recovered_active_owner():
    pool = SharedTransportPool[FakeTransport[int]]()
    old = pool.acquire("shared", FakeTransport[int])
    transport = old.transport
    transport.current_dispatcher.stop_error = OSError("close failed")
    with pytest.raises(TransportError):
        old.close()
    current = pool.acquire("shared", lambda: pytest.fail("must recover same object"))
    old.close()
    old.close()
    assert pool.lease_count("shared") == 1
    assert current.transport is transport
    assert transport.get_health().state is TransportState.OPEN
    current.close()
    assert pool.active_keys() == ()


def test_old_close_retry_does_not_touch_a_replacement_pool_entry():
    pool = SharedTransportPool[FakeTransport[int]]()
    old = pool.acquire("replace", FakeTransport[int])
    transport = old.transport
    transport.current_dispatcher.stop_error = OSError("close failed")
    with pytest.raises(TransportError):
        old.close()
    recovered = pool.acquire("replace", lambda: transport)
    recovered.close()
    replacement = pool.acquire("replace", FakeTransport[int])
    old.close()
    assert replacement.transport is not transport
    assert replacement.transport.get_health().state is TransportState.OPEN
    assert pool.lease_count("replace") == 1
    replacement.close()


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_interrupted_final_close_remains_retryable(interrupt):
    pool = SharedTransportPool[FakeTransport[int]]()
    lease = pool.acquire("interrupt", FakeTransport[int])
    transport = lease.transport
    error = interrupt("close interrupted")
    transport.current_dispatcher.stop_error = error
    with pytest.raises(interrupt) as captured:
        lease.close()
    assert captured.value is error
    lease.close()
    assert pool.active_keys() == ()


def test_close_callback_can_reenter_the_same_lease_without_deadlock():
    pool = SharedTransportPool[FakeTransport[int]]()

    class CallbackTransport(FakeTransport[int]):
        callback = None

        def close(self):
            if self.callback is not None:
                self.callback()
            super().close()

    lease = pool.acquire("callback", CallbackTransport)
    lease.transport.callback = lease.close
    lease.close()
    assert pool.active_keys() == ()


def test_acquire_waits_for_an_in_progress_cleanup_retry():
    pool = SharedTransportPool[FakeTransport[int]]()
    old = pool.acquire("serialized-retry", FakeTransport[int])
    transport = old.transport
    transport.current_dispatcher.stop_error = OSError("close failed")
    with pytest.raises(TransportError):
        old.close()
    entered = threading.Event()
    release = threading.Event()
    acquired = threading.Event()
    started_acquire = threading.Event()
    original_close = transport.close
    leases = []
    errors = []

    def close():
        entered.set()
        assert release.wait(3)
        original_close()

    transport.close = close

    def retry():
        try:
            old.close()
        except BaseException as error:
            errors.append(error)

    def acquire():
        started_acquire.set()
        try:
            leases.append(pool.acquire("serialized-retry", FakeTransport[int]))
            acquired.set()
        except BaseException as error:
            errors.append(error)

    first = threading.Thread(target=retry)
    second = threading.Thread(target=acquire)
    first.start()
    try:
        assert entered.wait(3)
        second.start()
        assert started_acquire.wait(3)
        assert not acquired.wait(0.05)
    finally:
        release.set()
        first.join(3)
        if second.ident is not None:
            second.join(3)
    assert not first.is_alive() and not second.is_alive()
    assert not errors and acquired.is_set()
    assert leases[0].transport is not transport
    assert pool.lease_count("serialized-retry") == 1
    leases[0].close()


@pytest.mark.parametrize("new_factory", [False, True])
def test_failed_open_and_cleanup_recover_same_transport(new_factory: bool) -> None:
    pool = SharedTransportPool[ManagedMessageTransport[int]]()
    transports: list[ManagedMessageTransport[int]] = []
    dispatchers: list[OpeningDispatcher] = []

    def create_dispatcher(
        on_bus_error: Callable[[Exception], None],
    ) -> OpeningDispatcher:
        # A replacement dispatcher is only safe once the prior one stopped.
        assert not any(dispatcher.live for dispatcher in dispatchers)
        first = not dispatchers
        dispatcher = OpeningDispatcher(
            on_bus_error,
            subscribe_error=OSError("subscribe failed") if first else None,
            cleanup_error=OSError("cleanup failed") if first else None,
        )
        dispatchers.append(dispatcher)
        return dispatcher

    def create() -> ManagedMessageTransport[int]:
        transport = ManagedMessageTransport(create_dispatcher, lambda message: message)
        transports.append(transport)
        return transport

    with pytest.raises(TransportError, match="subscribe failed"):
        pool.acquire("can0", create)

    assert pool.active_keys() == ("can0",)
    assert pool.lease_count("can0") == 0
    assert dispatchers[0].live
    assert transports[0].get_health().state is TransportState.FAULTED

    def replacement_factory() -> ManagedMessageTransport[int]:
        pytest.fail("acquire must not bypass the retained transport")

    retry_factory = replacement_factory if new_factory else create
    with pytest.raises(TransportError, match="cleanup failed"):
        pool.acquire("can0", retry_factory)
    assert pool.lease_count("can0") == 0
    assert pool.active_keys() == ("can0",)
    assert len(transports) == len(dispatchers) == 1

    dispatchers[0].cleanup_error = None
    first = pool.acquire("can0", retry_factory)
    second = pool.acquire("can0", replacement_factory)
    assert first.transport is second.transport is transports[0]
    assert pool.lease_count("can0") == 2
    assert len(transports) == 1
    assert len(dispatchers) == 2
    assert not dispatchers[0].live and dispatchers[1].live

    first.close()
    assert pool.lease_count("can0") == 1
    assert dispatchers[1].live
    second.close()
    assert pool.active_keys() == ()
    assert pool.lease_count("can0") == 0
    assert not any(dispatcher.live for dispatcher in dispatchers)


@pytest.mark.parametrize("error_type", [TransportError, KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_pool_rolls_back_open_failure_and_preserves_original_error(
    error_type: type[BaseException], cleanup_fails: bool
) -> None:
    error = error_type("open failed")

    class PartialOpenTransport(FakeTransport[int]):
        def open(self) -> None:
            super().open()
            if cleanup_fails:
                self.current_dispatcher.stop_error = OSError("cleanup failed")
            raise error

    transport = PartialOpenTransport()
    pool = SharedTransportPool[FakeTransport[int]]()
    with pytest.raises(error_type) as captured:
        pool.acquire("can0", lambda: transport)

    assert captured.value is error
    assert pool.lease_count("can0") == 0
    assert transport.current_dispatcher.stop_count == 1
    if cleanup_fails:
        assert pool.active_keys() == ("can0",)
        assert transport.get_health().state is TransportState.FAULTED
        # A subsequent acquire must retain ownership even if recover's new
        # open attempt also fails. No lease is issued for that attempt.
        with pytest.raises(error_type):
            pool.acquire("can0", FakeTransport[int])
        assert pool.active_keys() == ("can0",)
        assert pool.lease_count("can0") == 0
        transport.current_dispatcher.stop_error = None
        transport.close()
    else:
        assert pool.active_keys() == ()
        assert transport.get_health().state is TransportState.CLOSED
        # Successfully rolled-back objects do not prevent a fresh factory.
        lease = pool.acquire("can0", FakeTransport[int])
        assert lease.transport is not transport
        lease.close()
        assert pool.active_keys() == ()


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_pool_retains_entry_when_open_cleanup_is_interrupted(
    interrupt_type: type[BaseException],
) -> None:
    error = TransportError("open failed")
    interrupt = interrupt_type("cleanup interrupted")

    class InterruptedCleanupTransport(FakeTransport[int]):
        def open(self) -> None:
            super().open()
            raise error

        def close(self) -> None:
            raise interrupt

    transport = InterruptedCleanupTransport()
    pool = SharedTransportPool[FakeTransport[int]]()
    try:
        with pytest.raises(interrupt_type) as captured:
            pool.acquire("can0", lambda: transport)
        assert captured.value is interrupt
        assert captured.value.__context__ is error
        assert pool.active_keys() == ("can0",)
        assert pool.lease_count("can0") == 0
    finally:
        FakeTransport.close(transport)


def test_failed_factory_does_not_leave_a_pool_entry() -> None:
    pool = SharedTransportPool[FakeTransport[int]]()

    def create() -> FakeTransport[int]:
        raise TransportError("factory failed")

    with pytest.raises(TransportError, match="factory failed"):
        pool.acquire("can0", create)
    assert pool.active_keys() == ()
    assert pool.lease_count("can0") == 0
