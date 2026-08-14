"""Reference-counted shared transport pool tests."""

import threading

import pytest

from linkerbot_transport.contracts import TransportState
from linkerbot_transport.errors import ClosedError, TransportError
from linkerbot_transport.pool import SharedTransportPool
from linkerbot_transport.testing import FakeTransport


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
