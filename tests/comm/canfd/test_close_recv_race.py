"""Concurrency model tests for the CANFD ctypes backend.

These tests pin the new lock split introduced in fix#141:

- ``send()`` and ``receive()`` must run truly concurrently — the previous
  single ``_lock`` serialized them, which made the dispatcher's 10 ms
  receive timeout block every transmit. Vendor PDF §4-2 explicitly
  encourages a dedicated long-running receive thread alongside transmits.
- ``close()`` must follow a strict ordering: flip ``_closed_event`` →
  drain ``_tx_lock`` / ``_rx_lock`` → call ``CAN_CloseDevice`` → release
  lifecycle. Skipping the drain would let an in-flight vendor call write
  into a closed USB handle, which is undefined behaviour on the vendor side.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pytest

from linkerbot_transport import ErrorCode, SharedTransportPool, TransportState
from linkerbot_transport.canfd import (
    CANFDMessage,
    CANFDTransport,
    CANRouteKey,
    enumerate_minicanfd_channels,
)
from linkerbot_transport.canfd import ctypes_backend as backend
from linkerbot_transport.canfd import dispatcher as dispatcher_module
from linkerbot_transport.canfd.dispatcher import CANFDMessageDispatcher
from linkerbot_transport.errors import ClosedError, TimeoutError, TransportError


class _FakeFn:
    def __init__(self, result: int = 0) -> None:
        self.result = result
        self.calls: list[tuple] = []
        self.restype = None
        self.argtypes = None
        self._hook = None

    def __call__(self, *args):
        self.calls.append(args)
        if self._hook is not None:
            return self._hook(*args)
        return self.result


class _SlowReceiveFake:
    """Library fake where CANFD_Receive blocks until released.

    Used to assert that a long-running receive does NOT block send.
    """

    def __init__(self) -> None:
        self._name = "/fake/slow.so"
        self.LibCANbus_Init = _FakeFn(0)
        self.LibCANbus_Exit = _FakeFn(0)
        self.CAN_ScanDevice = _FakeFn(1)
        self.CAN_OpenDevice = _FakeFn(0)
        self.CAN_CloseDevice = _FakeFn(0)
        self.CAN_ReadDevInfo = _FakeFn(0)
        self.CANFD_Init = _FakeFn(0)
        self.CANFD_Transmit = _FakeFn(1)
        self.CANFD_Receive = _FakeFn(0)

        self.recv_entered = threading.Event()
        self.recv_release = threading.Event()
        self.send_observed_during_recv = threading.Event()

        def receive_hook(dev, channel, buffer, max_frames, timeout_ms):
            self.recv_entered.set()
            # Block until the test signals release; in the previous design
            # this would also hold _lock, starving send.
            self.recv_release.wait(timeout=2.0)
            return 0

        def send_hook(dev, channel, frame, count, timeout_ms):
            # If we reach send while receive is still blocked, the lock split
            # worked. Mark it.
            if self.recv_entered.is_set() and not self.recv_release.is_set():
                self.send_observed_during_recv.set()
            return 1

        self.CANFD_Receive._hook = receive_hook
        self.CANFD_Transmit._hook = send_hook


@pytest.fixture(autouse=True)
def _reset_lifecycle_state():
    with backend._lifecycle_lock:
        backend._lifecycle_references.clear()
        backend._lifecycle_lib.clear()
    yield
    with backend._lifecycle_lock:
        backend._lifecycle_references.clear()
        backend._lifecycle_lib.clear()


def test_send_is_not_blocked_by_a_running_receive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fix#141 lock split: send must not wait on receive."""
    lib = _SlowReceiveFake()
    monkeypatch.setattr(backend, "_load_library", lambda library_path: lib)
    interface = backend.CANFDInterface()

    def call_receive() -> None:
        try:
            interface.receive(timeout_ms=2000)
        except TransportError:
            pass

    recv_thread = threading.Thread(target=call_receive, daemon=True)
    recv_thread.start()
    # Wait for receive to be holding the vendor lock.
    assert lib.recv_entered.wait(timeout=1.0)

    # Now issue a send from the main thread; before the fix, this would
    # block until receive returned. Time-bound to detect regression.
    start = time.monotonic()
    interface.send(CANFDMessage(arbitration_id=1, data=b"x"))
    elapsed = time.monotonic() - start
    assert elapsed < 0.1, f"send blocked for {elapsed:.3f}s while receive was running"
    assert lib.send_observed_during_recv.is_set()

    # Release receive and cleanup.
    lib.recv_release.set()
    recv_thread.join(timeout=2.0)
    interface.close()


def test_close_drains_in_flight_send_before_calling_vendor_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """close() must wait for an in-flight send before CAN_CloseDevice.

    This protects the vendor library from a use-after-close on its USB
    handle. The test holds CANFD_Transmit inside the vendor call, then
    asserts close() blocks until the transmit returns, AND that
    CAN_CloseDevice is invoked only after the transmit has finished.
    """
    lib = _SlowReceiveFake()
    monkeypatch.setattr(backend, "_load_library", lambda library_path: lib)
    interface = backend.CANFDInterface()

    send_inside = threading.Event()
    send_release = threading.Event()
    transmit_finished = threading.Event()

    def slow_send(dev, channel, frame, count, timeout_ms):
        send_inside.set()
        send_release.wait(timeout=2.0)
        transmit_finished.set()
        return 1

    lib.CANFD_Transmit._hook = slow_send

    send_thread = threading.Thread(
        target=lambda: interface.send(CANFDMessage(arbitration_id=1, data=b"x")),
        daemon=True,
    )
    send_thread.start()
    assert send_inside.wait(timeout=1.0)
    # CAN_CloseDevice must not have been called yet.
    assert lib.CAN_CloseDevice.calls == []

    close_done = threading.Event()

    def call_close() -> None:
        interface.close()
        close_done.set()

    close_thread = threading.Thread(target=call_close, daemon=True)
    close_thread.start()

    # close() should block waiting for the in-flight send.
    time.sleep(0.05)
    assert not close_done.is_set(), "close() returned before send drained"
    assert lib.CAN_CloseDevice.calls == [], "CAN_CloseDevice called mid-transmit"

    # Allow send to finish; close should then proceed and call vendor close.
    send_release.set()
    assert transmit_finished.wait(timeout=1.0)
    assert close_done.wait(timeout=1.0)
    assert lib.CAN_CloseDevice.calls == [(0, 0)]

    send_thread.join(timeout=1.0)
    close_thread.join(timeout=1.0)


def test_close_fails_subsequent_calls_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After close, send/receive must raise immediately, not enter vendor."""
    lib = _SlowReceiveFake()
    monkeypatch.setattr(backend, "_load_library", lambda library_path: lib)
    interface = backend.CANFDInterface()

    interface.close()

    transmit_before = len(lib.CANFD_Transmit.calls)
    with pytest.raises(TransportError, match="closed"):
        interface.send(CANFDMessage(arbitration_id=1, data=b""))
    with pytest.raises(TransportError, match="closed"):
        interface.receive()
    assert len(lib.CANFD_Transmit.calls) == transmit_before


def test_concurrent_sends_serialize_through_tx_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multiple threads sending must serialize on _tx_lock, not crash."""
    lib = _SlowReceiveFake()
    monkeypatch.setattr(backend, "_load_library", lambda library_path: lib)
    interface = backend.CANFDInterface()
    # Replace transmit with a non-blocking fast path.
    lib.CANFD_Transmit._hook = lambda *args: 1

    errors: list[Exception] = []

    def send_many() -> None:
        try:
            for _ in range(50):
                interface.send(CANFDMessage(arbitration_id=1, data=b"x"))
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=send_many, daemon=True) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert errors == []
    assert len(lib.CANFD_Transmit.calls) == 200
    interface.close()


def _start_call(operation: Callable[[], object]):
    errors = []
    done = threading.Event()

    def run():
        try:
            operation()
        except Exception as error:
            errors.append(error)
        finally:
            done.set()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, done, errors


@pytest.fixture(params=["Linux", "Windows"])
def bounded_native(monkeypatch, request):
    monkeypatch.setattr(backend.platform, "system", lambda: request.param)
    monkeypatch.setattr(backend, "_CLOSE_WAIT_TIMEOUT_S", 0.05, raising=False)
    monkeypatch.setattr(dispatcher_module, "THREAD_JOIN_TIMEOUT_S", 0.01)
    lib = _SlowReceiveFake()

    def idle_receive(*args):
        time.sleep(0.001)
        return 0

    lib.CANFD_Receive._hook = idle_receive
    monkeypatch.setattr(backend, "_load_library", lambda path: lib)
    interface = backend.CANFDInterface()
    try:
        yield interface, lib
    finally:
        interface.close()


@pytest.mark.parametrize("operation", ["receive", "send", "send_batch", "info"])
def test_close_timeout_retains_native_ownership_until_io_returns(
    bounded_native, operation
):
    interface, lib = bounded_native
    entered = threading.Event()
    release = threading.Event()
    message = CANFDMessage(1, b"x")

    def block(*args):
        entered.set()
        assert release.wait(timeout=5)
        return args[-2] if operation in ("send", "send_batch") else 0

    if operation == "receive":
        lib.CANFD_Receive._hook = block
        call = interface.receive
    elif operation == "info":
        lib.CAN_ReadDevInfo._hook = block
        call = interface.read_dev_info
    else:
        lib.CANFD_Transmit._hook = block

        def call():
            if operation == "send":
                return interface.send(message)
            return interface.send_batch([message])

    io_thread, _, io_errors = _start_call(call)
    closer = None
    try:
        assert entered.wait(timeout=1)
        closer, done, errors = _start_call(interface.close)
        assert done.wait(timeout=0.5), "close must not wait for native I/O forever"
        assert len(errors) == 1
        assert isinstance(errors[0], TimeoutError)
        assert interface._closed_event.is_set()
        assert interface._close_complete_event.is_set()
        assert not interface._close_succeeded
        assert interface._device_open
        assert lib.CAN_CloseDevice.calls == []
        assert lib.LibCANbus_Exit.calls == []
        if not interface._windows:
            assert not interface._lifecycle_ref.released
            assert sum(backend._lifecycle_snapshot().values()) == 1
        for new_call in (interface.receive, lambda: interface.send(message)):
            with pytest.raises(TransportError, match="closed"):
                new_call()
        result = enumerate_minicanfd_channels()
        assert [failure.stage for failure in result.failures] == ["busy"]
    finally:
        release.set()
        io_thread.join(timeout=1)
        if closer is not None:
            closer.join(timeout=1)
        interface.close()
    assert not io_thread.is_alive()
    assert not closer.is_alive()
    assert io_errors == []
    assert len(lib.CAN_CloseDevice.calls) == 1
    assert len(lib.LibCANbus_Exit.calls) == (0 if interface._windows else 1)


def test_concurrent_close_waiter_times_out_without_releasing_owner(bounded_native):
    interface, lib = bounded_native
    entered = threading.Event()
    release = threading.Event()

    def close(*args):
        entered.set()
        assert release.wait(timeout=5)
        return 0

    lib.CAN_CloseDevice._hook = close
    owner, _, owner_errors = _start_call(interface.close)
    waiter = None
    try:
        assert entered.wait(timeout=1)
        waiter, done, errors = _start_call(interface.close)
        assert done.wait(timeout=0.5), (
            "a concurrent close caller must return on timeout"
        )
        assert len(errors) == 1
        assert isinstance(errors[0], TimeoutError)
        assert not interface._close_complete_event.is_set()
        assert interface._device_open
        assert len(lib.CAN_CloseDevice.calls) == 1
        assert lib.LibCANbus_Exit.calls == []
    finally:
        release.set()
        owner.join(timeout=1)
        if waiter is not None:
            waiter.join(timeout=1)
        interface.close()
    assert owner_errors == []
    assert not owner.is_alive()
    assert not waiter.is_alive()
    assert interface._close_complete_event.is_set()
    assert len(lib.CAN_CloseDevice.calls) == 1


def test_pool_retains_timed_out_native_close_and_recovers_after_receive_returns(
    bounded_native,
):
    interface, lib = bounded_native
    entered = threading.Event()
    release = threading.Event()

    def receive(*args):
        entered.set()
        assert release.wait(timeout=5)
        return 0

    lib.CANFD_Receive._hook = receive
    instances = []

    def create(on_error):
        current = interface if not instances else backend.CANFDInterface()
        dispatcher = CANFDMessageDispatcher(interface=current, on_bus_error=on_error)
        instances.append(dispatcher)
        return dispatcher

    pool = SharedTransportPool()
    transport = CANFDTransport(dispatcher_factory=create)
    key = transport.pool_key
    lease = pool.acquire(key, lambda: transport)
    closer = None
    try:
        route = transport.subscribe_route(CANRouteKey(1, True))
        assert entered.wait(timeout=1)
        closer, done, errors = _start_call(lease.close)
        assert done.wait(timeout=0.5)
        assert len(errors) == 1
        assert isinstance(errors[0], TimeoutError)
        health = transport.get_health()
        assert health.state is TransportState.FAULTED
        assert health.last_error_code is ErrorCode.TIMEOUT
        assert pool.lease_count(key) == 0
        assert pool.active_keys() == (key,)
        assert not instances[0]._interface_closed
        with pytest.raises(ClosedError):
            route.receive(timeout_s=0)
        with pytest.raises(TimeoutError):
            pool.acquire(key, lambda: pytest.fail("must retain the old owner"))
        assert len(instances) == 1
        assert len(lib.CAN_OpenDevice.calls) == 1
        assert lib.CAN_CloseDevice.calls == []
        release.set()
        instances[0]._recv_thread.join(timeout=1)
        assert not instances[0]._recv_thread.is_alive()
        fresh = pool.acquire(key, lambda: pytest.fail("must reuse the transport"))
        assert fresh.transport is transport
        assert transport.get_health().state is TransportState.OPEN
        assert len(instances) == 2
        assert len(lib.CAN_OpenDevice.calls) == 2
        fresh.close()
        lease.close()
        assert pool.active_keys() == ()
        assert len(lib.CAN_CloseDevice.calls) == 2
    finally:
        release.set()
        if closer is not None:
            closer.join(timeout=1)
        transport.close()
        pool.close_idle(key)
        for dispatcher in instances:
            dispatcher.stop()
            assert not dispatcher._recv_thread.is_alive()
            assert not dispatcher._send_thread.is_alive()


def test_stalled_native_send_fails_receipts_without_releasing_active_handle(
    bounded_native,
):
    interface, lib = bounded_native
    entered = threading.Event()
    release = threading.Event()

    def send(*args):
        entered.set()
        assert release.wait(timeout=5)
        return args[-2]

    lib.CANFD_Transmit._hook = send
    dispatcher = CANFDMessageDispatcher(interface=interface)
    closer = None
    try:
        first = dispatcher.send(CANFDMessage(1, b"first"))
        assert entered.wait(timeout=1)
        second = dispatcher.send(CANFDMessage(1, b"second"))
        closer, done, errors = _start_call(dispatcher.stop)
        assert done.wait(timeout=0.5)
        assert len(errors) == 1
        assert isinstance(errors[0], TimeoutError)
        for receipt in (first, second):
            with pytest.raises(ClosedError):
                receipt.result(timeout=0)
        assert lib.CAN_CloseDevice.calls == []
        assert not dispatcher._interface_closed
        release.set()
        dispatcher._send_thread.join(timeout=1)
        dispatcher.stop()
        assert len(lib.CANFD_Transmit.calls) == 1
        assert len(lib.CAN_CloseDevice.calls) == 1
        for receipt in (first, second):
            with pytest.raises(ClosedError):
                receipt.result(timeout=0)
    finally:
        release.set()
        if closer is not None:
            closer.join(timeout=1)
        dispatcher.stop()
        assert not dispatcher._recv_thread.is_alive()
        assert not dispatcher._send_thread.is_alive()


@pytest.mark.parametrize("phase", ["enumeration", "lifecycle"])
def test_close_timeout_on_shared_lock_preserves_remaining_cleanup(monkeypatch, phase):
    monkeypatch.setattr(backend, "_CLOSE_WAIT_TIMEOUT_S", 0.05)
    lib = _SlowReceiveFake()
    monkeypatch.setattr(backend, "_load_library", lambda path: lib)
    interface = backend.CANFDInterface()
    lock = (
        backend._enumeration_lock if phase == "enumeration" else backend._lifecycle_lock
    )
    closer = None
    try:
        with lock:
            closer, done, errors = _start_call(interface.close)
            assert done.wait(timeout=0.5)
            assert len(errors) == 1
            assert isinstance(errors[0], TimeoutError)
            assert phase in str(errors[0])
            assert not interface._close_succeeded
            assert not interface._lifecycle_ref.released
            assert len(lib.CAN_CloseDevice.calls) == (
                0 if phase == "enumeration" else 1
            )
            assert lib.LibCANbus_Exit.calls == []
    finally:
        if closer is not None:
            closer.join(timeout=1)
        interface.close()
    assert interface._close_succeeded
    assert len(lib.CAN_CloseDevice.calls) == 1
    assert len(lib.LibCANbus_Exit.calls) == 1


def test_close_uses_one_budget_for_transmit_and_receive_drain(monkeypatch):
    lib = _SlowReceiveFake()
    monkeypatch.setattr(backend, "_load_library", lambda path: lib)
    interface = backend.CANFDInterface()
    clock = [0.0]

    class DrainingLock:
        def __init__(self):
            self.held = False

        def acquire(self, *, timeout):
            clock[0] += min(timeout, 0.6)
            self.held = timeout >= 0.6
            return self.held

        def release(self):
            assert self.held
            self.held = False

    tx = DrainingLock()
    rx = DrainingLock()
    try:
        with monkeypatch.context() as patch:
            patch.setattr(backend, "_CLOSE_WAIT_TIMEOUT_S", 1.0)
            patch.setattr(backend.time, "monotonic", lambda: clock[0])
            patch.setattr(interface, "_tx_lock", tx)
            patch.setattr(interface, "_rx_lock", rx)
            with pytest.raises(TimeoutError, match="receive I/O"):
                interface.close()
        assert clock[0] == pytest.approx(1.0)
        assert not tx.held
        assert not rx.held
        assert lib.CAN_CloseDevice.calls == []
        assert lib.LibCANbus_Exit.calls == []
    finally:
        interface.close()


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit])
def test_interrupted_close_retains_ownership_and_releases_close_lock(
    monkeypatch, error_type
):
    lib = _SlowReceiveFake()
    monkeypatch.setattr(backend, "_load_library", lambda path: lib)
    interface = backend.CANFDInterface()
    interrupt = error_type("interrupted lock wait")

    class InterruptedLock:
        def acquire(self, **kwargs):
            raise interrupt

    try:
        with monkeypatch.context() as patch:
            patch.setattr(interface, "_tx_lock", InterruptedLock())
            with pytest.raises(error_type) as raised:
                interface.close()
        assert raised.value is interrupt
        assert interface._close_complete_event.is_set()
        assert not interface._close_lock.locked()
        assert not interface._lifecycle_ref.released
        assert lib.CAN_CloseDevice.calls == []
        assert lib.LibCANbus_Exit.calls == []
    finally:
        interface.close()
