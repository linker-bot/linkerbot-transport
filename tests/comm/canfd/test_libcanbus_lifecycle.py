"""LibCANbus_Init / LibCANbus_Exit refcount tests.

The vendor library's process-level lifecycle (PDF §3.1/§3.2) was previously
never invoked by the SDK, leaking ``LibCANbus_Init``-allocated resources for
the lifetime of the process. These tests pin the refcount semantics:

- ``LibCANbus_Init`` runs exactly once for any number of concurrent
  ``CANFDInterface`` instances backed by the same library handle.
- ``LibCANbus_Exit`` runs exactly once, after every interface has closed.
- A constructor failure after acquire (e.g. ``CANFD_Init`` returns non-zero)
  releases the refcount so partial initialization does not leak.
- Garbage-collecting an unclosed interface still releases the refcount.
- Calling ``LibCANbus_Init`` itself returning non-zero surfaces a ``TransportError``
  and does not bump the refcount.
"""

from __future__ import annotations

import gc
import threading
from collections.abc import Callable
from typing import Any

import pytest

from linkerbot_transport.canfd import ctypes_backend as backend
from linkerbot_transport.errors import TransportError


class _FakeFn:
    def __init__(
        self,
        result: int = 0,
        hook: Callable[..., int] | None = None,
    ) -> None:
        self.result = result
        self.hook = hook
        self.calls: list[tuple[Any, ...]] = []
        self.restype: Any = None
        self.argtypes: list[Any] | None = None

    def __call__(self, *args: Any) -> int:
        self.calls.append(args)
        if self.hook is not None:
            return self.hook(*args)
        return self.result


class _FakeLib:
    """Stand-in for the vendor ``CDLL``; explicit ``_name`` keys the refcount."""

    def __init__(self, name: str = "/fake/libcanbus.so") -> None:
        self._name = name
        self.LibCANbus_Init = _FakeFn(0)
        self.LibCANbus_Exit = _FakeFn(0)
        self.CAN_ScanDevice = _FakeFn(1)
        self.CAN_OpenDevice = _FakeFn(0)
        self.CAN_CloseDevice = _FakeFn(0)
        self.CAN_ReadDevInfo = _FakeFn(0)
        self.CANFD_Init = _FakeFn(0)
        self.CANFD_Transmit = _FakeFn(1)
        self.CANFD_Receive = _FakeFn(0)


@pytest.fixture(autouse=True)
def _reset_lifecycle_state():
    """Wipe the module-level refcount table between tests for isolation."""
    with backend._lifecycle_lock:
        backend._lifecycle_refcount.clear()
        backend._lifecycle_lib.clear()
    yield
    with backend._lifecycle_lock:
        backend._lifecycle_refcount.clear()
        backend._lifecycle_lib.clear()


def _patch_loader(monkeypatch: pytest.MonkeyPatch, lib: _FakeLib) -> None:
    monkeypatch.setattr(backend, "_load_library", lambda library_path: lib)


def test_init_calls_libcanbus_init_once(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = _FakeLib()
    _patch_loader(monkeypatch, lib)

    interface = backend.CANFDInterface()

    assert len(lib.LibCANbus_Init.calls) == 1
    assert lib.LibCANbus_Exit.calls == []
    snapshot = backend._lifecycle_snapshot()
    assert sum(snapshot.values()) == 1

    interface.close()
    assert len(lib.LibCANbus_Exit.calls) == 1
    assert backend._lifecycle_snapshot() == {}


def test_multiple_interfaces_share_one_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = _FakeLib()
    _patch_loader(monkeypatch, lib)

    a = backend.CANFDInterface(channel_index=0)
    b = backend.CANFDInterface(channel_index=1)

    # Init still runs only once across two interfaces sharing the same lib.
    assert len(lib.LibCANbus_Init.calls) == 1
    assert len(lib.LibCANbus_Exit.calls) == 0

    a.close()
    assert len(lib.LibCANbus_Exit.calls) == 0  # b still holds the refcount

    b.close()
    assert len(lib.LibCANbus_Exit.calls) == 1
    assert backend._lifecycle_snapshot() == {}


def test_partial_init_failure_releases_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = _FakeLib()
    lib.CANFD_Init.result = 3  # vendor init fails after we already acquired
    _patch_loader(monkeypatch, lib)

    with pytest.raises(TransportError, match="CANFD_Init"):
        backend.CANFDInterface()

    # Acquire ran (Init called), but partial-init guard must release on
    # exception so the refcount is back to zero and Exit ran.
    assert len(lib.LibCANbus_Init.calls) == 1
    assert len(lib.LibCANbus_Exit.calls) == 1
    assert backend._lifecycle_snapshot() == {}


def test_init_exception_closes_device_and_releases_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = _FakeLib()

    def raise_from_init(*args: Any) -> int:
        raise OSError("vendor init crashed")

    lib.CANFD_Init.hook = raise_from_init
    _patch_loader(monkeypatch, lib)

    with pytest.raises(OSError, match="vendor init crashed"):
        backend.CANFDInterface()

    assert lib.CAN_CloseDevice.calls == [(0, 0)]
    assert len(lib.LibCANbus_Exit.calls) == 1
    assert backend._lifecycle_snapshot() == {}


def test_libcanbus_init_nonzero_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = _FakeLib()
    lib.LibCANbus_Init.result = -1
    _patch_loader(monkeypatch, lib)

    with pytest.raises(TransportError, match="LibCANbus_Init"):
        backend.CANFDInterface()

    # Refcount must not bump on Init failure so a later acquire retries.
    assert backend._lifecycle_snapshot() == {}
    assert len(lib.LibCANbus_Exit.calls) == 0


def test_close_failure_retains_refcount_until_retry_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = _FakeLib()
    lib.CAN_CloseDevice.result = 1
    _patch_loader(monkeypatch, lib)
    interface = backend.CANFDInterface()

    with pytest.raises(TransportError, match="CAN_CloseDevice"):
        interface.close()

    key = backend._library_key(lib)
    assert len(lib.LibCANbus_Exit.calls) == 0
    assert backend._lifecycle_snapshot() == {key: 1}

    lib.CAN_CloseDevice.result = 0
    interface.close()

    assert lib.CAN_CloseDevice.calls == [(0, 0), (0, 0)]
    assert len(lib.LibCANbus_Exit.calls) == 1
    assert backend._lifecycle_snapshot() == {}


def test_close_exception_retains_refcount_until_retry_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = _FakeLib()

    def raise_from_close(*args: Any) -> int:
        raise OSError("vendor close crashed")

    lib.CAN_CloseDevice.hook = raise_from_close
    _patch_loader(monkeypatch, lib)
    interface = backend.CANFDInterface()

    with pytest.raises(TransportError, match="vendor close crashed"):
        interface.close()

    key = backend._library_key(lib)
    assert len(lib.LibCANbus_Exit.calls) == 0
    assert backend._lifecycle_snapshot() == {key: 1}

    lib.CAN_CloseDevice.hook = None
    interface.close()

    assert lib.CAN_CloseDevice.calls == [(0, 0), (0, 0)]
    assert len(lib.LibCANbus_Exit.calls) == 1
    assert backend._lifecycle_snapshot() == {}


def test_release_serializes_exit_before_next_acquire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = _FakeLib()
    exit_started = threading.Event()
    allow_exit = threading.Event()
    second_init_started = threading.Event()
    init_count = 0

    def observe_init(*args: Any) -> int:
        nonlocal init_count
        init_count += 1
        if init_count == 2:
            second_init_started.set()
        return 0

    def blocking_exit(*args: Any) -> int:
        exit_started.set()
        assert allow_exit.wait(timeout=2.0)
        return 0

    lib.LibCANbus_Init.hook = observe_init
    lib.LibCANbus_Exit.hook = blocking_exit
    _patch_loader(monkeypatch, lib)
    first = backend.CANFDInterface()

    close_thread = threading.Thread(target=first.close, daemon=True)
    close_thread.start()
    assert exit_started.wait(timeout=1.0)

    opened: list[backend.CANFDInterface] = []
    open_attempted = threading.Event()

    def open_next() -> None:
        open_attempted.set()
        opened.append(backend.CANFDInterface())

    open_thread = threading.Thread(target=open_next, daemon=True)
    open_thread.start()
    assert open_attempted.wait(timeout=1.0)
    assert not second_init_started.wait(timeout=0.05)
    assert opened == []

    allow_exit.set()
    close_thread.join(timeout=1.0)
    open_thread.join(timeout=1.0)
    assert not close_thread.is_alive()
    assert not open_thread.is_alive()
    assert len(opened) == 1
    assert second_init_started.is_set()
    assert len(lib.LibCANbus_Init.calls) == 2

    opened[0].close()


def test_garbage_collection_releases_refcount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = _FakeLib()
    _patch_loader(monkeypatch, lib)

    interface = backend.CANFDInterface()
    assert backend._lifecycle_snapshot() == {backend._library_key(lib): 1}

    del interface
    gc.collect()

    # __del__ best-effort close should have run.
    assert len(lib.LibCANbus_Exit.calls) == 1
    assert backend._lifecycle_snapshot() == {}


def test_libraries_with_different_names_get_independent_refcounts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib_a = _FakeLib(name="/fake/a.so")
    lib_b = _FakeLib(name="/fake/b.so")

    # Cycle the loader so each interface picks up its own fake.
    libs = [lib_a, lib_b]
    monkeypatch.setattr(backend, "_load_library", lambda library_path: libs.pop(0))

    a = backend.CANFDInterface()
    b = backend.CANFDInterface()

    assert len(lib_a.LibCANbus_Init.calls) == 1
    assert len(lib_b.LibCANbus_Init.calls) == 1
    assert sum(backend._lifecycle_snapshot().values()) == 2

    a.close()
    b.close()
    assert backend._lifecycle_snapshot() == {}
    assert len(lib_a.LibCANbus_Exit.calls) == 1
    assert len(lib_b.LibCANbus_Exit.calls) == 1


def test_release_after_finalize_unsafe_is_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = _FakeLib()
    _patch_loader(monkeypatch, lib)
    interface = backend.CANFDInterface()

    # Simulate interpreter shutdown: the atexit hook flips _finalize_safe.
    monkeypatch.setattr(backend, "_finalize_safe", False)

    interface.close()
    # CAN_CloseDevice still runs (it's harmless and bounded), but the
    # lifecycle release short-circuits so Exit does not run on a torn-down lib.
    assert len(lib.LibCANbus_Exit.calls) == 0


def test_missing_libcanbus_init_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = _FakeLib()
    del lib.LibCANbus_Init  # simulate a too-old vendor library
    _patch_loader(monkeypatch, lib)

    with pytest.raises(TransportError, match="missing LibCANbus_Init"):
        backend.CANFDInterface()


def test_native_handle_unifies_absolute_and_loader_library_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _FakeLib("libcanbus.so")
    second = _FakeLib("/resolved/libcanbus.so")
    first._handle = second._handle = 1234
    libs = iter((first, second))
    monkeypatch.setattr(backend, "_load_library", lambda path: next(libs))

    a = backend.CANFDInterface(channel_index=0)
    b = backend.CANFDInterface(channel_index=1)
    try:
        assert backend._library_key(first) == backend._library_key(second)
        assert first.LibCANbus_Init.calls == [()]
        assert second.LibCANbus_Init.calls == []
        a.close()
        assert first.LibCANbus_Exit.calls == []
    finally:
        a.close()
        b.close()
    assert first.LibCANbus_Exit.calls == [()]
    assert second.LibCANbus_Exit.calls == []


@pytest.mark.parametrize("raises", [False, True])
def test_failed_library_exit_is_retried_before_another_initialization(
    monkeypatch: pytest.MonkeyPatch, raises: bool
) -> None:
    lib = _FakeLib()
    _patch_loader(monkeypatch, lib)
    interface = backend.CANFDInterface()
    original = OSError("USB cleanup failed")

    def fail_exit():
        if raises:
            raise original
        return -9

    lib.LibCANbus_Exit.hook = fail_exit
    with pytest.raises(TransportError, match="LibCANbus_Exit"):
        interface.close()
    assert backend._lifecycle_snapshot() == {}
    assert backend._library_key(lib) in backend._lifecycle_lib
    with pytest.raises(TransportError, match="LibCANbus_Exit") as caught:
        backend.CANFDInterface()
    if raises:
        assert caught.value.cause is original
    assert lib.LibCANbus_Init.calls == [()]
    assert lib.CAN_OpenDevice.calls == [(0, 0)]

    lib.LibCANbus_Exit.hook = None
    recovered = backend.CANFDInterface()
    try:
        assert len(lib.LibCANbus_Init.calls) == 2
        exits_before_retry = len(lib.LibCANbus_Exit.calls)
        interface.close()
        assert len(lib.LibCANbus_Exit.calls) == exits_before_retry
        assert backend._lifecycle_snapshot() == {backend._library_key(lib): 1}
        assert lib.CAN_CloseDevice.calls == [(0, 0)]
    finally:
        recovered.close()
    assert backend._lifecycle_lib == {}


@pytest.mark.parametrize("failure", ["status", "exception", "interrupt"])
def test_failed_library_exit_can_retry_close_without_double_channel_release(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    lib = _FakeLib()
    _patch_loader(monkeypatch, lib)
    interface = backend.CANFDInterface()

    def fail_exit() -> int:
        if failure == "interrupt":
            raise KeyboardInterrupt("interrupted cleanup")
        if failure == "exception":
            raise OSError("failed cleanup")
        return -9

    lib.LibCANbus_Exit.hook = fail_exit
    expected_error = KeyboardInterrupt if failure == "interrupt" else TransportError
    with pytest.raises(expected_error):
        interface.close()
    assert backend._lifecycle_snapshot() == {}
    assert not interface._close_succeeded
    assert backend._library_key(lib) in backend._lifecycle_lib

    lib.LibCANbus_Exit.hook = None
    interface.close()
    interface.close()
    assert interface._close_succeeded
    assert lib.CAN_CloseDevice.calls == [(0, 0)]
    assert len(lib.LibCANbus_Exit.calls) == 2
    assert backend._lifecycle_lib == {}


def test_initialization_failure_retains_interface_when_library_exit_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = _FakeLib()
    lib.CANFD_Init.result = 3
    lib.LibCANbus_Exit.result = -9
    _patch_loader(monkeypatch, lib)

    with pytest.raises(TransportError, match="CANFD_Init") as caught:
        backend.CANFDInterface()
    interface = caught.value._failed_interface
    assert not interface._close_succeeded
    assert backend._lifecycle_snapshot() == {}
    assert backend._library_key(lib) in backend._lifecycle_lib

    lib.LibCANbus_Exit.result = 0
    interface.close()
    assert lib.CAN_CloseDevice.calls == [(0, 0)]
    assert len(lib.LibCANbus_Exit.calls) == 2
    assert backend._lifecycle_lib == {}
