"""MiniCANFD enumeration contracts for both ABIs, without vendor hardware."""

import ctypes
import os
import threading
from dataclasses import asdict
from types import SimpleNamespace

import pytest

from linkerbot_transport.canfd import (
    CANFDTransport,
    enumerate_minicanfd_channels,
    enumeration,
)
from linkerbot_transport.canfd import ctypes_backend as backend
from linkerbot_transport.errors import TransportError, ValidationError


class Function:
    def __init__(self, arity, result=0, hook=None):
        self.arity = arity
        self.result = result
        self.hook = hook
        self.calls = []
        self.argtypes = None
        self.restype = None

    def __call__(self, *args):
        assert len(args) == self.arity
        assert len(self.argtypes) == self.arity
        self.calls.append(args)
        return self.hook(*args) if self.hook else self.result


class Library:
    def __init__(self, path, windows=False):
        self._name = str(path)
        channel_args = 1 if windows else 2
        self.CAN_ScanDevice = Function(0, 2)
        self.CAN_OpenDevice = Function(channel_args)
        self.CAN_CloseDevice = Function(channel_args)
        self.CAN_ReadDevInfo = Function(2)
        self.CANFD_Init = Function(channel_args + 1)
        self.CANFD_Transmit = Function(channel_args + 3, 1)
        self.CANFD_Receive = Function(channel_args + 3)
        if windows:
            self.CAN_GetDevType = Function(1, 1)
        else:
            self.LibCANbus_Init = Function(0)
            self.LibCANbus_Exit = Function(0)
            self.CAN_GetCHType = Function(2, 1)


@pytest.fixture
def library_factory(monkeypatch, tmp_path):
    monkeypatch.setattr(backend, "_lifecycle_refcount", {})
    monkeypatch.setattr(backend, "_lifecycle_lib", {})
    monkeypatch.setattr(backend, "_open_channel_counts", {})
    monkeypatch.setattr(backend, "_scanned_device_counts", {})

    def make(windows=False, legacy=False):
        monkeypatch.setattr(
            backend.platform, "system", lambda: "Windows" if windows else "Linux"
        )
        lib = Library(
            tmp_path / ("HCanbus.dll" if windows else "libcanbus.so"), windows
        )
        if legacy:
            del lib.LibCANbus_Init, lib.LibCANbus_Exit, lib.CAN_GetCHType
            lib.CAN_GetDevType = Function(1, 1)
        monkeypatch.setattr(backend, "_load_library", lambda path: lib)
        return lib

    return make


def assert_no_channel_io(lib):
    for name in (
        "CAN_OpenDevice",
        "CAN_CloseDevice",
        "CAN_ReadDevInfo",
        "CANFD_Init",
        "CANFD_Transmit",
        "CANFD_Receive",
    ):
        assert getattr(lib, name).calls == []


def test_windows_native_channel_numbers_and_connection_reconstruction(library_factory):
    lib = library_factory(windows=True)
    lib.CAN_ScanDevice.result = 4
    lib.CAN_GetDevType.hook = lambda dev: (1, 0, 2, 1)[dev]

    result = enumerate_minicanfd_channels()

    assert result.failures == ()
    assert [(item.device_index, item.channel_index) for item in result.channels] == [
        (0, 0),
        (3, 0),
    ]
    assert lib.CAN_GetDevType.calls == [(0,), (1,), (2,), (3,)]
    assert lib.CAN_GetDevType.argtypes == [ctypes.c_uint]
    transport = CANFDTransport(**asdict(result.channels[1]))
    assert transport.pool_key.device_index == 3
    assert transport.pool_key.channel_index == 0
    assert transport.pool_key.library_path == lib._name
    assert_no_channel_io(lib)


def test_windows_distributed_library_type_symbol_spelling(library_factory):
    lib = library_factory(windows=True)
    lib.CAN_GeDevType = lib.CAN_GetDevType
    del lib.CAN_GetDevType

    result = enumerate_minicanfd_channels()

    assert len(result.channels) == 2
    assert result.failures == ()
    assert lib.CAN_GeDevType.calls == [(0,), (1,)]


def test_system_loader_name_is_resolved_before_connection_reconstruction(
    library_factory, monkeypatch
):
    lib = library_factory()
    resolved_path = lib._name
    lib._name = "libcanbus.so"
    monkeypatch.setattr(
        enumeration, "_native_library_path", lambda loaded, windows: resolved_path
    )

    result = enumerate_minicanfd_channels(linux_channels={0: (0,), 1: ()})

    assert result.failures == ()
    assert result.channels[0].library_path == resolved_path
    transport = CANFDTransport(**asdict(result.channels[0]))
    assert transport.pool_key.library_path == resolved_path


@pytest.mark.skipif(os.name == "nt", reason="Linux dladdr contract")
def test_linux_native_path_lookup_uses_loaded_symbol_without_calling_it():
    system = ctypes.CDLL(None)
    lib = SimpleNamespace(CAN_ScanDevice=system.printf)

    path = enumeration._native_library_path(lib, False)

    assert os.path.isabs(path)
    assert os.path.isfile(path)


def test_windows_native_path_lookup_uses_module_handle(monkeypatch, tmp_path):
    expected = str(tmp_path / "HCanbus.dll")

    def get_path(handle, buffer, size):
        assert handle == 1234 and size == 32768
        buffer.value = expected
        return len(expected)

    kernel = SimpleNamespace(GetModuleFileNameW=Function(3, hook=get_path))
    monkeypatch.setattr(ctypes, "WinDLL", lambda *args, **kwargs: kernel, raising=False)

    assert (
        enumeration._native_library_path(SimpleNamespace(_handle=1234), True)
        == expected
    )


def test_linux_uses_only_declared_channels_and_filters_classic_and_lin(library_factory):
    lib = library_factory()
    lib.CAN_GetCHType.hook = lambda dev, channel: {
        (0, 0): 1,
        (1, 0): 0,
        (1, 1): 2,
        (1, 2): 1,
    }[dev, channel]

    result = enumerate_minicanfd_channels(linux_channels={0: (0,), 1: (0, 1, 2, 2)})

    assert result.failures == ()
    assert [(item.device_index, item.channel_index) for item in result.channels] == [
        (0, 0),
        (1, 2),
    ]
    assert lib.CAN_GetCHType.calls == [(0, 0), (1, 0), (1, 1), (1, 2)]
    assert lib.CAN_GetCHType.argtypes == [ctypes.c_uint, ctypes.c_uint]
    assert lib.LibCANbus_Init.calls == [()]
    assert lib.LibCANbus_Exit.calls == [()]
    assert backend._lifecycle_snapshot() == {}
    assert_no_channel_io(lib)


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("count", [0, 1, 3])
def test_linux_defaults_to_channel_zero_on_every_scanned_adapter(
    library_factory, legacy, count
):
    lib = library_factory(legacy=legacy)
    lib.CAN_ScanDevice.result = count

    result = enumerate_minicanfd_channels()

    assert result.failures == ()
    assert [(item.device_index, item.channel_index) for item in result.channels] == [
        (device, 0) for device in range(count)
    ]
    assert lib.CAN_ScanDevice.calls == [()]
    if legacy:
        assert lib.CAN_GetDevType.calls == [(device,) for device in range(count)]
    else:
        assert lib.CAN_GetCHType.calls == [(device, 0) for device in range(count)]
        assert lib.LibCANbus_Init.calls == [()]
        assert lib.LibCANbus_Exit.calls == [()]
    assert backend._lifecycle_snapshot() == {}
    assert_no_channel_io(lib)


@pytest.mark.parametrize("legacy", [False, True])
def test_linux_explicit_empty_layout_does_not_enable_defaults(library_factory, legacy):
    lib = library_factory(legacy=legacy)

    result = enumerate_minicanfd_channels(linux_channels={})

    assert result.channels == ()
    assert [(failure.stage, failure.device_index) for failure in result.failures] == [
        ("layout", 0),
        ("layout", 1),
    ]
    if legacy:
        assert lib.CAN_GetDevType.calls == []
    else:
        assert lib.CAN_GetCHType.calls == []
    assert_no_channel_io(lib)


def test_linux_partial_layout_keeps_valid_channels_and_explicitly_skips_empty(
    library_factory,
):
    lib = library_factory()
    lib.CAN_ScanDevice.result = 3

    result = enumerate_minicanfd_channels(linux_channels={0: (0,), 1: (), 8: (0,)})

    assert [(item.device_index, item.channel_index) for item in result.channels] == [
        (0, 0)
    ]
    assert [(item.stage, item.device_index) for item in result.failures] == [
        ("layout", 2),
        ("layout", 8),
    ]
    assert "not scanned" in str(result.failures[1].error)


@pytest.mark.parametrize("windows", [False, True])
def test_no_adapters_is_successful_empty_result(library_factory, windows):
    lib = library_factory(windows)
    lib.CAN_ScanDevice.result = 0

    result = enumerate_minicanfd_channels()

    assert result.channels == result.failures == ()
    assert_no_channel_io(lib)


@pytest.mark.parametrize("windows", [False, True])
def test_scan_failure_preserves_driver_status_and_releases_linux_lifecycle(
    library_factory, windows
):
    lib = library_factory(windows)
    lib.CAN_ScanDevice.result = -7

    result = enumerate_minicanfd_channels()

    assert result.channels == ()
    assert result.failures[0].stage == "scan"
    assert "status -7" in str(result.failures[0].error)
    if not windows:
        assert lib.LibCANbus_Exit.calls == [()]
        assert backend._lifecycle_snapshot() == {}


def test_type_failure_is_isolated_and_retains_original_error(library_factory):
    lib = library_factory(windows=True)
    original = OSError("USB device disconnected")

    def get_type(dev):
        if dev == 0:
            raise original
        return 1

    lib.CAN_GetDevType.hook = get_type
    result = enumerate_minicanfd_channels()

    assert [item.device_index for item in result.channels] == [1]
    assert result.failures[0].error.cause is original
    assert (result.failures[0].device_index, result.failures[0].channel_index) == (0, 0)


@pytest.mark.parametrize("value", [-1, 3, 255])
def test_unknown_or_failed_channel_type_is_diagnostic(library_factory, value):
    lib = library_factory(windows=True)
    lib.CAN_GetDevType.result = value

    result = enumerate_minicanfd_channels()

    assert not result.channels
    assert len(result.failures) == 2
    assert str(value) in str(result.failures[0].error)


def test_missing_type_entry_does_not_infer_fd_or_try_linux_device_type_alias(
    library_factory,
):
    lib = library_factory()
    del lib.CAN_GetCHType
    lib.CAN_GetDevType = Function(1, 1)

    result = enumerate_minicanfd_channels(linux_channels={0: (0,), 1: ()})

    assert result.channels == ()
    assert result.failures[0].stage == "type"
    assert "CAN_GetCHType" in str(result.failures[0].error)
    assert lib.CAN_GetDevType.calls == []


def test_legacy_linux_enumerates_confirmed_channels_and_opens_native_abi(
    library_factory,
):
    lib = library_factory(legacy=True)

    result = enumerate_minicanfd_channels(linux_channels={0: (0,), 1: (0,)})

    assert result.failures == ()
    assert [(c.device_index, c.channel_index) for c in result.channels] == [
        (0, 0),
        (1, 0),
    ]
    assert lib.CAN_GetDevType.calls == [(0,), (1,)]
    assert lib.CAN_GetDevType.argtypes == [ctypes.c_uint]
    assert_no_channel_io(lib)
    with backend.CANFDInterface(**asdict(result.channels[1])):
        assert lib.CAN_OpenDevice.calls == [(1, 0)]
        assert len(lib.CANFD_Init.calls) == 1
    assert lib.CAN_CloseDevice.calls == [(1, 0)]
    assert backend._lifecycle_snapshot() == {}


def test_legacy_linux_device_type_does_not_validate_other_channels(library_factory):
    lib = library_factory(legacy=True)

    result = enumerate_minicanfd_channels(linux_channels={0: (0, 1), 1: ()})

    assert [(c.device_index, c.channel_index) for c in result.channels] == [(0, 0)]
    assert result.failures[0].stage == "layout"
    assert result.failures[0].channel_index == 1
    assert lib.CAN_GetDevType.calls == [(0,)]
    with pytest.raises(ValidationError, match="only confirmed channel 0"):
        backend.CANFDInterface(channel_index=1)
    assert lib.CAN_OpenDevice.calls == []


@pytest.mark.parametrize("legacy", [False, True])
def test_linux_explicit_channels_keep_type_checks_and_failure_isolation(
    library_factory, legacy
):
    lib = library_factory(legacy=legacy)
    lib.CAN_ScanDevice.result = 5
    original = OSError("USB device disconnected")

    def get_type(device, *channels):
        if device == 3:
            raise original
        return (1, 0, 2, None, 1)[device]

    get_type_function = lib.CAN_GetDevType if legacy else lib.CAN_GetCHType
    get_type_function.hook = get_type

    result = enumerate_minicanfd_channels(
        linux_channels={device: (0,) for device in range(5)}
    )

    assert [(c.device_index, c.channel_index) for c in result.channels] == [
        (0, 0),
        (4, 0),
    ]
    assert len(result.failures) == 1
    failure = result.failures[0]
    assert (failure.stage, failure.device_index, failure.channel_index) == (
        "type",
        3,
        0,
    )
    assert failure.error.cause is original
    assert len(get_type_function.calls) == 5
    assert_no_channel_io(lib)


@pytest.mark.parametrize("channel_zero_type", [0, 2, -1])
def test_linux_default_type_fallback_finds_fd_channel_one(
    library_factory, channel_zero_type
):
    lib = library_factory()
    lib.CAN_ScanDevice.result = 1
    lib.CAN_GetCHType.hook = lambda device, channel: (
        channel_zero_type if channel == 0 else 1
    )

    result = enumerate_minicanfd_channels()

    assert [(c.device_index, c.channel_index) for c in result.channels] == [(0, 1)]
    assert lib.CAN_GetCHType.calls == [(0, 0), (0, 1)]
    assert lib.CAN_ScanDevice.calls == [()]
    assert lib.LibCANbus_Exit.calls == [()]
    assert_no_channel_io(lib)
    if channel_zero_type == -1:
        assert len(result.failures) == 1
        assert result.failures[0].channel_index == 0
    else:
        assert not result.failures


@pytest.mark.parametrize("channel_type", [0, 2, -1])
def test_linux_default_both_unavailable_channels_return_diagnostics(
    library_factory, channel_type
):
    lib = library_factory()
    lib.CAN_ScanDevice.result = 1
    lib.CAN_GetCHType.result = channel_type

    result = enumerate_minicanfd_channels()

    assert not result.channels
    assert result.failures
    assert result.failures[-1].stage == "type"
    assert result.failures[-1].device_index == 0
    assert result.failures[-1].channel_index == 1
    assert lib.CAN_GetCHType.calls == [(0, 0), (0, 1)]
    assert_no_channel_io(lib)


def test_legacy_default_non_fd_channel_does_not_query_unverifiable_channel_one(
    library_factory,
):
    lib = library_factory(legacy=True)
    lib.CAN_ScanDevice.result = 1
    lib.CAN_GetDevType.result = 0

    result = enumerate_minicanfd_channels()

    assert not result.channels
    assert result.failures[0].stage == "layout"
    assert result.failures[0].channel_index == 1
    assert lib.CAN_GetDevType.calls == [(0,)]
    assert_no_channel_io(lib)


def test_unknown_linux_export_set_is_not_treated_as_legacy(library_factory):
    lib = library_factory()
    del lib.LibCANbus_Init, lib.LibCANbus_Exit

    result = enumerate_minicanfd_channels(linux_channels={0: (0,), 1: ()})

    assert result.failures[0].stage == "lifecycle"
    assert not result.channels
    assert lib.CAN_ScanDevice.calls == []


def test_missing_library_failure_is_not_empty_success(monkeypatch):
    monkeypatch.setattr(backend.platform, "system", lambda: "Linux")
    original = TransportError("selected library is missing its USB dependency")

    def fail(path):
        assert path == "/explicit/libcanbus.so"
        raise original

    monkeypatch.setattr(backend, "_load_library", fail)
    result = enumerate_minicanfd_channels("/explicit/libcanbus.so")

    assert result.channels == ()
    assert result.failures[0].stage == "library"
    assert result.failures[0].error is original


@pytest.mark.parametrize("symbol", ["LibCANbus_Init", "LibCANbus_Exit"])
def test_old_linux_library_without_lifecycle_is_rejected_before_scanning(
    library_factory, symbol
):
    lib = library_factory()
    delattr(lib, symbol)

    result = enumerate_minicanfd_channels()

    assert result.failures[0].stage == "lifecycle"
    assert symbol in str(result.failures[0].error)
    assert lib.CAN_ScanDevice.calls == []


@pytest.mark.parametrize(
    "bad_layout",
    [{True: (0,)}, {-1: (0,)}, {0: (-1,)}, {0: (True,)}, {0: (2**32,)}, {0: "01"}, []],
)
def test_invalid_layout_never_loads_driver(monkeypatch, bad_layout):
    monkeypatch.setattr(backend.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        backend, "_load_library", lambda path: pytest.fail("unexpected driver load")
    )

    with pytest.raises(ValidationError):
        enumerate_minicanfd_channels(linux_channels=bad_layout)


def test_windows_rejects_linux_numbering_configuration(library_factory):
    lib = library_factory(windows=True)
    with pytest.raises(ValidationError, match="only valid on Linux"):
        enumerate_minicanfd_channels(linux_channels={0: (0, 1)})
    assert lib.CAN_ScanDevice.calls == []


@pytest.mark.parametrize("windows", [False, True])
def test_enumeration_does_not_touch_live_or_pending_close_channels(
    library_factory, windows
):
    lib = library_factory(windows)
    interface = backend.CANFDInterface()
    try:
        for pending_close in (False, True):
            if pending_close:
                lib.CAN_CloseDevice.result = -1
                with pytest.raises(TransportError, match="CAN_CloseDevice"):
                    interface.close()
            result = enumerate_minicanfd_channels()
            assert result.failures[0].stage == "busy"
            assert lib.CAN_ScanDevice.calls == [()]
            if not windows:
                assert backend._lifecycle_snapshot() == {backend._library_key(lib): 1}
                assert lib.LibCANbus_Exit.calls == []
    finally:
        lib.CAN_CloseDevice.result = 0
        interface.close()
    result = enumerate_minicanfd_channels(
        linux_channels=None if windows else {0: (0,), 1: ()}
    )
    assert result.failures == ()


def test_enumeration_and_open_share_lifecycle_and_serialize_scan(
    library_factory, monkeypatch
):
    lib = library_factory()
    scan_entered = threading.Event()
    allow_scan = threading.Event()
    acquire_entered = threading.Event()
    results = []
    interfaces = []
    thread_errors = []
    original_acquire = backend._lifecycle_acquire

    def acquire(lib):
        ref = original_acquire(lib)
        if backend._lifecycle_snapshot()[ref.key] == 2:
            acquire_entered.set()
        return ref

    monkeypatch.setattr(backend, "_lifecycle_acquire", acquire)

    def scan():
        if not scan_entered.is_set():
            scan_entered.set()
            assert allow_scan.wait(2)
        return 2

    def run_scan():
        try:
            results.append(
                enumerate_minicanfd_channels(linux_channels={0: (0,), 1: ()})
            )
        except BaseException as error:
            thread_errors.append(error)

    def open_channel():
        try:
            interfaces.append(backend.CANFDInterface())
        except BaseException as error:
            thread_errors.append(error)

    lib.CAN_ScanDevice.hook = scan
    scanner = threading.Thread(target=run_scan)
    opener = threading.Thread(target=open_channel)
    scanner.start()
    try:
        assert scan_entered.wait(1)
        opener.start()
        assert acquire_entered.wait(1)
        assert lib.CAN_OpenDevice.calls == []
    finally:
        allow_scan.set()
        scanner.join(2)
        if opener.ident is not None:
            opener.join(2)
    try:
        assert not scanner.is_alive() and not opener.is_alive()
        assert thread_errors == []
        assert results[0].failures == ()
        assert len(interfaces) == 1
        assert lib.LibCANbus_Exit.calls == []
    finally:
        for interface in interfaces:
            interface.close()
    assert lib.LibCANbus_Exit.calls == [()]


@pytest.mark.parametrize(
    "windows,legacy", [(False, False), (False, True), (True, False)]
)
def test_additional_opens_reuse_snapshot_until_every_channel_closes(
    library_factory, windows, legacy
):
    lib = library_factory(windows, legacy)
    first = backend.CANFDInterface(device_index=0)
    second = None
    try:
        lib.CAN_ScanDevice.result = 3  # A newly plugged adapter must not rescan.
        second = backend.CANFDInterface(device_index=1)
        assert lib.CAN_ScanDevice.calls == [()]
        assert enumerate_minicanfd_channels().failures[0].stage == "busy"
        with pytest.raises(
            TransportError, match="close all channels before rescanning"
        ):
            backend.CANFDInterface(device_index=2)
        first.close()
        assert backend._scanned_device_counts[backend._library_key(lib)] == 2
        assert lib.CAN_ScanDevice.calls == [()]
    finally:
        first.close()
        if second is not None:
            second.close()
    assert backend._scanned_device_counts == {}
    with backend.CANFDInterface(device_index=2):
        assert lib.CAN_ScanDevice.calls == [(), ()]
    assert backend._scanned_device_counts == {}


def test_failed_device_close_keeps_scan_snapshot_for_other_opens(library_factory):
    lib = library_factory()
    first = backend.CANFDInterface()
    second = None
    try:
        lib.CAN_CloseDevice.result = -3
        with pytest.raises(TransportError, match="CAN_CloseDevice"):
            first.close()
        second = backend.CANFDInterface(device_index=1)
        assert lib.CAN_ScanDevice.calls == [()]
        assert backend._scanned_device_counts[backend._library_key(lib)] == 2
    finally:
        lib.CAN_CloseDevice.result = 0
        first.close()
        if second is not None:
            second.close()
    assert backend._scanned_device_counts == {}


def test_cleanup_failure_is_reported_and_retried_before_next_init(library_factory):
    lib = library_factory()
    lib.CAN_ScanDevice.result = -1
    lib.LibCANbus_Exit.result = -9

    first = enumerate_minicanfd_channels()
    second = enumerate_minicanfd_channels()

    assert [failure.stage for failure in first.failures] == ["scan", "cleanup"]
    assert "status -9" in str(first.failures[1].error)
    assert second.failures[0].stage == "lifecycle"
    assert lib.CAN_ScanDevice.calls == [()]
    assert lib.LibCANbus_Init.calls == [()]
    assert backend._library_key(lib) in backend._lifecycle_lib

    lib.LibCANbus_Exit.result = 0
    lib.CAN_ScanDevice.result = 0
    third = enumerate_minicanfd_channels()

    assert third.failures == ()
    assert len(lib.LibCANbus_Init.calls) == 2
    assert len(lib.LibCANbus_Exit.calls) == 4
    assert backend._lifecycle_lib == {}


def test_failed_initialization_cleanup_blocks_enumeration_until_retry(library_factory):
    lib = library_factory()
    lib.CANFD_Init.result = -1
    lib.CAN_CloseDevice.result = -3
    with pytest.raises(TransportError, match="CANFD_Init") as caught:
        backend.CANFDInterface()

    result = enumerate_minicanfd_channels()

    assert result.failures[0].stage == "busy"
    assert lib.CAN_ScanDevice.calls == [()]
    lib.CAN_CloseDevice.result = 0
    caught.value._failed_interface.close()
    assert not backend._open_channel_counts
    assert backend._lifecycle_snapshot() == {}


def test_cleanup_exception_retains_original_error_and_blocks_open(library_factory):
    lib = library_factory()
    lib.CAN_ScanDevice.result = 0
    original = OSError("USB resources still busy")

    def fail():
        raise original

    lib.LibCANbus_Exit.hook = fail
    result = enumerate_minicanfd_channels()

    assert result.failures[0].stage == "cleanup"
    assert result.failures[0].error.cause is original
    with pytest.raises(TransportError, match="USB resources still busy"):
        backend.CANFDInterface()
    assert lib.LibCANbus_Init.calls == [()]
    assert lib.CAN_OpenDevice.calls == []

    lib.LibCANbus_Exit.hook = None
    assert enumerate_minicanfd_channels().failures == ()


def test_scan_interrupt_is_propagated_after_lifecycle_release(library_factory):
    lib = library_factory()

    def interrupt():
        raise KeyboardInterrupt

    lib.CAN_ScanDevice.hook = interrupt
    with pytest.raises(KeyboardInterrupt):
        enumerate_minicanfd_channels()
    assert lib.LibCANbus_Exit.calls == [()]
    assert backend._lifecycle_snapshot() == {}
