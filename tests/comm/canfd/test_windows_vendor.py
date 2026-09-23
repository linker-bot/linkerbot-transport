"""Windows DLL ABI contracts; no vendor binary or physical device required."""

import ctypes
import struct

import pytest

from linkerbot_transport import SharedTransportPool
from linkerbot_transport.canfd import CANFDConfigOptions, CANFDMessage, CANFDTransport
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


class WindowsDLL:
    # Intentionally no LibCANbus_Init/Exit exports.
    def __init__(self):
        self.CAN_ScanDevice = Function(0, 2)
        self.CAN_OpenDevice = Function(1)
        self.CAN_CloseDevice = Function(1)
        self.CAN_ReadDevInfo = Function(2)
        self.CANFD_Init = Function(2)
        self.CANFD_Transmit = Function(4, hook=lambda dev, ptr, count, timeout: count)
        self.CANFD_Receive = Function(4)


@pytest.fixture(autouse=True)
def windows_platform(monkeypatch):
    monkeypatch.setattr(backend.platform, "system", lambda: "Windows")
    monkeypatch.setattr(backend, "_dll_directory_handles", {})


def test_vendor_layout_matches_documented_windows_abi():
    assert ctypes.sizeof(backend.CanFDConfig) == 20
    assert backend.CanFDConfig.NomPre.offset == 8
    assert backend.CanFDConfig.NomPre.size == 2
    assert backend.CanFDConfig.Config.offset == 17
    assert backend.CanFDConfig.Model.offset == 18
    assert backend.CanFDConfig.Cantype.offset == 19
    assert ctypes.sizeof(backend.CanFDMsg) == 80
    assert backend.CanFDMsg.DLC.offset == 9
    assert backend.CanFDMsg.Data.offset == 16
    assert ctypes.sizeof(backend.DevInfo) == 160
    lib = WindowsDLL()
    backend._bind_signatures(lib)
    assert lib.CANFD_Init.argtypes == [
        ctypes.c_uint,
        ctypes.POINTER(backend.CanFDConfig),
    ]
    assert lib.CANFD_Receive.argtypes == [
        ctypes.c_uint,
        ctypes.POINTER(backend.CanFDMsg),
        ctypes.c_int,
        ctypes.c_int,
    ]


def test_native_receive_limit_is_checked_before_allocation(monkeypatch):
    lib = WindowsDLL()
    monkeypatch.setattr(backend, "_load_library", lambda path: lib)
    interface = backend.CANFDInterface()
    try:
        with pytest.raises(ValidationError, match="2500"):
            interface.receive(max_frames=2501)
        assert lib.CANFD_Receive.calls == []
    finally:
        interface.close()


def test_windows_open_config_send_receive_batch_and_close(monkeypatch):
    lib = WindowsDLL()
    monkeypatch.setattr(backend, "_load_library", lambda path: lib)
    expected_config = struct.pack(
        "<IIH10B", 1_000_000, 5_000_000, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 1
    )

    def initialize(dev, ptr):
        assert dev == 1
        assert ctypes.string_at(ptr, 20) == expected_config
        return 0

    def receive(dev, frames, count, timeout):
        assert (dev, count, timeout) == (1, 64, 10)
        payload = bytes(range(64))
        raw = (
            struct.pack("<IIBBBBBBBB", 0x123, 98765, 0x0C, 15, 1, 0, 0, 0, 0, 0)
            + payload
        )
        ctypes.memmove(ctypes.addressof(frames), raw, 80)
        return 1

    lib.CANFD_Init.hook = initialize
    lib.CANFD_Receive.hook = receive
    interface = backend.CANFDInterface(device_index=1)
    try:
        assert lib.CAN_OpenDevice.calls == [(1,)]
        message = CANFDMessage(0x123, b"hello", dlc=15, frame_type=0x0C)
        interface.send(message)
        dev, ptr, count, timeout = lib.CANFD_Transmit.calls[0]
        assert (dev, count, timeout) == (1, 1, 10)
        raw = ctypes.string_at(ptr, 80)
        assert raw[:12] == struct.pack("<IIBBBB", 0x123, 0, 0x0C, 15, 1, 0)
        assert raw[16:] == b"hello" + bytes(59)
        interface.send_batch([message, message])
        assert lib.CANFD_Transmit.calls[-1][2] == 2
        frames = interface.receive()
        assert frames == [
            CANFDMessage(
                0x123, bytes(range(64)), dlc=15, frame_type=0x0C, timestamp=98765
            )
        ]
        interface.read_dev_info()
        assert len(lib.CAN_ReadDevInfo.calls[-1]) == 2
    finally:
        interface.close()
    interface.close()
    assert lib.CAN_CloseDevice.calls == [(1,)]


def test_windows_refuses_ambiguous_two_dimensional_channel_before_loading(monkeypatch):
    def unexpected(*args):
        pytest.fail("must not load any DLL")

    monkeypatch.setattr(backend, "_load_library", unexpected)
    with pytest.raises(ValidationError, match="channel_index"):
        CANFDTransport(device_index=0, channel_index=1)
    with pytest.raises(ValidationError, match="channel_index"):
        backend.CANFDInterface(device_index=0, channel_index=1)


def test_managed_failed_init_and_cleanup_retains_handle_until_recovery(monkeypatch):
    lib = WindowsDLL()
    lib.CANFD_Init.result = -1
    lib.CAN_CloseDevice.result = -1
    monkeypatch.setattr(backend, "_load_library", lambda path: lib)
    transport = CANFDTransport()
    pool = SharedTransportPool()
    with pytest.raises(TransportError, match="CANFD_Init"):
        pool.acquire(transport.pool_key, lambda: transport)
    assert pool.active_keys() == (transport.pool_key,)
    assert lib.CAN_OpenDevice.calls == [(0,)]
    lib.CAN_CloseDevice.result = 0
    lib.CANFD_Init.result = 0
    lease = pool.acquire(
        transport.pool_key, lambda: pytest.fail("must reuse retained transport")
    )
    lease.close()
    assert lib.CAN_OpenDevice.calls == [(0,), (0,)]
    assert pool.active_keys() == ()


def test_windows_scan_failure_never_closes_an_unopened_channel(monkeypatch):
    lib = WindowsDLL()
    lib.CAN_ScanDevice.result = -1
    monkeypatch.setattr(backend, "_load_library", lambda path: lib)
    with pytest.raises(TransportError, match="CAN_ScanDevice"):
        backend.CANFDInterface()
    assert lib.CAN_OpenDevice.calls == []
    assert lib.CAN_CloseDevice.calls == []


def test_missing_export_reports_incompatible_library():
    lib = WindowsDLL()
    del lib.CANFD_Transmit
    with pytest.raises(TransportError, match="missing CANFD_Transmit"):
        backend._bind_signatures(lib)


def test_explicit_bad_dll_cannot_fall_back_to_another_library(monkeypatch, tmp_path):
    calls = []

    def fail(path):
        calls.append(path)
        raise OSError("incorrect executable format")

    monkeypatch.setattr(backend, "_ctypes_loader", lambda: fail)
    monkeypatch.setenv("LINKERBOT_CANFD_LIB", str(tmp_path / "alternative.dll"))
    path = tmp_path / "bad.dll"
    with pytest.raises(TransportError, match="matching HCanbus.dll"):
        backend._load_library(path)
    assert calls == [str(path)]


def test_dll_directory_is_retained_for_delayed_dependencies(monkeypatch, tmp_path):
    path = tmp_path / "HCanbus.dll"
    lib = WindowsDLL()
    directories = []
    handle = object()

    def add_directory(directory):
        directories.append(directory)
        return handle

    monkeypatch.setattr(backend, "_library_handle_cache", {})
    monkeypatch.setattr(backend.os, "add_dll_directory", add_directory, raising=False)
    monkeypatch.setattr(backend, "_ctypes_loader", lambda: lambda path: lib)
    assert backend._load_library(path) is lib
    assert backend._load_library(path) is lib
    assert directories == [str(tmp_path)]
    assert backend._dll_directory_handles == {str(tmp_path): handle}


def test_explicit_library_path_is_fixed_before_cwd_changes(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    transport = CANFDTransport(library_path="HCanbus.dll", config=CANFDConfigOptions())
    assert transport.pool_key.library_path == str(tmp_path / "HCanbus.dll")
    monkeypatch.chdir(tmp_path.parent)
    assert transport.pool_key.library_path == str(tmp_path / "HCanbus.dll")
