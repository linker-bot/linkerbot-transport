"""Real python-can PCAN initialization with only the native driver replaced."""

import sys
import time

import can
import pytest
from can.interfaces.pcan import pcan

from linkerbot_transport import SharedTransportPool, TransportState, _pcan
from linkerbot_transport.can import CANTransport
from linkerbot_transport.canfd import PythonCANFDTransport
from linkerbot_transport.errors import TransportError


class NativePCAN:
    def __init__(self):
        self.initialized = False
        self.init_error = None
        self.init_status = pcan.PCAN_ERROR_OK
        self.configure_error = None
        self.configure_status = pcan.PCAN_ERROR_OK
        self.configure_option = pcan.PCAN_ALLOW_ERROR_FRAMES
        self.release_error = None
        self.release_status = pcan.PCAN_ERROR_OK
        self.init_calls = []
        self.release_calls = []
        self.config_calls = []

    def Initialize(self, channel, *args):
        return self._initialize("classic", channel, args)

    def InitializeFD(self, channel, *args):
        return self._initialize("fd", channel, args)

    def _initialize(self, mode, channel, args):
        self.init_calls.append((mode, channel, args))
        if self.init_error is not None:
            raise self.init_error
        if self.init_status == pcan.PCAN_ERROR_OK:
            self.initialized = True
        return self.init_status

    def Uninitialize(self, channel):
        self.release_calls.append(channel)
        if self.release_error is not None:
            raise self.release_error
        if self.release_status == pcan.PCAN_ERROR_OK:
            self.initialized = False
        return self.release_status

    def GetValue(self, channel, option):
        if option == pcan.PCAN_API_VERSION:
            return pcan.PCAN_ERROR_OK, b"4.10.0"
        return pcan.PCAN_ERROR_OK, 123

    def SetValue(self, channel, option, value):
        self.config_calls.append((channel, option, value))
        if self.configure_error is not None and option == self.configure_option:
            raise self.configure_error
        if option == self.configure_option:
            return self.configure_status
        return pcan.PCAN_ERROR_OK

    def GetErrorText(self, *args):
        return pcan.PCAN_ERROR_OK, b"native initialization failed"

    def Read(self, channel):
        return pcan.PCAN_ERROR_QRCVEMPTY, None, None

    def ReadFD(self, channel):
        return self.Read(channel)


@pytest.fixture
def native(monkeypatch):
    driver = NativePCAN()
    monkeypatch.setattr(pcan, "PCANBasic", lambda: driver)
    monkeypatch.setattr(pcan, "HAS_EVENTS", False)
    return driver


@pytest.fixture(params=[False, True], ids=["classic", "fd"])
def transport_factory(request):
    def create(**kwargs):
        if request.param:
            return PythonCANFDTransport(
                "PCAN_USBBUS1",
                "pcan",
                timing=can.BitTimingFd.from_sample_point(
                    f_clock=80_000_000,
                    nom_bitrate=1_000_000,
                    nom_sample_point=80,
                    data_bitrate=5_000_000,
                    data_sample_point=75,
                ),
                **kwargs,
            )
        return CANTransport("PCAN_USBBUS1", "pcan", **kwargs)

    return create


@pytest.mark.parametrize(
    "error",
    [None, OSError("configure failed"), KeyboardInterrupt("configure interrupted")],
)
def test_partial_initialization_rolls_back_once(native, transport_factory, error):
    native.configure_error = error
    native.configure_status = -1
    transport = transport_factory()
    with pytest.raises(
        KeyboardInterrupt if isinstance(error, KeyboardInterrupt) else TransportError
    ):
        transport.open()
    assert not native.initialized
    assert native.release_calls == [pcan.PCAN_CHANNEL_NAMES["PCAN_USBBUS1"]]
    transport.close()
    transport.close()
    assert len(native.release_calls) == 1
    assert transport.get_health().state is TransportState.CLOSED


@pytest.mark.parametrize("failure", ["return", "exception"])
def test_unsuccessful_initialize_never_uninitializes_existing_channel(
    native, transport_factory, failure
):
    native.initialized = True  # An existing native user outside this instance.
    if failure == "return":
        native.init_status = -1
    else:
        native.init_error = OSError("Initialize failed")
    transport = transport_factory()
    with pytest.raises(TransportError):
        transport.open()
    transport.close()
    assert native.initialized
    assert native.release_calls == []


@pytest.mark.parametrize("interrupt", [False, True])
def test_failed_rollback_retained_by_pool_and_retried_before_reopen(
    native, transport_factory, interrupt
):
    native.configure_error = (
        KeyboardInterrupt("configure interrupted")
        if interrupt
        else OSError("configure failed")
    )
    native.release_status = -1
    transport = transport_factory()
    pool = SharedTransportPool()
    try:
        with pytest.raises(
            KeyboardInterrupt if interrupt else TransportError, match="configure"
        ):
            pool.acquire(transport.pool_key, lambda: transport)
        assert pool.active_keys() == (transport.pool_key,)
        assert native.initialized
        assert len(native.init_calls) == 1
        conflicting = transport_factory(bus_options={"auto_reset": True})
        with pytest.raises(TransportError, match="different settings"):
            conflicting.open()
        conflicting.close()
    finally:
        native.release_status = pcan.PCAN_ERROR_OK
        native.configure_error = None
        lease = pool.acquire(transport.pool_key, lambda: pytest.fail("lost owner"))
        assert len(native.init_calls) == 2
        lease.close()
    assert not native.initialized
    assert pool.active_keys() == ()


@pytest.mark.parametrize("failure", ["return", "exception", "interrupt"])
def test_shutdown_checks_uninitialize_status_and_retries(
    native, transport_factory, failure
):
    transport = transport_factory()
    transport.open()
    native.release_status = -1
    native.release_error = {
        "return": None,
        "exception": OSError("Uninitialize failed"),
        "interrupt": KeyboardInterrupt("Uninitialize interrupted"),
    }[failure]
    error_type = KeyboardInterrupt if failure == "interrupt" else TransportError
    try:
        with pytest.raises(error_type, match="Uninitialize"):
            transport.close()
        assert native.initialized
        assert transport.get_health().state is TransportState.FAULTED
        with pytest.raises(error_type, match="Uninitialize"):
            transport.recover()
        assert len(native.init_calls) == 1
    finally:
        native.release_status = pcan.PCAN_ERROR_OK
        native.release_error = None
        transport.close()
    assert not native.initialized


def test_missing_driver_does_not_attempt_native_cleanup(monkeypatch, transport_factory):
    def unavailable():
        raise OSError("driver missing")

    monkeypatch.setattr(pcan, "PCANBasic", unavailable)
    transport = transport_factory()
    with pytest.raises(TransportError, match="driver missing"):
        transport.open()
    transport.close()
    assert transport.get_health().state is TransportState.CLOSED


def windows_events(monkeypatch, native):
    closed = []
    monkeypatch.setattr(pcan, "HAS_EVENTS", True)
    monkeypatch.setattr(pcan, "IS_WINDOWS", True)
    monkeypatch.setattr(pcan, "IS_LINUX", False)
    monkeypatch.setattr(pcan, "CreateEvent", lambda *args: 123, raising=False)
    monkeypatch.setattr(pcan, "WAIT_OBJECT_0", 0, raising=False)
    monkeypatch.setattr(
        pcan, "WaitForSingleObject", lambda *args: time.sleep(0.001), raising=False
    )

    def release_event(handle):
        assert not native.initialized
        closed.append(handle)

    monkeypatch.setattr(_pcan, "_close_windows_event", release_event)
    return closed


def test_windows_event_registration_failure_releases_channel_then_event(
    monkeypatch, native, transport_factory
):
    closed = windows_events(monkeypatch, native)
    native.configure_option = pcan.PCAN_RECEIVE_EVENT
    native.configure_error = OSError("event registration failed")
    transport = transport_factory()
    with pytest.raises(TransportError, match="event registration"):
        transport.open()
    transport.close()
    assert len(native.release_calls) == 1
    assert closed == [123]


def test_windows_event_close_failure_does_not_repeat_uninitialize(
    monkeypatch, native, transport_factory
):
    windows_events(monkeypatch, native)

    def fail(handle):
        raise OSError("CloseHandle failed")

    monkeypatch.setattr(_pcan, "_close_windows_event", fail)
    transport = transport_factory()
    transport.open()
    try:
        with pytest.raises(TransportError, match="CloseHandle"):
            transport.close()
        assert not native.initialized
        assert len(native.release_calls) == 1
    finally:
        closed = []
        monkeypatch.setattr(_pcan, "_close_windows_event", closed.append)
        transport.close()
    assert closed == [123]
    assert len(native.release_calls) == 1


def test_windows_event_remains_owned_until_channel_release_succeeds(
    monkeypatch, native, transport_factory
):
    closed = windows_events(monkeypatch, native)
    transport = transport_factory()
    transport.open()
    native.release_status = -1
    try:
        with pytest.raises(TransportError, match="Uninitialize"):
            transport.close()
        assert closed == []
        assert native.initialized
    finally:
        native.release_status = pcan.PCAN_ERROR_OK
        transport.close()
    assert closed == [123]


@pytest.mark.skipif(sys.platform != "win32", reason="requires Windows event handles")
def test_real_windows_event_handle_is_closed(native, transport_factory, monkeypatch):
    from _winapi import WaitForSingleObject

    monkeypatch.setattr(pcan, "HAS_EVENTS", True)
    close_event = _pcan._close_windows_event
    handles = []

    def close(handle):
        close_event(handle)
        handles.append(handle)

    monkeypatch.setattr(_pcan, "_close_windows_event", close)
    transport = transport_factory()
    transport.open()
    transport.close()
    assert len(handles) == 1
    with pytest.raises(OSError):
        WaitForSingleObject(handles[0], 0)


def test_open_does_not_modify_global_python_can_factory(native, transport_factory):
    # Instance adaptation must not replace upstream classes/factories globally.
    factory = pcan.PCANBasic
    transport = transport_factory()
    transport.open()
    try:
        assert pcan.PCANBasic is factory
    finally:
        transport.close()
