import ctypes
import os
import threading
import weakref
from collections import deque
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from linkerbot_transport import TransportState
from linkerbot_transport.canfd import (
    CANFDConfigOptions,
    CANFDMessage,
    CANFDTransport,
    CANRouteKey,
)
from linkerbot_transport.canfd import ctypes_backend as backend
from linkerbot_transport.canfd.dispatcher import CANFDMessageDispatcher
from linkerbot_transport.errors import ClosedError, TransportError, ValidationError


class FakeFunction:
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


class FakeLibrary:
    def __init__(self) -> None:
        self._name = "fake-libcanbus"
        self.LibCANbus_Init = FakeFunction(0)
        self.LibCANbus_Exit = FakeFunction(0)
        self.CAN_ScanDevice = FakeFunction(1)
        self.CAN_OpenDevice = FakeFunction(0)
        self.CAN_CloseDevice = FakeFunction(0)
        self.CAN_ReadDevInfo = FakeFunction(0)
        self.CANFD_Init = FakeFunction(0)
        self.CANFD_Transmit = FakeFunction(1)
        self.CANFD_Receive = FakeFunction(0)


def make_interface(
    monkeypatch: pytest.MonkeyPatch, lib: FakeLibrary
) -> backend.CANFDInterface:
    monkeypatch.setattr(backend, "_load_library", lambda library_path: lib)
    return backend.CANFDInterface()


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize(
    "configured,override,dlc",
    [(0x04, 0, 9), (0, None, 9), (0x04, 0x08, 1), (0x08, None, 1)],
)
def test_invalid_effective_classic_frame_never_enters_native_transmit(
    monkeypatch, batch, configured, override, dlc
):
    lib = FakeLibrary()
    monkeypatch.setattr(backend, "_load_library", lambda _: lib)
    interface = backend.CANFDInterface(config=CANFDConfigOptions(frame_type=configured))
    invalid = CANFDMessage(
        1, bytes(backend.dlc_to_length(dlc)), dlc=dlc, frame_type=override
    )
    valid = CANFDMessage(2, b"ok", frame_type=0x04)
    try:
        with pytest.raises(ValidationError):
            if batch:
                interface.send_batch([valid, invalid])
            else:
                interface.send(invalid)
        assert not lib.CANFD_Transmit.calls
        interface.send(valid)
        assert len(lib.CANFD_Transmit.calls) == 1
    finally:
        interface.close()


@pytest.mark.parametrize("dlc", range(9, 16))
def test_native_fd_still_accepts_all_large_dlcs(monkeypatch, dlc):
    lib = FakeLibrary()
    interface = make_interface(monkeypatch, lib)
    try:
        interface.send(CANFDMessage(1, bytes(backend.dlc_to_length(dlc)), dlc=dlc))
        assert len(lib.CANFD_Transmit.calls) == 1
        assert interface._tx_frame.FrameType == 0x04
        assert interface._tx_frame.DLC == dlc
    finally:
        interface.close()


@pytest.mark.parametrize("frame_type,dlc", [(0, 8), (0x10, 8), (0x14, 15), (0x0C, 15)])
def test_native_frame_validation_preserves_other_vendor_bits(
    monkeypatch, frame_type, dlc
):
    lib = FakeLibrary()
    interface = make_interface(monkeypatch, lib)
    try:
        interface.send(CANFDMessage(1, b"ok", dlc=dlc, frame_type=frame_type))
        assert interface._tx_frame.FrameType == frame_type
        assert len(lib.CANFD_Transmit.calls) == 1
    finally:
        interface.close()


@pytest.fixture
def isolated_library_caches() -> Iterator[None]:
    with backend._library_load_lock:
        backend._library_handle_cache.clear()
    with backend._dependency_load_lock:
        backend._dependency_preload_attempted.clear()
        backend._dependency_handle_cache.clear()
    yield
    with backend._library_load_lock:
        backend._library_handle_cache.clear()
    with backend._dependency_load_lock:
        backend._dependency_preload_attempted.clear()
        backend._dependency_handle_cache.clear()


def test_ctypes_struct_fields() -> None:
    assert [name for name, _ in backend.CanFDConfig._fields_] == [
        "NomBaud",
        "DatBaud",
        "NomPre",
        "NomTseg1",
        "NomTseg2",
        "NomSJW",
        "DatPre",
        "DatTseg1",
        "DatTseg2",
        "DatSJW",
        "Config",
        "Model",
        "Cantype",
    ]
    assert [name for name, _ in backend.CanFDMsg._fields_][-1] == "Data"
    assert backend.CanFDMsg.Data.size == 64


def test_bind_signatures_sets_required_vendor_signatures() -> None:
    lib = FakeLibrary()

    backend._bind_signatures(lib)

    assert lib.CAN_ScanDevice.restype is ctypes.c_int
    assert lib.CAN_OpenDevice.argtypes == [ctypes.c_uint, ctypes.c_uint]
    assert lib.CANFD_Init.argtypes == [
        ctypes.c_uint,
        ctypes.c_uint,
        ctypes.POINTER(backend.CanFDConfig),
    ]
    assert lib.CANFD_Transmit.argtypes == [
        ctypes.c_uint,
        ctypes.c_uint,
        ctypes.POINTER(backend.CanFDMsg),
        ctypes.c_uint,
        ctypes.c_int,
    ]


def test_init_scans_opens_and_initializes(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()

    interface = make_interface(monkeypatch, lib)

    assert interface.device_index == 0
    assert lib.CAN_ScanDevice.calls == [()]
    assert lib.CAN_OpenDevice.calls == [(0, 0)]
    assert len(lib.CANFD_Init.calls) == 1


def test_init_rejects_negative_device_index(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()
    monkeypatch.setattr(backend, "_load_library", lambda library_path: lib)

    with pytest.raises(ValidationError):
        backend.CANFDInterface(device_index=-1)


def test_init_rejects_scan_error(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()
    lib.CAN_ScanDevice.result = -1

    with pytest.raises(TransportError, match="CAN_ScanDevice"):
        make_interface(monkeypatch, lib)


def test_init_rejects_no_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()
    lib.CAN_ScanDevice.result = 0

    with pytest.raises(TransportError, match="No CANFD adapter"):
        make_interface(monkeypatch, lib)


def test_init_rejects_device_index_out_of_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = FakeLibrary()

    monkeypatch.setattr(backend, "_load_library", lambda library_path: lib)
    with pytest.raises(TransportError, match="out of range"):
        backend.CANFDInterface(device_index=1)


def test_init_open_failure_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()
    lib.CAN_OpenDevice.result = 2

    with pytest.raises(TransportError, match="CAN_OpenDevice"):
        make_interface(monkeypatch, lib)


def test_init_failure_closes_device(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()
    lib.CANFD_Init.result = 3

    with pytest.raises(TransportError, match="CANFD_Init"):
        make_interface(monkeypatch, lib)

    assert lib.CAN_CloseDevice.calls == [(0, 0)]


def test_send_translates_message_to_vendor_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = FakeLibrary()
    interface = make_interface(monkeypatch, lib)
    message = CANFDMessage(
        arbitration_id=0x123456,
        data=b"abc",
        is_extended_id=True,
    )

    interface.send(message, timeout_ms=25)

    _, _, frame_ptr, count, timeout_ms = lib.CANFD_Transmit.calls[0]
    frame = frame_ptr._obj
    assert frame.ID == 0x123456
    assert frame.DLC == 3
    assert frame.FrameType == 0x04
    assert frame.ExternFlag == 1
    assert bytes(frame.Data[:3]) == b"abc"
    assert count == 1
    assert timeout_ms == 25


def test_send_inherits_configured_brs_frame_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = FakeLibrary()
    monkeypatch.setattr(backend, "_load_library", lambda library_path: lib)
    interface = backend.CANFDInterface(config=CANFDConfigOptions(frame_type=0x0C))

    interface.send(CANFDMessage(arbitration_id=1, data=b"x"))

    frame = lib.CANFD_Transmit.calls[0][2]._obj
    assert frame.FrameType == 0x0C


@pytest.mark.parametrize("status", [0, -1, 2])
def test_send_failure_raises(monkeypatch: pytest.MonkeyPatch, status: int) -> None:
    lib = FakeLibrary()
    lib.CANFD_Transmit.result = status
    interface = make_interface(monkeypatch, lib)

    with pytest.raises(TransportError, match="CANFD_Transmit"):
        interface.send(CANFDMessage(arbitration_id=1, data=b""))


def test_receive_timeout_returns_empty_list(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()
    interface = make_interface(monkeypatch, lib)

    assert interface.receive() == []


def test_receive_negative_status_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()
    lib.CANFD_Receive.result = -1
    interface = make_interface(monkeypatch, lib)

    with pytest.raises(TransportError, match="CANFD_Receive"):
        interface.receive()


def test_receive_rejects_vendor_count_above_requested_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = FakeLibrary()
    lib.CANFD_Receive.result = 2
    interface = make_interface(monkeypatch, lib)

    with pytest.raises(TransportError, match="more frames than requested"):
        interface.receive(max_frames=1)


def test_receive_translates_vendor_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()

    def receive(
        dev: Any, channel: Any, buffer: Any, max_frames: Any, timeout_ms: Any
    ) -> int:
        _ = dev, channel, max_frames, timeout_ms
        frame = buffer[0]
        frame.ID = 0x123
        frame.TimeStamp = 456
        frame.FrameType = 0x0C
        frame.DLC = 9
        frame.ExternFlag = 1
        frame.Data[0] = 1
        frame.Data[11] = 12
        return 1

    lib.CANFD_Receive = FakeFunction(hook=receive)
    interface = make_interface(monkeypatch, lib)

    messages = interface.receive()

    assert messages == [
        CANFDMessage(
            arbitration_id=0x123,
            data=bytes([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12]),
            dlc=9,
            is_extended_id=True,
            frame_type=0x0C,
            timestamp=456,
        )
    ]


def received_frame(data=b"a", **fields):
    frame = backend.CanFDMsg(ID=0x123, FrameType=0x04, DLC=len(data))
    frame.Data[: len(data)] = data
    for name, value in fields.items():
        setattr(frame, name, value)
    return frame


@pytest.fixture(params=["Linux", "Windows"])
def native_receive(monkeypatch, request):
    monkeypatch.setattr(backend.platform, "system", lambda: request.param)
    batches = deque()
    lib = FakeLibrary()

    def receive(*args):
        assert len(args) == (4 if request.param == "Windows" else 5)
        buffer, maximum, timeout = args[-3:]
        if not batches:
            threading.Event().wait(min(timeout / 1000, 0.001))
            return 0
        batch = batches[0]
        count = min(maximum, len(batch))
        for index in range(count):
            frame = batch.popleft()
            ctypes.memmove(ctypes.addressof(buffer[index]), ctypes.byref(frame), 80)
        if not batch:
            batches.popleft()
        return count

    lib.CANFD_Receive = FakeFunction(hook=receive)
    interface = make_interface(monkeypatch, lib)
    try:
        yield interface, lib, batches
    finally:
        interface.close()


def test_receive_filters_remote_and_transmit_echo_without_reading_payload(
    native_receive, monkeypatch
):
    interface, lib, batches = native_receive
    batches.append(deque([received_frame(b"OK")]))
    assert interface.receive(timeout_ms=0)[0].data == b"OK"

    # The next RTR leaves the old payload in the reused native slot. The echo
    # has the same ID as a device reply, so only the frame flags distinguish it.
    def receive(*args):
        buffer = args[-3]
        buffer[0].RemoteFlag = 1
        buffer[1].FrameType = 0x05
        buffer[1].DLC = 255
        return 2

    lib.CANFD_Receive.hook = receive

    def unexpected_copy(*args):
        pytest.fail("RTR and transmit echoes must not read a data payload")

    monkeypatch.setattr(backend.ctypes, "string_at", unexpected_copy)
    assert interface.receive(timeout_ms=0) == []


@pytest.mark.parametrize("frame_type", [0x02, 0x06, 0x07])
def test_failed_frame_reports_diagnostics_without_payload(
    native_receive, monkeypatch, frame_type
):
    interface, _, batches = native_receive
    batches.append(deque([received_frame(FrameType=frame_type)]))

    def unexpected_copy(*args):
        pytest.fail("a failed frame must not read a data payload")

    monkeypatch.setattr(backend.ctypes, "string_at", unexpected_copy)
    with pytest.raises(TransportError, match=f"FrameType=0x{frame_type:02x}"):
        interface.receive(timeout_ms=0)


@pytest.mark.parametrize("fields", [{"RemoteFlag": 1}, {"FrameType": 0x05}])
def test_filtered_frame_still_reports_bus_diagnostics(native_receive, fields):
    interface, _, batches = native_receive
    batches.append(deque([received_frame(BusSatus=0x04, **fields)]))
    with pytest.raises(TransportError, match="BusSatus=0x04"):
        interface.receive(timeout_ms=0)


@pytest.mark.parametrize("status", [0x00, 0x08, 0x20, 0x28])
def test_normal_status_and_historical_error_counts_keep_data(native_receive, status):
    interface, _, batches = native_receive
    batches.append(deque([received_frame(BusSatus=status, TECounter=9, RECounter=11)]))
    assert [frame.data for frame in interface.receive(timeout_ms=0)] == [b"a"]
    assert interface.receive(timeout_ms=0) == []


@pytest.mark.parametrize(
    "fields",
    [
        *({"BusSatus": 0x28 | bit} for bit in (0x01, 0x02, 0x04, 0x10, 0x40, 0x80)),
        *({"ErrSatus": code} for code in (1, 2, 3, 4, 5, 6, 255)),
    ],
)
def test_successful_data_precedes_its_diagnostic_and_batch_suffix(
    native_receive, fields
):
    interface, lib, batches = native_receive
    fields = {"BusSatus": 0x28, "TimeStamp": 456, **fields}
    batches.append(
        deque(
            [
                received_frame(b"a", TECounter=9, RECounter=11, **fields),
                received_frame(b"b"),
            ]
        )
    )
    assert [frame.data for frame in interface.receive(timeout_ms=0)] == [b"a"]
    with pytest.raises(TransportError) as raised:
        interface.receive(timeout_ms=0)
    for detail in (
        "ID=0x123",
        "TimeStamp=456",
        "FrameType=0x04",
        f"BusSatus=0x{fields['BusSatus']:02x}",
        f"ErrSatus=0x{fields.get('ErrSatus', 0):02x}",
        "TECounter=9",
        "RECounter=11",
    ):
        assert detail in str(raised.value)
    assert len(lib.CANFD_Receive.calls) == 1
    assert [frame.data for frame in interface.receive(timeout_ms=0)] == [b"b"]
    assert len(lib.CANFD_Receive.calls) == 1
    assert interface.receive(timeout_ms=0) == []


@pytest.mark.parametrize("maximum", [1, 2, 64])
def test_receive_data_and_errors_keep_order_across_batches(native_receive, maximum):
    interface, _, batches = native_receive
    batches.append(
        deque(
            [
                received_frame(b"a"),
                received_frame(RemoteFlag=1),
                received_frame(DLC=16),
                received_frame(b"b"),
                received_frame(ID=0x800),
                received_frame(b"c"),
                received_frame(FrameType=0x06),
                received_frame(b"d"),
            ]
        )
    )
    events = []
    for _ in range(12):
        try:
            messages = interface.receive(max_frames=maximum, timeout_ms=0)
            assert len(messages) <= maximum
            events.extend(message.data for message in messages)
        except (ValidationError, TransportError) as error:
            events.append(type(error))
    assert events == [
        b"a",
        ValidationError,
        b"b",
        ValidationError,
        b"c",
        TransportError,
        b"d",
    ]


def test_pending_suffix_respects_smaller_receive_limit(native_receive):
    interface, lib, batches = native_receive
    batches.append(
        deque([received_frame(b"a"), received_frame(DLC=16)])
        + deque(received_frame(bytes([index])) for index in range(65))
    )
    assert [frame.data for frame in interface.receive(max_frames=100)] == [b"a"]
    with pytest.raises(ValidationError):
        interface.receive(max_frames=1)
    for index in range(65):
        assert [frame.data for frame in interface.receive(max_frames=1)] == [
            bytes([index])
        ]
        assert len(lib.CANFD_Receive.calls) == 1
    assert interface.receive(timeout_ms=0) == []


@pytest.mark.parametrize("bad_index", [0, 2])
def test_bad_frame_at_batch_boundary_keeps_neighbours(native_receive, bad_index):
    interface, lib, batches = native_receive
    frames = [received_frame(bytes([index])) for index in range(3)]
    frames[bad_index].DLC = 16
    batches.append(deque(frames))
    if bad_index:
        assert [frame.data for frame in interface.receive()] == [b"\x00", b"\x01"]
    with pytest.raises(ValidationError):
        interface.receive()
    if not bad_index:
        assert [frame.data for frame in interface.receive()] == [b"\x01", b"\x02"]
    assert len(lib.CANFD_Receive.calls) == 1
    assert interface.receive(timeout_ms=0) == []


def test_invalid_receive_does_not_consume_pending_error_or_data(native_receive):
    interface, lib, batches = native_receive
    batches.append(
        deque([received_frame(b"a"), received_frame(DLC=16), received_frame(b"b")])
    )
    assert [frame.data for frame in interface.receive()] == [b"a"]
    for kwargs in ({"max_frames": 0}, {"max_frames": 2501}, {"timeout_ms": -1}):
        with pytest.raises(ValidationError):
            interface.receive(**kwargs)
    with pytest.raises(ValidationError, match="dlc"):
        interface.receive()
    assert [frame.data for frame in interface.receive()] == [b"b"]
    assert len(lib.CANFD_Receive.calls) == 1


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit])
def test_native_conversion_interrupt_is_not_deferred(
    native_receive, monkeypatch, error_type
):
    interface, _, batches = native_receive
    batches.append(deque([received_frame(b"a"), received_frame(b"b")]))
    original_convert = interface._frame_to_message
    interrupt = error_type("interrupted")

    def convert(frame):
        if frame.Data[0] == ord("b"):
            raise interrupt
        return original_convert(frame)

    monkeypatch.setattr(interface, "_frame_to_message", convert)
    with pytest.raises(error_type) as raised:
        interface.receive()
    assert raised.value is interrupt


@pytest.mark.parametrize("close_fails", [False, True])
def test_close_discards_pending_native_error_and_large_batch(
    native_receive, close_fails
):
    interface, lib, batches = native_receive
    buffer_reference = None
    original_receive = lib.CANFD_Receive.hook

    def receive(*args):
        nonlocal buffer_reference
        buffer_reference = weakref.ref(args[-3])
        return original_receive(*args)

    lib.CANFD_Receive.hook = receive
    batches.append(
        deque([received_frame(b"a"), received_frame(DLC=16), received_frame(b"b")])
    )
    assert [frame.data for frame in interface.receive(max_frames=100)] == [b"a"]
    # FakeFunction records the raw buffer argument; release that test-owned
    # reference before checking that close releases the interface's reference.
    lib.CANFD_Receive.calls.clear()
    assert buffer_reference() is not None
    lib.CAN_CloseDevice.result = -1 if close_fails else 0
    try:
        if close_fails:
            with pytest.raises(TransportError, match="CAN_CloseDevice"):
                interface.close()
        else:
            interface.close()
        assert buffer_reference() is None
        with pytest.raises(TransportError, match="closed"):
            interface.receive()
    finally:
        lib.CAN_CloseDevice.result = 0
        interface.close()


def test_exhausted_batch_is_not_retained_by_deferred_error(native_receive):
    interface, lib, batches = native_receive
    reference = None
    original_receive = lib.CANFD_Receive.hook

    def receive(*args):
        nonlocal reference
        reference = weakref.ref(args[-3])
        return original_receive(*args)

    lib.CANFD_Receive.hook = receive
    batches.append(deque([received_frame(b"a"), received_frame(DLC=16)]))
    assert [frame.data for frame in interface.receive(max_frames=100)] == [b"a"]
    lib.CANFD_Receive.calls.clear()
    assert reference() is None
    with pytest.raises(ValidationError):
        interface.receive()


def test_native_transport_routes_data_reports_errors_and_recovers(
    native_receive, caplog
):
    interface, lib, batches = native_receive
    ready = threading.Event()
    initial = [interface]
    original_receive = lib.CANFD_Receive.hook

    def receive(*args):
        assert ready.wait(timeout=2)
        return original_receive(*args)

    lib.CANFD_Receive.hook = receive

    def create(on_error):
        current = initial.pop() if initial else backend.CANFDInterface()
        return CANFDMessageDispatcher(
            interface=current, on_bus_error=on_error, max_consecutive_errors=2
        )

    transport = CANFDTransport(dispatcher_factory=create)
    transport.open()
    try:
        route = transport.subscribe_route(CANRouteKey(0x123, False))
        batches.append(
            deque(
                [
                    received_frame(b"a", BusSatus=0x28, ErrSatus=3),
                    received_frame(RemoteFlag=1),
                    received_frame(FrameType=0x05),
                    received_frame(b"b"),
                ]
            )
        )
        ready.set()
        assert route.receive(timeout_s=1).message.data == b"a"
        assert route.receive(timeout_s=1).message.data == b"b"
        assert route.try_receive() is None
        assert transport.get_health().state is TransportState.OPEN
        assert transport.get_health().rx_frames == 2
        assert caplog.text.count("CANFD receive error (1/2)") == 1

        batches.append(
            deque(
                [
                    received_frame(DLC=16),
                    received_frame(FrameType=0x06),
                    received_frame(b"old"),
                ]
            )
        )
        with pytest.raises(TransportError, match="FrameType=0x06"):
            route.receive(timeout_s=1)
        health = transport.get_health()
        assert health.state is TransportState.FAULTED
        assert health.rx_frames == 2

        transport.recover()
        with pytest.raises(TransportError):
            route.receive(timeout_s=0)
        fresh = transport.subscribe_route(CANRouteKey(0x123, False))
        batches.append(deque([received_frame(b"new")]))
        received = fresh.receive(timeout_s=1)
        assert received.message.data == b"new"
        assert received.generation == health.generation + 1
        assert fresh.try_receive() is None
        transport.close()
        with pytest.raises(ClosedError):
            fresh.receive(timeout_s=0)
    finally:
        ready.set()
        transport.close()


def test_read_dev_info_decodes_strings(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()

    def read_dev_info(dev: Any, info_ptr: Any) -> int:
        _ = dev
        info = info_ptr._obj
        info.HW_Type = b"adapter"
        info.HW_Ser = b"serial"
        info.HW_Ver = b"hw"
        info.FW_Ver = b"fw"
        info.MF_Date = b"date"
        return 0

    lib.CAN_ReadDevInfo = FakeFunction(hook=read_dev_info)
    interface = make_interface(monkeypatch, lib)

    assert interface.read_dev_info() == {
        "hw_type": "adapter",
        "hw_serial": "serial",
        "hw_version": "hw",
        "fw_version": "fw",
        "manufacture_date": "date",
    }


def test_library_candidates_use_explicit_or_env_without_silent_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LINKERBOT_CANFD_LIB", "/env/libcanbus.so")

    candidates = backend._library_candidates(Path("/explicit/libcanbus.so"))

    assert candidates == [Path("/explicit/libcanbus.so")]
    assert backend._library_candidates(None) == [Path("/env/libcanbus.so")]


@pytest.fixture(params=["Linux", "Windows"])
def library_search_platform(monkeypatch, request):
    monkeypatch.setattr(backend.platform, "system", lambda: request.param)
    monkeypatch.delenv("LINKERBOT_CANFD_LIB", raising=False)
    monkeypatch.setattr(backend, "_library_handle_cache", {})
    monkeypatch.setattr(backend, "_dll_directory_handles", {})
    monkeypatch.setattr(backend, "_preload_vendor_dependencies", lambda: None)
    monkeypatch.setattr(backend.os, "add_dll_directory", None, raising=False)
    return request.param


def test_library_candidates_default_to_system_loader(
    library_search_platform, monkeypatch, tmp_path
) -> None:
    package_root = tmp_path / "installation"
    monkeypatch.setattr(
        backend, "__file__", str(package_root / "linkerbot_transport/canfd/backend.py")
    )
    monkeypatch.chdir(tmp_path)
    candidates = backend._library_candidates(None)
    name = backend._default_library_name()

    expected = [name]
    if library_search_platform != "Windows":
        expected.append(Path("/usr/local/lib") / name)
    expected.extend(
        [package_root / "vendor/libcanbus" / name, package_root / "hand" / name]
    )
    assert candidates == expected


def test_default_library_search_does_not_load_a_cwd_library(
    library_search_platform, monkeypatch, tmp_path
):
    monkeypatch.chdir(tmp_path)
    local_library = tmp_path / backend._default_library_name()
    local_library.touch()
    calls = []

    def load(candidate):
        calls.append(candidate)
        # Model a loadable local binary with all earlier installation
        # candidates missing. Loading it must require explicit selection.
        if candidate == str(local_library):
            return object()
        raise OSError("not installed")

    monkeypatch.setattr(backend, "_ctypes_loader", lambda: load)
    with pytest.raises(TransportError, match="library_path"):
        backend._load_library(None)
    assert str(local_library) not in calls
    assert backend._library_handle_cache == {}


def test_default_system_loader_selection_is_shared_across_cwd_changes(
    library_search_platform, monkeypatch, tmp_path
):
    calls = []
    handle = object()

    def load(candidate):
        calls.append(candidate)
        return handle

    monkeypatch.setattr(backend, "_ctypes_loader", lambda: load)
    assert backend._load_library(None) is handle
    monkeypatch.chdir(tmp_path)
    assert backend._load_library(None) is handle
    assert calls == [backend._default_library_name()]


@pytest.mark.parametrize("selection", ["argument", "environment"])
@pytest.mark.parametrize("spelling", ["name", "dot", "home"])
def test_selected_library_path_is_used_for_loading_and_caching(
    library_search_platform, monkeypatch, tmp_path, selection, spelling
):
    monkeypatch.chdir(tmp_path)
    name = backend._default_library_name()
    path = {"name": name, "dot": f"./{name}", "home": f"~/minicanfd/{name}"}[spelling]
    expected = str(Path(path).expanduser().resolve())
    calls = []
    handle = object()

    def load(candidate):
        calls.append(candidate)
        return handle

    monkeypatch.setattr(backend, "_ctypes_loader", lambda: load)
    if selection == "environment":
        monkeypatch.setenv("LINKERBOT_CANFD_LIB", path)
        assert backend._load_library(None) is handle
    else:
        assert backend._load_library(path) is handle
    # An absolute spelling must reuse the exact handle just loaded, rather
    # than cache an unrelated system-loader result under the local file key.
    assert backend._load_library(Path(expected)) is handle
    assert calls == [expected]


@pytest.mark.parametrize("selection", ["argument", "environment"])
def test_selected_missing_library_does_not_use_another_installed_or_cwd_file(
    library_search_platform, monkeypatch, tmp_path, selection
):
    monkeypatch.chdir(tmp_path)
    name = backend._default_library_name()
    (tmp_path / name).touch()
    missing = tmp_path / "missing" / name
    calls = []

    def load(candidate):
        calls.append(candidate)
        if candidate == str(missing):
            raise OSError("selected library is missing")
        return object()

    monkeypatch.setattr(backend, "_ctypes_loader", lambda: load)
    if selection == "argument":
        monkeypatch.setenv("LINKERBOT_CANFD_LIB", str(tmp_path / name))
        path = missing
    else:
        monkeypatch.setenv("LINKERBOT_CANFD_LIB", str(missing))
        path = None
    with pytest.raises(TransportError, match="selected library is missing"):
        backend._load_library(path)
    assert calls == [str(missing)]


def test_load_library_error_mentions_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingLoader:
        def __call__(self, path: str):
            raise OSError("missing")

    monkeypatch.setattr(backend, "_ctypes_loader", lambda: FailingLoader())
    monkeypatch.setattr(
        backend, "_library_candidates", lambda library_path: ["missing.so"]
    )

    with pytest.raises(TransportError, match="LINKERBOT_CANFD_LIB"):
        backend._load_library(None)


def test_repeated_interfaces_share_cached_handle_but_keep_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    isolated_library_caches: None,
) -> None:
    path = tmp_path / "libcanbus.so"
    path.touch()
    lib = FakeLibrary()
    lib._name = str(path)
    load_calls: list[str] = []

    def load(candidate: str) -> FakeLibrary:
        load_calls.append(candidate)
        return lib

    monkeypatch.setattr(backend, "_preload_vendor_dependencies", lambda: None)
    monkeypatch.setattr(backend, "_ctypes_loader", lambda: load)

    first = backend.CANFDInterface(library_path=path)
    first.close()
    second = backend.CANFDInterface(library_path=path)
    second.close()

    assert load_calls == [str(path)]
    assert len(lib.LibCANbus_Init.calls) == 2
    assert len(lib.LibCANbus_Exit.calls) == 2


def test_library_cache_merges_symlink_with_real_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    isolated_library_caches: None,
) -> None:
    real_path = tmp_path / "libcanbus.so.1"
    alias_path = tmp_path / "libcanbus.so"
    real_path.touch()
    try:
        alias_path.symlink_to(real_path)
    except OSError:
        if os.name == "nt":
            pytest.skip("Windows runner does not allow symlink creation")
        raise
    loaded: list[object] = []

    def load(candidate: str) -> object:
        handle = object()
        loaded.append(handle)
        return handle

    monkeypatch.setattr(backend, "_preload_vendor_dependencies", lambda: None)
    monkeypatch.setattr(backend, "_ctypes_loader", lambda: load)

    via_alias = backend._load_library(alias_path)
    via_real_path = backend._load_library(real_path)

    assert via_alias is via_real_path
    assert loaded == [via_alias]


def test_library_cache_keeps_different_real_paths_separate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    isolated_library_caches: None,
) -> None:
    first_path = tmp_path / "a" / "libcanbus.so"
    second_path = tmp_path / "b" / "libcanbus.so"
    first_path.parent.mkdir()
    second_path.parent.mkdir()
    first_path.touch()
    second_path.touch()
    loaded: list[object] = []

    def load(candidate: str) -> object:
        handle = object()
        loaded.append(handle)
        return handle

    monkeypatch.setattr(backend, "_preload_vendor_dependencies", lambda: None)
    monkeypatch.setattr(backend, "_ctypes_loader", lambda: load)

    first = backend._load_library(first_path)
    second = backend._load_library(second_path)

    assert first is not second
    assert loaded == [first, second]


def test_concurrent_library_loads_create_one_cached_handle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    isolated_library_caches: None,
) -> None:
    path = tmp_path / "libcanbus.so"
    path.touch()
    start = threading.Barrier(9)
    load_calls: list[str] = []
    handle = object()

    def load(candidate: str) -> object:
        load_calls.append(candidate)
        return handle

    monkeypatch.setattr(backend, "_preload_vendor_dependencies", lambda: None)
    monkeypatch.setattr(backend, "_ctypes_loader", lambda: load)
    results: list[object] = []

    def load_from_thread() -> None:
        start.wait()
        results.append(backend._load_library(path))

    threads = [threading.Thread(target=load_from_thread) for _ in range(8)]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(timeout=1.0)

    assert all(not thread.is_alive() for thread in threads)
    assert load_calls == [str(path)]
    assert len(results) == 8
    assert all(result is handle for result in results)


def test_linux_dependency_preload_is_attempted_once_and_retained(
    monkeypatch: pytest.MonkeyPatch,
    isolated_library_caches: None,
) -> None:
    handle = object()
    calls: list[tuple[str, int]] = []

    def load(name: str, *, mode: int) -> object:
        calls.append((name, mode))
        return handle

    monkeypatch.setattr(backend.platform, "system", lambda: "Linux")
    monkeypatch.setattr(backend.ctypes, "CDLL", load)

    backend._preload_vendor_dependencies()
    backend._preload_vendor_dependencies()

    key = backend._canonical_library_key(backend._LINUX_USB_LIBRARY_NAME)
    assert calls == [(backend._LINUX_USB_LIBRARY_NAME, backend.ctypes.RTLD_GLOBAL)]
    assert backend._dependency_handle_cache == {key: handle}


def test_linux_dependency_preload_failure_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
    isolated_library_caches: None,
) -> None:
    calls: list[str] = []

    def fail(name: str, *, mode: int) -> object:
        _ = mode
        calls.append(name)
        raise OSError("missing")

    monkeypatch.setattr(backend.platform, "system", lambda: "Linux")
    monkeypatch.setattr(backend.ctypes, "CDLL", fail)

    backend._preload_vendor_dependencies()
    backend._preload_vendor_dependencies()

    assert calls == [backend._LINUX_USB_LIBRARY_NAME]
    assert backend._dependency_handle_cache == {}


def test_windows_loader_uses_one_cached_windll_handle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    isolated_library_caches: None,
) -> None:
    path = tmp_path / "HCanbus.dll"
    path.touch()
    handle = object()
    calls: list[str] = []

    def load(candidate: str) -> object:
        calls.append(candidate)
        return handle

    monkeypatch.setattr(backend.platform, "system", lambda: "Windows")
    monkeypatch.setattr(backend.ctypes, "WinDLL", load, raising=False)

    first = backend._load_library(path)
    second = backend._load_library(path)

    assert first is handle
    assert second is handle
    assert calls == [str(path)]


def test_close_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()
    interface = make_interface(monkeypatch, lib)

    interface.close()
    interface.close()

    assert lib.CAN_CloseDevice.calls == [(0, 0)]


def test_concurrent_close_waits_for_io_device_close_and_lifecycle_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = FakeLibrary()
    send_entered = threading.Event()
    receive_entered = threading.Event()
    vendor_close_entered = threading.Event()
    lifecycle_exit_entered = threading.Event()
    allow_send = threading.Event()
    allow_receive = threading.Event()
    allow_vendor_close = threading.Event()
    allow_lifecycle_exit = threading.Event()

    def blocking_send(*args: Any) -> int:
        _ = args
        send_entered.set()
        if not allow_send.wait(timeout=2.0):
            raise TimeoutError("test did not release CANFD_Transmit")
        return 1

    def blocking_receive(*args: Any) -> int:
        _ = args
        receive_entered.set()
        if not allow_receive.wait(timeout=2.0):
            raise TimeoutError("test did not release CANFD_Receive")
        return 0

    def blocking_vendor_close(*args: Any) -> int:
        _ = args
        vendor_close_entered.set()
        if not allow_vendor_close.wait(timeout=2.0):
            raise TimeoutError("test did not release CAN_CloseDevice")
        return 0

    def blocking_lifecycle_exit(*args: Any) -> int:
        _ = args
        lifecycle_exit_entered.set()
        if not allow_lifecycle_exit.wait(timeout=2.0):
            raise TimeoutError("test did not release LibCANbus_Exit")
        return 0

    lib.CANFD_Transmit.hook = blocking_send
    lib.CANFD_Receive.hook = blocking_receive
    lib.CAN_CloseDevice.hook = blocking_vendor_close
    lib.LibCANbus_Exit.hook = blocking_lifecycle_exit
    interface = make_interface(monkeypatch, lib)

    errors: list[Exception] = []
    send_done = threading.Event()
    receive_done = threading.Event()
    first_close_done = threading.Event()
    second_close_done = threading.Event()

    def run(operation: Callable[[], object], done: threading.Event) -> None:
        try:
            operation()
        except Exception as error:
            errors.append(error)
        finally:
            done.set()

    message = CANFDMessage(arbitration_id=1, data=b"x")
    send_thread = threading.Thread(
        target=run,
        args=(lambda: interface.send(message), send_done),
        daemon=True,
    )
    receive_thread = threading.Thread(
        target=run,
        args=(interface.receive, receive_done),
        daemon=True,
    )
    send_thread.start()
    receive_thread.start()
    assert send_entered.wait(timeout=1.0)
    assert receive_entered.wait(timeout=1.0)

    first_close_thread = threading.Thread(
        target=run,
        args=(interface.close, first_close_done),
        daemon=True,
    )
    first_close_thread.start()
    assert interface._closed_event.wait(timeout=1.0)

    second_close_thread = threading.Thread(
        target=run,
        args=(interface.close, second_close_done),
        daemon=True,
    )
    second_close_thread.start()
    assert not second_close_done.wait(timeout=0.05)

    # The owner drains transmit before receive, and neither close caller may
    # return while either in-flight vendor call still owns its direction lock.
    allow_send.set()
    assert send_done.wait(timeout=1.0)
    assert not vendor_close_entered.wait(timeout=0.05)
    assert not second_close_done.is_set()

    allow_receive.set()
    assert receive_done.wait(timeout=1.0)
    assert vendor_close_entered.wait(timeout=1.0)
    assert not first_close_done.is_set()
    assert not second_close_done.is_set()

    # Device close alone is not completion: the process-level lifecycle exit
    # must finish before a waiter can safely return and reopen the library.
    allow_vendor_close.set()
    assert lifecycle_exit_entered.wait(timeout=1.0)
    assert not first_close_done.is_set()
    assert not second_close_done.is_set()

    allow_lifecycle_exit.set()
    assert first_close_done.wait(timeout=1.0)
    assert second_close_done.wait(timeout=1.0)
    for thread in (
        send_thread,
        receive_thread,
        first_close_thread,
        second_close_thread,
    ):
        thread.join(timeout=1.0)
        assert not thread.is_alive()

    assert errors == []
    assert lib.CAN_CloseDevice.calls == [(0, 0)]
    assert len(lib.LibCANbus_Init.calls) == 1
    assert len(lib.LibCANbus_Exit.calls) == 1

    reopened = backend.CANFDInterface()
    assert len(lib.LibCANbus_Init.calls) == 2
    reopened.close()
    assert len(lib.CAN_CloseDevice.calls) == 2
    assert len(lib.LibCANbus_Exit.calls) == 2


def test_concurrent_close_waiter_retries_when_vendor_close_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lib = FakeLibrary()
    vendor_close_entered = threading.Event()
    allow_vendor_close = threading.Event()

    close_attempts = 0

    def failing_once_vendor_close(*args: Any) -> int:
        nonlocal close_attempts
        _ = args
        close_attempts += 1
        if close_attempts > 1:
            return 0
        vendor_close_entered.set()
        if not allow_vendor_close.wait(timeout=2.0):
            raise TimeoutError("test did not release CAN_CloseDevice")
        raise OSError("vendor close crashed")

    lib.CAN_CloseDevice.hook = failing_once_vendor_close
    interface = make_interface(monkeypatch, lib)
    first_errors: list[Exception] = []
    second_errors: list[Exception] = []
    first_done = threading.Event()
    second_done = threading.Event()

    def call_close(errors: list[Exception], done: threading.Event) -> None:
        try:
            interface.close()
        except Exception as error:
            errors.append(error)
        finally:
            done.set()

    first_thread = threading.Thread(
        target=call_close,
        args=(first_errors, first_done),
        daemon=True,
    )
    first_thread.start()
    assert vendor_close_entered.wait(timeout=1.0)

    second_thread = threading.Thread(
        target=call_close,
        args=(second_errors, second_done),
        daemon=True,
    )
    second_thread.start()
    assert not second_done.wait(timeout=0.05)

    allow_vendor_close.set()
    assert first_done.wait(timeout=1.0)
    assert second_done.wait(timeout=1.0)
    first_thread.join(timeout=1.0)
    second_thread.join(timeout=1.0)

    assert len(first_errors) == 1
    assert isinstance(first_errors[0], TransportError)
    assert "vendor close crashed" in str(first_errors[0])
    assert second_errors == []
    assert lib.CAN_CloseDevice.calls == [(0, 0), (0, 0)]
    assert len(lib.LibCANbus_Exit.calls) == 1
    assert interface._close_succeeded
    assert interface._close_complete_event.is_set()


def test_close_failure_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()
    lib.CAN_CloseDevice.result = 1
    interface = make_interface(monkeypatch, lib)

    with pytest.raises(TransportError, match="CAN_CloseDevice"):
        interface.close()

    assert not interface._close_succeeded
    lib.CAN_CloseDevice.result = 0
    interface.close()

    assert interface._close_succeeded
    assert lib.CAN_CloseDevice.calls == [(0, 0), (0, 0)]


def test_default_config_maps_to_vendor_struct(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()

    make_interface(monkeypatch, lib)

    config = lib.CANFD_Init.calls[0][2]._obj
    assert config.NomBaud == 1_000_000
    assert config.DatBaud == 5_000_000
    assert config.Config == 0x07
    assert config.Model == 0
    assert config.Cantype == 1


def test_custom_config_maps_to_vendor_struct(monkeypatch: pytest.MonkeyPatch) -> None:
    lib = FakeLibrary()
    monkeypatch.setattr(backend, "_load_library", lambda library_path: lib)

    backend.CANFDInterface(config=CANFDConfigOptions(frame_type=0x04, config=0x01))

    config = lib.CANFD_Init.calls[0][2]._obj
    assert config.Config == 0x01
