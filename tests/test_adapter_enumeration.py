"""OS/driver enumeration must never initialize a bus or touch a serial port."""

from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest

from linkerbot_transport import discovery
from linkerbot_transport.errors import ValidationError


def pcan_entry(handle=1, *, condition=1, fd=True, name=b"PCAN-USB"):
    return SimpleNamespace(
        channel_handle=handle,
        channel_condition=condition,
        device_features=int(fd),
        device_name=name,
        device_id=45,
        controller_number=0,
    )


def fake_pcan(monkeypatch, entries=(), *, status=0, driver_error=None):
    queries = []

    class Driver:
        def __init__(self):
            if driver_error is not None:
                raise driver_error

        def GetValue(self, *args):
            queries.append(args)
            return status, entries

    basic = SimpleNamespace(
        PCANBasic=Driver,
        PCAN_NONEBUS=0,
        PCAN_ATTACHED_CHANNELS=42,
        PCAN_ERROR_OK=0,
        PCAN_CHANNEL_NAMES={
            f"PCAN_USBBUS{value}": SimpleNamespace(value=value) for value in (1, 2, 3)
        },
        PCAN_CHANNEL_AVAILABLE=1,
        PCAN_CHANNEL_OCCUPIED=2,
        FEATURE_FD_CAPABLE=1,
    )
    monkeypatch.setattr(discovery.importlib, "import_module", lambda name: basic)
    return queries


def test_pcan_enumerates_native_names_capabilities_and_busy_diagnostics(monkeypatch):
    queries = fake_pcan(
        monkeypatch,
        (
            pcan_entry(),
            pcan_entry(2, fd=False),
            pcan_entry(3, condition=3),
        ),
    )
    result = discovery.enumerate_pcan_channels()
    assert queries == [(0, 42)]
    assert [(c.channel, c.supports_fd) for c in result.channels] == [
        ("PCAN_USBBUS1", True),
        ("PCAN_USBBUS2", False),
    ]
    assert result.channels[0].device_id == 45
    assert [(f.stage, f.channel) for f in result.failures] == [("busy", "PCAN_USBBUS3")]
    with pytest.raises(FrozenInstanceError):
        result.channels[0].channel = "changed"


def test_pcan_missing_driver_and_query_failure_are_distinct_from_empty(monkeypatch):
    fake_pcan(monkeypatch)
    assert discovery.enumerate_pcan_channels().failures == ()
    original = OSError("PCANBasic.dll missing")
    fake_pcan(monkeypatch, driver_error=original)
    result = discovery.enumerate_pcan_channels()
    assert result.failures[0].stage == "driver"
    assert result.failures[0].error.cause is original
    fake_pcan(monkeypatch, status=16)
    result = discovery.enumerate_pcan_channels()
    assert result.failures[0].stage == "scan"
    assert "16" in str(result.failures[0].error)


def test_pcan_one_unknown_channel_does_not_discard_others(monkeypatch):
    fake_pcan(monkeypatch, (pcan_entry(99), pcan_entry(2)))
    result = discovery.enumerate_pcan_channels()
    assert len(result.failures) == len(result.channels) == 1
    assert result.channels[0].channel == "PCAN_USBBUS2"


@pytest.mark.parametrize(
    "bad", [pcan_entry(fd=False), pcan_entry(condition=0), pcan_entry(condition=3)]
)
def test_pcan_conflicting_or_unavailable_duplicate_never_reappears(monkeypatch, bad):
    fake_pcan(monkeypatch, (pcan_entry(), bad, pcan_entry()))
    result = discovery.enumerate_pcan_channels()
    assert result.channels == ()
    assert result.failures


def test_pcan_consistent_duplicate_is_listed_once(monkeypatch):
    fake_pcan(monkeypatch, (pcan_entry(), pcan_entry()))
    assert len(discovery.enumerate_pcan_channels().channels) == 1


def port(name, *, known=True):
    return SimpleNamespace(
        device=name,
        vid=0x16D0 if known else 123,
        pid=0x117E,
        description="serial device",
        serial_number="SN1",
    )


def fake_serial(monkeypatch, entries):
    monkeypatch.setattr(
        discovery.importlib,
        "import_module",
        lambda name: SimpleNamespace(comports=lambda: entries),
    )


def test_slcan_auto_filter_keeps_fd_firmware_unknown(monkeypatch):
    fake_serial(monkeypatch, (port("COM4"), port("COM5", known=False)))
    result = discovery.enumerate_slcan_channels()
    assert result.failures == ()
    assert [c.channel for c in result.channels] == ["COM4"]
    assert result.channels[0].supports_fd is None
    assert result.channels[0].serial_number == "SN1"


def test_explicit_serial_selection_accepts_unknown_usb_and_reports_missing(monkeypatch):
    fake_serial(monkeypatch, (port("COM4"), port("COM5", known=False)))
    result = discovery.enumerate_slcan_channels(ports=("COM5", "COM6"))
    assert [c.channel for c in result.channels] == ["COM5"]
    assert [(f.stage, f.channel) for f in result.failures] == [("unavailable", "COM6")]


def test_serial_windows_case_and_duplicate_names(monkeypatch):
    monkeypatch.setattr(discovery, "_port_key", str.casefold)
    fake_serial(monkeypatch, (port("COM4"), port("com4")))
    result = discovery.enumerate_slcan_channels(ports=("com4", "COM4"))
    assert len(result.channels) == 1
    assert result.failures == ()


@pytest.mark.parametrize("ports", ["COM4", ("",), (4,), True])
def test_invalid_ports_rejected_before_loading_optional_driver(ports, monkeypatch):
    def forbidden(name):
        pytest.fail("invalid arguments must fail before driver import")

    monkeypatch.setattr(discovery.importlib, "import_module", forbidden)
    with pytest.raises(ValidationError):
        discovery.enumerate_slcan_channels(ports=ports)


@pytest.mark.parametrize(
    "enumerate_channels",
    [discovery.enumerate_pcan_channels, discovery.enumerate_slcan_channels],
)
def test_missing_optional_dependency_is_a_failure(enumerate_channels, monkeypatch):
    def missing(name):
        raise ImportError("optional driver missing")

    monkeypatch.setattr(discovery.importlib, "import_module", missing)
    result = enumerate_channels()
    assert result.channels == ()
    assert result.failures[0].stage == "dependency"


def test_serial_scan_error_is_preserved(monkeypatch):
    def failed():
        raise OSError("serial enumeration failed")

    monkeypatch.setattr(
        discovery.importlib,
        "import_module",
        lambda name: SimpleNamespace(comports=failed),
    )
    assert discovery.enumerate_slcan_channels().failures[0].stage == "scan"
