"""Interrupt cleanup at real ownership boundaries; retry must be idempotent."""

import inspect
import sys
from contextlib import contextmanager
from unittest.mock import Mock

import can
import pytest

from linkerbot_transport import SharedTransportPool
from linkerbot_transport._python_can import _CHANNELS, ChannelClaim, rollback_bus_open
from linkerbot_transport.can import CANTransport
from linkerbot_transport.can.can import CANMessageDispatcher
from linkerbot_transport.canfd import ctypes_backend as native
from linkerbot_transport.canfd.python_can_backend import PythonCANFDBackend
from linkerbot_transport.errors import TransportError, ValidationError


@contextmanager
def interrupt_before(function, fragments):
    lines, start = inspect.getsourcelines(function)
    targets = [
        start + i
        for i, line in enumerate(lines)
        if any(part in line for part in fragments)
    ]
    assert targets, "test must identify an actual ownership boundary"
    target = targets[0]
    fired = []

    def trace(frame, event, arg):
        if (
            not fired
            and event == "line"
            and frame.f_code is function.__code__
            and frame.f_lineno == target
        ):
            fired.append(True)
            raise KeyboardInterrupt("ownership boundary")
        return trace

    previous = sys.gettrace()
    sys.settrace(trace)
    try:
        yield
    finally:
        sys.settrace(previous)
    assert fired


def test_retry_of_interrupted_release_preserves_other_live_claim():
    first = ChannelClaim("virtual", "release-token", (1,))
    second = ChannelClaim("virtual", "release-token", (1,))
    try:
        with pytest.raises(KeyboardInterrupt):
            with interrupt_before(
                ChannelClaim.release,
                ["if current.count == 0:", "if not self._use.owners"],
            ):
                first.release()
        first.release()
        assert _CHANNELS[("virtual", "release-token")].count == 1
        with pytest.raises(TransportError, match="different settings"):
            ChannelClaim("virtual", "release-token", (2,))
    finally:
        first.release()
        second.release()


@pytest.mark.parametrize("kind", ["classic", "fd"])
def test_closed_backend_retries_unfinished_claim_release(monkeypatch, kind):
    owner = (
        CANMessageDispatcher("release-close", "virtual", start_send_worker=False)
        if kind == "classic"
        else PythonCANFDBackend("release-close", "virtual")
    )
    close = owner.stop if kind == "classic" else owner.close
    release = ChannelClaim.release
    calls = []

    def interrupted_once(self):
        if not calls:
            calls.append(True)
            raise KeyboardInterrupt("claim release interrupted")
        return release(self)

    monkeypatch.setattr(ChannelClaim, "release", interrupted_once)
    try:
        with pytest.raises(KeyboardInterrupt):
            close()
        close()
        close()
        assert ("virtual", "release-close") not in _CHANNELS
    finally:
        monkeypatch.setattr(ChannelClaim, "release", release)
        claim = owner._channel_claim if kind == "classic" else owner._claim
        claim.release()
        close()


@pytest.mark.parametrize("partial_bus", [False, True])
def test_rollback_retains_cleanup_owner_when_claim_release_is_interrupted(
    monkeypatch, partial_bus
):
    first = ChannelClaim("virtual", "release-rollback", (1,))
    error = RuntimeError("open failed")
    bus = (
        can.Bus(interface="virtual", channel="release-rollback", ignore_config=True)
        if partial_bus
        else None
    )
    if bus is not None:
        error._failed_bus = bus
    release = ChannelClaim.release
    calls = []

    def once(self):
        if not calls:
            calls.append(True)
            raise KeyboardInterrupt("release interrupted")
        release(self)

    monkeypatch.setattr(ChannelClaim, "release", once)
    try:
        with pytest.raises(KeyboardInterrupt) as raised:
            rollback_bus_open(error, first)
        owner = raised.value._failed_dispatcher
        owner.stop()
        owner.stop()
        assert ("virtual", "release-rollback") not in _CHANNELS
    finally:
        monkeypatch.setattr(ChannelClaim, "release", release)
        if bus is not None:
            bus.shutdown()
        first.release()


def test_native_release_interrupt_cannot_lose_or_double_consume_reference():
    lib = Mock(spec=["LibCANbus_Init", "LibCANbus_Exit"])
    lib.LibCANbus_Init.return_value = lib.LibCANbus_Exit.return_value = 0
    first = native._lifecycle_acquire(lib)
    second = native._lifecycle_acquire(lib)
    try:
        with pytest.raises(KeyboardInterrupt):
            with interrupt_before(
                native._lifecycle_release,
                ["count = max(0, count - 1)", "if ref.owners:"],
            ):
                native._lifecycle_release(first)
        native._lifecycle_release(first)
        assert native._lifecycle_snapshot()[first.key] == 1
        lib.LibCANbus_Exit.assert_not_called()
        native._lifecycle_release(second)
        assert first.key not in native._lifecycle_snapshot()
        lib.LibCANbus_Exit.assert_called_once()
        third = native._lifecycle_acquire(lib)
        try:
            native._lifecycle_release(first)
            assert native._lifecycle_snapshot()[third.key] == 1
            assert lib.LibCANbus_Exit.call_count == 1
        finally:
            native._lifecycle_release(third)
    finally:
        native._lifecycle_release(first)
        native._lifecycle_release(second)


def test_slcan_aliases_share_pool_key_and_resolved_driver_settings(monkeypatch):
    calls = []

    def create(**kwargs):
        calls.append(kwargs)
        return can.Bus(interface="virtual", channel="alias-bus", ignore_config=True)

    monkeypatch.setattr("linkerbot_transport.can.can.open_bus", create)
    first = CANTransport("COM5@1000000", "slcan")
    same = CANTransport("COM5", "slcan", bus_options={"tty_baudrate": 1_000_000})
    assert first.pool_key == same.pool_key
    pool = SharedTransportPool()
    lease = pool.acquire(first.pool_key, lambda: first)
    other = pool.acquire(same.pool_key, lambda: same)
    try:
        assert lease.transport is other.transport
        assert len(calls) == 1
        assert calls[0]["channel"] == "COM5"
        assert calls[0]["tty_baudrate"] == 1_000_000
        with pytest.raises(TransportError, match="different settings"):
            CANMessageDispatcher("COM5@115200", "slcan")
    finally:
        other.close()
        lease.close()


@pytest.mark.parametrize("factory", [CANTransport, CANMessageDispatcher])
@pytest.mark.parametrize(
    "channel,options",
    [
        ("COM5@1000000", {"tty_baudrate": 115200}),
        ("COM5@0", {}),
        ("COM5@bad", {}),
        ("COM5@@1000000", {}),
    ],
)
def test_invalid_or_conflicting_slcan_baud_rejected_before_io(
    factory, channel, options
):
    with pytest.raises(ValidationError):
        factory(channel, "slcan", bus_options=options)
