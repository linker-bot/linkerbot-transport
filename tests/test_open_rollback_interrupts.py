"""All constructor rollback layers preserve process interrupts and ownership."""

import threading

import pytest

from linkerbot_transport.can import can as classic
from linkerbot_transport.canfd import ctypes_backend as native
from linkerbot_transport.canfd import dispatcher as flexible


@pytest.mark.parametrize("kind", ["classic", "fd", "native"])
@pytest.mark.parametrize("original_type", [OSError, KeyboardInterrupt])
@pytest.mark.parametrize("cleanup_type", [KeyboardInterrupt, SystemExit])
def test_constructor_rollback_retains_owner_on_the_propagated_interrupt(
    monkeypatch, kind, original_type, cleanup_type
):
    original = original_type("startup failed")
    cleanup = cleanup_type("cleanup interrupted")

    class Backend:
        closed = False
        close_calls = 0

        def close(self):
            self.close_calls += 1
            if self.close_calls == 1:
                raise cleanup
            self.closed = True

        shutdown = close

    backend = Backend()
    expected = cleanup if isinstance(original, Exception) else original
    if kind == "native":
        monkeypatch.setattr(native, "_load_library", lambda path: object())
        monkeypatch.setattr(native, "_bind_signatures", lambda *args, **kwargs: None)
        monkeypatch.setattr(native, "_lifecycle_acquire", lambda lib: None)

        def fail_open(self):
            raise original

        monkeypatch.setattr(native.CANFDInterface, "_open", fail_open)
        monkeypatch.setattr(
            native.CANFDInterface, "close", lambda self: backend.close()
        )
        construct = native.CANFDInterface
        attribute = "_failed_interface"
    else:
        real_start = threading.Thread.start
        prefix = (
            "CANMessageDispatcher" if kind == "classic" else "CANFDMessageDispatcher"
        )

        def fail_start(thread):
            if thread.name == prefix + ".recv_loop":
                raise original
            real_start(thread)

        monkeypatch.setattr(threading.Thread, "start", fail_start)
        if kind == "classic":
            monkeypatch.setattr(classic, "open_bus", lambda **kwargs: backend)

            def construct():
                return classic.CANMessageDispatcher("rollback-interrupt", "virtual")
        else:

            def construct():
                return flexible.CANFDMessageDispatcher(interface=backend)

        attribute = "_failed_dispatcher"
    owner = None
    try:
        with pytest.raises(type(expected)) as caught:
            construct()
        assert caught.value is expected
        if isinstance(original, Exception):
            assert caught.value.__cause__ is original
        owner = getattr(caught.value, attribute)
        assert not backend.closed
    finally:
        if owner is not None:
            if kind == "native":
                owner.close()
            else:
                owner.stop()
                assert not owner._recv_thread.is_alive()
                assert not owner._send_thread.is_alive()
    assert backend.closed
