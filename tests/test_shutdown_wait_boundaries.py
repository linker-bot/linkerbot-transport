"""Concurrent shutdown callers wait boundedly without releasing live resources."""

import threading

import pytest

from linkerbot_transport.can import can as classic
from linkerbot_transport.canfd import dispatcher as flexible
from linkerbot_transport.errors import TimeoutError


@pytest.mark.parametrize("kind", ["classic", "fd"])
def test_second_stop_times_out_while_first_owns_backend_close(monkeypatch, kind):
    entered, release, second_done = (threading.Event() for _ in range(3))
    results = {}

    class Backend:
        calls = 0

        def recv(self, timeout):
            release.wait(0.005)
            return None

        def receive(self, *, max_frames, timeout_ms):
            release.wait(0.005)
            return []

        def send(self, *args, **kwargs):
            pass

        def close(self):
            self.calls += 1
            entered.set()
            assert release.wait(3)

        shutdown = close

    backend = Backend()
    if kind == "classic":
        monkeypatch.setattr(classic, "THREAD_JOIN_TIMEOUT_S", 0.02)
        monkeypatch.setattr(classic, "open_bus", lambda **kwargs: backend)
        dispatcher = classic.CANMessageDispatcher("controlled-stop", "virtual")
    else:
        monkeypatch.setattr(flexible, "THREAD_JOIN_TIMEOUT_S", 0.02)
        dispatcher = flexible.CANFDMessageDispatcher(interface=backend)

    def stop(index):
        try:
            dispatcher.stop()
            results[index] = None
        except BaseException as error:
            results[index] = error
        finally:
            if index == 2:
                second_done.set()

    first = threading.Thread(target=stop, args=(1,))
    second = threading.Thread(target=stop, args=(2,))
    first.start()
    try:
        assert entered.wait(1)
        second.start()
        assert second_done.wait(0.5), "second stop must not wait for backend close"
        assert isinstance(results[2], TimeoutError)
        assert "cleanup ownership retained" in str(results[2])
        assert backend.calls == 1
    finally:
        release.set()
        first.join(2)
        if second.ident is not None:
            second.join(2)
        assert not first.is_alive() and not second.is_alive()
        dispatcher.stop()
        assert not dispatcher._recv_thread.is_alive()
        assert not dispatcher._send_thread.is_alive()
    assert results[1] is None
    assert backend.calls == 1
