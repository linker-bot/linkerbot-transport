"""FD synchronous transports avoid an idle TX worker; queued sends start once."""

import threading

import pytest

from linkerbot_transport import SendOutcome
from linkerbot_transport.canfd import CANFDMessage
from linkerbot_transport.canfd import dispatcher as dispatcher_module
from linkerbot_transport.canfd import transport as transport_module
from linkerbot_transport.canfd.dispatcher import CANFDMessageDispatcher
from linkerbot_transport.errors import ClosedError, TransportError, ValidationError


class Backend:
    def __init__(self):
        self.closed = threading.Event()
        self.accepted = []

    def receive(self, *, max_frames, timeout_ms):
        self.closed.wait(timeout_ms / 1000)
        return []

    def send(self, message):
        self.accepted.append(message.arbitration_id)

    def close(self):
        self.closed.set()


@pytest.mark.parametrize("kind", ["vendor", "python-can", "socketcan"])
def test_builtin_high_level_fd_transports_never_start_tx_for_sync_send(
    monkeypatch, kind
):
    backend = Backend()
    if kind == "vendor":
        monkeypatch.setattr(
            dispatcher_module, "CANFDInterface", lambda **kwargs: backend
        )
        transport = transport_module.CANFDTransport()
    elif kind == "python-can":
        monkeypatch.setattr(
            transport_module, "PythonCANFDBackend", lambda *args, **kwargs: backend
        )
        transport = transport_module.PythonCANFDTransport("lazy-fd", "virtual")
    else:
        monkeypatch.setattr(
            transport_module, "SocketCANFDBackend", lambda *args, **kwargs: backend
        )
        transport = transport_module.SocketCANFDTransport("lazy-can0")
    transport.open()
    dispatcher = transport._dispatcher
    try:
        assert dispatcher._recv_thread.is_alive()
        assert dispatcher._send_thread.ident is None
        assert (
            transport.send(CANFDMessage(1, b"x")).result(0).outcome
            is SendOutcome.DISPATCHED
        )
        assert backend.accepted == [1]
        assert dispatcher._send_thread.ident is None
    finally:
        transport.close()
        assert not dispatcher._recv_thread.is_alive()
        assert not dispatcher._send_thread.is_alive()
        assert backend.closed.is_set()


def test_low_level_eager_default_is_preserved():
    dispatcher = CANFDMessageDispatcher(interface=Backend())
    try:
        assert dispatcher._send_thread.is_alive()
    finally:
        dispatcher.stop()


def test_concurrent_first_queued_sends_start_exactly_one_worker(monkeypatch):
    backend = Backend()
    dispatcher = CANFDMessageDispatcher(interface=backend, start_send_worker=False)
    real_start = threading.Thread.start
    starts = []
    receipts, errors = [], []
    barrier = threading.Barrier(9)

    def count_start(thread):
        if thread is dispatcher._send_thread:
            starts.append(thread)
        real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", count_start)

    def send(i):
        barrier.wait(timeout=2)
        try:
            receipts.append(dispatcher.send(CANFDMessage(i, b"x")))
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=send, args=(i,)) for i in range(8)]
    try:
        for thread in threads:
            thread.start()
        barrier.wait(timeout=2)
        for thread in threads:
            thread.join(2)
            assert not thread.is_alive()
        assert errors == [] and len(receipts) == 8
        for receipt in receipts:
            assert receipt.result(1).outcome is SendOutcome.DISPATCHED
        assert len(starts) == 1
        assert sorted(backend.accepted) == list(range(8))
    finally:
        for thread in threads:
            if thread.ident is not None:
                thread.join(2)
        dispatcher.stop()
        assert not dispatcher._recv_thread.is_alive()
        assert not dispatcher._send_thread.is_alive()


@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt])
def test_lazy_start_failure_accepts_no_receipt_and_requires_new_dispatcher(
    monkeypatch, error_type
):
    backend = Backend()
    dispatcher = CANFDMessageDispatcher(interface=backend, start_send_worker=False)
    real_start = threading.Thread.start
    original = error_type("TX thread start failed")
    attempts = []

    def fail_once(thread):
        if thread is dispatcher._send_thread:
            attempts.append(thread)
            raise original
        real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_once)
    try:
        with pytest.raises(error_type) as caught:
            dispatcher.send(CANFDMessage(1, b"x"))
        assert caught.value is original
        assert not dispatcher._receipts._pending
        assert dispatcher._send_queue.empty()
        assert dispatcher._send_thread.ident is None
        with pytest.raises(TransportError, match="startup failed"):
            dispatcher.send(CANFDMessage(2, b"x"))
        assert len(attempts) == 1
        assert backend.accepted == []
    finally:
        dispatcher.stop()
        assert not dispatcher._recv_thread.is_alive()
        assert not dispatcher._send_thread.is_alive()

    replacement = CANFDMessageDispatcher(interface=Backend(), start_send_worker=False)
    try:
        assert (
            replacement.send(CANFDMessage(3, b"x")).result(1).outcome
            is SendOutcome.DISPATCHED
        )
    finally:
        replacement.stop()


def test_stop_before_first_queued_send_does_not_start_worker():
    backend = Backend()
    dispatcher = CANFDMessageDispatcher(interface=backend, start_send_worker=False)
    dispatcher.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        dispatcher.send(CANFDMessage(1, b"x"))
    assert dispatcher._send_thread.ident is None
    assert backend.accepted == []
    assert backend.closed.is_set()


@pytest.mark.parametrize(
    "stop_before_enqueue", [True, False], ids=["stop-first", "enqueue-first"]
)
def test_stop_racing_lazy_start_leaves_no_worker_or_receipt(
    monkeypatch, stop_before_enqueue
):
    backend = Backend()
    dispatcher = CANFDMessageDispatcher(interface=backend, start_send_worker=False)
    real_start = threading.Thread.start
    real_ensure_worker = dispatcher._ensure_send_worker
    real_enqueue = dispatcher._send_queue.put_nowait
    entered, release, stopping, stopped, enqueued = (
        threading.Event() for _ in range(5)
    )
    receipts, send_errors, stop_errors = [], [], []

    def gated_start(thread):
        if thread is dispatcher._send_thread:
            entered.set()
            assert release.wait(2)
        real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", gated_start)

    def gated_ensure_worker():
        real_ensure_worker()
        if stop_before_enqueue:
            # Startup has released _error_lock, but the send is not admitted.
            # Let stop finish before send checks admission again.
            assert stopped.wait(2)

    def record_enqueue(item):
        real_enqueue(item)
        enqueued.set()

    monkeypatch.setattr(dispatcher, "_ensure_send_worker", gated_ensure_worker)
    monkeypatch.setattr(dispatcher._send_queue, "put_nowait", record_enqueue)

    def send():
        try:
            receipts.append(dispatcher.send(CANFDMessage(1, b"x")))
        except BaseException as error:
            send_errors.append(error)

    def stop():
        stopping.set()
        try:
            if not stop_before_enqueue:
                # Receipt registration and enqueue precede stop's admission
                # gate; the admitted receipt must reach a terminal state.
                assert enqueued.wait(2)
            dispatcher.stop()
        except BaseException as error:
            stop_errors.append(error)
        finally:
            stopped.set()

    producer, closer = threading.Thread(target=send), threading.Thread(target=stop)
    producer.start()
    try:
        assert entered.wait(1)
        closer.start()
        assert stopping.wait(1)
    finally:
        release.set()
        if closer.ident is None:
            # Unblock the producer even if setup failed before closer.start().
            stopped.set()
        producer.join(2)
        if closer.ident is not None:
            closer.join(2)
        dispatcher.stop()
        assert not producer.is_alive() and not closer.is_alive()
    assert stop_errors == []
    if stop_before_enqueue:
        assert receipts == []
        assert len(send_errors) == 1
        assert type(send_errors[0]) is RuntimeError
        assert str(send_errors[0]) == "Cannot send on a stopped CANFDMessageDispatcher"
        assert not enqueued.is_set()
        assert dispatcher._send_queue.empty()
        assert backend.accepted == []
    else:
        assert send_errors == [] and len(receipts) == 1
        assert receipts[0].done()
        assert receipts[0].outcome in (SendOutcome.DISPATCHED, SendOutcome.FAILED)
        if receipts[0].outcome is SendOutcome.FAILED:
            assert isinstance(receipts[0].exception(0), ClosedError)
    assert not dispatcher._receipts.has_pending()
    assert not dispatcher._recv_thread.is_alive()
    assert not dispatcher._send_thread.is_alive()
    assert backend.closed.is_set()


def test_invalid_lazy_start_option_is_rejected_before_backend_construction(monkeypatch):
    monkeypatch.setattr(
        dispatcher_module,
        "CANFDInterface",
        lambda **kwargs: pytest.fail("must validate before I/O"),
    )
    with pytest.raises(ValidationError, match="start_send_worker must be bool"):
        CANFDMessageDispatcher(start_send_worker=1)
