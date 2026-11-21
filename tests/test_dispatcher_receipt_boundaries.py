"""Physical batch results survive reentrant and concurrent completion cleanup."""

import threading

import pytest

from linkerbot_transport import SendOutcome, SendReceipt
from linkerbot_transport.canfd import CANFDMessage
from linkerbot_transport.canfd.dispatcher import CANFDMessageDispatcher
from linkerbot_transport.errors import ClosedError, TransportError
from linkerbot_transport.receipts import _ReceiptTracker


class BatchBackend:
    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()
        self.closed = threading.Event()
        self.accepted = []
        self.batches = []

    def send(self, message):
        self.started.set()
        assert self.release.wait(2)
        self.accepted.append(message.arbitration_id)

    def send_batch(self, messages):
        ids = [message.arbitration_id for message in messages]
        self.batches.append(ids)
        self.accepted.extend(ids)

    def receive(self, *, max_frames, timeout_ms):
        self.closed.wait(timeout_ms / 1000)
        return []

    def close(self):
        self.release.set()
        self.closed.set()


@pytest.mark.parametrize("operation", ["stop", "fault"])
def test_batch_success_cannot_be_overwritten_by_completion_callback(operation):
    backend = BatchBackend()
    dispatcher = CANFDMessageDispatcher(interface=backend)
    callback_done = threading.Event()
    callback_errors = []
    try:
        first = dispatcher.send(CANFDMessage(1, b"a"))
        assert backend.started.wait(1)
        second = dispatcher.send(CANFDMessage(2, b"b"))
        third = dispatcher.send(CANFDMessage(3, b"c"))

        def completed(_):
            try:
                if operation == "stop":
                    dispatcher.stop()
                else:
                    dispatcher._handle_bus_error(TransportError("controlled fault"))
                # Cleanup also publishes confirmed remaining successes.
                assert third.result(timeout=0).outcome is SendOutcome.DISPATCHED
            except BaseException as error:
                callback_errors.append(error)
            finally:
                callback_done.set()

        second.add_done_callback(completed)
        backend.release.set()
        assert callback_done.wait(2)
        assert callback_errors == []
        assert backend.accepted == [1, 2, 3]
        assert backend.batches == [[2, 3]]
        for receipt in (first, second, third):
            assert receipt.result(timeout=0).outcome is SendOutcome.DISPATCHED
    finally:
        backend.release.set()
        dispatcher.stop()
        assert not dispatcher._send_thread.is_alive()
        assert not dispatcher._recv_thread.is_alive()


def test_concurrent_stop_preserves_entire_confirmed_batch():
    tracker = _ReceiptTracker()
    receipts = tuple(SendReceipt() for _ in range(3))
    for receipt in receipts:
        tracker.register(receipt)
    entered = threading.Event()
    release = threading.Event()
    errors = []

    def completed(_):
        entered.set()
        assert release.wait(2)

    receipts[0].add_done_callback(completed)

    def succeed():
        try:
            tracker.succeed_many(receipts)
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=succeed)
    worker.start()
    try:
        assert entered.wait(1)
        tracker.fail_all(ClosedError("stopping"))
        assert all(
            receipt.result(0).outcome is SendOutcome.DISPATCHED for receipt in receipts
        )
    finally:
        release.set()
        worker.join(2)
        assert not worker.is_alive()
    assert errors == []
    assert tracker._pending == {}


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_interrupted_publication_keeps_success_and_finishes_batch_suffix(
    monkeypatch, interrupt_type
):
    tracker = _ReceiptTracker()
    receipts = tuple(SendReceipt() for _ in range(3))
    for receipt in receipts:
        tracker.register(receipt)
    dispatch = receipts[0]._dispatch
    interrupted = interrupt_type("publish interrupted once")

    def interrupt_once():
        monkeypatch.setattr(receipts[0], "_dispatch", dispatch)
        raise interrupted

    monkeypatch.setattr(receipts[0], "_dispatch", interrupt_once)
    with pytest.raises(interrupt_type) as caught:
        tracker.succeed_many(receipts)
    assert caught.value is interrupted
    assert not receipts[0].done()
    assert all(receipt.done() for receipt in receipts[1:])
    tracker.fail_all(ClosedError("retry cleanup"))
    assert all(
        receipt.result(0).outcome is SendOutcome.DISPATCHED for receipt in receipts
    )
    assert tracker._pending == {}


def test_cleanup_callback_can_interrupt_without_orphaning_suffix():
    tracker = _ReceiptTracker()
    receipts = tuple(SendReceipt() for _ in range(3))
    for receipt in receipts:
        tracker.register(receipt)
    original = KeyboardInterrupt("completion interrupted")

    def interrupt(_):
        raise original

    receipts[0].add_done_callback(interrupt)
    with pytest.raises(KeyboardInterrupt) as caught:
        tracker.succeed_many(receipts)
    assert caught.value is original
    assert all(
        receipt.result(0).outcome is SendOutcome.DISPATCHED for receipt in receipts
    )
    assert tracker._pending == {}


def test_batch_failure_stays_unknown_when_cleanup_reenters():
    tracker = _ReceiptTracker()
    first, second = SendReceipt(), SendReceipt()
    tracker.register(first)
    tracker.register(second)
    original = TransportError("physical outcome unknown")
    first.add_done_callback(lambda _: tracker.fail_all(ClosedError("stopped")))
    tracker.fail(first, original)
    assert first.exception(0) is original
    assert isinstance(second.exception(0), ClosedError)
    assert tracker._pending == {}


def test_many_reentrant_stop_callbacks_have_bounded_publication_depth():
    tracker = _ReceiptTracker()
    receipts = tuple(SendReceipt() for _ in range(256))
    depth = maximum = calls = 0

    def stop(_):
        nonlocal depth, maximum, calls
        depth += 1
        maximum = max(maximum, depth)
        calls += 1
        try:
            tracker.fail_all(ClosedError("reentrant stop"))
        finally:
            depth -= 1

    for receipt in receipts:
        tracker.register(receipt)
        receipt.add_done_callback(stop)
    tracker.fail_all(ClosedError("outer stop"))
    assert calls == 256
    assert maximum == 1
    assert all(isinstance(receipt.exception(0), ClosedError) for receipt in receipts)
    assert tracker._pending == {}
    assert tracker._publishers == {}


def test_many_queued_stop_callbacks_do_not_recurse_or_reclose_backend():
    class CountingBackend(BatchBackend):
        closes = 0

        def close(self):
            self.closes += 1
            super().close()

    backend = CountingBackend()
    dispatcher = CANFDMessageDispatcher(interface=backend)
    errors, receipts = [], []

    def stop(_):
        backend.release.set()
        try:
            dispatcher.stop()
        except BaseException as error:
            errors.append(error)

    try:
        first = dispatcher.send(CANFDMessage(1, b"x"))
        assert backend.started.wait(1)
        for i in range(256):
            receipt = dispatcher.send(CANFDMessage(i + 2, b"x"))
            receipt.add_done_callback(stop)
            receipts.append(receipt)
        dispatcher.stop()
        assert errors == []
        assert first.done() and all(receipt.done() for receipt in receipts)
        assert backend.closes == 1
        assert dispatcher._receipts._pending == {}
    finally:
        backend.release.set()
        dispatcher.stop()
        assert not dispatcher._recv_thread.is_alive()
        assert not dispatcher._send_thread.is_alive()
