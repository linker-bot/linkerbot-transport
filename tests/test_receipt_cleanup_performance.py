"""Failure cleanup stays linear when completion callbacks reenter it."""

import threading

import pytest

from linkerbot_transport.errors import ClosedError
from linkerbot_transport.receipts import SendReceipt, _ReceiptTracker


def test_unchanged_reentrant_cleanup_does_not_rescan_receipts(monkeypatch):
    tracker = _ReceiptTracker()
    receipts = [SendReceipt() for _ in range(512)]
    error = ClosedError("outer stop")
    inspected = 0
    resolve = tracker._resolve_locked

    def counted_resolve(receipts, error):
        nonlocal inspected
        inspected += len(receipts)
        resolve(receipts, error)

    monkeypatch.setattr(tracker, "_resolve_locked", counted_resolve)
    for receipt in receipts:
        tracker.register(receipt)
        receipt.add_done_callback(lambda _: tracker.fail_all(error))

    tracker.fail_all(error)

    # Bound work rather than wall time, so slow or shared CI still detects
    # the previous triangular scan without a flaky microsecond threshold.
    assert inspected <= 2 * len(receipts)
    assert all(receipt.exception(0) is error for receipt in receipts)
    assert not tracker.has_pending()


@pytest.mark.parametrize("pre_resolved", [False, True])
def test_reentrant_cleanup_adopts_new_receipts_and_preserves_first_error(pre_resolved):
    tracker = _ReceiptTracker()
    first, second, added = (SendReceipt() for _ in range(3))
    original = ClosedError("outer stop")
    nested = ClosedError("nested stop")
    transferred = ClosedError("cancelled endpoint")
    for receipt in (first, second):
        tracker.register(receipt)

    def add_work(_):
        if pre_resolved:
            tracker.register_failed(added, transferred)
        else:
            tracker.register(added)
        tracker.fail_all(nested)

    first.add_done_callback(add_work)
    tracker.fail_all(original)

    assert first.exception(0) is original
    assert second.exception(0) is original
    assert added.exception(0) is (transferred if pre_resolved else nested)
    assert not tracker.has_pending()


def test_concurrent_cleanup_resolves_new_registration_before_owner_exits():
    tracker = _ReceiptTracker()
    first, added = SendReceipt(), SendReceipt()
    original, concurrent = ClosedError("outer"), ClosedError("concurrent")
    tracker.register(first)
    entered, release = threading.Event(), threading.Event()
    errors = []

    def hold_publication(_):
        entered.set()
        assert release.wait(2)

    def fail():
        try:
            tracker.fail_all(original)
        except BaseException as error:
            errors.append(error)

    first.add_done_callback(hold_publication)
    worker = threading.Thread(target=fail)
    worker.start()
    try:
        assert entered.wait(1)
        tracker.register(added)
        tracker.fail_all(concurrent)
        assert not added.done()
    finally:
        release.set()
        worker.join(2)
        assert not worker.is_alive()

    assert errors == []
    assert first.exception(0) is original
    assert added.exception(0) is concurrent
    assert not tracker.has_pending()


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_interrupted_nested_resolution_can_be_retried(monkeypatch, interrupt_type):
    tracker = _ReceiptTracker()
    first, added = SendReceipt(), SendReceipt()
    original = ClosedError("outer")
    nested = ClosedError("nested")
    interrupt = interrupt_type("resolution interrupted")
    resolve = tracker._resolve_locked
    tracker.register(first)

    def add_work(_):
        tracker.register(added)

        def interrupted(receipts, error):
            monkeypatch.setattr(tracker, "_resolve_locked", resolve)
            raise interrupt

        monkeypatch.setattr(tracker, "_resolve_locked", interrupted)
        tracker.fail_all(nested)

    first.add_done_callback(add_work)
    with pytest.raises(interrupt_type) as caught:
        tracker.fail_all(original)
    assert caught.value is interrupt
    assert first.exception(0) is original
    assert not added.done()

    tracker.fail_all(nested)
    assert added.exception(0) is nested
    assert not tracker.has_pending()
