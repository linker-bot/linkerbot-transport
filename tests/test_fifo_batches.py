"""Atomic admission, timing and physical-prefix guarantees for FIFO batches."""

import threading

import pytest

from linkerbot_transport import (
    ClosedError,
    QueueFullError,
    SendReceipt,
    TimeoutError,
    TransmitArbiter,
    TransmitMode,
    TransportError,
    ValidationError,
)


def make_arbiter(dispatch=None, capacity=7, clock=None):
    sent = []

    def send(value):
        sent.append(value)
        return SendReceipt.completed()

    arbiter = TransmitArbiter(
        dispatch or send,
        mode=TransmitMode.CALLER,
        fifo_capacity=capacity,
        **({} if clock is None else {"monotonic": clock}),
    )
    arbiter.open(generation=1)
    return arbiter, sent


def test_capacity_counts_frames_and_rejects_whole_batch():
    arbiter, sent = make_arbiter()
    try:
        first = arbiter.submit_fifo_batch(range(5), endpoint_id="arm")
        with pytest.raises(QueueFullError):
            arbiter.submit_fifo_batch(range(3), endpoint_id="arm")
        assert arbiter.get_health().pending_fifo == 5
        arbiter.drain()
        assert sent == list(range(5))
        assert first.sent_frames == first.frame_count == 5
        assert first.result().generation == 1
    finally:
        arbiter.close()


def test_expired_unstarted_batch_sends_nothing_and_age_budget_rejects():
    now = [1.0]
    arbiter, sent = make_arbiter(clock=lambda: now[0])
    try:
        receipt = arbiter.submit_fifo_batch([1, 2], endpoint_id="arm", deadline_at_s=2)
        now[0] = 3
        assert arbiter.get_health().oldest_pending_age_s == 2
        with pytest.raises(TimeoutError):
            arbiter.submit_fifo_batch([3, 4], endpoint_id="arm", max_pending_age_s=0.5)
        arbiter.drain()
        with pytest.raises(TimeoutError):
            receipt.result()
        assert sent == [] and receipt.sent_frames == 0
        health = arbiter.get_health()
        assert health.expired == health.rejected_stale == 1
        assert health.oldest_pending_age_s is None
    finally:
        arbiter.close()


def test_deadline_does_not_truncate_started_batch():
    now = [1.0]
    sent = []

    def dispatch(value):
        sent.append(value)
        now[0] = 10
        return SendReceipt.completed()

    arbiter, _ = make_arbiter(dispatch, clock=lambda: now[0])
    try:
        receipt = arbiter.submit_fifo_batch(
            [1, 2, 3], endpoint_id="arm", deadline_at_s=2
        )
        arbiter.drain()
        receipt.result()
        assert sent == [1, 2, 3] and receipt.sent_frames == 3
    finally:
        arbiter.close()


def test_failure_reports_prefix_and_next_batch_remains_ordered():
    sent = []

    def dispatch(value):
        if value == 2:
            raise TransportError("physical failure")
        sent.append(value)
        return SendReceipt.completed()

    arbiter, _ = make_arbiter(dispatch)
    try:
        receipt = arbiter.submit_fifo_batch([1, 2, 3], endpoint_id="arm")
        next_receipt = arbiter.submit_fifo_batch([4, 5], endpoint_id="arm")
        arbiter.drain()
        with pytest.raises(TransportError):
            receipt.result()
        next_receipt.result()
        assert receipt.sent_frames == 1
        assert sent == [1, 4, 5]
    finally:
        arbiter.close()


def test_concurrent_batches_are_never_interleaved():
    arbiter, sent = make_arbiter(capacity=140)
    barrier = threading.Barrier(3)

    def submit(prefix):
        barrier.wait()
        for i in range(10):
            arbiter.submit_fifo_batch(
                [(prefix, i, j) for j in range(7)], endpoint_id="arm"
            )

    threads = [threading.Thread(target=submit, args=(p,)) for p in ("a", "b")]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(2)
    try:
        arbiter.drain()
        assert len(sent) == 140
        for i in range(0, 140, 7):
            assert [v[2] for v in sent[i : i + 7]] == list(range(7))
            assert len({v[:2] for v in sent[i : i + 7]}) == 1
    finally:
        arbiter.close()


def test_cancel_and_reopen_do_not_retain_batch_capacity_or_payloads():
    arbiter, sent = make_arbiter()
    receipt = arbiter.submit_fifo_batch(range(7), endpoint_id="arm")
    arbiter.cancel_endpoint("arm", TransportError("cancel"))
    assert arbiter.get_health().pending_fifo == 0 and receipt.done()
    arbiter.close()
    arbiter.open(generation=2)
    try:
        current = arbiter.submit_fifo_batch([8] * 7, endpoint_id="arm")
        arbiter.drain()
        assert current.result().generation == 2 and sent == [8] * 7
    finally:
        arbiter.close()


def test_close_during_batch_finishes_current_frame_and_cancels_suffix():
    sent, completed = [], []

    def dispatch(value):
        sent.append(value)
        arbiter.close()  # caller owns drain: close must not deadlock
        return SendReceipt.completed()

    arbiter, _ = make_arbiter(dispatch)
    receipt = arbiter.submit_fifo_batch([1, 2, 3], endpoint_id="arm")
    receipt.add_done_callback(
        lambda done: completed.append((done.outcome, done.sent_frames))
    )
    arbiter.drain()
    with pytest.raises(ClosedError):
        receipt.result()
    assert sent == [1] and receipt.sent_frames == 1
    assert len(completed) == 1 and completed[0][1] == 1
    assert arbiter.get_health().failed == 1
    arbiter.open(generation=2)
    assert arbiter.get_health().pending_total == 0
    arbiter.close()


def test_in_flight_age_rejects_before_capacity_is_exhausted():
    now = [1.0]
    entered, release = threading.Event(), threading.Event()

    def dispatch(value):
        entered.set()
        assert release.wait(2)
        return SendReceipt.completed()

    arbiter = TransmitArbiter(dispatch, monotonic=lambda: now[0])
    arbiter.open(generation=1)
    try:
        first = arbiter.submit_fifo_batch([1, 2], endpoint_id="arm")
        assert entered.wait(1)
        now[0] = 2
        health = arbiter.get_health()
        assert health.in_flight_frames == 2 and health.pending_fifo == 0
        assert health.oldest_pending_age_s == 1
        with pytest.raises(TimeoutError):
            arbiter.submit_fifo_batch([3], endpoint_id="arm", max_pending_age_s=0.1)
        release.set()
        first.result(timeout=1)
    finally:
        release.set()
        arbiter.close()


def test_oversized_iterable_is_bounded_and_rejected_whole():
    consumed = []

    def infinite():
        while True:
            consumed.append(1)
            yield 1

    arbiter, _ = make_arbiter(capacity=7)
    try:
        with pytest.raises(QueueFullError):
            arbiter.submit_fifo_batch(infinite(), endpoint_id="arm")
        assert len(consumed) == 8
        assert arbiter.get_health().pending_total == 0
        with pytest.raises(ValidationError):
            arbiter.submit_fifo_batch([], endpoint_id="arm")
    finally:
        arbiter.close()


@pytest.mark.parametrize("age", [0, -1, True, float("nan"), float("inf")])
def test_invalid_admission_age_is_rejected(age):
    arbiter, _ = make_arbiter()
    try:
        with pytest.raises(ValidationError):
            arbiter.submit_fifo_batch([1], endpoint_id="arm", max_pending_age_s=age)
        assert arbiter.get_health().accepted == 0
    finally:
        arbiter.close()
