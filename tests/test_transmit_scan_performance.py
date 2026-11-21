"""Candidate selection preserves the reference order without endpoint rescans."""

import random

import pytest

from linkerbot_transport.errors import ClosedError, QueueFullError, TimeoutError
from linkerbot_transport.receipts import SendOutcome, SendReceipt
from linkerbot_transport.transmit import TransmitArbiter, TransmitMode


class ReferenceArbiter(TransmitArbiter):
    """Original minimum selection and endpoint eviction as a differential oracle."""

    def _claim_next_locked(self):
        now = self._monotonic()
        candidates = list(self._latest.values())
        candidates.extend(queue[0] for queue in self._fifo.values() if queue)
        expected = min(candidates, key=lambda job: self._rank(job, now))
        claimed = super()._claim_next_locked()
        assert claimed is expected
        return claimed

    def _forget_idle_endpoint_locked(self, endpoint_id):
        if endpoint_id in self._fifo:
            return
        if any(key[0] == endpoint_id for key in self._latest):
            return
        if (
            self._in_flight is not None
            and self._in_flight.options.endpoint_id == endpoint_id
        ):
            return
        self._last_served.pop(endpoint_id, None)


def exercise(arbiter_type, seed):
    rng = random.Random(seed)
    clock = [10.0]
    sent, receipts, snapshots = [], [], []

    def dispatch(message):
        sent.append(message)
        clock[0] += 0.003
        return SendReceipt.completed(monotonic=lambda: clock[0])

    arbiter = arbiter_type(
        dispatch, mode=TransmitMode.CALLER, monotonic=lambda: clock[0]
    )
    arbiter.open(generation=1)
    try:
        for index in range(500):
            operation = rng.randrange(8)
            endpoint = rng.randrange(8)
            family = rng.randrange(4)
            priority = rng.randrange(-2, 4)
            deadline = clock[0] + 0.01 if index % 3 == 0 else None
            try:
                receipt = None
                if operation < 2:
                    receipt = arbiter.submit_latest(
                        index,
                        endpoint_id=endpoint,
                        command_family=family,
                        priority=priority,
                        deadline_at_s=deadline,
                    )
                elif operation == 2:
                    receipt = arbiter.replace_latest_if_pending(
                        index,
                        endpoint_id=endpoint,
                        command_family=family,
                        priority=priority,
                        deadline_at_s=deadline,
                    )
                elif operation == 3:
                    receipt = arbiter.submit_fifo(
                        index, endpoint_id=endpoint, priority=priority
                    )
                elif operation == 4:
                    receipt = arbiter.submit_fifo_batch(
                        [index, -index], endpoint_id=endpoint, priority=priority
                    )
                elif operation == 5:
                    arbiter.cancel_endpoint(endpoint, ClosedError("removed"))
                elif operation == 6:
                    arbiter.drain()
                else:
                    # Exercise both sides of the 50 ms aging quantum.
                    clock[0] += rng.choice([0.001, 0.049, 0.05, 0.101])
                if receipt is not None:
                    receipts.append(receipt)
            except (QueueFullError, TimeoutError):
                snapshots.append("rejected")
            snapshots.append(arbiter.get_health())
        arbiter.drain()
    finally:
        arbiter.close()
    outcomes = [
        type(receipt.exception(0)).__name__
        if receipt.exception(0) is not None
        else (receipt.result(0).outcome, getattr(receipt, "sent_frames", 1))
        for receipt in receipts
    ]
    return sent, outcomes, snapshots, arbiter.get_health()


@pytest.mark.parametrize("seed", [0, 17, 51, 2026])
def test_mixed_scheduling_matches_original_selection_and_endpoint_lifetime(seed):
    assert exercise(TransmitArbiter, seed) == exercise(ReferenceArbiter, seed)


def test_finished_endpoints_are_not_compared_against_all_remaining_latest():
    comparisons = 0

    class Endpoint:
        __hash__ = object.__hash__

        def __eq__(self, other):
            nonlocal comparisons
            comparisons += 1
            return self is other

    arbiter = TransmitArbiter(
        lambda _: SendReceipt.completed(),
        latest_capacity=128,
        mode=TransmitMode.CALLER,
    )
    arbiter.open(generation=1)
    receipts = []
    try:
        for index in range(128):
            receipts.append(
                arbiter.submit_latest(
                    index, endpoint_id=Endpoint(), command_family="target"
                )
            )
        arbiter.drain()
        assert all(
            receipt.done() and receipt.exception(0) is None for receipt in receipts
        )
        # Deterministic complexity bound, independent of machine speed.
        assert comparisons <= 4 * len(receipts)
        assert arbiter._last_served == {}
    finally:
        arbiter.close()


def test_unmatched_nan_endpoint_cancellation_preserves_queued_sends():
    sent = []

    def dispatch(message):
        sent.append(message)
        return SendReceipt.completed()

    arbiter = TransmitArbiter(dispatch, mode=TransmitMode.CALLER)
    arbiter.open(generation=1)
    try:
        endpoint = float("nan")
        receipts = [
            arbiter.submit_latest(
                "position", endpoint_id=endpoint, command_family="position"
            ),
            arbiter.submit_latest(
                "speed", endpoint_id=endpoint, command_family="speed"
            ),
            arbiter.submit_fifo("other", endpoint_id="other"),
        ]

        # Preserve the existing equality-based cancellation semantics: NaN
        # matches no latest key, so no queued membership may be discarded.
        assert arbiter.cancel_endpoint(endpoint, ClosedError("removed")) == 0
        assert arbiter.get_health().pending_total == 3
        assert all(not receipt.done() for receipt in receipts)

        arbiter.drain()

        assert sorted(sent) == ["other", "position", "speed"]
        assert all(
            receipt.result(0).outcome is SendOutcome.DISPATCHED for receipt in receipts
        )
        health = arbiter.get_health()
        assert health.running
        assert health.pending_total == 0
        assert health.dispatched == 3
        assert health.failed == 0
    finally:
        arbiter.close()


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_interrupted_endpoint_removal_rebuilds_membership_before_continuing(
    interrupt_type,
):
    sent = []

    def dispatch(message):
        sent.append(message)
        return SendReceipt.completed()

    arbiter = TransmitArbiter(dispatch, mode=TransmitMode.CALLER)
    arbiter.open(generation=1)
    first = arbiter.submit_latest("a1", endpoint_id="a", command_family="position")
    second = arbiter.submit_latest("a2", endpoint_id="a", command_family="speed")
    other = arbiter.submit_latest("b", endpoint_id="b", command_family="position")
    original = ClosedError("removed first")

    class InterruptedRemoval(dict):
        interrupted = False

        def __delitem__(self, key):
            super().__delitem__(key)
            if not self.interrupted:
                self.interrupted = True
                raise interrupt_type("after authoritative queue removal")

    arbiter._latest = InterruptedRemoval(arbiter._latest)
    try:
        with pytest.raises(interrupt_type):
            arbiter.cancel_endpoint("a", original)
        arbiter.cancel_endpoint("a", ClosedError("retry"))
        assert first.exception(0) is original
        assert second.exception(0) is original
        assert not other.done()
        arbiter.drain()
        assert sent == ["b"]
        assert other.exception(0) is None
        assert arbiter._last_served == {}
    finally:
        arbiter.close()
