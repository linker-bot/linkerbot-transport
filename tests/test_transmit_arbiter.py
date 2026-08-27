"""Bounded TX arbitration, replacement and lifecycle contract tests."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pytest

from linkerbot_transport import (
    SendOptions,
    SendOutcome,
    SendPolicy,
    SendReceipt,
    TransmitArbiter,
)
from linkerbot_transport.errors import (
    ClosedError,
    QueueFullError,
    TimeoutError,
    TransportError,
    ValidationError,
)


class FakeClock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class ControlledDispatch:
    def __init__(
        self,
        *,
        blocked_message: str | None = None,
        result_factory: Callable[[str], SendReceipt] | None = None,
    ) -> None:
        self.blocked_message = blocked_message
        self.result_factory = result_factory or (lambda _: SendReceipt.completed())
        self.started = threading.Event()
        self.release = threading.Event()
        self.sent: list[str] = []

    def __call__(self, message: str) -> SendReceipt:
        if message == self.blocked_message:
            self.started.set()
            if not self.release.wait(timeout=2.0):
                return SendReceipt.failed(TimeoutError("test dispatch gate timed out"))
        self.sent.append(message)
        return self.result_factory(message)


@pytest.mark.parametrize(
    "options",
    [
        SendOptions(endpoint_id="hand", policy=SendPolicy.FIFO),
        SendOptions(
            endpoint_id="hand",
            policy=SendPolicy.LATEST,
            command_family="angles",
        ),
    ],
)
def test_send_options_accept_valid_policies(options: SendOptions) -> None:
    assert options.endpoint_id == "hand"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"endpoint_id": [], "policy": SendPolicy.FIFO},
        {"endpoint_id": "hand", "policy": "fifo"},
        {"endpoint_id": "hand", "policy": SendPolicy.LATEST},
        {
            "endpoint_id": "hand",
            "policy": SendPolicy.FIFO,
            "command_family": "angles",
        },
        {"endpoint_id": "hand", "policy": SendPolicy.FIFO, "priority": True},
        {
            "endpoint_id": "hand",
            "policy": SendPolicy.FIFO,
            "deadline_at_s": float("nan"),
        },
    ],
)
def test_send_options_reject_invalid_metadata(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        SendOptions(**kwargs)  # ty: ignore[invalid-argument-type]


def test_constructor_does_not_start_worker() -> None:
    before = _arbiter_threads()

    arbiter = TransmitArbiter[str](lambda _: SendReceipt.completed())

    assert _arbiter_threads() == before
    assert not arbiter.get_health().running


def test_latest_replaces_only_unclaimed_job() -> None:
    dispatch = ControlledDispatch(blocked_message="block")
    arbiter = TransmitArbiter[str](dispatch)
    arbiter.open(generation=7)
    try:
        blocker = arbiter.submit_fifo("block", endpoint_id="gate")
        assert dispatch.started.wait(timeout=1.0)

        old = arbiter.submit_latest("old", endpoint_id="l6", command_family="angles")
        latest = arbiter.submit_latest(
            "latest", endpoint_id="l6", command_family="angles"
        )

        old_result = old.result(timeout=0)
        assert old_result.outcome is SendOutcome.SUPERSEDED
        assert old_result.claimed_at_s is None
        assert old_result.generation == 7
        health = arbiter.get_health()
        assert health.pending_latest == 1
        assert health.superseded == 1

        dispatch.release.set()
        assert blocker.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert latest.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert dispatch.sent == ["block", "latest"]
    finally:
        dispatch.release.set()
        arbiter.close()


def test_new_latest_does_not_supersede_claimed_send() -> None:
    dispatch = ControlledDispatch(blocked_message="first")
    arbiter = TransmitArbiter[str](dispatch)
    arbiter.open(generation=1)
    try:
        first = arbiter.submit_latest(
            "first", endpoint_id="l6", command_family="angles"
        )
        assert dispatch.started.wait(timeout=1.0)
        assert first.claimed_at_s is not None

        second = arbiter.submit_latest(
            "second", endpoint_id="l6", command_family="angles"
        )
        assert not first.done()
        assert arbiter.get_health().pending_latest == 1

        dispatch.release.set()
        assert first.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert second.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert dispatch.sent == ["first", "second"]
    finally:
        dispatch.release.set()
        arbiter.close()


def test_latest_slots_are_independent_per_command_family() -> None:
    dispatch = ControlledDispatch(blocked_message="block")
    arbiter = TransmitArbiter[str](dispatch)
    arbiter.open(generation=1)
    try:
        arbiter.submit_fifo("block", endpoint_id="gate")
        assert dispatch.started.wait(timeout=1.0)
        angles = arbiter.submit_latest(
            "angles", endpoint_id="l6", command_family="angles"
        )
        speeds = arbiter.submit_latest(
            "speeds", endpoint_id="l6", command_family="speeds"
        )

        assert arbiter.get_health().pending_latest == 2
        dispatch.release.set()
        assert angles.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert speeds.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert set(dispatch.sent[1:]) == {"angles", "speeds"}
    finally:
        dispatch.release.set()
        arbiter.close()


def test_latest_stress_keeps_one_pending_and_sends_final_target() -> None:
    dispatch = ControlledDispatch(blocked_message="block")
    arbiter = TransmitArbiter[str](dispatch)
    arbiter.open(generation=1)
    try:
        arbiter.submit_fifo("block", endpoint_id="gate")
        assert dispatch.started.wait(timeout=1.0)
        receipts = [
            arbiter.submit_latest(
                f"target-{index}", endpoint_id="l6", command_family="angles"
            )
            for index in range(1000)
        ]

        health = arbiter.get_health()
        assert health.pending_latest == 1
        assert health.superseded == 999
        assert all(
            receipt.result(timeout=0).outcome is SendOutcome.SUPERSEDED
            for receipt in receipts[:-1]
        )

        dispatch.release.set()
        assert receipts[-1].result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert dispatch.sent[-1] == "target-999"
    finally:
        dispatch.release.set()
        arbiter.close()


def test_fifo_is_bounded_and_preserves_each_endpoint_order() -> None:
    dispatch = ControlledDispatch(blocked_message="block")
    arbiter = TransmitArbiter[str](dispatch, fifo_capacity=4)
    arbiter.open(generation=1)
    try:
        arbiter.submit_latest("block", endpoint_id="gate", command_family="blocker")
        assert dispatch.started.wait(timeout=1.0)
        receipts = [
            arbiter.submit_fifo("a1", endpoint_id="a"),
            arbiter.submit_fifo("a2", endpoint_id="a"),
            arbiter.submit_fifo("b1", endpoint_id="b"),
            arbiter.submit_fifo("b2", endpoint_id="b"),
        ]
        with pytest.raises(QueueFullError):
            arbiter.submit_fifo("overflow", endpoint_id="c")
        assert arbiter.get_health().pending_fifo == 4

        dispatch.release.set()
        for receipt in receipts:
            assert receipt.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert dispatch.sent == ["block", "a1", "b1", "a2", "b2"]
        assert arbiter.get_health().rejected_full == 1
    finally:
        dispatch.release.set()
        arbiter.close()


def test_latest_key_count_is_bounded_but_existing_key_remains_replaceable() -> None:
    dispatch = ControlledDispatch(blocked_message="block")
    arbiter = TransmitArbiter[str](dispatch, latest_capacity=2)
    arbiter.open(generation=1)
    try:
        arbiter.submit_fifo("block", endpoint_id="gate")
        assert dispatch.started.wait(timeout=1.0)
        old = arbiter.submit_latest("a1", endpoint_id="a", command_family="angles")
        replacement = arbiter.submit_latest(
            "a2", endpoint_id="a", command_family="angles"
        )
        second_key = arbiter.submit_latest(
            "b1", endpoint_id="b", command_family="angles"
        )
        with pytest.raises(QueueFullError, match="latest slots are full"):
            arbiter.submit_latest("c1", endpoint_id="c", command_family="angles")

        assert old.result(timeout=0).outcome is SendOutcome.SUPERSEDED
        assert arbiter.get_health().pending_latest == 2
        dispatch.release.set()
        assert replacement.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert second_key.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
    finally:
        dispatch.release.set()
        arbiter.close()


def test_aging_prevents_new_high_priority_work_from_starving_old_work() -> None:
    clock = FakeClock()
    dispatch = ControlledDispatch(blocked_message="block")
    arbiter = TransmitArbiter[str](
        dispatch,
        aging_quantum_s=0.1,
        monotonic=clock,
    )
    arbiter.open(generation=1)
    try:
        arbiter.submit_latest("block", endpoint_id="gate", command_family="blocker")
        assert dispatch.started.wait(timeout=1.0)
        old = arbiter.submit_fifo("old-low", endpoint_id="slow", priority=0)
        clock.advance(1.0)
        new = arbiter.submit_fifo("new-high", endpoint_id="fast", priority=5)

        dispatch.release.set()
        assert old.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert new.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert dispatch.sent == ["block", "old-low", "new-high"]
    finally:
        dispatch.release.set()
        arbiter.close()


def test_expired_pending_send_fails_without_reaching_dispatch() -> None:
    clock = FakeClock()
    dispatch = ControlledDispatch(blocked_message="block")
    arbiter = TransmitArbiter[str](dispatch, monotonic=clock)
    arbiter.open(generation=1)
    try:
        arbiter.submit_latest("block", endpoint_id="gate", command_family="blocker")
        assert dispatch.started.wait(timeout=1.0)
        stale = arbiter.submit_fifo(
            "stale", endpoint_id="l6", deadline_at_s=clock() + 1.0
        )
        clock.advance(2.0)
        dispatch.release.set()

        with pytest.raises(TimeoutError, match="expired before dispatch"):
            stale.result(timeout=1.0)
        assert "stale" not in dispatch.sent
        assert stale.outcome is SendOutcome.FAILED
    finally:
        dispatch.release.set()
        arbiter.close()


def test_close_fails_pending_and_reopen_does_not_replay_it() -> None:
    dispatch = ControlledDispatch(blocked_message="claimed")
    arbiter = TransmitArbiter[str](dispatch)
    arbiter.open(generation=1)
    first = arbiter.submit_latest("claimed", endpoint_id="l6", command_family="angles")
    assert dispatch.started.wait(timeout=1.0)
    old = arbiter.submit_latest(
        "old-pending", endpoint_id="l6", command_family="angles"
    )
    close_errors: list[TimeoutError] = []

    def close() -> None:
        try:
            arbiter.close()
        except TimeoutError as error:
            close_errors.append(error)

    closer = threading.Thread(target=close)
    closer.start()
    try:
        with pytest.raises(ClosedError):
            old.result(timeout=1.0)
        with pytest.raises(ClosedError):
            arbiter.submit_fifo("during-close", endpoint_id="l6")
    finally:
        dispatch.release.set()
    closer.join(timeout=1.0)

    assert not closer.is_alive()
    assert close_errors == []
    assert first.result(timeout=1.0).outcome is SendOutcome.DISPATCHED

    arbiter.open(generation=2)
    try:
        fresh = arbiter.submit_latest(
            "fresh", endpoint_id="l6", command_family="angles"
        )
        result = fresh.result(timeout=1.0)
        assert result.outcome is SendOutcome.DISPATCHED
        assert result.generation == 2
        assert dispatch.sent == ["claimed", "fresh"]
    finally:
        arbiter.close()


def test_cancel_endpoint_fails_only_its_unclaimed_work() -> None:
    dispatch = ControlledDispatch(blocked_message="block")
    arbiter = TransmitArbiter[str](dispatch)
    arbiter.open(generation=1)
    try:
        blocker = arbiter.submit_fifo("block", endpoint_id="gate")
        assert dispatch.started.wait(timeout=1.0)
        l6_latest = arbiter.submit_latest(
            "l6-angle", endpoint_id="l6", command_family="angles"
        )
        l6_fifo = arbiter.submit_fifo("l6-clear", endpoint_id="l6")
        o6_fifo = arbiter.submit_fifo("o6-clear", endpoint_id="o6")
        expected = ClosedError("l6 endpoint closed")

        assert arbiter.cancel_endpoint("l6", expected) == 2

        with pytest.raises(ClosedError, match="l6 endpoint closed"):
            l6_latest.result(timeout=0)
        with pytest.raises(ClosedError, match="l6 endpoint closed"):
            l6_fifo.result(timeout=0)
        health = arbiter.get_health()
        assert health.pending_latest == 0
        assert health.pending_fifo == 1
        assert health.failed == 2

        dispatch.release.set()
        assert blocker.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert o6_fifo.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert dispatch.sent == ["block", "o6-clear"]
    finally:
        dispatch.release.set()
        arbiter.close()


def test_cancel_endpoint_waits_for_its_claimed_send() -> None:
    dispatch = ControlledDispatch(blocked_message="l6-angle")
    arbiter = TransmitArbiter[str](dispatch)
    arbiter.open(generation=1)
    claimed = arbiter.submit_latest(
        "l6-angle", endpoint_id="l6", command_family="angles"
    )
    assert dispatch.started.wait(timeout=1.0)
    other = arbiter.submit_fifo("o6-clear", endpoint_id="o6")
    cancelled: list[int] = []
    cancellation_started = threading.Event()

    def cancel() -> None:
        cancellation_started.set()
        cancelled.append(
            arbiter.cancel_endpoint("l6", ClosedError("closed"), timeout_s=1.0)
        )

    canceller = threading.Thread(target=cancel)
    canceller.start()
    try:
        assert cancellation_started.wait(timeout=1.0)
        assert canceller.is_alive()
        assert not other.done()
    finally:
        dispatch.release.set()
    canceller.join(timeout=1.0)

    try:
        assert not canceller.is_alive()
        assert cancelled == [0]
        assert claimed.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert other.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert dispatch.sent == ["l6-angle", "o6-clear"]
    finally:
        arbiter.close()


def test_cancel_endpoint_times_out_when_claimed_send_does_not_drain() -> None:
    dispatch = ControlledDispatch(blocked_message="l6-angle")
    arbiter = TransmitArbiter[str](dispatch)
    arbiter.open(generation=1)
    claimed = arbiter.submit_fifo("l6-angle", endpoint_id="l6")
    assert dispatch.started.wait(timeout=1.0)

    try:
        with pytest.raises(TimeoutError, match="did not drain"):
            arbiter.cancel_endpoint(
                "l6",
                ClosedError("closed"),
                timeout_s=0.01,
            )
    finally:
        dispatch.release.set()

    try:
        assert claimed.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
    finally:
        arbiter.close()


def test_dispatch_failure_is_classified_and_worker_continues() -> None:
    expected = OSError("write failed")

    def result_for(message: str) -> SendReceipt:
        if message == "bad":
            return SendReceipt.failed(expected)
        return SendReceipt.completed()

    dispatch = ControlledDispatch(result_factory=result_for)
    arbiter = TransmitArbiter[str](dispatch)
    arbiter.open(generation=1)
    try:
        failed = arbiter.submit_fifo("bad", endpoint_id="l6")
        good = arbiter.submit_fifo("good", endpoint_id="l6")

        with pytest.raises(TransportError) as raised:
            failed.result(timeout=1.0)
        assert raised.value.cause is expected
        assert good.result(timeout=1.0).outcome is SendOutcome.DISPATCHED
        assert arbiter.get_health().failed == 1
    finally:
        arbiter.close()


def test_unexpected_worker_failure_completes_claimed_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arbiter = TransmitArbiter[str](lambda _: SendReceipt.completed())

    def crash_after_claim(job: object) -> None:
        _ = job
        raise RuntimeError("scheduler invariant failed")

    monkeypatch.setattr(arbiter, "_dispatch_claimed", crash_after_claim)
    arbiter.open(generation=1)
    receipt = arbiter.submit_fifo("message", endpoint_id="l6")
    try:
        with pytest.raises(TransportError, match="worker failed") as raised:
            receipt.result(timeout=1.0)
        assert isinstance(raised.value.cause, RuntimeError)
        assert receipt.outcome is SendOutcome.FAILED
        assert not arbiter.get_health().running
    finally:
        arbiter.close()


def test_repeated_lifecycle_does_not_leak_worker_threads() -> None:
    before = _arbiter_threads()
    arbiter = TransmitArbiter[str](lambda _: SendReceipt.completed())

    for generation in range(1, 101):
        arbiter.open(generation=generation)
        arbiter.close()

    assert _wait_until(lambda: _arbiter_threads() == before)


def _arbiter_threads() -> set[int | None]:
    return {
        thread.ident
        for thread in threading.enumerate()
        if thread.name == "LinkerbotTransmitArbiter.tx"
    }


def _wait_until(predicate: Callable[[], bool], timeout_s: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.001)
    return predicate()
