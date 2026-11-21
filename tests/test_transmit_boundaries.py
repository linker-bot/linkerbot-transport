"""Interrupt, callback and endpoint ownership boundaries of TX arbitration."""

import gc
import subprocess
import sys
import textwrap
import threading
import weakref

import pytest

from linkerbot_transport import SendOutcome, SendReceipt, TransmitArbiter, TransmitMode
from linkerbot_transport.errors import ClosedError, TransportError


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("stage", ["dispatch", "downstream", "scheduler"])
def test_caller_interrupt_stops_remaining_work_and_propagates(
    monkeypatch, error_type, stage
):
    interrupted = error_type("stop requested")
    sent = []

    def dispatch(message):
        sent.append(message)
        if stage == "dispatch":
            raise interrupted
        return SendReceipt.failed(interrupted)

    arbiter = TransmitArbiter(dispatch, mode=TransmitMode.CALLER)
    arbiter.open(generation=1)
    first = arbiter.submit_fifo("first", endpoint_id="device")
    later = arbiter.submit_fifo("later", endpoint_id="device")
    original_dispatch = arbiter._dispatch_claimed
    if stage == "scheduler":

        def crash(_):
            raise interrupted

        monkeypatch.setattr(arbiter, "_dispatch_claimed", crash)
    try:
        with pytest.raises(error_type) as raised:
            arbiter.drain()
        assert raised.value is interrupted
        assert sent == ([] if stage == "scheduler" else ["first"])
        for receipt in (first, later):
            assert receipt.done()
            assert isinstance(receipt.exception(), TransportError)
            assert receipt.exception().cause is interrupted
        health = arbiter.get_health()
        assert not health.running and not health.in_flight
        assert health.pending_total == 0 and health.failed == 2
        with pytest.raises(ClosedError):
            arbiter.submit_fifo("rejected", endpoint_id="device")
        monkeypatch.setattr(arbiter, "_dispatch_claimed", original_dispatch)
    finally:
        arbiter.close()


@pytest.mark.parametrize(
    "stage",
    [
        "dispatch",
        "publication-success",
        "publication-failure",
        "claim-fifo",
        "claim-latest",
        "claim-batch",
        "claim-before-fifo-removal",
    ],
)
def test_caller_real_sigint_does_not_dispatch_later_commands(stage):
    script = textwrap.dedent("""
        import inspect
        import signal
        import sys
        from linkerbot_transport import SendReceipt, TransmitArbiter, TransmitMode
        from linkerbot_transport.errors import TransportError
        stage = STAGE
        calls = []
        def dispatch(message):
            calls.append(message)
            if stage == 'dispatch':
                signal.raise_signal(signal.SIGINT)
            if stage == 'publication-failure':
                return SendReceipt.failed(TransportError('physical send failed'))
            return SendReceipt.completed()
        arbiter = TransmitArbiter(dispatch, mode=TransmitMode.CALLER)
        arbiter.open(generation=1)
        if stage == 'claim-latest':
            first = arbiter.submit_latest('first', endpoint_id='device', command_family='target')
        elif stage == 'claim-batch':
            first = arbiter.submit_fifo_batch(['first', 'suffix'], endpoint_id='device')
        else:
            first = arbiter.submit_fifo('first', endpoint_id='device')
        later = arbiter.submit_fifo('later', endpoint_id='device')
        if stage != 'dispatch':
            if stage.startswith('claim-'):
                method = arbiter._claim_next_locked
                if stage == 'claim-before-fifo-removal':
                    boundary = 'claimed = queue.popleft()'
                elif stage == 'claim-latest':
                    boundary = 'if not job.receipt._claim()'
                else:
                    boundary = 'self._fifo_size -= job.frame_count'
            else:
                method = arbiter._dispatch_claimed
                boundary = ('job.receipt._fail(failure)' if stage == 'publication-failure'
                            else 'job.receipt._dispatch()')
            lines, start = inspect.getsourcelines(method)
            target = start + next(i for i, line in enumerate(lines) if boundary in line)
            fired = False
            def trace(frame, event, arg):
                global fired
                if (not fired and event == 'line' and frame.f_code is method.__code__
                        and frame.f_lineno == target):
                    fired = True
                    signal.raise_signal(signal.SIGINT)
                return trace
            sys.settrace(trace)
        try:
            try:
                arbiter.drain()
            except KeyboardInterrupt:
                pass
            else:
                raise AssertionError('SIGINT was swallowed')
            assert calls == ([] if stage.startswith('claim-') else ['first'])
            assert first.done() and later.done()
            health = arbiter.get_health()
            assert not health.running
            assert health.accepted == health.dispatched + health.failed == 2
        finally:
            sys.settrace(None)
            arbiter.close()
    """)
    script = script.replace("STAGE", repr(stage))
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=5
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("mode", list(TransmitMode))
@pytest.mark.parametrize("failed", [False, True])
def test_interrupt_during_receipt_publication_never_loses_claimed_receipt(
    monkeypatch, mode, failed
):
    # Install the gate before the worker can claim its first job.
    original = SendReceipt._dispatch
    original_fail = SendReceipt._fail
    armed = threading.Event()
    release = threading.Event()
    interrupted = KeyboardInterrupt("interrupt before receipt publication")

    def interrupt_publication(receipt):
        if armed.is_set():
            armed.clear()
            raise interrupted
        return original(receipt)

    def interrupt_failure_publication(receipt, error):
        if armed.is_set():
            armed.clear()
            raise interrupted
        return original_fail(receipt, error)

    def dispatch(_):
        assert release.wait(2)
        downstream = (
            SendReceipt.failed(TransportError("physical send failed"))
            if failed
            else SendReceipt.completed()
        )
        armed.set()
        return downstream

    arbiter = TransmitArbiter(dispatch, mode=mode)
    monkeypatch.setattr(SendReceipt, "_dispatch", interrupt_publication)
    monkeypatch.setattr(SendReceipt, "_fail", interrupt_failure_publication)
    arbiter.open(generation=1)
    first = arbiter.submit_fifo("first", endpoint_id="device")
    later = arbiter.submit_fifo("later", endpoint_id="device")
    try:
        release.set()
        if mode is TransmitMode.CALLER:
            with pytest.raises(KeyboardInterrupt) as raised:
                arbiter.drain()
            assert raised.value is interrupted
        for receipt in (first, later):
            with pytest.raises(TransportError):
                receipt.result(timeout=1)
            assert receipt.exception().cause is interrupted
        assert not arbiter.get_health().running
        assert not arbiter.get_health().in_flight
        health = arbiter.get_health()
        assert health.accepted == 2
        assert health.dispatched == (0 if failed else 1)
        assert health.failed == (2 if failed else 1)
        assert health.dispatched + health.failed == health.accepted
        arbiter.close()
        assert first.done() and later.done()
    finally:
        release.set()
        arbiter.close()


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit])
def test_interrupted_batch_preserves_sent_prefix_and_never_replays(error_type):
    sent = []
    interrupted = error_type("stop batch")

    def dispatch(message):
        if message == "interrupt":
            raise interrupted
        sent.append(message)
        return SendReceipt.completed()

    arbiter = TransmitArbiter(dispatch, mode=TransmitMode.CALLER)
    arbiter.open(generation=1)
    batch = arbiter.submit_fifo_batch(
        ["sent", "interrupt", "suffix"], endpoint_id="device"
    )
    later = arbiter.submit_fifo("later", endpoint_id="other")
    try:
        with pytest.raises(error_type):
            arbiter.drain()
        assert batch.sent_frames == 1
        assert batch.exception().cause is interrupted
        assert later.done()
        arbiter.open(generation=2)
        fresh = arbiter.submit_fifo("fresh", endpoint_id="device")
        arbiter.drain()
        assert fresh.result(timeout=0).generation == 2
        assert sent == ["sent", "fresh"]
    finally:
        arbiter.close()


@pytest.mark.parametrize("mode", list(TransmitMode))
@pytest.mark.parametrize("failed", [False, True])
def test_completion_callback_can_cancel_endpoint_without_waiting_for_itself(
    mode, failed
):
    entered, release, callback_done = (threading.Event() for _ in range(3))
    errors = []
    cancelled = []

    def dispatch(message):
        if message == "first":
            entered.set()
            assert release.wait(2)
            if failed:
                return SendReceipt.failed(TransportError("physical send failed"))
        return SendReceipt.completed()

    arbiter = TransmitArbiter(dispatch, mode=mode)
    arbiter.open(generation=1)
    first = arbiter.submit_fifo("first", endpoint_id="device")
    pending = arbiter.submit_fifo("pending", endpoint_id="device")
    other = arbiter.submit_fifo("other", endpoint_id="other")

    def completed(_):
        try:
            health = arbiter.get_health()
            assert not health.in_flight
            assert (health.failed if failed else health.dispatched) == 1
            cancelled.append(
                arbiter.cancel_endpoint(
                    "device", ClosedError("endpoint closed"), timeout_s=0.01
                )
            )
        except BaseException as error:
            errors.append(error)
        finally:
            callback_done.set()

    first.add_done_callback(completed)
    try:
        release.set()
        if mode is TransmitMode.CALLER:
            arbiter.drain()
        assert callback_done.wait(1)
        assert not errors
        assert cancelled == [1]
        with pytest.raises(ClosedError):
            pending.result(timeout=0)
        assert other.result(timeout=1).outcome is SendOutcome.DISPATCHED
    finally:
        release.set()
        arbiter.close()


@pytest.mark.parametrize("policy", ["fifo", "latest", "batch"])
def test_completed_endpoints_are_released_during_long_lived_open(policy):
    class Endpoint:
        pass

    arbiter = TransmitArbiter(
        lambda _: SendReceipt.completed(),
        mode=TransmitMode.CALLER,
        latest_capacity=1,
        fifo_capacity=1,
    )
    arbiter.open(generation=1)
    references = []
    try:
        for _ in range(1024):
            endpoint = Endpoint()
            references.append(weakref.ref(endpoint))
            if policy == "latest":
                arbiter.submit_latest(
                    "data", endpoint_id=endpoint, command_family="target"
                )
            elif policy == "batch":
                arbiter.submit_fifo_batch(["data"], endpoint_id=endpoint)
            else:
                arbiter.submit_fifo("data", endpoint_id=endpoint)
            arbiter.drain()
            arbiter.cancel_endpoint(endpoint, ClosedError("endpoint removed"))
            del endpoint
        gc.collect()
        assert all(reference() is None for reference in references)
        assert arbiter.get_health().pending_total == 0
        assert arbiter.get_health().dispatched == 1024
    finally:
        arbiter.close()


@pytest.mark.parametrize("mode", [TransmitMode.CALLER, TransmitMode.WORKER])
@pytest.mark.parametrize("original_type", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("callback_type", [KeyboardInterrupt, SystemExit])
def test_cleanup_callback_interrupt_cannot_orphan_suffix_or_replace_original(
    mode, original_type, callback_type
):
    entered, release = threading.Event(), threading.Event()
    original = original_type("dispatch interrupted")
    callback_error = callback_type("completion interrupted")

    def dispatch(_):
        entered.set()
        assert release.wait(2)
        raise original

    arbiter = TransmitArbiter(dispatch, mode=mode)
    arbiter.open(generation=1)
    receipts = [arbiter.submit_fifo(i, endpoint_id="device") for i in range(3)]

    def interrupted(_):
        raise callback_error

    receipts[1].add_done_callback(interrupted)
    try:
        release.set()
        if mode is TransmitMode.CALLER:
            with pytest.raises(original_type) as caught:
                arbiter.drain()
            assert caught.value is original
        else:
            assert entered.wait(1)
            arbiter._worker.join(2)
            assert not arbiter._worker.is_alive()
        assert all(receipt.done() for receipt in receipts)
        for receipt in receipts:
            error = receipt.exception(0)
            assert isinstance(error, TransportError) and error.cause is original
        health = arbiter.get_health()
        assert health.failed == 3 and health.pending_total == 0 and not health.in_flight
    finally:
        release.set()
        arbiter.close()


@pytest.mark.parametrize("operation", ["close", "cancel"])
@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_stop_and_cancel_finish_detached_receipts_before_callback_interrupt(
    operation, interrupt_type
):
    arbiter = TransmitArbiter(
        lambda _: SendReceipt.completed(), mode=TransmitMode.CALLER
    )
    arbiter.open(generation=1)
    receipts = [arbiter.submit_fifo(i, endpoint_id="device") for i in range(3)]
    interrupt = interrupt_type("completion interrupted")

    def interrupted(_):
        raise interrupt

    receipts[0].add_done_callback(interrupted)
    try:
        with pytest.raises(interrupt_type) as caught:
            if operation == "close":
                arbiter.close()
            else:
                arbiter.cancel_endpoint("device", ClosedError("device removed"))
        assert caught.value is interrupt
        assert all(receipt.done() for receipt in receipts)
        assert all(
            isinstance(receipt.exception(0), ClosedError) for receipt in receipts
        )
        assert arbiter.get_health().failed == 3
    finally:
        arbiter.close()


@pytest.mark.parametrize("operation", ["close", "cancel", "drain"])
def test_first_cleanup_sigint_keeps_unpublished_receipt_for_retry(operation):
    script = textwrap.dedent("""
        import inspect
        import signal
        import sys
        from linkerbot_transport import SendReceipt, TransmitArbiter, TransmitMode
        from linkerbot_transport.errors import ClosedError, DeviceNotReadyError
        operation = OPERATION
        original = KeyboardInterrupt('physical dispatch interrupted')
        def dispatch(_):
            raise original
        arbiter = TransmitArbiter(dispatch, mode=TransmitMode.CALLER)
        arbiter.open(generation=1)
        receipts = [arbiter.submit_fifo(i, endpoint_id='device') for i in range(3)]
        # Caller cleanup publishes queued receipts before its orphaned claim.
        target_receipt = receipts[1] if operation == 'drain' else receipts[0]
        lines, start = inspect.getsourcelines(SendReceipt._fail)
        target = start + next(i for i, line in enumerate(lines)
                              if 'self._outcome = SendOutcome.FAILED' in line)
        fired = False
        def trace(frame, event, arg):
            global fired
            if (not fired and event == 'line'
                    and frame.f_code is SendReceipt._fail.__code__
                    and frame.f_locals.get('self') is target_receipt
                    and frame.f_lineno == target):
                fired = True
                signal.raise_signal(signal.SIGINT)
            return trace
        sys.settrace(trace)
        try:
            try:
                if operation == 'close':
                    arbiter.close()
                elif operation == 'cancel':
                    arbiter.cancel_endpoint('device', ClosedError('endpoint removed'))
                else:
                    arbiter.drain()
            except KeyboardInterrupt as error:
                if operation == 'drain':
                    assert error is original
            else:
                raise AssertionError('SIGINT was swallowed')
            assert fired and not target_receipt.done()
            assert all(r.done() for r in receipts if r is not target_receipt)
            assert arbiter.get_health().failed == 3
            if operation != 'cancel':
                try:
                    arbiter.open(generation=2)
                except DeviceNotReadyError:
                    pass
                else:
                    raise AssertionError('opened over interrupted cleanup')
            arbiter.close()
            assert all(r.done() for r in receipts)
            assert arbiter.get_health().failed == 3
            if operation == 'drain':
                assert all(r.exception(0).cause is original for r in receipts)
            else:
                expected = 'endpoint removed' if operation == 'cancel' else 'closed before dispatch'
                assert expected in str(target_receipt.exception(0))
            arbiter.open(generation=2)
        finally:
            sys.settrace(None)
            arbiter.close()
    """).replace("OPERATION", repr(operation))
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=5
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_nested_endpoint_cancellation_drains_new_cleanup_receipts():
    arbiter = TransmitArbiter(
        lambda _: SendReceipt.completed(), mode=TransmitMode.CALLER
    )
    arbiter.open(generation=1)
    first = arbiter.submit_fifo("first", endpoint_id="a")
    second = arbiter.submit_fifo("second", endpoint_id="b")
    other = arbiter.submit_fifo("other", endpoint_id="c")
    first_error, second_error = ClosedError("a removed"), ClosedError("b removed")
    cancelled = []
    first.add_done_callback(
        lambda _: cancelled.append(arbiter.cancel_endpoint("b", second_error))
    )
    try:
        assert arbiter.cancel_endpoint("a", first_error) == 1
        assert cancelled == [1]
        assert first.exception(0) is first_error
        assert second.exception(0) is second_error
        arbiter.drain()
        assert other.result(0).outcome is SendOutcome.DISPATCHED
        assert arbiter.get_health().failed == 2
        assert not arbiter._cleanup_receipts.has_pending()
    finally:
        arbiter.close()


def test_concurrent_endpoint_cancellations_keep_their_original_causes(monkeypatch):
    arbiter = TransmitArbiter(
        lambda _: SendReceipt.completed(), mode=TransmitMode.CALLER
    )
    arbiter.open(generation=1)
    first = arbiter.submit_fifo("first", endpoint_id="a")
    second = arbiter.submit_fifo("second", endpoint_id="b")
    first_error, second_error = ClosedError("a removed"), ClosedError("b removed")
    entered, release = threading.Event(), threading.Event()
    publish = arbiter._cleanup_receipts.fail_all
    errors, counts = [], []

    def gated_publish(error):
        if error is first_error:
            entered.set()
            assert release.wait(2)
        return publish(error)

    monkeypatch.setattr(arbiter._cleanup_receipts, "fail_all", gated_publish)

    def cancel_first():
        try:
            counts.append(arbiter.cancel_endpoint("a", first_error))
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=cancel_first)
    thread.start()
    try:
        assert entered.wait(1)
        assert arbiter.cancel_endpoint("b", second_error) == 1
        assert first.exception(0) is first_error
        assert second.exception(0) is second_error
    finally:
        release.set()
        thread.join(2)
        assert not thread.is_alive()
        arbiter.close()
    assert errors == [] and counts == [1]
    assert arbiter.get_health().failed == 2
