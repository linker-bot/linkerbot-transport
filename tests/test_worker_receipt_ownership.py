"""Controlled worker startup, false join status and receipt handoff windows."""

import queue
import threading

import can
import pytest

from linkerbot_transport import SendOutcome, SendReceipt, TransmitArbiter, TransmitMode
from linkerbot_transport.can import can as classic
from linkerbot_transport.canfd import CANFDMessage
from linkerbot_transport.canfd import dispatcher as flexible
from linkerbot_transport.errors import (
    ClosedError,
    DeviceNotReadyError,
    TimeoutError,
    TransportError,
)


class Backend:
    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.incoming = queue.Queue()
        self.calls = []
        self.fail = False

    def send(self, message, **kwargs):
        self._send([message])

    def send_batch(self, messages):
        self._send(messages)

    def _send(self, messages):
        self.calls.append(messages)
        self.entered.set()
        assert self.release.wait(2)
        if self.fail:
            raise TransportError("controlled send failure")

    def recv(self, timeout):
        try:
            return self.incoming.get(timeout=min(timeout, 0.01))
        except queue.Empty:
            return None

    def receive(self, *, max_frames, timeout_ms):
        frame = self.recv(timeout=timeout_ms / 1000)
        return [] if frame is None else [frame]

    def close(self):
        pass

    shutdown = close


def create_dispatcher(monkeypatch, kind, backend):
    if kind == "classic":
        monkeypatch.setattr(classic, "open_bus", lambda **kw: backend)
        monkeypatch.setattr(classic, "THREAD_JOIN_TIMEOUT_S", 0.01)
        return classic.CANMessageDispatcher(
            "worker-ownership", "virtual", start_send_worker=False
        )
    monkeypatch.setattr(flexible, "THREAD_JOIN_TIMEOUT_S", 0.01)
    return flexible.CANFDMessageDispatcher(interface=backend, start_send_worker=False)


@pytest.mark.parametrize("before_target", [False, True])
def test_interrupted_start_keeps_worker_until_actual_exit(monkeypatch, before_target):
    gate = threading.Event()
    entered = threading.Event()
    real_start = threading.Thread.start
    arbiter = TransmitArbiter(
        lambda frame: SendReceipt.completed(), join_timeout_s=0.01
    )
    old_run = arbiter._run

    def gated_run():
        entered.set()
        assert gate.wait(2)
        old_run()

    monkeypatch.setattr(arbiter, "_run", gated_run)
    created = []

    def interrupted(thread):
        if thread.name == "LinkerbotTransmitArbiter.tx":
            created.append(thread)
            if not before_target:
                real_start(thread)
                assert entered.wait(1)
            raise KeyboardInterrupt("start interrupted")
        real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", interrupted)
    try:
        with pytest.raises(KeyboardInterrupt):
            arbiter.open(generation=1)
        assert arbiter._worker is created[0]
        with pytest.raises(DeviceNotReadyError, match="still closing"):
            arbiter.open(generation=2)
        with pytest.raises(TimeoutError):
            arbiter.close()
        if before_target:
            # Model an OS thread whose bootstrap becomes visible only later.
            real_start(created[0])
        gate.set()
        created[0].join(1)
        arbiter.close()
        monkeypatch.setattr(threading.Thread, "start", real_start)
        arbiter.open(generation=2)
    finally:
        gate.set()
        if created and created[0].ident is None:
            real_start(created[0])
        if created:
            created[0].join(1)
        arbiter.close()


def test_arbiter_does_not_trust_false_thread_death(monkeypatch):
    entered, release = threading.Event(), threading.Event()

    def dispatch(frame):
        entered.set()
        assert release.wait(2)
        return SendReceipt.completed()

    arbiter = TransmitArbiter(dispatch, join_timeout_s=0.01)
    arbiter.open(generation=1)
    receipt = arbiter.submit_fifo(1, endpoint_id="one")
    worker = arbiter._worker
    try:
        assert entered.wait(1)
        # Deterministic equivalent of the stale Thread bookkeeping left by
        # an interrupted CPython 3.10 join, without modifying private state.
        monkeypatch.setattr(worker, "is_alive", lambda: False)
        monkeypatch.setattr(worker, "join", lambda timeout: None)
        with pytest.raises(TimeoutError):
            arbiter.close()
        with pytest.raises(DeviceNotReadyError):
            arbiter.open(generation=2)
    finally:
        release.set()
        threading.Thread.join(worker, 1)
        arbiter.close()
    assert receipt.result(timeout=0).outcome is SendOutcome.DISPATCHED


@pytest.mark.parametrize("kind", ["classic", "fd"])
def test_dispatcher_waits_for_actual_callback_exit(monkeypatch, kind):
    backend = Backend()
    dispatcher = create_dispatcher(monkeypatch, kind, backend)
    entered, release = threading.Event(), threading.Event()

    def callback(frame):
        entered.set()
        assert release.wait(2)

    dispatcher.subscribe(callback)
    backend.incoming.put(
        can.Message(arbitration_id=1) if kind == "classic" else CANFDMessage(1, b"")
    )
    worker = dispatcher._recv_thread
    try:
        assert entered.wait(1)
        monkeypatch.setattr(worker, "is_alive", lambda: False)
        monkeypatch.setattr(worker, "join", lambda timeout: None)
        with pytest.raises(RuntimeError, match="did not stop"):
            dispatcher.stop()
    finally:
        release.set()
        threading.Thread.join(worker, 1)
        dispatcher.stop()


@pytest.mark.parametrize("replace", [False, True])
def test_superseded_receipt_survives_interrupted_publication(monkeypatch, replace):
    arbiter = TransmitArbiter(
        lambda frame: SendReceipt.completed(), mode=TransmitMode.CALLER
    )
    arbiter.open(generation=1)
    old = arbiter.submit_latest(1, endpoint_id="one", command_family="target")
    supersede = old._supersede
    calls = []

    def once():
        if not calls:
            calls.append(True)
            raise KeyboardInterrupt("publication interrupted")
        return supersede()

    monkeypatch.setattr(old, "_supersede", once)
    try:
        with pytest.raises(KeyboardInterrupt):
            (arbiter.replace_latest_if_pending if replace else arbiter.submit_latest)(
                2, endpoint_id="one", command_family="target"
            )
        assert not old.done()
        arbiter.close()
        assert old.result(timeout=0).outcome is SendOutcome.SUPERSEDED
    finally:
        monkeypatch.setattr(old, "_supersede", supersede)
        arbiter.close()
        if not old.done():
            old._fail(ClosedError("test cleanup"))


@pytest.mark.parametrize("kind", ["classic", "fd", "fd-batch"])
@pytest.mark.parametrize("fail", [False, True])
def test_queued_receipts_are_claimed_before_physical_io(monkeypatch, kind, fail):
    backend = Backend()
    backend.fail = fail
    dispatcher = create_dispatcher(
        monkeypatch, "classic" if kind == "classic" else "fd", backend
    )
    message = (
        can.Message(arbitration_id=1) if kind == "classic" else CANFDMessage(1, b"")
    )
    receipts = []
    # Gate startup so the FD batch is deterministically present as a group.
    real_ensure = dispatcher._ensure_send_worker
    monkeypatch.setattr(dispatcher, "_ensure_send_worker", lambda: None)
    try:
        receipts = [
            dispatcher.send(message) for _ in range(3 if kind == "fd-batch" else 1)
        ]
        real_ensure()
        assert backend.entered.wait(1)
        for receipt in receipts:
            assert receipt.claimed_at_s is not None
            assert receipt.claimed_order is not None
            assert not receipt.done()
        before_return = __import__("time").monotonic()
        backend.release.set()
        for receipt in receipts:
            if fail:
                with pytest.raises(TransportError):
                    receipt.result(timeout=1)
                assert receipt.claimed_at_s is not None
            else:
                result = receipt.result(timeout=1)
                assert result.claimed_at_s <= before_return <= result.completed_at_s
        assert len(backend.calls[0]) == len(receipts)
    finally:
        backend.release.set()
        dispatcher.stop()


@pytest.mark.parametrize("replace", [False, True])
def test_interruption_before_slot_update_never_dispatches_superseded_job(
    monkeypatch, replace
):
    sent = []
    arbiter = TransmitArbiter(
        lambda frame: sent.append(frame) or SendReceipt.completed(),
        mode=TransmitMode.CALLER,
    )
    arbiter.open(generation=1)
    old = arbiter.submit_latest(1, endpoint_id="one", command_family="target")
    register = arbiter._cleanup_receipts.register_superseded

    def interrupted(receipt):
        register(receipt)
        raise KeyboardInterrupt("slot update interrupted")

    monkeypatch.setattr(arbiter._cleanup_receipts, "register_superseded", interrupted)
    try:
        with pytest.raises(KeyboardInterrupt):
            (arbiter.replace_latest_if_pending if replace else arbiter.submit_latest)(
                2, endpoint_id="one", command_family="target"
            )
        monkeypatch.setattr(arbiter._cleanup_receipts, "register_superseded", register)
        arbiter.drain()
        assert sent == []
        assert old.result(timeout=0).outcome is SendOutcome.SUPERSEDED
        assert old.claimed_at_s is None
    finally:
        monkeypatch.setattr(arbiter._cleanup_receipts, "register_superseded", register)
        arbiter.close()


def test_sigint_during_close_does_not_discard_a_running_worker():
    import os
    import signal

    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def dispatch(frame):
        entered.set()
        try:
            assert release.wait(2)
            return SendReceipt.completed()
        finally:
            finished.set()

    arbiter = TransmitArbiter(dispatch, join_timeout_s=0.1)
    arbiter.open(generation=1)
    worker = arbiter._worker
    arbiter.submit_fifo(1, endpoint_id="one")
    timer = threading.Timer(0.02, lambda: os.kill(os.getpid(), signal.SIGINT))
    try:
        assert entered.wait(1)
        timer.start()
        with pytest.raises(KeyboardInterrupt):
            arbiter.close()
        assert not finished.is_set()
        with pytest.raises(TimeoutError):
            arbiter.close()
        with pytest.raises(DeviceNotReadyError):
            arbiter.open(generation=2)
    finally:
        timer.cancel()
        if timer.ident is not None:
            timer.join(1)
        release.set()
        assert finished.wait(1)
        threading.Thread.join(worker, 1)
        arbiter.close()


@pytest.mark.parametrize("kind", ["classic", "fd"])
def test_reply_event_order_follows_queue_claim_before_send_returns(monkeypatch, kind):
    from linkerbot_transport.routing import ReceiveRouter

    backend = Backend()
    delivered = threading.Event()
    router = ReceiveRouter(lambda message: message.arbitration_id)
    router.open(generation=1)
    route = router.subscribe(1)
    dispatcher = create_dispatcher(monkeypatch, kind, backend)

    def on_frame(frame):
        router.route(frame, generation=1)
        delivered.set()

    dispatcher.subscribe(on_frame)

    def send(messages):
        backend.incoming.put(messages[0])
        assert delivered.wait(1)
        backend.entered.set()
        assert backend.release.wait(2)

    monkeypatch.setattr(backend, "_send", send)
    message = (
        can.Message(arbitration_id=1) if kind == "classic" else CANFDMessage(1, b"")
    )
    try:
        receipt = dispatcher.send(message)
        assert backend.entered.wait(1)
        reply = route.receive(timeout_s=1)
        assert receipt.claimed_order is not None
        assert receipt.claimed_order < reply.received_order
        assert not receipt.done()
        backend.release.set()
        receipt.result(timeout=1)
    finally:
        backend.release.set()
        dispatcher.stop()
        router.close()


@pytest.mark.parametrize("kind", ["classic", "fd"])
def test_stop_winning_before_transmit_admission_leaves_receipt_unclaimed(
    monkeypatch, kind
):
    backend = Backend()
    dispatcher = create_dispatcher(monkeypatch, kind, backend)
    errors = []

    def stop():
        try:
            dispatcher.stop()
        except BaseException as error:
            errors.append(error)

    stopper = threading.Thread(target=stop)
    dispatcher._transmit_lock.acquire()
    locked = True
    try:
        receipt = dispatcher.send(
            can.Message(arbitration_id=1) if kind == "classic" else CANFDMessage(1, b"")
        )
        stopper.start()
        with pytest.raises(ClosedError):
            receipt.result(timeout=1)
        dispatcher._transmit_lock.release()
        locked = False
        stopper.join(1)
        assert not stopper.is_alive()
        assert errors == []
        assert receipt.claimed_at_s is None
        assert backend.calls == []
    finally:
        if locked:
            dispatcher._transmit_lock.release()
        backend.release.set()
        if stopper.ident is not None:
            stopper.join(1)
        dispatcher.stop()
