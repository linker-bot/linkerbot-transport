# Transport runtime

This guide describes the current `main` source. See
[source installation](../README.md#install-the-current-source) for setup.

## Lifecycle

Classic CAN and CAN FD transports share an explicit lifecycle:

```python
from linkerbot_transport.can import CANTransport

transport = CANTransport("can0")
try:
    transport.open()
    health = transport.get_health()
finally:
    transport.close()
```

Built-in high-level CAN FD transports start only their receive worker on open.
They send synchronously through `send_now()`; if their dispatcher is later used
for queued `send()`, its transmit worker starts on demand. Direct low-level
`CANFDMessageDispatcher` construction keeps eager startup by default and accepts
`start_send_worker=False` for the same lazy behavior. The first queued send can
therefore pay thread-startup cost; a startup failure accepts no queued receipt.
If a CAN FD queued-send worker cannot start, the dispatcher stops
and rejects later sends. Stop it and create a new dispatcher, or call the owning
transport's `recover()`. An interrupted `Thread.start()` may already have created
a native thread, so starting the same uncertain thread again is unsafe.

Construction does not open hardware or start workers. `open()` and `close()` are
idempotent. A failed cleanup remains visible to `close()`/`recover()` so the
resource can be released explicitly. Recovery creates a new connection
generation, closes routes from the previous generation, and does not replay
previous commands. Register routes again after recovery.

Concurrent lifecycle calls are serialized. A synchronous callback cannot reenter
`open()`, `close()` or `recover()` while another lifecycle operation on that
transport is in progress: it raises `DeviceNotReadyError` without replacing the
outer operation's resources. Return from the callback and retry afterward.
The pool similarly rejects reentrant acquire/cleanup for the same key while its
factory, open, recover or close is running; it does not issue a new lease.
If ordinary startup fails and rollback receives a process interrupt, the
interrupt propagates with the startup failure as its cause and any pending
cleanup owner is retained. An original process interrupt remains primary.

Classic and FD dispatchers bound the wait for another backend-close attempt and
retain cleanup responsibility on `TimeoutError`. Worker joins and transmit-drain
waits have separate budgets, not a single one-second stop deadline. An underlying
driver shutdown call without a timeout cannot be preempted by the SDK.

Classic CAN queued-send worker startup failure also stops the dispatcher and
requires a new dispatcher or transport recovery. The original startup interrupt
propagates, and later sends cannot retry the same uncertain `Thread.start()`.

Use `SharedTransportPool` when several SDK devices share one physical channel.
The first lease opens the transport and the final lease closes it. A transport
whose cleanup is still pending remains in the pool. The releasing lease can
retry `close()` to finish idle cleanup without reopening the channel or releasing
the same reference twice. Its `transport` property remains closed after the
reference was released. A later matching `acquire()` can still recover the
retained object; an old lease retry never closes a recovered active owner or a
replacement entry.

If `acquire()` raises before returning a lease, call `pool.close_idle(key)` to
retry cleanup for that key. It closes only a retained connection with zero active
leases, serialized with `acquire()` for that key, without reopening the channel. Missing
keys and connections with active leases are left untouched. Close errors propagate
and leave the entry available for another cleanup or recovery attempt.

Pool factory/open/recover/close operations run outside the metadata lock; cached
pool queries and unrelated keys continue while one channel is closing. Each
lease has its own reference token, so an interrupted release can be retried
without consuming another lease. An interrupted acquire that returns no handle
does not leave an active reference; an idle retained connection still needs
`close_idle()` or recovery.

## Exact receive routes

Routes are exclusive, bounded subscriptions. A received frame is assigned one
key and delivered to the matching route; it is not broadcast to every
subscriber. A classic CAN route key includes the complete arbitration ID and
the standard/extended flag:

```python
import can

from linkerbot_transport.can import CANRouteKey, CANTransport

transport = CANTransport("can0")
try:
    transport.open()
    with transport.subscribe_route(
        CANRouteKey(arbitration_id=0x28, is_extended_id=False),
        queue_capacity=64,
    ) as route:
        receipt = transport.send(
            can.Message(arbitration_id=0x28, data=[0xC2], is_extended_id=False)
        )
        receipt.result(timeout=0.1)
        message = route.receive(timeout_s=0.02)
finally:
    transport.close()
```

Responses using several arbitration IDs can be registered atomically with
`subscribe_routes()`. The complete group shares one bounded queue and arrival
order; registration either claims every key or fails without claiming any.
Input iteration stops after at most `max_routes + 1` keys, so an oversized or
infinite iterable is rejected without being fully materialized. `try_receive()`
returns `None` only while the inbox is active and empty; terminal errors,
including `TimeoutError`, propagate just as they do from `receive()`.

`CANTransport` also accepts `route_key_of` for protocols that encode a changing
address in the CAN ID. The extractor receives the original `can.Message`, must
return a `CANRouteKey`, and must be fast and free of I/O. Extractor failures are
counted and the frame is dropped. The delivered message keeps its original ID,
flags and payload.

An actor may pass `on_available=event.set` to receive edge-triggered readiness
notifications. The callback runs synchronously on the receiving or terminating
thread, must return immediately, and must not perform protocol I/O. It is also
called when the route closes or aborts. Notifications run outside the Transport
and Router state locks, so callbacks may read their cached `get_health()` snapshots.
They may also read pool metadata and unregister routes. A callback does not wait
for another active notifier: that notifier attempts its terminal edge on return.
Do not acquire or clean up a pool key whose lifecycle is waiting for the callback.
Terminal notification attempts are not replayed after a callback interrupt,
because its side effects may already have happened. Other routes are still
notified before the original interrupt propagates; an interruption before an
attempt leaves retry responsibility in the router. Retry interrupted router or
subscription close before opening a new router generation.

`RoutedMessage` carries a monotonic receive timestamp, connection generation and
per-generation sequence. A full inbox drops the newly received frame. Route
misses, key errors, overflow, close drops and generation rejections are exposed
through health metrics.

## Health and generations

`get_health()` is a cache-only operation. It reports connection state, generation,
recent and consecutive errors, reconnect count, route counters, and cumulative
physical-channel `tx_frames`, `rx_frames`, `tx_bytes` and `rx_bytes`.

Transmit counters increase after the backend accepts a frame. Receive counters
increase when the backend returns a frame, even when routing later drops it.
Byte counters use the CAN/CAN FD data-field length selected by DLC; they exclude
arbitration, CRC, bit stuffing and inter-frame spacing, so they are not direct
bus-utilization measurements.

Closing, aborting, or recovering closes routes and wakes blocked receivers.
Frames arriving from a previous callback generation are counted but are never
delivered to the new generation. A delayed fault from an old dispatcher cannot
abort routes opened by recovery, even if it passed the initial callback gate
before recovery started. Standalone `ReceiveRouter` callers can use
`abort(error, generation=n)` to terminate only the matching generation; omitting
`generation` retains unconditional termination of the current routes.

`max_consecutive_errors` applies separately to receive and send operations.
Classic CAN and CAN FD share one transmit failure streak across synchronous
sends (`transport.send()` / `send_now()`) and queued sends. A native batch counts
as one backend call. A successful send resets this streak; successful receives,
an idle queue, and validation failures neither increment nor reset it.
`TransportError` and `TimeoutError` count toward the limit; non-I/O SDK errors,
such as `ValidationError`, are returned only to the affected caller. Raw backend
`ValueError` and `TypeError` become `ValidationError`; existing SDK exceptions
retain their types and causes, and process interrupts propagate immediately.

At the limit, the dispatcher rejects new sends, fails pending queued receipts,
and reports the bus fault once after releasing the transmit lock. The transport
becomes `FAULTED` and aborts its receive routes. Recover explicitly and register
routes again; failed commands are never replayed. The health snapshot's
`consecutive_errors` tracks recent activity and may also reset on a successful
receive, so it is distinct from the transmit failure streak. A terminal send
fault is not counted twice through its callback and receipt, and a late error
from an old send cannot update a recovered generation.
A late successful send still increases cumulative physical transmit counters,
but cannot reset the replacement connection's error count or transmit timestamp.

## Send receipts

`transport.send()` returns a `SendReceipt`. Completion means that the physical
backend accepted the frame, not that the device executed it:

```python
from concurrent.futures import TimeoutError as FutureWaitTimeout
from linkerbot_transport.errors import TimeoutError as SendTimeout, TransportError

receipt = transport.send(message)
try:
    result = receipt.result(timeout=0.1)
except SendTimeout:
    # The backend send timed out; the physical outcome may be unknown.
    ...
except FutureWaitTimeout:
    # Only the wait ended; the receipt may complete later. Do not replay it.
    ...
except TransportError:
    # The backend rejected or failed the send.
    ...
```

A Future wait timeout does not cancel the send. This wait limit does not bound
the synchronous backend call inside `transport.send()`. Catch the SDK send
timeout first: on newer Python versions it also inherits the standard timeout.

Successful results report `SendOutcome.DISPATCHED` and generation/timestamp
metadata. A replaced latest target reports `SendOutcome.SUPERSEDED`; a physical
failure reports `SendOutcome.FAILED` and raises a stable transport exception.
Stopping a transport fails outstanding receipts. Control frames are not retried
or replayed automatically.

The low-level CAN FD queue commits the entire confirmed physical batch before
publishing its first completion callback. A callback that stops the dispatcher
cannot turn another successful frame in that batch into a failed receipt.
An interrupted publication retains its result and cleanup owner. This does not
make a short or failed backend batch safe to retry: its physical outcome may
still be partial or unknown. Cleanup publishes the entire detached receipt group
even if a completion callback interrupts; an original caller process interrupt
remains primary. Reentrant stop callbacks do not recursively publish the queue.

Transmit cleanup records each detached receipt's failure before removing its job
from the queue. Nested or concurrent endpoint cancellation keeps the original
cause for each endpoint. If publication is interrupted before a receipt becomes
terminal, retry `close()` to finish it; opening a new generation is rejected while
that cleanup remains pending. Cleanup callbacks cannot replace an original
process interrupt or leave the rest of the detached receipts without an owner.

## TransmitArbiter

Use `TransmitArbiter` as the single writer when multiple devices or command
families share a transport. Continuous commands use one latest slot per
`(endpoint_id, command_family)`; discrete operations use a bounded FIFO:

```python
import time

from linkerbot_transport import TransmitArbiter

generation = transport.get_health().generation
arbiter = TransmitArbiter(transport.send, fifo_capacity=32)
try:
    arbiter.open(generation=generation)
    receipt = arbiter.submit_latest(
        frame,
        endpoint_id="left-hand",
        command_family="angles",
        deadline_at_s=time.monotonic() + 0.02,
    )
    arbiter.submit_fifo(clear_fault_frame, endpoint_id="left-hand")
finally:
    arbiter.close()
```

`deadline_at_s` is an absolute monotonic deadline. `replace_latest_if_pending()`
replaces only an unclaimed target and returns `None` once the previous target is
already in flight. Arbiter health is cache-only and reports pending, accepted,
dispatched, superseded, failed and capacity-rejection counters.
The arbiter publishes dispatch completion and counters before running receipt
callbacks, so a completed receipt's callback can cancel its own endpoint without
waiting for itself. Service history is retained only for endpoints with queued
or in-flight work; continuously active endpoints keep their fairness history.

For an actor that already owns the I/O loop, use `TransmitMode.CALLER` and call
`drain()` from that actor:

```python
from linkerbot_transport import TransmitArbiter, TransmitMode

arbiter = TransmitArbiter(transport.send, mode=TransmitMode.CALLER)
try:
    arbiter.open(generation=generation)
    receipt = arbiter.submit_fifo(frame, endpoint_id="left-hand")
    arbiter.drain()
    receipt.result(timeout=0.1)
finally:
    arbiter.close()
```

Prepare all frames before `submit_fifo_batch()` when they must be admitted as one
group. Capacity is measured in frames; admission is all-or-none and frames in a
batch are not interleaved with another job. `max_pending_age_s` rejects a new
batch when the oldest queued or in-flight work has reached the age limit.
`deadline_at_s` is checked before the first frame; it does not truncate a
batch that has already started.

### Optional explicit batch dispatch

Built-in managed transports provide synchronous `send_batch()`. Pass it
explicitly to an arbiter to dispatch an already submitted FIFO batch in blocks
of at most 32 frames, without waiting to collect additional requests:

```python
arbiter = TransmitArbiter(
    transport.send,
    dispatch_batch=transport.send_batch,
    mode=TransmitMode.CALLER,
)
arbiter.open(generation=transport.get_health().generation)
receipt = arbiter.submit_fifo_batch(frames, endpoint_id="arm")
arbiter.drain()
receipt.result(timeout=0.1)
```

A native-capable CAN FD backend makes two calls for 51 frames (32 + 19).
Single frames retain the single-send path. Dispatchers/backends without native
batching send frames individually; existing arbiters without `dispatch_batch`
keep their current path. Third-party `MessageTransport` and `CANFDBackend`
implementations do not need new methods. Existing arm/hand wrappers need a
separate explicit integration to use this capability.

Every block checks `expected_generation`; open the arbiter using the same
transport's current generation. Shutdown drains the current physical call and
cancels the suffix between calls. It cannot retract an entire block already
given to the driver. Per-frame fallback can stop between completed frames.
The deadline is still checked only before the whole FIFO batch starts.
The existing consecutive physical-error threshold counts backend calls, not
individual frames inside a native block.

`sent_frames` is the confirmed successful prefix. A short or failed native
block adds no confirmed frames even when it had physical effects. Neither that
block nor its suffix is replayed automatically; this count is not a safe replay
offset. Health counters and progress are committed before receipt completion.
A later process interrupt preserves a fully confirmed success, propagates
unchanged, and terminates other unsent work.

Direct `transport.send_batch(messages)` accepts 1..32 frames. Oversized
iterators consume at most 33 items before rejection; empty batches are rejected.
Its optional `on_progress(count)` callback synchronously transfers this block's
confirmed progress outside I/O and state locks. It must be lightweight and
accept repeated cumulative counts; raising stops remaining frames. A custom
arbiter callback has the signature
`callback(messages, *, expected_generation, on_progress) -> BatchSendReceipt`.
A complete progress count does not override an invalid or failed returned
receipt; those errors remain observable on the FIFO batch receipt.
`BatchSendReceipt.sent_frames` reports the successful prefix if physical I/O
stops the unsent suffix. Admission is atomic; physical execution and device
acknowledgement are not.

In caller mode, `KeyboardInterrupt`, `SystemExit` and other process interrupts
stop admission and terminate unsent receipts before propagating to the caller.
The batch receipt keeps its successfully dispatched prefix. Reopening starts a
new generation without replaying abandoned commands.

## CAN FD frame values

`CANFDMessage.frame_type` defaults to `0x04` (FD without bit-rate switching).
Use `0x0C` for FD with BRS when the device supports it. `None` inherits the
backend mode and keeps BRS disabled for SocketCAN. `dlc` is the protocol encoding
`0..15`; conversion to data-field length and zero padding happen at the backend
boundary. Classic frames carry at most eight data bytes.

Worker shutdown uses an explicit exit handoff after target cleanup, rather than
`Thread.is_alive()` or an interrupted `join()` result. An arbiter whose start
is interrupted keeps its worker reference and rejects reopening until that
worker has exited; a close timeout retains that responsibility. An uncertain
start with no observed exit remains unresolved, rather than authorizing a
second transmitter. Superseded latest receipts retain their recorded outcome
if publication is interrupted, and `close()` retries publication. Low-level
queued classic and FD sends record claim time/order after transmit admission
and before physical I/O, including failed attempts and every frame in a batch.
