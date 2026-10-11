# Transport runtime

This guide describes the public runtime contracts shared by classic CAN and CAN
FD transports. All transports use an explicit lifecycle:

```python
from linkerbot_transport.can import CANTransport

transport = CANTransport("can0")
transport.open()
try:
    health = transport.get_health()
finally:
    transport.close()
```

Construction does not open hardware or start workers. `open()` and `close()` are
idempotent. A failed cleanup remains visible to `close()`/`recover()` so the
resource can be released explicitly. Recovery creates a new connection
generation, closes routes from the previous generation, and does not replay
previous commands. Register routes again after recovery.

Use `SharedTransportPool` when several SDK devices share one physical channel.
The first lease opens the transport and the final lease closes it. A transport
whose cleanup is still pending remains in the pool. The releasing lease can
retry `close()` to finish idle cleanup without reopening the channel or releasing
the same reference twice. Its `transport` property remains closed after the
reference was released. A later matching `acquire()` can still recover the
retained object; an old lease retry never closes a recovered active owner or a
replacement entry.

## Exact receive routes

Routes are exclusive, bounded subscriptions. A received frame is assigned one
key and delivered to the matching route; it is not broadcast to every
subscriber. A classic CAN route key includes the complete arbitration ID and
the standard/extended flag:

```python
import can

from linkerbot_transport.can import CANRouteKey, CANTransport

transport = CANTransport("can0")
transport.open()
route = transport.subscribe_route(
    CANRouteKey(arbitration_id=0x28, is_extended_id=False),
    queue_capacity=64,
)
try:
    receipt = transport.send(
        can.Message(arbitration_id=0x28, data=[0xC2], is_extended_id=False)
    )
    receipt.result(timeout=0.1)
    message = route.receive(timeout_s=0.02)
finally:
    route.close()
    transport.close()
```

Responses using several arbitration IDs can be registered atomically with
`subscribe_routes()`. The complete group shares one bounded queue and arrival
order; registration either claims every key or fails without claiming any.

`CANTransport` also accepts `route_key_of` for protocols that encode a changing
address in the CAN ID. The extractor receives the original `can.Message`, must
return a `CANRouteKey`, and must be fast and free of I/O. Extractor failures are
counted and the frame is dropped. The delivered message keeps its original ID,
flags and payload.

An actor may pass `on_available=event.set` to receive edge-triggered readiness
notifications. The callback runs synchronously on the receive path, must return
immediately, and must not perform protocol I/O. It is also called when the route
closes or aborts.

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
delivered to the new generation.

## Send receipts

`transport.send()` returns a `SendReceipt`. Completion means that the physical
backend accepted the frame, not that the device executed it:

```python
from linkerbot_transport.errors import TimeoutError, TransportError

receipt = transport.send(message)
try:
    result = receipt.result(timeout=0.1)
except TimeoutError:
    # The bounded wait ended; the receipt may complete later.
    ...
except TransportError:
    # The backend rejected or failed the send.
    ...
```

Successful results report `SendOutcome.DISPATCHED` and generation/timestamp
metadata. A replaced latest target reports `SendOutcome.SUPERSEDED`; a physical
failure reports `SendOutcome.FAILED` and raises a stable transport exception.
Stopping a transport fails outstanding receipts. Control frames are not retried
or replayed automatically.

## TransmitArbiter

Use `TransmitArbiter` as the single writer when multiple devices or command
families share a transport. Continuous commands use one latest slot per
`(endpoint_id, command_family)`; discrete operations use a bounded FIFO:

```python
import time

from linkerbot_transport import TransmitArbiter

generation = transport.get_health().generation
arbiter = TransmitArbiter(transport.send, fifo_capacity=32)
arbiter.open(generation=generation)
try:
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

For an actor that already owns the I/O loop, use `TransmitMode.CALLER` and call
`drain()` from that actor:

```python
from linkerbot_transport import TransmitArbiter, TransmitMode

arbiter = TransmitArbiter(transport.send, mode=TransmitMode.CALLER)
arbiter.open(generation=generation)
receipt = arbiter.submit_fifo(frame, endpoint_id="left-hand")
arbiter.drain()
receipt.result(timeout=0.1)
```

Prepare all frames before `submit_fifo_batch()` when they must be admitted as one
group. Capacity is measured in frames; admission is all-or-none and frames in a
batch are not interleaved with another job. `max_pending_age_s` rejects work
that is already too old, while `deadline_at_s` is checked before dispatch.
`BatchSendReceipt.sent_frames` reports the successful prefix if physical I/O
stops the unsent suffix. Admission is atomic; physical execution and device
acknowledgement are not.

## CAN FD frame values

`CANFDMessage.frame_type` defaults to `0x04` (FD without bit-rate switching).
Use `0x0C` for FD with BRS when the device supports it. `None` inherits the
backend mode and keeps BRS disabled for SocketCAN. `dlc` is the protocol encoding
`0..15`; conversion to data-field length and zero padding happen at the backend
boundary. Classic frames carry at most eight data bytes.
