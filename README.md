# linkerbot-transport

[English](README.md) |
[简体中文](https://gitea.linkerhub.work/zhuzhixiang/linkerbot-transport/src/branch/main/README_zh.md)

`linkerbot-transport` owns the physical communication layer shared by Linkerbot
device SDKs. The first release provides classic CAN, vendor CAN FD and Linux
SocketCAN FD implementations, together with stable errors and small concurrency
primitives used at transport boundaries.

The package does not depend on ROS, hand models, arm models or kinematics.

## Installation

Install only the backend required by the application:

```bash
pip install "linkerbot-transport[can]"
pip install "linkerbot-transport[canfd]"
```

The dependency-free base package can be installed for error types and
concurrency primitives:

```bash
pip install linkerbot-transport
```

## Public API

Only the `linkerbot_transport` namespace is public. Classic CAN and CAN FD
applications construct an explicit-lifecycle transport:

```python
from linkerbot_transport.can import CANTransport
from linkerbot_transport.canfd import (
    CANFDMessage,
    CANFDTransport,
    SocketCANFDTransport,
)
```

The package does not provide the historical `linkerbot.comm`,
`linkerbot.exceptions`, `linkerbot.queue` or `linkerbot.relay` namespaces.
Auto-starting dispatcher classes are internal channel workers rather than a
supported user API. Device SDKs and applications must use `CANTransport`,
`CANFDTransport` or `SocketCANFDTransport`.

Stable exceptions live in `linkerbot_transport.errors`. Catch the narrowest
category required by the caller; `LinkerbotError` is the common base when an
integration needs one final mapping boundary:

```python
from linkerbot_transport.errors import (
    LinkerbotError,
    TimeoutError,
    TransportError,
)

try:
    result = receipt.result(timeout=0.1)
except TimeoutError:
    # The bounded wait expired; the receipt can still complete later.
    ...
except TransportError:
    # The physical backend rejected or failed the send.
    ...
except LinkerbotError:
    # Validation, lifecycle and queue errors reach this final boundary.
    ...
```

Modern transports have an explicit lifecycle. Construction does not open a
device or start worker threads:

```python
from linkerbot_transport.can import CANTransport

transport = CANTransport("can0")
transport.open()
try:
    health = transport.get_health()
finally:
    transport.close()
```

Use `SharedTransportPool` when several devices share the same physical channel.
The first lease opens the transport and the final lease closes it. Recovery
replaces the connection and never replays messages from an earlier generation.

Modern CAN and CAN FD transports receive through exact, exclusive routes rather
than broadcast callbacks. The RX thread extracts one key and performs one hash
lookup, then places the frame in that route's bounded inbox. For CAN, the key is
the complete arbitration ID plus the standard/extended frame flag:

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
    received = route.receive(timeout_s=0.02)
    assert received.generation == transport.get_health().generation
finally:
    route.close()
    transport.close()
```

Protocols whose responses use several arbitration IDs can register them as one
atomic route group. Every exact key still uses one hash lookup, while all
matching frames enter the same bounded queue in arrival order:

```python
routes = transport.subscribe_routes(
    (
        CANRouteKey(arbitration_id=0x02000000, is_extended_id=True),
        CANRouteKey(arbitration_id=0x02006000, is_extended_id=True),
    ),
    queue_capacity=64,
)
```

The capacity applies to the combined stream. Registration either claims every
key or none, and closing the subscription releases the complete group.

`RoutedMessage` also contains a monotonic receive timestamp and per-generation
sequence number. A full inbox drops the newly received frame and increments
the route and transport overflow counters. Route misses, key errors, stale
generation frames, queue depth and close drops are likewise cached in
`get_health()` without device I/O.

Transport health also reports cumulative `tx_frames`, `rx_frames`, `tx_bytes`
and `rx_bytes` for the complete physical channel across connection
generations. A transmit is counted only after backend success; every frame
received from the backend is counted even if routing later misses, overflows or
rejects an old generation. Byte counters use the CAN/CAN FD data-field length
selected by DLC. They exclude arbitration, CRC, bit stuffing and inter-frame
spacing, so they must not be treated as direct bus-utilization measurements.

Closing, aborting or recovering a transport closes all current routes and
wakes blocked receivers. A route belongs to exactly one connection generation:
after `recover()`, register fresh routes before accepting device traffic. Frames
from an earlier callback generation are counted and never delivered to the new
route.

Every message transport send returns a `SendReceipt`. Completion means the
physical backend accepted the message, not merely that a software queue did.
Use `receipt.result(timeout=...)` when the caller needs to observe success or a
stable send exception. A successful result reports `SendOutcome.DISPATCHED`,
the connection generation, and monotonic acceptance, claim and completion
timestamps. Superseded sends report `SendOutcome.SUPERSEDED`; failed sends raise
a stable transport exception and expose `SendOutcome.FAILED` on the receipt.
Classic CAN and CAN FD background queues follow the same contract; stopped or
failed transports fail every outstanding receipt and do not automatically
retry control frames.

Use `TransmitArbiter` as the single writer when multiple devices or command
families share one physical transport. Continuous commands use a keyed latest
slot, so only the newest unclaimed value for `(endpoint_id, command_family)` is
retained. Discrete operations use the separately bounded FIFO:

```python
import time

from linkerbot_transport import TransmitArbiter

transport.open()
generation = transport.get_health().generation
arbiter = TransmitArbiter(transport.send, fifo_capacity=32)
arbiter.open(generation=generation)
try:
    angle_receipt = arbiter.submit_latest(
        angle_frame,
        endpoint_id="left-hand",
        command_family="angles",
        deadline_at_s=time.monotonic() + 0.02,
    )
    replacement = arbiter.replace_latest_if_pending(
        newer_angle_frame,
        endpoint_id="left-hand",
        command_family="angles",
        deadline_at_s=time.monotonic() + 0.02,
    )
    clear_fault_receipt = arbiter.submit_fifo(
        clear_fault_frame,
        endpoint_id="left-hand",
    )
finally:
    arbiter.close()
    transport.close()
```

`deadline_at_s` is an absolute `time.monotonic()` deadline, not a relative
timeout. Closing or aborting the arbiter fails every pending send from the old
connection generation; reopening never replays those messages. Arbiter health
is a zero-I/O snapshot containing pending counts and accepted, dispatched,
superseded, failed and capacity-rejection counters.

`replace_latest_if_pending()` returns `None` once the prior keyed job has been
claimed or no such job exists. It never queues a second job behind an in-flight
request, which lets protocol runtimes preserve response-correlation ordering.

A failed CAN FD batch fails every receipt because the backend cannot identify a
partial success safely. Device runtimes receive through exact bounded routes;
the internal channel workers are not part of the 1.0 compatibility contract.

CAN FD messages default to FrameType `0x04` (FD without bit-rate switching).
Set `frame_type=0x0C` explicitly only for devices that support BRS. A message
with `frame_type=None` inherits the vendor interface configuration; SocketCAN
inherits the backend's `fd` mode and keeps BRS disabled.
