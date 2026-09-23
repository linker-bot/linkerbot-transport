# linkerbot-transport

[English](README.md) |
[简体中文](https://gitea.linkerhub.work/zhuzhixiang/linkerbot-transport/src/branch/main/README_zh.md)

`linkerbot-transport` owns the physical communication layer shared by Linkerbot
device SDKs. The first release provides classic CAN, vendor CAN FD and Linux
SocketCAN FD implementations, together with stable errors and small concurrency
primitives used at transport boundaries.

The package does not depend on ROS, hand models, arm models or kinematics.

## Installation

Windows MiniCANFD uses `CANFDTransport` with the vendor's `HCanbus.dll`.
Its ABI uses one enumerated channel number, unlike Linux's device/channel pair.
See [MiniCANFD setup, ABI and validation](docs/minicanfd.md) for the source-only
Windows changes, driver requirements and precise channel semantics.

Install only the backend required by the application:

```bash
python -m pip install \
  --index-url https://gitea.linkerhub.work/api/packages/zhuzhixiang/pypi/simple \
  --extra-index-url https://pypi.org/simple \
  "linkerbot-transport[can]==1.0.0"
python -m pip install \
  --index-url https://gitea.linkerhub.work/api/packages/zhuzhixiang/pypi/simple \
  --extra-index-url https://pypi.org/simple \
  "linkerbot-transport[canfd]==1.0.0"
```

The dependency-free base package can be installed for error types and
concurrency primitives:

```bash
python -m pip install \
  --index-url https://gitea.linkerhub.work/api/packages/zhuzhixiang/pypi/simple \
  "linkerbot-transport==1.0.0"
```

## Public API

### Windows PCAN / classic CAN

Install PEAK's PCAN driver and PCAN-Basic matching the Python process
architecture, then use the `can` extra. No Linux network interface is needed:

```python
from linkerbot_transport.can import CANTransport

transport = CANTransport(
    "PCAN_USBBUS1", "pcan", bitrate=1_000_000,
    bus_options={"auto_reset": True},
)
transport.open()
try:
    health = transport.get_health()
finally:
    transport.close()
```

`CANTransport("can0")` continues to use python-can's Linux SocketCAN backend.
The Linux link bitrate is configured externally; passing `bitrate` does not
change an existing kernel link. Construction performs no device I/O. Driver
loading happens in `open()` and failures retain their original cause.

`bitrate`, `receive_own_messages` and scalar `bus_options` are copied into the
connection key. Options must be strings, numbers, booleans or `None`; use named
parameters for channel, interface and bitrate. PCAN's `device_id` override is
rejected: select an explicit channel so its identity remains stable. Ambient
python-can config files and environment settings are ignored. Conflicting
settings for an already-open channel in this process fail before a second
driver open. Use `SharedTransportPool` with `transport.pool_key` to share
identical connections; this does not lock out other processes.

Virtual-bus and driver-stub tests cover configuration, routing, recovery and
cleanup. They do not establish Windows hardware compatibility or achievable
control rates; those require the target PCAN adapter and Windows driver.

### CAN FD via python-can / SLCAN

The source checkout adds `PythonCANFDTransport` (not yet in release 1.0.0).
Install it with `python -m pip install -e ".[canfd]"`; the extra includes
pyserial. On Windows, use an explicit COM port and firmware implementing the
CANable 2.0 SLCAN FD extensions:

```python
from linkerbot_transport.canfd import PythonCANFDTransport

transport = PythonCANFDTransport(
    "COM5", "slcan", bitrate=1_000_000, data_bitrate=5_000_000,
    bus_options={"tty_baudrate": 1_000_000},
)
```

The same lifecycle, routes, `CANFDMessage` and send receipts apply. `0x04`
means FD without BRS; set `frame_type=0x0C` for BRS. SLCAN translates the two
rates into python-can's `BitTimingFd`; its current data-rate choices are 2 and
5 Mbit/s. Ordinary classic-only SLCAN firmware cannot carry FD frames.
Serial baudrate is separate from CAN bitrate and does not promise a polling
rate. See [backend configuration and Windows validation](docs/python-can.md).

`SocketCANFDTransport` keeps its Linux link validation/reconfiguration and
shares the python-can frame conversion. `PythonCANFDTransport(...,
"socketcan")` also validates the preconfigured link, without reconfiguring it.
These paths use one receive worker and direct sends, with no extra forwarding
thread added by the adapter. The existing internal FD queue worker is retained.

### Lifecycle and routing

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
Opening also rolls back on `KeyboardInterrupt` or worker startup failure. The
original interrupt is re-raised. If cleanup fails, the connection stays faulted
and retains its dispatcher: retry `close()` before opening again. A failed or
interrupted `close()` never reports the connection as closed. Neither cleanup
nor recovery automatically replays motion commands.

The first lease opens the transport and the final lease closes it. Recovery
replaces the connection and never replays messages from an earlier generation.
If opening or final closing fails and cleanup remains incomplete, the pool
retains the transport with zero leases. The next `acquire()` for that key
recovers the retained object instead of invoking another factory. Failed opens
that are fully cleaned up leave no entry in `active_keys()`.

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

For protocols that encode changing status bits in the CAN ID, classic
`CANTransport` also accepts `route_key_of=extract_address`. The function receives
the original `can.Message` and returns a stable `CANRouteKey`; subscribers use
that same key. It must be fast and perform no I/O. The delivered message retains
its original arbitration ID, flags and payload. Keep the standard/extended flag
in the extracted key. Extractor exceptions are counted as route-key errors and
the malformed frame is dropped, as with default routing. Custom extractors are
distinguished by callable identity in `pool_key`; reuse the same callable only
when its routing policy is identical. Device SDKs own the protocol extraction.

The capacity applies to the combined stream. Registration either claims every
key or none, and closing the subscription releases the complete group.

An SDK actor can pass `on_available=event.set` when subscribing. The callback
is edge-triggered when an empty route becomes readable and is also invoked when
the route closes or aborts, so the actor can consume the transport queue
directly without a second mailbox thread. It runs synchronously on the routing
path, must return immediately, and must never perform protocol work or block.
An exception is logged and disables that route's notifier without stopping RX.

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

Runtimes that already own an I/O actor can use `TransmitMode.CALLER` to avoid a
second permanent TX worker. Submission still only admits bounded work and
returns a receipt; the actor then calls `drain()` to dispatch it. The first
concurrent caller temporarily owns the writer and drains work admitted while it
is active. `drain()` performs transport I/O and therefore belongs only in the
I/O actor, never in a user or ROS callback:

```python
from linkerbot_transport import TransmitArbiter, TransmitMode

arbiter = TransmitArbiter(transport.send, mode=TransmitMode.CALLER)
arbiter.open(generation=generation)
receipt = arbiter.submit_fifo(frame, endpoint_id="left-hand")
arbiter.drain()
receipt.result(timeout=0.1)
```

For an inseparable group of FIFO targets, prepare all frames first and use one
atomic admission:

```python
receipt = arbiter.submit_fifo_batch(
    joint_frames,
    endpoint_id="arm",
    deadline_at_s=time.monotonic() + 0.02,
    max_pending_age_s=0.02,
)
```

Capacity counts frames: a seven-frame batch needs seven free FIFO slots or raises
`QueueFullError` without enqueuing any of it. `max_pending_age_s` is optional and
rejects with `TimeoutError` if queued or in-flight work has already reached that
age. It does not predict bus latency. The deadline is checked before the first
frame; an already-started batch is not truncated by its deadline. Normal jobs and
other batches cannot interleave within it. Close or a backend failure can still
stop an unsent suffix. `receipt.sent_frames` reports the successful backend prefix
on failure, while `receipt.frame_count` records the admitted size. Acceptance is
atomic; physical execution and motor acknowledgement are not.

`get_health()` exposes `expired`, `rejected_stale`, `oldest_pending_age_s` (including
in-flight work), and `in_flight_frames` (the entire claimed batch). Pending counts
measure frames; accepted/dispatched/failed/expired counters measure submissions,
so a seven-frame batch counts as one job. Existing single-message calls and their
default policies are unchanged.

Classic `CANTransport` uses direct dispatch without an idle legacy TX worker.
Direct internal-dispatcher users still get the original eager worker by default;
`start_send_worker=False` defers it until asynchronous `send()` is actually used.

A failed CAN FD batch fails every receipt because the backend cannot identify a
partial success safely. Device runtimes receive through exact bounded routes;
the internal channel workers are not part of the 1.0 compatibility contract.

CAN FD messages default to FrameType `0x04` (FD without bit-rate switching).
Set `frame_type=0x0C` explicitly only for devices that support BRS. A message
with `frame_type=None` inherits the vendor interface configuration; SocketCAN
inherits the backend's `fd` mode and keeps BRS disabled.
