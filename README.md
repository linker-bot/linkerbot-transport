# linkerbot-transport

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

## Imports

New code should use the canonical namespace:

```python
from linkerbot_transport.can import CANMessageDispatcher
from linkerbot_transport.canfd import CANFDMessage, CANFDMessageDispatcher
from linkerbot_transport.errors import TransportError
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

The legacy dispatcher classes still open immediately when constructed. They
remain available only to preserve existing SDK behavior during migration. A
failed CAN FD batch fails every receipt because the backend cannot identify a
partial success safely.

CAN FD messages default to FrameType `0x04` (FD without bit-rate switching).
Set `frame_type=0x0C` explicitly only for devices that support BRS. A message
with `frame_type=None` inherits the vendor interface configuration; SocketCAN
inherits the backend's `fd` mode and keeps BRS disabled.

The historical `linkerbot.comm`, `linkerbot.exceptions`, `linkerbot.queue` and
`linkerbot.relay` imports remain available during the compatibility period.
