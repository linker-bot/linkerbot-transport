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

The legacy dispatcher classes still open immediately when constructed. They
remain available only to preserve existing SDK behavior during migration.
Queued CAN FD sends return a receipt; call `receipt.result(timeout=...)` when
the caller must distinguish queue admission from physical backend success. A
failed batch fails every receipt without retrying, because partial delivery is
not observable and replaying control frames may be unsafe.

The historical `linkerbot.comm`, `linkerbot.exceptions`, `linkerbot.queue` and
`linkerbot.relay` imports remain available during the compatibility period.
