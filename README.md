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

The dependency-free base package can be installed for error types, contracts
and test doubles:

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

The historical `linkerbot.comm`, `linkerbot.exceptions`, `linkerbot.queue` and
`linkerbot.relay` imports remain available during the compatibility period.
