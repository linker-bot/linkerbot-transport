# linkerbot-transport

`linkerbot-transport` is the physical communication layer shared by Linkerbot
device SDKs. It provides classic CAN, CAN FD, exact receive routing, observable
send results, shared connection leases, and bounded transmit scheduling.

[简体中文](README_zh.md)

The package is independent of ROS, hand models, arm models, and kinematics.

## Install the current source

The current hand and transport versions are not published to the package index.
Install both repositories in editable mode so changes in either checkout are
used immediately. The commands below assume both repositories are under
`~/src` and create an environment named `py-linkerbot`:

### uv

```bash
cd ~/src
uv venv py-linkerbot --python 3.12
source py-linkerbot/bin/activate

uv pip install -e "./linkerbot-transport[all]"
uv pip install -e ./linkerbot-hand
```

On Windows PowerShell, activate with
`./py-linkerbot/Scripts/Activate.ps1` before running the two install commands.

### pip

```bash
cd ~/src
python3.12 -m venv py-linkerbot
source py-linkerbot/bin/activate
python -m pip install --upgrade pip setuptools wheel

python -m pip install -e "./linkerbot-transport[all]"
python -m pip install -e ./linkerbot-hand
```

Verify that imports resolve to the checkouts:

```bash
python -c "import linkerbot_hand, linkerbot_transport; print(linkerbot_hand.__file__); print(linkerbot_transport.__file__)"
```

### Install a released package

Install the backend used by the application:

```bash
python -m pip install \
  --index-url https://gitea.linkerhub.work/api/packages/zhuzhixiang/pypi/simple \
  --extra-index-url https://pypi.org/simple \
  "linkerbot-transport[can]==1.0.0"
```

Use `canfd` for python-can SLCAN/FD support, or install the base package when
only the shared errors and concurrency primitives are needed:

```bash
python -m pip install \
  --index-url https://gitea.linkerhub.work/api/packages/zhuzhixiang/pypi/simple \
  --extra-index-url https://pypi.org/simple \
  "linkerbot-transport[canfd]==1.0.0"

python -m pip install \
  --index-url https://gitea.linkerhub.work/api/packages/zhuzhixiang/pypi/simple \
  "linkerbot-transport==1.0.0"
```

## Select a backend

| Bus | Entry point | Typical channel | Platform |
| --- | --- | --- | --- |
| Classic CAN | `CANTransport` | `can0`, `PCAN_USBBUS1` | Linux SocketCAN, Windows PCAN |
| Vendor CAN FD | `CANFDTransport` | MiniCANFD device/channel | Windows and Linux |
| python-can CAN FD | `PythonCANFDTransport` | `COM5`, `/dev/ttyACM0` | SLCAN FD and python-can backends |
| Linux SocketCAN FD | `SocketCANFDTransport` | `can0` | Linux |

Backend configuration, adapter prerequisites, frame conversion and validation
are documented in [Backend configuration](docs/python-can.md),
[MiniCANFD](docs/minicanfd.md), and [Adapter enumeration](docs/adapter-enumeration.md).

## Minimal lifecycle

Construction is side-effect free. `open()` acquires the backend and starts the
transport; always close it in `finally`:

```python
from linkerbot_transport.can import CANTransport

transport = CANTransport("can0")
transport.open()
try:
    health = transport.get_health()
    print(health.state, health.generation)
finally:
    transport.close()
```

PCAN and SLCAN use the same lifecycle with their backend-specific channel and
options. MiniCANFD setup and channel enumeration use the APIs in the linked
backend guides.

## Public API at a glance

Import transports from `linkerbot_transport.can` and
`linkerbot_transport.canfd`; shared contracts, errors, routes, receipts,
`SharedTransportPool`, and `TransmitArbiter` are exported from
`linkerbot_transport`. A send receipt confirms backend acceptance; it does not
confirm device execution. Receive routes are exact, bounded, and tied to one
connection generation.

Read [Transport runtime](docs/runtime.md) for route subscriptions, health
metrics, receipts, latest-wins/FIFO arbitration, batch admission, and caller
draining. The [documentation index](docs/README.md) lists all guides.
