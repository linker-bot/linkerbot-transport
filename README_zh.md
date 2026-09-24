# linkerbot-transport

`linkerbot-transport` 是 Linkerbot 设备 SDK 共用的物理通信层，提供经典 CAN、
CAN FD、精确接收路由、可观测发送回执、共享连接租约和有界发送调度。

[English](README.md)

本包不依赖 ROS、灵巧手型号、机械臂型号或运动学库。

## 安装当前源码

当前 hand 和 transport 版本尚未发布到包仓库。需要同时对两个源码仓库使用 editable
安装，这样修改会立即生效。以下命令假设两个仓库位于 `~/src`，并创建名为
`py-linkerbot` 的虚拟环境：

### uv

```bash
cd ~/src
uv venv py-linkerbot --python 3.12
source py-linkerbot/bin/activate

uv pip install -e "./linkerbot-transport[all]"
uv pip install -e ./linkerbot-hand
```

Windows PowerShell 激活环境使用 `./py-linkerbot/Scripts/Activate.ps1`，然后执行上面的
两个安装命令。

### pip

```bash
cd ~/src
python3.12 -m venv py-linkerbot
source py-linkerbot/bin/activate
python -m pip install --upgrade pip setuptools wheel

python -m pip install -e "./linkerbot-transport[all]"
python -m pip install -e ./linkerbot-hand
```

确认导入路径来自源码目录：

```bash
python -c "import linkerbot_hand, linkerbot_transport; print(linkerbot_hand.__file__); print(linkerbot_transport.__file__)"
```

### 安装已发布版本

按应用使用的后端安装：

```bash
python -m pip install \
  --index-url https://gitea.linkerhub.work/api/packages/zhuzhixiang/pypi/simple \
  --extra-index-url https://pypi.org/simple \
  "linkerbot-transport[can]==1.0.0"
```

python-can SLCAN/CAN FD 使用 `canfd`；只使用公共异常和并发原语时安装基础包：

```bash
python -m pip install \
  --index-url https://gitea.linkerhub.work/api/packages/zhuzhixiang/pypi/simple \
  --extra-index-url https://pypi.org/simple \
  "linkerbot-transport[canfd]==1.0.0"

python -m pip install \
  --index-url https://gitea.linkerhub.work/api/packages/zhuzhixiang/pypi/simple \
  "linkerbot-transport==1.0.0"
```

## 后端选择

| 总线 | 入口 | 常见通道 | 平台 |
| --- | --- | --- | --- |
| 经典 CAN | `CANTransport` | `can0`、`PCAN_USBBUS1` | Linux SocketCAN、Windows PCAN |
| 厂商 CAN FD | `CANFDTransport` | MiniCANFD 设备/通道 | Windows、Linux |
| python-can CAN FD | `PythonCANFDTransport` | `COM5`、`/dev/ttyACM0` | SLCAN FD 与 python-can 后端 |
| Linux SocketCAN FD | `SocketCANFDTransport` | `can0` | Linux |

后端配置、适配器依赖、帧转换和验证见[后端配置](docs/python-can.md)、
[MiniCANFD](docs/minicanfd.md)和[适配器枚举](docs/adapter-enumeration.md)。

## 最小生命周期

构造阶段不产生设备副作用。`open()` 获取后端并启动传输，使用 `finally` 关闭：

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

PCAN 和 SLCAN 使用相同的生命周期，只需替换通道和后端参数。MiniCANFD 的配置和
通道枚举使用对应后端文档中的入口。

## 公共 API 概览

Transport 从 `linkerbot_transport.can` 和 `linkerbot_transport.canfd` 导入；
公共契约、异常、路由、回执、`SharedTransportPool` 和 `TransmitArbiter` 从
`linkerbot_transport` 导入。发送回执表示后端已接收发送请求，不表示设备已经执行。
接收路由是精确、有界的，并绑定到一个连接代次。

路由订阅、健康指标、发送回执、latest-wins/FIFO 仲裁、批量准入和调用方排空方式见
[传输运行时](docs/runtime_zh.md)，完整文档列表见[文档索引](docs/README.md)。
