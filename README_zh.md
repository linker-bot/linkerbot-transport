# linkerbot-transport

[English](https://gitea.linkerhub.work/zhuzhixiang/linkerbot-transport/src/branch/main/README.md) |
[简体中文](README_zh.md)

`linkerbot-transport` 是 Linkerbot 设备 SDK 共用的物理通信层。当前提供经典
CAN、厂商 CAN FD 和 Linux SocketCAN FD 实现，以及传输边界需要的稳定异常、
精确接收路由、发送回执、共享连接和有界发送调度能力。

本包不依赖 ROS、灵巧手型号、机械臂型号或运动学库。

## 安装

只安装应用需要的后端：

```bash
pip install "linkerbot-transport[can]"
pip install "linkerbot-transport[canfd]"
```

如果只需要异常类型和通用并发契约，可以安装不带 `python-can` 的基础包：

```bash
pip install linkerbot-transport
```

## 公共 API

唯一公共命名空间是 `linkerbot_transport`。经典 CAN 和 CAN FD 应用都应构造
具有显式生命周期的 Transport：

```python
from linkerbot_transport.can import CANTransport
from linkerbot_transport.canfd import (
    CANFDMessage,
    CANFDTransport,
    SocketCANFDTransport,
)
```

本包不再提供历史上的 `linkerbot.comm`、`linkerbot.exceptions`、
`linkerbot.queue` 或 `linkerbot.relay` 命名空间。自动启动的 dispatcher
只是内部通道 worker，不属于用户 API。设备 SDK 和应用只能使用
`CANTransport`、`CANFDTransport` 或 `SocketCANFDTransport`。

稳定异常位于 `linkerbot_transport.errors`。调用方应捕获所需的最具体异常；
需要统一映射错误时，可以在最后一层捕获公共基类 `LinkerbotError`：

```python
from linkerbot_transport.errors import (
    LinkerbotError,
    TimeoutError,
    TransportError,
)

try:
    result = receipt.result(timeout=0.1)
except TimeoutError:
    # 有界等待超时；receipt 之后仍可能完成。
    ...
except TransportError:
    # 物理后端拒绝发送或发送失败。
    ...
except LinkerbotError:
    # 在最终边界处理参数、生命周期和队列错误。
    ...
```

## 生命周期

构造 Transport 不会打开设备，也不会启动工作线程。连接必须由调用方显式管理：

```python
from linkerbot_transport.can import CANTransport

transport = CANTransport("can0")
transport.open()
try:
    health = transport.get_health()
finally:
    transport.close()
```

`open()` 和 `close()` 幂等。`recover()` 会替换连接、增加连接代次并关闭旧路由，
不会重放旧代次中的控制帧。恢复后必须重新注册接收路由。

多个设备共用同一个物理通道时，使用 `SharedTransportPool`。第一个 lease 打开
Transport，最后一个 lease 关闭 Transport，从而避免重复建立总线连接。

## 精确接收路由

CAN 和 CAN FD 使用精确、独占、有界的接收路由，不向所有订阅者广播帧。接收线程
从帧中提取一次 key，执行一次哈希查找，再把帧放入对应路由的有界 inbox。
经典 CAN 的 key 由完整仲裁 ID 和标准帧/扩展帧标志共同组成：

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

协议响应分布在多个仲裁 ID 时，可以原子注册一组路由：

```python
routes = transport.subscribe_routes(
    (
        CANRouteKey(arbitration_id=0x02000000, is_extended_id=True),
        CANRouteKey(arbitration_id=0x02006000, is_extended_id=True),
    ),
    queue_capacity=64,
)
```

容量限制作用于整组数据流。注册要么一次取得全部 key，要么一个也不取得；关闭订阅
会释放整组 key。所有命中帧按到达顺序进入同一个队列。

SDK actor 可在订阅时传入 `on_available=event.set`。空路由首次变为可读，以及路由关闭
或中止时，都会以边沿方式通知 actor，使其可以直接消费 Transport 队列，不再增加第二层
mailbox 线程。回调在路由路径同步执行，必须立即返回，不能执行协议事务或阻塞；回调抛出
异常时会记录日志并禁用该路由的后续通知，但不会终止 RX。

`RoutedMessage` 包含单调时钟接收时间、连接代次和该代次内的序号。inbox 已满时，
新帧会被丢弃，同时增加路由和 Transport 的 overflow 计数。路由未命中、key 提取
错误、旧代次帧、队列深度和关闭时丢帧也都会记录在缓存健康状态中。

## 健康状态

`get_health()` 只读取缓存，不产生设备 I/O。它同时报告连接状态、连接代次、最近错误、
连续错误、重连次数、路由状态，以及整个物理通道跨连接代次累计的 `tx_frames`、
`rx_frames`、`tx_bytes` 和 `rx_bytes`。

发送计数只在物理后端成功接收帧后增加。接收计数在后端交出帧时增加，即使之后发生
路由未命中、队列溢出或旧代次拒绝。字节数只计算 DLC 对应的数据字段，不包含仲裁、
CRC、位填充和帧间隔，因此不能直接视为 CAN 总线利用率。

关闭、中止或恢复 Transport 会关闭当前全部路由并唤醒阻塞中的接收方。旧 callback
代次到达的帧仍会计入统计，但不会投递到新路由。

## 发送回执

每次 `transport.send()` 都返回 `SendReceipt`。完成表示物理后端已经接收发送请求，
而不是仅仅进入软件队列。需要确认结果时调用 `receipt.result(timeout=...)`：

- 成功返回 `SendOutcome.DISPATCHED`，并携带连接代次及 accepted、claimed、completed
  单调时间戳。
- 被新目标覆盖返回 `SendOutcome.SUPERSEDED`。
- 物理发送失败抛出稳定的 Transport 异常，同时 receipt 状态为
  `SendOutcome.FAILED`。

Transport 停止或故障时，所有未完成回执都会失败；控制帧不会被自动重试或在恢复后
重放。

## 单写入者和 latest-wins

多个设备或命令族共用物理通道时，使用 `TransmitArbiter` 作为唯一写入者。连续控制
命令使用按 `(endpoint_id, command_family)` 区分的容量 1 latest slot，离散操作使用
独立的有界 FIFO：

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

`deadline_at_s` 是绝对 `time.monotonic()` 截止时间，不是相对 timeout。关闭或中止
arbiter 会失败旧连接代次的全部待发送任务，重新打开后不会重放。arbiter 健康状态是
零 I/O 快照，包含 pending、accepted、dispatched、superseded、failed 和容量拒绝计数。

`replace_latest_if_pending()` 只覆盖尚未被 worker 领取的目标；旧目标已经在途或不存在
时返回 `None`，不会再排入第二个目标，从而保护协议请求与响应的相关性。

已经拥有 I/O actor 的运行时可使用 `TransmitMode.CALLER`，避免再创建常驻 TX worker。
提交仍然只负责接纳有界任务并返回 receipt，随后由 actor 调用 `drain()` 完成实际发送。
多个 actor 并发调用时，第一个调用者临时取得单写入权，并继续处理其持有期间新进入的
任务。`drain()` 会执行 Transport I/O，只能由 I/O actor 调用，不能放在用户或 ROS
callback 中：

```python
from linkerbot_transport import TransmitArbiter, TransmitMode

arbiter = TransmitArbiter(transport.send, mode=TransmitMode.CALLER)
arbiter.open(generation=generation)
receipt = arbiter.submit_fifo(frame, endpoint_id="left-hand")
arbiter.drain()
receipt.result(timeout=0.1)
```

## CAN FD 说明

CAN FD 批量发送失败时，后端无法可靠判断部分成功数量，因此同一批次的所有 receipt
都会失败。设备运行时应通过精确有界路由接收数据；内部 worker 不属于 1.0 兼容契约。

CAN FD 消息默认使用 FrameType `0x04`，即启用 FD 但不启用位速率切换。只有设备明确
支持 BRS 时才设置 `frame_type=0x0C`。`frame_type=None` 表示继承厂商接口配置；
SocketCAN 继承后端的 `fd` 模式，并保持 BRS 关闭。
