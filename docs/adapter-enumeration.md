# PCAN / SLCAN 适配器枚举

以下 API 属于当前源码，尚未包含在 1.0.0 发布包。枚举只读取驱动或操作系统信息，
不打开 CAN 通道或串口，不发送 CAN/SLCAN 指令，不更改速率。

```python
from linkerbot_transport.discovery import (
    enumerate_pcan_channels,
    enumerate_slcan_channels,
)

pcan = enumerate_pcan_channels()
slcan = enumerate_slcan_channels()  # 自动筛选 CANable 2.0 USB 候选
selected = enumerate_slcan_channels(ports=("COM5",))  # 明确指定其他适配器

for channel in pcan.channels:
    print(channel.interface, channel.channel, channel.supports_fd)
for failure in pcan.failures:
    print(failure.stage, failure.channel, failure.error)
```

返回 `PythonCANEnumerationResult`，`channels` 和 `failures` 均为不可变元组。
`PythonCANChannel` 保存原生后端名、通道名、FD 能力，以及可用的设备编号、控制器号、
描述、串口序列号和 USB VID/PID。这些信息描述适配器，不代表灵巧手型号或身份。

PCAN 使用 python-can 提供的 PCANBasic 包装读取 `PCAN_ATTACHED_CHANNELS`，不逐个
尝试打开 `PCAN_USBBUS1..N`。为保留错误诊断，不使用会将缺库和驱动查询失败折叠为
空列表的通用检测封装。仅返回可用通道；占用、未知句柄及查询失败分别保留诊断。
此状态只是查询时的快照，实际连接仍须处理竞争占用、拔出或初始化失败。
需要安装 `linkerbot-transport[can]`、对应平台的 PCAN 驱动及 PCANBasic 动态库。

SLCAN 使用 pyserial 的串口枚举。自动模式仅选择 `16D0:117E`；其他 VID/PID 需明确
传入 `ports`，这些端口必须出现在系统枚举结果中。返回 `supports_fd=None`：USB 标识
不是固件能力证明，FD 通信仍需支持 CANable 2.0 `d/D/b/B` 扩展的固件。经典 CAN 固件
使用 `CANTransport`；FD 固件使用 `PythonCANFDTransport`，由应用或 hand 的探测策略
选择。枚举不检查串口是否被占用，因为占用检查可能需要打开端口。
安装 `linkerbot-transport[canfd]` 可获得 pyserial 依赖。

`PythonCANEnumerationFailure` 保留 `interface`、`stage`、可选 `channel` 和
`TransportError`；底层异常保存在 `error.cause`。阶段包括 `dependency`、`driver`、
`scan`、`channel`、`busy`、`unavailable`。参数错误直接抛 `ValidationError`；
操作系统/驱动错误进入结果，单个通道异常不丢弃其他有效通道。

枚举结果不承诺永久稳定的通道名，也不猜测跨驱动的物理设备对应关系。
MiniCANFD 使用独立的 [厂商库枚举入口](minicanfd.md)，不经过 PCAN/SLCAN 枚举。
