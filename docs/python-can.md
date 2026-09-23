# python-can 后端配置与 Windows 验收

以下新增配置适用于当前源码；未在 1.0.0 发布包中发布。

| 用途 | 入口 | 通道示例 | 系统 / 前提 |
| --- | --- | --- | --- |
| 经典 CAN / PCAN | `CANTransport(channel, "pcan", bitrate=...)` | `PCAN_USBBUS1` | Windows / PEAK 驱动与匹配 Python 位数的 PCAN-Basic |
| CAN FD / SLCAN | `PythonCANFDTransport(channel, "slcan", ...)` | `COM5`、`/dev/ttyACM0` | Windows、Linux / pyserial、CANable 2.0 FD 扩展固件 |
| CAN FD / SocketCAN | `SocketCANFDTransport(channel, ...)` | `can0` | Linux / 已配置链路；仅显式 `auto_reconfigure=True` 才调整系统链路 |
| 通用 python-can CAN FD | `PythonCANFDTransport(channel, interface, ...)` | 由后端指定 | 驱动必须支持 FD；未逐一验证 python-can 的所有硬件后端 |

源码安装：`python -m pip install -e ".[all]"`。构造对象不打开设备，
`open()` 才加载驱动或打开串口；结束时在 `finally` 中 `close()`。
Windows 路径不调用 Linux 的 `ip`、`AF_CAN` 或 `fileno()`。

## 参数和消息契约

- `bitrate` 是仲裁速率，`data_bitrate` 是 FD 数据速率，单位均为 bit/s。
  SLCAN 的数据速率限于 python-can 4.6.1 支持的 2 / 5 Mbps。
- `bus_options={"tty_baudrate": 1_000_000}` 是串口速率。
  `COM5@1000000` 等价；两处指定不同串口速率会报错。
  `sleep_after_open`、`rtscts` 等标量参数由 python-can 接收。
- 显式 `timing=can.BitTimingFd(...)` 可配置其他后端的时序；其中的双速率须与
  具名速率一致。SLCAN 只使用双速率，不通过串口传递完整时序分段参数。
- `CANFDMessage.dlc` 是协议编码 0–15，发送到 python-can 时换算为字节数。
  显式 DLC 容量大于数据长度时补零至线上的容量；接收保留线上全部数据。
- `frame_type=None` 继承后端 FD 模式但不开 BRS；`0x04` 为 FD，`0x0C` 为
  FD+BRS，`0x00` 为经典帧。同一 FD 总线可收发经典帧。经典帧最多 8 字节。
- 不支持通过 python-can 的 FrameType 传入厂商定时发送等其他控制位。
- 收到远程帧时不生成设备数据；错误帧上报传输异常，由现有连续错误策略处理。
  python-can 接收时间转换为整数微秒；其时间基准由驱动提供，不能与厂商适配器时间戳
  直接相减。事务计时继续使用路由层的单调时钟。
- `SendReceipt` 成功表示驱动接受帧，不表示总线 ACK 或设备执行成功。
  恢复连接不会重发历史控制指令，恢复后需重新订阅路由。

参数在构造时快照，忽略 python-can 外部配置文件与环境变量。
`SharedTransportPool` 使用 `transport.pool_key` 复用相同配置；同一进程中的同后端、
同通道不能在未关闭旧连接时切换速率或 FD 模式。不同别名、不同后端、不同进程是否指向
同一硬件，库无法自动判断，应用应统一通道命名并集中管理连接。

## 验证范围

自动化测试使用真实 python-can 的 VirtualBus 和 SLCAN 解析器；SLCAN 的串口端点由
测试桩提供，校验初始化命令、标准/扩展 FD、BRS、DLC、路由、恢复及失败清理。
CI 配置包含 Windows/Linux、Python 3.10/3.13；配置存在不等于运行成功。
这些测试不证明 USB 硬件性能。Windows 实机验收应记录：

1. Windows/Python/驱动/适配器固件版本，通道、终端电阻和双速率配置。
2. 使用独立接收端核对标准/扩展经典帧、FD 与 FD+BRS、0–64 字节 DLC 转换。
3. 多次打开关闭、断开 USB 后报错、重连恢复、配置冲突、旧路由失效，无历史命令重放。
4. 按真实报文长度和往返帧数逐级升频，最终目标负载连续至少 1 分钟；记录实际完成率、
   超时、丢帧、队列峰值、延迟分位数及 CPU。SLCAN ASCII 编码与串口链路可能先于 CAN
   总线成为瓶颈，不能套用 Linux SocketCAN 的已有上限。

本次本机没有 Windows 主机上的实机验证结果。
