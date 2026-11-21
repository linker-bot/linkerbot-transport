# MiniCANFD 动态库：Windows 与 Linux

本指南对应当前 `main` 源码。
安装方式见 [README](../README_zh.md#安装当前源码)。

Windows 使用 `HCanbus.dll`，Linux 使用 `libcanbus.so`。厂商二进制和原始协议材料
不随 Python 包分发。

## 使用方式

```python
from linkerbot_transport.canfd import CANFDConfigOptions, CANFDTransport

transport = CANFDTransport(
    device_index=0,  # Windows: DLL 扫描所得的 devNum
    channel_index=0,
    library_path=r"C:\MiniCANFD\HCanbus.dll",
    config=CANFDConfigOptions(nom_baud=1_000_000, dat_baud=5_000_000),
)
try:
    transport.open()
    health = transport.get_health()
finally:
    transport.close()
```

Windows 必须安装厂商 USB 驱动，以及匹配 Python 进程位数的 DLL 和 Visual C++
运行库。64 位 Windows 上的 32 位 Python 仍需 32 位 DLL。优先使用明确的绝对路径，
相关依赖 DLL 放在同一目录；该目录的加载句柄保留到进程结束，以支持延迟加载。

`library_path` 优先于 `LINKERBOT_CANFD_LIB`。未传路径时，在 `open()` 读取环境变量；
也未设置环境变量时才使用系统及默认目录搜索。显式路径或环境路径加载失败会直接报错，
不会悄悄换另一份库。使用共享池时保持环境配置固定，推荐显式路径以明确连接身份。
构造阶段不会加载 DLL、扫描 USB 或创建收发线程。

默认搜索依次使用系统加载器、Linux 的 `/usr/local/lib`，以及包安装目录中的
`vendor/libcanbus` 和历史兼容目录 `hand`。SDK 不再自动追加当前工作目录；此前只把库放在
启动目录的应用，需要显式传入 `library_path="./libcanbus.so"`（Windows 为
`"./HCanbus.dll"`），或将 `LINKERBOT_CANFD_LIB` 设为所选文件路径，推荐绝对路径。
显式路径和环境路径均展开 `~` 并按文件的绝对真实路径加载、缓存，单独的文件名同样表示
所选的本地文件，不作为系统库名搜索。`CANFDTransport` 的显式路径在构造时固定；环境路径
在打开时解析，直接枚举和底层接口则在调用时解析路径。

系统加载器仍遵循进程的动态库搜索配置（例如 Linux 的 `LD_LIBRARY_PATH`）；本变更移除
SDK 自行添加的工作目录回退，不限制系统配置或显式选择的目录。动态库加载会执行原生代码，
所选安装位置和显式文件应由应用管理。

## 显式枚举 FD 通道

`enumerate_minicanfd_channels()` 只扫描适配器并查询通道类型，不打开 CAN 通道、不配置速率、不收发 CAN 帧，
不识别连接的灵巧手。导入函数本身不加载动态库。

Windows 扫描返回原生通道编号，可以直接枚举：

```python
from linkerbot_transport.canfd import CANFDTransport, enumerate_minicanfd_channels

result = enumerate_minicanfd_channels(r"C:\MiniCANFD\HCanbus.dll")
for failure in result.failures:
    print(failure.stage, failure.device_index, failure.channel_index, failure.error)
for channel in result.channels:
    transport = CANFDTransport(
        device_index=channel.device_index,
        channel_index=channel.channel_index,
        library_path=channel.library_path,
    )  # 构造后仍关闭；明确选择目标和通信参数后才调用 open()
```

返回 `MiniCANFDEnumerationResult`，包含不可变元组 `channels` 和 `failures`。
每个 `MiniCANFDChannel` 保存 `device_index`、`channel_index` 和已加载库的路径，
不会把系统加载器找到的库误解释为工作目录下的同名文件。
`MiniCANFDEnumerationFailure` 保留失败阶段、相关原生索引以及 `TransportError`；
底层异常在 `error.cause` 中保留。通道列表为空时应同时检查失败列表：无适配器、
只有经典 CAN/LIN 通道、库缺失、扫描失败和布局未知是不同情况。

Linux V1.3 的 `CAN_ScanDevice()` 只返回物理设备数，`Dev_Info` 没有通道总数，
`CAN_GetCHType(devNum, chNum)` 也未定义越界查询的有效范围。默认接线约定为每块
适配器先检查通道 0：省略 `linux_channels` 时，按实际扫描数量检查所有适配器的
通道 0，无需提前知道数量。通道 0 不是 FD 或类型查询失败时，继续检查通道 1；
若两路都不能枚举为 FD，则返回诊断，不尝试更大的通道编号。例如扫描到三块适配器时，
首先检查
`(0, 0)`、`(1, 0)`、`(2, 0)`：

```python
result = enumerate_minicanfd_channels(
    "/opt/minicanfd/libcanbus.so",
)
```

身份探测由上层 SDK 执行。`linkerbot-hand` 先探测通道 0；无身份候选且探测连接
已成功关闭后，再请求同一适配器的通道 1。通道 0 已被类型检查排除时，底层枚举直接
检查通道 1；只有 FD 通道进入身份探测，两路均无候选时由 hand 报告
`no_hand_found`。旧版设备级库仅支持通道 0，枚举、占用和清理失败仍保留诊断。

需要覆盖默认选择时，按厂商规格或已确认的接线显式提供 `linux_channels`，例如
`{0: (0, 1), 1: (0,)}` 表示设备 0 的通道 0/1、设备 1 的通道 0。显式映射保持严格
行为：未配置的已扫描设备逐项返回 `layout` 诊断，空序列表示有意跳过该设备，
指定但未扫描到的设备同样给出诊断。`linux_channels={}` 不会启用默认布局。
默认通道 0 或显式配置的其他通道都需要经过类型检查：`1` 才列入 FD 通道，
`0` 为经典 CAN、`2` 为 LIN。
未知类型或查询失败会保留诊断，并继续检查其余通道。Windows 不接受 `linux_channels`。

带显式 Init/Exit 的 Linux 库使用 `CAN_GetCHType(devNum, chNum)`；Windows 使用
`CAN_GetDevType(devNum)`，并接受已核实的 DLL 导出拼写 `CAN_GeDevType`。
Linux 库也可能提供不含 Init/Exit 和 `CAN_GetCHType` 的设备级 API，通过单参数
`CAN_GetDevType(device_index)` 读取设备类型，生命周期由库内部管理。该 API 同样默认
检查每块适配器的通道 0，目前只支持该通道；显式指定其他通道时仍报告 `layout`，
不会把设备类型推广到其他通道。

带 Init/Exit 的库使用 `CAN_GetCHType`；只导出 Init/Exit 其中一个，或不符合上述已核实
导出组合的库，仍返回诊断，不静默忽略生命周期要求。

枚举与本进程的打开/关闭操作协调。同一库仍有打开或关闭失败待清理的通道时，
返回 `busy` 诊断，不刷新厂商全局枚举表；先完成已有连接的关闭再重扫。
打开同一库的第二个设备或其他已确认通道时，复用首个连接的扫描快照，不再次调用
`CAN_ScanDevice()`。超出快照的设备索引会被拒绝；插拔后应关闭全部旧连接再重扫，
不能在旧连接仍存活时更新索引。最后一个通道成功关闭才使快照失效。
它不调用可能内部打开/关闭 USB 的 `CAN_ReadDevInfo`，也不保证其他进程未占用设备。
Linux 扫描借用共享 Init/Exit 引用；Exit 失败保留待清理的库状态，返回 `cleanup`
诊断，后续枚举或打开会先重试清理，成功前不会再次 Init。扫描索引可能因插拔而改变，
枚举到通道也不等于它当前可打开，调用方仍须处理实际连接失败并验证目标设备身份。

## ABI 和通道语义

依据厂商《动态库接口函数使用说明》Windows/Linux V1.3，结构体相同，但函数参数不同：

| 接口 | Windows HCanbus.dll | Linux libcanbus.so |
| --- | --- | --- |
| 进程初始化 / 退出 | DLL 不导出 `LibCANbus_Init/Exit`，不调用 | 有导出时共享 Init/Exit；设备级 API 由库内部管理 |
| 扫描 | `CAN_ScanDevice()` | `CAN_ScanDevice()` |
| 打开 / 关闭 | `CAN_OpenDevice(devNum)` / `CAN_CloseDevice(devNum)` | 相同函数名，参数为 `(device_index, channel_index)` |
| FD 初始化 | `CANFD_Init(devNum, config*)` | `CANFD_Init(device_index, channel_index, config*)` |
| 收发 | `(devNum, frames*, count, timeout_ms)` | `(device_index, channel_index, frames*, count, timeout_ms)` |
| 适配器信息 | `CAN_ReadDevInfo(devNum, info*)` | `CAN_ReadDevInfo(device_index, info*)` |

Windows 的 `device_index` 对应 DLL 枚举的**通道编号**，`channel_index` 必须为 0；
传入其他值在打开前拒绝。不能假设双通道适配器编号等于 `device * 2 + channel`，
也不能把 Linux 的两级编号原样复制过来。Windows 设备插拔后枚举顺序可能变化，
应用应确认目标通道再打开。手型号自动发现不属于本层的工作。

使用 `ctypes.WinDLL` 匹配 Windows `__stdcall`（尤其是 32 位进程），Linux 使用 CDLL。
`CanFD_Config` 为 20 字节：`NomPre` 为 16 位、偏移 8；`Config/Model/Cantype` 分别在
偏移 17/18/19。`CanFD_Msg` 为 80 字节，`DLC` 在偏移 9，64 字节数据从偏移 16 开始。
`Dev_Info` 为 160 字节。DLC 仍为 0–15 编码，时间戳为适配器原始微秒值。

对本地 x64 DLL 做过静态导出与反汇编核对：初始化使用第二个参数作为配置指针，配置
复制长度 20 字节，发送帧步长 80 字节，与上述定义一致。核对样本 SHA-256：
`1c7a7fdb2b78f276cea84127f9ad72af6f7fe4004132f151586cd7899db8c5a8`。
这只是对该样本的静态证据，不代表所有厂商 DLL 版本已完成硬件验收。

## 发送帧校验

单帧和批量发送在调用厂商 `CANFD_Transmit` 前，按消息的 `frame_type` 或继承的
`CANFDConfigOptions.frame_type` 校验有效帧类型。经典帧（未设置 `0x04`）的 DLC
不得超过 8，BRS（`0x08`）必须同时启用 FD。无效组合报告 `ValidationError`，整次
批量调用不发送任何帧；正常 FD DLC 9–15 及其他厂商控制位不受此校验限制。

## 接收数据、诊断与批次顺序

接收按同一份 V1.3 手册第 8–9 页的表 1–3 解释状态字段，两种平台使用相同策略：

- `RemoteFlag` 非零的 RTR 没有设备载荷；`FrameType & 0x01` 表示发送回传，两者均不进入
  设备回复路由，也不读取其 Data 或按 DLC 解析载荷。`FrameType & 0x02` 明确表示传输失败，
  报告 `TransportError`，不生成设备数据，即使它同时带有发送回传标记。
- `BusSatus` 的 `0x08` 为总线正常、`0x20` 为发送邮箱空，它们及其组合不是错误。
  `0x01/0x02/0x04/0x10/0x40/0x80` 分别指错误警告、错误被动、总线离线、仲裁错误、
  接收寄存器溢出和满，作为诊断上报。`ErrSatus` 是非组合错误码，非零值按原值报告，
  不把它当位掩码；未知码也保留原值。`TECounter`、`RECounter` 非零本身不拒绝当前数据。
- 成功的数据帧可能附带异常总线状态快照；快照不等于本帧载荷无效。先交付包含该帧的有效
  前缀，再报告诊断。异常文本包含设备/通道、ID、原始微秒时间戳、FrameType、BusSatus、
  ErrSatus 和收发错误计数；RTR/回传上附带的异常诊断也不会静默丢弃。

原生 `CANFD_Receive` 一次已取出的整批报文不能退回驱动。批次中遇到错误诊断或解析异常时，
先交付此前的有效报文，下一次参数有效的 `receive()` 调用在读取后缀或再次调用驱动前报告
该异常一次，随后继续原批次的后缀。例如 A → 坏帧 → B 依次交付 A、报告异常、交付 B。
没有前置有效数据时立即抛错；多个错误各按原顺序报告。改变 `max_frames` 也不会越过未报告
错误，返回的数据数量仍受本次上限约束。

最多暂存一个已取出的原生批次，大小不超过 2,500 帧；默认容量内继续复用预分配接收数组。
关闭在等待已有接收结束后丢弃待报告异常和未交付后缀，原生关闭失败待重试时同样禁止继续
读取这些旧数据；恢复后的新连接不重放它们。`KeyboardInterrupt` / `SystemExit` 立即传播。
Dispatcher 继续使用已有连续接收错误策略；本改动不增加 CAN 请求或修改公共消息结构。

## 异常与验收

打开/初始化失败保留原始原因。若初始化之后的关闭也失败，SDK 保留该句柄，后续
`close()` 或 `recover()` 重试清理；共享连接池不会把这个失败项当成已经释放。
普通初始化失败后的清理若收到首次进程中断，优先传播中断并以初始化失败作为 cause，
保留待清理句柄；初始化本身已是进程中断时仍保留原中断。
成功恢复不重发已提交命令。正常关闭会等待在途收发结束，有显式 Init/Exit 的 Linux 库
在最后一个用户关闭后才释放进程级资源。库句柄不主动卸载。
若通道已关闭但 `LibCANbus_Exit` 失败，`close()` 仍报错并保留清理重试；再次关闭只重试
尚未完成的库清理，不重复关闭通道或扣减引用，也不会影响随后已打开的新连接。

关闭时，等待并发关闭、在途收发、设备信息读取、枚举及生命周期锁共用一秒期限。
原生 I/O 未在期限内返回时，抛出 SDK 的 `TimeoutError`（`code=timeout`），禁止新的 I/O，
保留尚未完成的设备与生命周期清理所有权，不并发调用 `CAN_CloseDevice` 释放在用资源。
若只有生命周期锁等待超时，已成功关闭的通道不再重复关闭，后续仅重试剩余清理。
`CANFDMessageDispatcher.stop()` 仍包含已有的有界线程退出等待，因此其总用时可能超过一秒；
它会保留底层关闭超时，不将其覆盖为普通的线程退出失败。

通过 `CANFDTransport` 关闭失败后，健康状态为 `FAULTED`，连接池保留待清理项；
原生操作返回后可再次 `close()`、调用池的 `close_idle(key)`，或通过 `recover()` / 再次获取
租约先完成旧连接清理再打开新连接，不重放队列命令。并发关闭的等待者超时不会影响仍在
进行的关闭尝试。此期限约束 Python 锁等待，不能抢占已经进入原生的 `CAN_CloseDevice`
或 `LibCANbus_Exit`；不承诺这两个无超时参数的厂商函数一定在一秒内返回。

回归测试覆盖两套函数参数、结构体二进制布局、单帧/批量收发、DLL 路径和依赖目录、
初始化失败与清理重试。CI 提供 Windows/Linux、Python 3.10/3.13 的无硬件测试。
Windows 真机还需验证：驱动加载与枚举、FD/BRS 和 DLC、双向长时间负载、插拔与恢复、
多次打开关闭。当前未宣称 Windows 实机通过，静态 DLL 检查和测试桩不替代实机验证。
