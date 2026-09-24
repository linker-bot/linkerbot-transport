# MiniCANFD 动态库：Windows 与 Linux

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
transport.open()
try:
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
`CAN_GetCHType(devNum, chNum)` 也未定义越界查询的有效范围。当前可用资料未提供
可可靠识别型号及其通道数的完整对应表。因此需要按厂商规格或已确认的接线提供
`linux_channels`，不能由“扫描到 1 个设备”推断出 2 个通道：

```python
result = enumerate_minicanfd_channels(
    "/opt/minicanfd/libcanbus.so",
    linux_channels={0: (0, 1), 1: (0,)},
)
```

上例明确声明设备 0 有通道 0/1、设备 1 有通道 0，不能直接照搬为所有适配器的布局。
未提供布局的已扫描设备逐项返回 `layout` 诊断，**不会自动猜测通道**；空序列表示
有意跳过该设备。指定但未扫描到的设备同样给出诊断。用户提供的通道必须已经确认
存在，SDK 只查询这些通道的类型：`1` 才列入 FD 通道，`0` 为经典 CAN、`2` 为 LIN。
未知类型或查询失败会保留诊断，并继续检查其余通道。Windows 不接受 `linux_channels`。

带显式 Init/Exit 的 Linux 库使用 `CAN_GetCHType(devNum, chNum)`；Windows 使用
`CAN_GetDevType(devNum)`，并接受已核实的 DLL 导出拼写 `CAN_GeDevType`。
Linux 库也可能提供不含 Init/Exit 和 `CAN_GetCHType` 的设备级 API，通过单参数
`CAN_GetDevType(device_index)` 读取设备类型，生命周期由库内部管理。该 API 仍要求
明确提供 `linux_channels`，目前只支持已确认的通道 0；不会把设备类型推广到未经验证的
其他通道。两只已确认设备可传
`linux_channels={0: (0,), 1: (0,)}`。

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

## 异常与验收

打开/初始化失败保留原始原因。若初始化之后的关闭也失败，SDK 保留该句柄，后续
`close()` 或 `recover()` 重试清理；共享连接池不会把这个失败项当成已经释放。
成功恢复不重发已提交命令。正常关闭会等待在途收发结束，有显式 Init/Exit 的 Linux 库
在最后一个用户关闭后才释放进程级资源。库句柄不主动卸载。
若通道已关闭但 `LibCANbus_Exit` 失败，`close()` 仍报错并保留清理重试；再次关闭只重试
尚未完成的库清理，不重复关闭通道或扣减引用，也不会影响随后已打开的新连接。

本地回归覆盖两套函数参数、结构体二进制布局、单帧/批量收发、DLL 路径和依赖目录、
初始化失败与清理重试。CI 提供 Windows/Linux、Python 3.10/3.13 的无硬件测试。
Windows 真机还需验证：驱动加载与枚举、FD/BRS 和 DLC、双向长时间负载、插拔与恢复、
多次打开关闭。当前未宣称 Windows 实机通过，静态 DLL 检查和测试桩不替代实机验证。
