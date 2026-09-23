# MiniCANFD 动态库：Windows 与 Linux

Windows 使用 `HCanbus.dll`，Linux 使用 `libcanbus.so`。以下 Windows 修复属于
当前源码，尚未包含在 1.0.0 发布包中。不随 Python 包分发厂商二进制或原始协议材料。

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

## ABI 和通道语义

依据厂商《动态库接口函数使用说明》Windows/Linux V1.3，结构体相同，但函数参数不同：

| 接口 | Windows HCanbus.dll | Linux libcanbus.so |
| --- | --- | --- |
| 进程初始化 / 退出 | DLL 不导出 `LibCANbus_Init/Exit`，不调用 | 共享 Init/Exit 生命周期 |
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
成功恢复不重发历史命令。正常关闭会等待在途收发结束，Linux 最后一个用户关闭后
才释放进程级资源。库句柄不主动卸载。

本地回归覆盖两套函数参数、结构体二进制布局、单帧/批量收发、DLL 路径和依赖目录、
初始化失败与清理重试。CI 提供 Windows/Linux、Python 3.10/3.13 的无硬件测试。
Windows 真机还需验证：驱动加载与枚举、FD/BRS 和 DLC、双向长时间负载、插拔与恢复、
多次打开关闭。当前未宣称 Windows 实机通过，静态 DLL 检查和测试桩不替代实机验证。
