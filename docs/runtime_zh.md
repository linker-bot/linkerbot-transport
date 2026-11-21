# 传输运行时

本指南对应当前 `main` 源码；安装方式见
[README](../README_zh.md#安装当前源码)。

## 生命周期

经典 CAN 与 CAN FD 使用相同的显式生命周期：

```python
from linkerbot_transport.can import CANTransport

transport = CANTransport("can0")
try:
    transport.open()
    health = transport.get_health()
finally:
    transport.close()
```

内置高层 CAN FD Transport 打开时只启动接收 worker，同步发送走 `send_now()`；若调用方
随后选择 dispatcher 的排队 `send()`，发送 worker 才按需启动。直接构造低层
`CANFDMessageDispatcher` 默认仍提前启动，传 `start_send_worker=False` 可选择懒启动。
首次排队发送可能包含线程启动开销；启动失败时不会受理队列回执。
CAN FD 排队发送 worker 启动失败后，当前 dispatcher 停止并拒绝后续发送；
应先停止它，再新建 dispatcher，或调用其所属 Transport 的 `recover()`。
`Thread.start()` 被中断时可能已创建原生线程，因此不能重启同一个启动结果不确定的线程。

构造阶段不打开硬件或启动 worker。`open()`、`close()` 幂等；清理失败会保留在对象上，
可通过 `close()` 或 `recover()` 重试。恢复会生成新的连接代次、关闭旧路由且不重放旧命令，
恢复后需要重新注册路由。

不同线程的生命周期调用按序执行；同步回调不能在外层操作尚未结束时重入同一 Transport
的 `open()`、`close()` 或 `recover()`，此时抛 `DeviceNotReadyError`，不替换外层持有的资源。
回调返回后可重试。池中同 key 的工厂、打开、恢复或关闭正在执行时，重入申请/清理也会
拒绝，不发放新租约。普通初始化错误后的清理若收到进程中断，中断优先传播，原始错误
保留为 cause，待清理资源仍有持有者；初始化本身已是进程中断时，保留原中断优先级。

Classic 和 FD dispatcher 等待另一次底层关闭有明确期限，超时抛 `TimeoutError` 并保留
清理责任。线程 join 和发送排空分别计时，不构成“一秒内完成 stop”的总时限承诺；
没有超时参数的底层驱动关闭调用无法由 SDK 抢占。

Classic CAN 排队发送 worker 启动失败时同样停止当前 dispatcher，需要新建 dispatcher
或恢复 Transport；原始启动中断继续传播，后续发送不能重试同一个启动结果不确定的线程。

多个 SDK 设备共用物理通道时使用 `SharedTransportPool`。第一个租约打开 Transport，
最后一个租约关闭；清理失败时对象仍保留在池中。已释放的租约可再次 `close()` 重试空闲
清理，不重新打开通道或重复扣减引用，其 `transport` 属性仍不可用。
后续相同 key 的 `acquire()` 可以恢复该对象；旧租约重试不会关闭已恢复的活跃连接或替换项。

`acquire()` 抛出异常而没有返回租约时，调用 `pool.close_idle(key)` 重试该 key 的清理。
它与同 key 的 `acquire()` 串行执行，仅关闭池中没有活动租约的残留连接，不重新打开通道；
key 不存在或仍有活动租约时不执行关闭。关闭异常向调用方传播，条目保留供再次清理或恢复。

池的 factory/open/recover/close 在元数据锁外执行，一个通道关闭时仍可查询池快照或操作
其他 key。每个租约使用独立引用令牌，中断后重试不会扣掉其他租约；获取中断且没有返回
句柄时不遗留活动引用，空闲的残留连接仍由 `close_idle()` 或后续恢复处理。

## 精确接收路由

路由是独占、有界的订阅。每个帧只按一个 key 投递到匹配路由，不向所有订阅者广播。
经典 CAN 的 key 包含完整仲裁 ID 和标准帧/扩展帧标记：

```python
import can

from linkerbot_transport.can import CANRouteKey, CANTransport

transport = CANTransport("can0")
try:
    transport.open()
    with transport.subscribe_route(
        CANRouteKey(arbitration_id=0x28, is_extended_id=False),
        queue_capacity=64,
    ) as route:
        receipt = transport.send(
            can.Message(arbitration_id=0x28, data=[0xC2], is_extended_id=False)
        )
        receipt.result(timeout=0.1)
        message = route.receive(timeout_s=0.02)
finally:
    transport.close()
```

响应分布在多个仲裁 ID 时，使用 `subscribe_routes()` 原子注册一组 key。整组共享一个
有界队列并保持到达顺序；任意 key 无法取得时整组都不注册。
输入最多消耗 `max_routes + 1` 个 key，超大或无穷迭代器不会被完整物化。
`try_receive()` 仅在订阅仍活动且队列为空时返回 `None`；终止错误（包括 `TimeoutError`）
与 `receive()` 一样向调用方传播。

协议地址编码在 CAN ID 中变化时，`CANTransport` 可接收 `route_key_of`。提取器接收原始
`can.Message`，必须返回 `CANRouteKey`，只做快速计算且不能 I/O。提取异常会计数并丢弃该帧，
投递的消息仍保留原始 ID、标记和数据。

SDK actor 可在订阅时传入 `on_available=event.set`。回调在接收或终止路由的线程上同步执行，
必须立即返回，不能执行协议 I/O；路由关闭或中止时也会通知。通知在 Transport 与 Router
状态锁外执行，回调可以读取它们的 `get_health()` 缓存快照、池元数据或注销路由。
回调不会等待另一个正在执行的通知；后者返回时尝试自己的终止通知。不能在回调中等待
正在等该回调结束的同一池 key 的获取或清理。终止通知开始尝试后即不因中断重放，因为
其副作用可能已经发生；其他路由仍会在原中断传播前得到通知。尝试前被中断的通知责任
保留在 router 中，重新打开代次前须重试被中断的 router 或 subscription 关闭。
`RoutedMessage` 包含单调接收
时间、连接代次和代次内序号。队列满时丢弃新帧，路由未命中、提取异常、溢出、关闭丢帧和
代次拒绝都记录在健康状态中。

## 健康状态与连接代次

`get_health()` 只读取缓存，返回连接状态、代次、最近/连续错误、重连次数、路由计数，以及
物理通道累计的 `tx_frames`、`rx_frames`、`tx_bytes` 和 `rx_bytes`。

发送计数在后端接收发送帧后增加；接收计数在后端交出帧时增加，即使随后路由丢弃。字节数按
DLC 选择的数据字段长度计算，不包含仲裁、CRC、位填充和帧间隔，不能直接当作总线利用率。
关闭、中止或恢复会关闭路由并唤醒阻塞接收方；旧代次到达的帧只计数，不投递到新路由。
旧 dispatcher 的故障即使已在恢复前通过回调入口检查，也不能中止恢复后注册的新路由。
直接使用 `ReceiveRouter` 时，`abort(error, generation=n)` 只终止匹配代次；省略
`generation` 仍会无条件终止当前路由。

`max_consecutive_errors` 分别约束接收和发送方向。经典 CAN 和 CAN FD 的同步发送
（`transport.send()` / `send_now()`）、队列发送共用发送方向的连续物理错误计数；
原生批量发送按一次后端调用计数。成功发送会清零，接收成功、队列空闲和参数校验失败
既不增加也不清零发送方向的计数。`TransportError` / `TimeoutError` 计入阈值；
`ValidationError` 等非物理错误仅报告给当前调用方。后端原始 `ValueError` / `TypeError`
映射为 `ValidationError`，已有 SDK 异常保持类型和原始原因，进程中断立即传播。

达到阈值后，dispatcher 拒绝新的发送、使未完成的队列回执失败，并在发送锁之外通知一次
总线故障；Transport 转为 `FAULTED`，接收路由中止，需显式恢复后重新注册路由。
恢复不重放失败命令。健康快照中的 `consecutive_errors` 是最近活动的连续错误统计，
接收成功也可能将其清零；它不等同于独立的发送方向阈值计数。同一次终止发送故障不会
因回调和回执传播而重复计入健康统计，旧发送的迟到异常也不会写入恢复后的新代次。
旧发送的迟到成功仍增加累计物理发送计数，但不能清零新连接的错误计数或更新时间戳。

## 发送回执

`transport.send()` 返回 `SendReceipt`。完成表示物理后端已接收帧，不表示设备已执行：

```python
from concurrent.futures import TimeoutError as FutureWaitTimeout
from linkerbot_transport.errors import TimeoutError as SendTimeout, TransportError

receipt = transport.send(message)
try:
    result = receipt.result(timeout=0.1)
except SendTimeout:
    # 后端发送超时，物理结果可能未知。
    ...
except FutureWaitTimeout:
    # 仅等待期限耗尽，回执之后仍可能完成；不要重放。
    ...
except TransportError:
    # 后端拒绝或发送失败。
    ...
```

Future 等待超时不会取消发送，也不能限制 `transport.send()` 内部同步后端调用的总时长。
必须先捕获 SDK 发送超时：在较新的 Python 版本中它也继承标准库超时类型。

成功结果为 `SendOutcome.DISPATCHED`，带连接代次和时间戳；被替换的 latest 目标为
`SendOutcome.SUPERSEDED`；物理失败为 `SendOutcome.FAILED` 并抛出稳定传输异常。停止
Transport 会使未完成回执失败，控制帧不会自动重试或重放。

低层 CAN FD 队列在发布首个完成回调前，先提交整批已确认的物理发送结果；回调停止
dispatcher 不能把同批其他已成功帧改成失败。发布中断时保留结果与清理责任。
这不代表短发送或失败批次可以安全重试：其物理结果仍可能部分成功或未知。清理会在完成
回调中断时继续终结已脱离队列的整组回执，caller 原始进程中断保持优先；停止回调重入
不会递归发布剩余队列。

发送清理会在任务脱离队列前绑定回执的失败原因；嵌套或并发取消不同 endpoint 时，各自的
原因保持独立。若发布在回执终结前被中断，可重试 `close()` 完成；清理未结束时拒绝打开
新代次。清理回调不能替换原始进程中断，也不能使剩余已移出队列的回执失去清理责任人。

## TransmitArbiter

多个设备或命令族共用 Transport 时使用 `TransmitArbiter` 作为单写入者。连续控制命令按
`(endpoint_id, command_family)` 各使用一个 latest 槽位，离散操作使用有界 FIFO：

```python
import time

from linkerbot_transport import TransmitArbiter

generation = transport.get_health().generation
arbiter = TransmitArbiter(transport.send, fifo_capacity=32)
try:
    arbiter.open(generation=generation)
    receipt = arbiter.submit_latest(
        frame,
        endpoint_id="left-hand",
        command_family="angles",
        deadline_at_s=time.monotonic() + 0.02,
    )
    arbiter.submit_fifo(clear_fault_frame, endpoint_id="left-hand")
finally:
    arbiter.close()
```

`deadline_at_s` 是绝对单调时钟截止时间。`replace_latest_if_pending()` 只替换尚未领取的
目标，目标已在途时返回 `None`。arbiter 健康状态为零 I/O 快照，包含 pending、accepted、
dispatched、superseded、failed 和容量拒绝计数。
arbiter 在执行回执完成回调前提交发送结束与计数状态，回调可取消本端点而不等待自身。
调度历史只保留仍有排队或在途任务的端点，持续活动端点的公平调度历史保持有效。

已有 I/O actor 时使用 `TransmitMode.CALLER`，由该 actor 调用 `drain()`：

```python
from linkerbot_transport import TransmitArbiter, TransmitMode

arbiter = TransmitArbiter(transport.send, mode=TransmitMode.CALLER)
try:
    arbiter.open(generation=generation)
    receipt = arbiter.submit_fifo(frame, endpoint_id="left-hand")
    arbiter.drain()
    receipt.result(timeout=0.1)
finally:
    arbiter.close()
```

需要整组准入时先编码全部帧，再调用 `submit_fifo_batch()`。容量按帧计算，准入全有或全无，
组内不会插入其他任务。`max_pending_age_s` 在最老的排队或在途任务达到年龄上限时拒绝新
批次；`deadline_at_s` 仅在发送第一帧前检查，不中断已开始的批次。
物理 I/O 中止时通过 `BatchSendReceipt.sent_frames` 查看已发送前缀。准入原子不等于物理执行
或设备确认原子。

### 可选的显式批量发送

内置 managed transport 提供同步 `send_batch()`。为 arbiter 显式传入该回调后，
`submit_fifo_batch()` 会把已提交的一组帧分成最多 32 帧的物理块，不等待其他请求攒批：

```python
arbiter = TransmitArbiter(
    transport.send,
    dispatch_batch=transport.send_batch,
    mode=TransmitMode.CALLER,
)
arbiter.open(generation=transport.get_health().generation)
receipt = arbiter.submit_fifo_batch(frames, endpoint_id="arm")
arbiter.drain()
receipt.result(timeout=0.1)
```

具有原生批量能力的 CAN FD 后端对 51 帧执行两次调用（32＋19）。单帧仍走单帧接口，
不支持原生批量的 dispatcher/backend 逐帧发送；未传入 `dispatch_batch` 的既有调用方式
保持逐帧路径。该能力不要求第三方 `MessageTransport` 或 `CANFDBackend` 实现新方法。
arm/hand 的现有包装需要另行显式接入，升级 transport 本身不会自动加速这些包装。

每个块都校验 `expected_generation`；arbiter 应使用同一 transport 的当前代次打开。
停止会等待当前物理调用结束，再取消剩余后缀；无法撤回已交给驱动的整个块。
不支持批量时可在已完成帧之间取消。截止时间仍只在整组开始前检查。
物理失败按每次后端调用计入已有的连续错误阈值，不按块内帧数重复计数。

`sent_frames` 只累计确认成功的连续前缀。短发送或异常块即使产生部分物理效果，也不增加
确认计数；不自动重发该块或其后缀，调用方也不能把该计数当作安全的重放偏移。
成功块的健康统计与进度在回执完成前提交；已确认整组成功不会被后续进程中断改成失败，
但中断仍会原样传播并终止其他未发送工作。

直接调用 `transport.send_batch(messages)` 每次接受 1～32 帧，过长的迭代器最多读取
33 项便拒绝，空批次也会拒绝。可选 `on_progress(count)` 回调用于同步转交本块的确认
进度，在 I/O 锁和状态锁之外执行；它必须快速、支持重复的累计计数，抛错会阻止剩余帧。
自定义 arbiter 批量回调遵循
`callback(messages, *, expected_generation, on_progress) -> BatchSendReceipt`。
完整进度不会覆盖返回回执的代次、帧数校验错误或失败结果，这些错误仍由 FIFO 批次回执报告。

Caller 模式中的 `KeyboardInterrupt`、`SystemExit` 等进程中断会先停止受理、终结未发送
回执，再原样传播给调用方；批次保留已成功发送的前缀。重新打开后不会重放被终止的命令。

## CAN FD 帧值

`CANFDMessage.frame_type` 默认 `0x04`（FD、不启用 BRS）；设备支持时使用 `0x0C`。`None`
继承后端模式，SocketCAN 保持 BRS 关闭。`dlc` 使用协议编码 `0..15`，后端负责转换为数据
字段长度并补零；经典 CAN 最多携带 8 个数据字节。

后台线程在目标函数和清理均返回后交接退出状态，关闭不再依赖 `Thread.is_alive()`
或中断后的 `join()` 结果。arbiter 启动被中断时保留线程责任，确认该线程退出前拒绝
重新打开；关闭超时仍保留责任。若启动结果不明且始终没有观察到退出，不会据此允许
第二个发送线程启动。latest 的替换结果在发布中断后保持 `SUPERSEDED`，可通过
`close()` 重试发布。低层经典 CAN 和 FD 排队发送在获得发送资格后、物理 I/O 之前
记录认领时间及事件序号，失败尝试与批次内的每一帧均保留认领信息。
