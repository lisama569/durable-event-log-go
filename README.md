# durable-event-log-go

可嵌入的本地追加日志存储组件（包 `logstore`）。面向"进程异常退出、并发读写、
存储空间耗尽"等场景，保证：**一旦写入被确认，后续任何中断都不会造成该记录的
静默丢失**；异常数据能被准确识别并与正常数据区分。

## 快速开始

```go
l, err := logstore.Open("/var/myapp/log", logstore.Options{
    MaxSegmentBytes: 64 << 20, // 单段上限，超出滚动新段
    MaxBytes:        1 << 30,  // 总容量上限，0 表示不限
    SyncOnAppend:    true,     // 每次确认前 fsync
})
off, err := l.Append([]byte("hello"))   // 追加，返回全局偏移
rec, err := l.ReadAt(off)               // 指定位置读取
err = l.Replay(0, func(r logstore.Record) error { ... }) // 顺序重放
err = l.Trim(someOffset)                // 在线回收旧段
err = l.Close()
```

## 持久化语义

- 记录以自描述帧写入段文件：`magic | version | flags | length | crc32 | payload`。
- `Append` 返回成功即表示帧已写入内核（`SyncOnAppend=true` 时已 fsync 到盘）。
  恢复过程**只截断从未确认过的尾部**，已确认记录不会被修改或删除。
- `SyncOnAppend=false` 时可用 `Sync()` 手动刷盘；未刷盘的写入在机器掉电时可能
  丢失，但在进程崩溃（页缓存仍在）场景下可恢复——这属于调用方选择的语义，
  恢复结果同样可解释（见下文截断规则）。
- 元数据 `meta.json` 通过 tmp+rename+目录 fsync 原子更新，不会留下写一半的文件。

## 恢复与兼容规则

`Open` 时执行恢复，结论通过 `Log.RecoveryInfo()` 暴露（含恢复位置
`ValidEndOffset`、截断字节数与原因），测试日志中也会打印这些判据。

| 情况 | 规则 | 结果 |
|---|---|---|
| 尾部帧头/帧体不完整（撕裂写） | 截断到最后一条完好记录 | 正常打开，`TruncatedReason="torn-tail"` |
| 尾部帧 CRC 不匹配 | 同上 | 正常打开，`TruncatedReason="corrupt-tail"` |
| 非最末段中间损坏、段间偏移不连续 | 拒绝打开 | 返回 `ErrCorrupt`（携带段与偏移位置） |
| 元数据版本 < 当前版本 | 接受并升级落盘 | `MetaAction="upgraded"` |
| 元数据版本 > 当前版本 | 拒绝打开 | `ErrUnsupportedVersion` |
| 元数据为空或不可解析 | 拒绝打开 | `ErrMetaCorrupt` |
| 元数据不存在 | 以当前版本创建 | `MetaAction="created"` |

拒绝打开时不做任何写操作，不会留下半可用状态；恢复是确定性的，
同一目录重复恢复的结论一致（测试 `TestCrashWithoutClose` 验证了可重复性）。

## 容量管理与并发行为

- `MaxBytes` 达到上限时 `Append` 返回 `ErrNoSpace`（含需要/剩余字节数），
  **明确拒绝，不静默覆盖**已有数据。
- `Trim(offset)` 回收所有记录均小于 `offset` 的非活跃段：
  - 待删段有进行中的读取时整体返回 `ErrTrimInUse`，不做部分删除；
  - 活跃写入段永不删除，回收与新写入互不阻塞；
  - 回收后该区间读取返回 `ErrOffsetOutOfRange`，而非脏数据。
- 读写、回收通过互斥锁与段引用计数串行化共享状态；
  所有错误均可用 `errors.Is` 区分（`ErrNoSpace` / `ErrTrimInUse` /
  `ErrCorrupt` / `ErrTruncated` / `ErrOffsetOutOfRange` / `ErrClosed` 等）。

## 本地验证

```sh
go test ./logstore/ -v          # 功能 + 恢复测试（日志含恢复位置与判据）
go test ./logstore/ -race       # 并发竞争检测
```

测试覆盖：异常退出（`TestCrashWithoutClose`）、截断尾部
（`TestTornTailTruncated` / `TestCorruptTailTruncated`）、中间损坏拒绝
（`TestCorruptMiddleRejected`）、并发读写（`TestConcurrentWritersAndReaders`）、
容量耗尽与回收（`TestCapacityExhaustion` / `TestTrimInUseRejected`）、
版本兼容（`TestMetaVersionRules`）。
