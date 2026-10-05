# durable-event-log-go

可嵌入的、崩溃安全的本地只追加事件日志。在「写入 / 按位置读取 / 顺序重放 / 在线回收」
之上，本层新增了面向消费者的**订阅（游标）能力**：多个命名游标各自记住消费位置，
只有显式确认（Ack）才推进，进度跨进程重启持久化，并支持在日志末尾挂起等待新记录。

## 目录

- [数据模型与既有能力](#数据模型与既有能力)
- [订阅 / 游标语义](#订阅--游标语义)
- [投递保证](#投递保证)
- [实时等待（无轮询）](#实时等待无轮询)
- [游标与回收、容量、版本的关系](#游标与回收容量版本的关系)
- [游标状态的崩溃安全](#游标状态的崩溃安全)
- [错误速查](#错误速查)
- [本地验证](#本地验证)
- [文件结构](#文件结构)

## 数据模型与既有能力

- `Position` 是记录在日志中的**逻辑字节偏移**。段文件重写（回收前缀）后，
  存活记录的逻辑位置不变。
- `Store.Append(ctx, data)`：帧落盘并 fsync 后才返回位置。
- `Store.Read(ctx, pos)`：精确读某位置；段内偏移返回 `ErrOffsetNotAligned`，
  已回收返回 `ErrPositionReclaimed`，读到已提交末尾返回 `ErrEndOfLog`。
- `Store.Replay(ctx, start, fn)`：从某位置顺序回调，期间持读锁，回收会等待它。
- `Store.Compact(ctx, opts)`：在线回收（`Before` 或 `KeepBytes`），
  与 Append、另一次 Compact 串行；并发的第二个 Compact 立即得到
  `ErrCompactionInProgress`，不会让读写者永久等待。

## 订阅 / 游标语义

游标由 `Manager` 管理，状态持久化在日志目录下的 `cursors/` 子目录中
（每个游标一个文件 `<name>.cursor.json`）。

```go
s, _ := eventlog.Open(dir, eventlog.DefaultOptions())
defer s.Close()

mgr, _ := eventlog.NewManager(s)   // 挂载后回收会被游标下限钳制
defer mgr.Close()

// Beginning: 从现存最老记录开始；CursorAtEnd: 只消费创建之后的新记录；
// 也可以传一个仍然存在的记录边界 Position。
c, _ := mgr.Create(ctx, "orders", eventlog.Beginning)

for {
    rec, err := c.Next(ctx)        // 末尾无数据时挂起等待，见下
    if err != nil { /* ... */ }
    process(rec)
    if err := c.Ack(ctx, rec); err != nil { /* 磁盘满等：进度不会前进 */ }
}
```

关键规则：

1. **按名字隔离**。多个命名游标互不影响；一个游标 Ack 不会带动任何其他游标。
   重名 `Create` 得到 `ErrCursorExists`；打开不存在的名字得到 `ErrCursorNotFound`。
2. **只有 Ack 才推进**。`Next` 只投递、不移动游标。Ack 采用"确认到该记录为止"
   的语义：Ack 记录 R 后，下一个 `Next` 返回 R 的下一条（或在末尾等待）。
3. **不静默跳过**。Ack 一个超前于当前位置的位置会被拒绝（`ErrInvalidArgument`），
   因此无法用一次 Ack 跳过尚未确认的记录；重复 Ack 一条已确认记录是幂等空操作。
4. **进度跨重启**。Ack 在返回前已通过「临时文件 + fsync + 原子 rename + 目录 fsync」
   落盘；进程崩溃后重新 `Open` 同名游标，从最后一次完整 Ack 的位置继续。
5. **创建起点**：`Beginning` 解析为当前仍保留的最老记录；`CursorAtEnd` 解析为
   当下的已提交末尾（之后的新记录才会被投递）；显式位置必须是仍存在的记录边界，
   落在回收界之前会直接得到 `ErrCursorPositionReclaimed`。
6. 打开/创建：`Manager.Create`、`Manager.Open`、`Manager.OpenOrCreate`；
   `Manager.List` 给出每个游标的 `Next`、已确认序列号、积压字节以及是否已落后
   （`Lagging`）；`Manager.Delete` 删除持久状态（打开中的游标需先 `Close`）。

## 投递保证

**至少一次（at-least-once），不保证恰好一次。**

- 记录在 Ack **之前**投递到消费者；若在投递后、Ack 前崩溃，重启后同一条会再次投递。
- Ack 的持久化先于内存推进：写游标失败（ENOSPC、断电、I/O 错误）时，游标内存位置
  不前移，`Next` 仍返回同一条。调用方应当把处理写成幂等，或自行去重。
- 已确认的记录不会被再次投递（除非它的位置已经被回收导致游标整体失效，见下）；
  未确认的记录不会被静默跳过。

## 实时等待（无轮询）

`Next` 在游标未到末尾时立即返回下一条；在已提交末尾时**挂起**，直到：

- 有新的 Append 提交（在帧 fsync 之后唤醒，绝不读到半条）；
- 发生一次 Compact（地平线移动，唤醒后重新判定）；
- `ctx` 超时或被取消（返回 `context.DeadlineExceeded` / `context.Canceled`）；
- Store 或 Manager/游标被关闭（返回 `ErrClosed` / `ErrCursorClosed`）。

实现是边沿触发的等待/通知（`cond` 式 channel 关闭），**没有任何定时轮询或自旋**：
等待者在持有 Store 读锁期间注册 channel，随后在锁内复查末尾位置，因此"提交"与
"唤醒"之间不可能漏掉一次写入。并发写入、并发回收都不会让消费者永久卡住。
超时/取消退出时会注销等待者。

## 游标与回收、容量、版本的关系

### 回收保护（挂载 Manager 时）

`NewManager` 会把一个保留下限（retention floor）挂到 Store 上。回收时，
请求的截止位置会被**钳制到所有已知游标的最小下一位置**：

- 所有游标都确认过的前缀可以正常回收；
- 只要还有任意游标（包括崩溃未重启、只在磁盘上存在的游标）停在某个位置，
  该位置及其之后的记录就不会被删掉；
- 钳制决定会写入恢复/诊断报告（`RecoveryReport.Decisions`），可观测。

这样回收不会"悄悄删掉还没被安全消费的数据"。

### 脱离保护后：明确失败而非静默跳过

`Manager.Detach()` 会移除保留下限（已打开的游标仍可继续读写、仍持久化，
但不再阻止回收）；`Manager.Close()` 则同时关闭所有游标句柄。

脱离后若游标落后到其下一位置已被回收，后续 `Next` 会得到一个明确的、
可程序化判别的错误：

```go
rec, err := c.Next(ctx)
if errors.Is(err, eventlog.ErrCursorPositionReclaimed) {
    // 游标落后于回收地平线；它同时也 errors.Is(err, ErrPositionReclaimed)
}
```

这**不会**被当成"读到末尾"而挂起，**不会**错读成别的记录，游标位置也不会
在失败时自行移动；每次 `Next` 都会稳定地重复该失败，直到上层显式处理
（例如重置到现存边界、重建本地状态、或删除该游标重建）。重启后 `Manager.List`
会把这种游标标记为 `Lagging: true`，`Next` 同样给出该错误。

### 容量

`Options.MaxBytes` 是硬容量：Append 在触碰磁盘前检查，超限得到
`ErrCapacityExceeded`，日志保持健康、可读。游标本身的状态文件很小且放在
`cursors/` 子目录，不计入段容量。游标只能让回收"少回收"，因此一个长期不推进、
且 Manager 仍挂载的游标会使空间无法降到它的位置之下——这是刻意的保护：
要释放该空间，需先让消费者追上、或显式 `Detach`/`Delete` 后再回收。

### 版本兼容

游标文件使用独立的版本号命名空间（`cursor_version`），与段格式版本、
meta 版本互不影响：

- `1..currentCursorVersion`：接受，并在下一次 Ack 时升级写回；
- 更新的未知版本：以 `ErrCursorVersionUnknown` 拒绝，且**绝不覆盖**该文件，
  防止旧版本二进制写坏新状态；错误中保留具体版本号；
- `cursor_version=0`、空文件、无法解析的 JSON：按"写到一半"处理，
  得到 `ErrCursorStateCorrupt`。

`NewManager` 在加载目录时只要遇到一个损坏或未来版本的游标文件就整体报错、
文件原样保留，绝不做"部分加载 + 静默重置到 0"。

## 游标状态的崩溃安全

每个游标状态文件包含 `name / next_position / updated_sequence / cursor_version / crc32`，
除 CRC 外的字段全部纳入 IEEE CRC32。写入路径与 meta 相同：

1. 写 `<name>.cursor.json.tmp`；
2. fsync 临时文件；
3. `rename` 原子替换正式文件；
4. fsync 目录，保证 rename 持久。

因此断电、磁盘写满、被打断的结果只有两种：

- 上一份完整文件仍在（rename 未发生）；或
- 新的完整文件已就位（rename 已落盘）。

不会留下"半新半旧"的进度。崩溃遗留的 `.tmp` 会在下一次 `NewManager` 时被识别并
清理，正式文件不受影响。若 CRC 不符、JSON 被截断或字段非法，打开时得到带游标名、
位置和原因的 `*CursorError`（包装 `ErrCursorStateCorrupt`），而不是悄悄归零。

Ack 路径的内存更新严格发生在持久化成功**之后**，所以即使模拟 ENOSPC，
游标也停在上一个完整状态并会重投同一条记录。

## 错误速查

| 情况 | 判别 (`errors.Is`) |
| --- | --- |
| 游标名不存在 / 已存在 | `ErrCursorNotFound` / `ErrCursorExists` |
| 名字不合法（路径分隔符、保留名等） | `ErrCursorInvalidName` |
| Manager/游标已关闭 | `ErrCursorClosed`（常与 `ErrClosed` 一起） |
| 游标下一位置已被回收 | `ErrCursorPositionReclaimed`（也匹配 `ErrPositionReclaimed`） |
| 游标文件损坏/撕半/CRC 不符 | `ErrCursorStateCorrupt`（`*CursorError` 带名字和位置） |
| 游标文件是未来版本 | `ErrCursorVersionUnknown` |
| Ack 试图越过未确认记录 | `ErrInvalidArgument` |
| Next 挂起期间超时/取消 | `context.DeadlineExceeded` / `context.Canceled` |
| 日志容量超限 | `ErrCapacityExceeded` |
| 磁盘写满（游标或段） | `ErrDiskFull` |

`*CursorError` 同时承载 `Name`、出问题的 `Position`、`Kind` 与内部原因，
便于日志诊断。

## 本地验证

```bash
# 全量单测（含既有写入/重放/回收用例）
go test -count=1 ./...

# 竞态检测
go test -race -count=1 ./...

# 只看本轮新增的游标用例
go test -v -run 'TestCursor' ./...
```

重点用例与它们验证的行为：

| 测试 | 验证点 |
| --- | --- |
| `TestCursorProgressAcrossRestart` | Ack 进度跨 store+manager 关闭重开恢复；未 Ack 的记录重投 |
| `TestMultipleCursorsIndependent` | 多游标互不带动；超前 Ack 被拒；重名/未知名字报错 |
| `TestCursorRealtimeTailConsume` | 挂起的消费者被新写入即时唤醒、顺序投递、无忙等 |
| `TestCursorWaitTimeoutLeavesPositionIntact` | 超时退出且位置不前移，之后仍投递同一条 |
| `TestCursorWaitCancel` | 取消能退出挂起的 Next |
| `TestCursorWaitDuringCompaction` | 回收期间消费者不被永久卡住，之后收到新记录 |
| `TestCursorWaitWokenOnClose` | 关闭会唤醒挂起者并给出明确错误 |
| `TestCompactionClampedToLaggingCursor` | 挂载 Manager 时回收被最慢游标钳制，未消费数据不被删 |
| `TestDetachedManagerLaggingCursorFailsLoudly` | 脱离后落后游标明确失败、不挂起、不跳读、位置不动 |
| `TestCursorLagSurvivesRestart` | 落后状态跨重启被标记 Lagging 并明确失败 |
| `TestCursorAckWriteFailureKeepsOldProgress` | Ack 持久化失败（模拟 ENOSPC）内存不前移并重投 |
| `TestCursorTornStateDetected` | 截断/CRC 损坏/空文件都被明确拒绝；完整文件恢复后正常续读 |
| `TestCursorFutureVersionRejected` | 未来版本被稳定拒绝且文件不被覆盖 |
| `TestCursorStaleTempCleaned` | 崩溃残留 `.tmp` 被清理，不影响完整状态 |
| `TestCursorConcurrentConsumeCompactAppend` | 多消费者在边写边回收下无缺口、无死锁、无数据竞争 |

测试日志会打印消费到的位置（`pos:N`）、序列号、判定依据（例如钳制决定、
回收地平线、CRC 比对）和关键状态变化（cursor 推进、wake 延迟、Lagging 标记）。

也可以直接观察磁盘布局：

```text
<dir>/
  meta.json
  segment-00000000000000000000.log
  segment-00000000000000001120.log
  cursors/
    orders.cursor.json
    billing.cursor.json
```

## 文件结构

- `store.go` / `segment.go` / `format.go`：段、帧、Append/Read/Replay。
- `compaction.go`：在线回收；接入游标保留下限钳制与回收后唤醒。
- `recovery.go` / `meta.go`：崩溃恢复、元数据版本与原子落盘。
- `cursor.go`：`Manager` 与 `Cursor`（创建/打开/投递/Ack/列举/删除/脱离）。
- `cursor_format.go`：游标状态格式、CRC、版本规则、原子写盘。
- `cursor_errors.go`：游标层错误与 `CursorError`。
- `waiter.go`：边沿触发的等待/通知中枢。
- `retention.go`：Store 上的回收保护点挂接。
