# 设计与语义

## 1. 分层与数据流

```
            ┌────────────┐   RawRecord(map[string]any)
 Source ───▶│ Connector  │──────────────────────────────▶ Normalize ─▶ Event
 (file)     │ SourceReader│      缺字段/类型错/坏行 → BadRecord(分类原因)        (key,offset,payload,source)
            └────────────┘
                                                                │
                          ┌─────────────────────────────────────▼──────────────┐
                          │  Engine（每批次）                                    │
                          │  Read(batch) → 有界队列 → N workers → 幂等 Sink.Write │
                          │             ↓ 全部成功                                │
                          │        Store.Commit(offset, keys, badOrdinals) 原子  │
                          └──────────────────────────────────────────────────────┘
```

## 2. 连接器契约（`pkg/connector`）

- `Connector`：无状态工厂。`Kind()` 返回注册标识；`Schema()` 声明 key 字段与各字段类型；
  `Open(ctx, Config)` 返回一个运行句柄。
- `SourceReader`：`Tag()`（数据源物理标识，用于检查点绑定）、`Read(ctx, resumeOffset, maxRecords)`、`Close()`。
- **同一 Connector 实例重复 `Open` 必须返回 `connector_error/already_initialized`**；
  对已关闭 reader 再操作返回 `connector_error/not_initialized`；`Close` 后允许重新 `Open`。
- `Read` 返回 `ReadResult{Events, Bad, EndOrdinal, EOF}`：
  - 好记录进入 `Events`；无法归一化的记录进入 `Bad`，携带**行序号 + 截断原文 + 分类错误**。
  - `EndOrdinal` 是**物理扫描位置**（好+坏一起计数），用于位点越界校验。

### Offset 语义（内置连接器）

JSONL 与 CSV 均采用**十进制 1 基行序号**（CSV 不含表头），`""` 或 `"0"` 表示从头开始。
它与字节位置无关，因此同一文件在任何机器上行为一致。连接器只负责"从序号 N 之后读取"，
位点的**合法性（回退/越界/标签）由 checkpoint + engine 统一裁决**。

### 归一化（`connector.Normalize`）

- key 字段必须存在且是非空字符串；
- 字段按 Schema 声明的 `string/int64/float64/bool` 强类型转换；
  JSON 的数字经 `json.Number`/`float64` 读取，仅当为整数且在 int64 范围内才接受为 `int64`；
- 必填字段缺失 → `record_error/missing_field`；
- 值无法转成声明类型 → `record_error/type_mismatch`；
- 行本身无法解析（非法 JSON、畸形 CSV、空行）→ `record_error/malformed_record`。
以上是**显式拒绝**，由 engine 记录结构化日志并回调 `OnRecordError`，绝不静默跳过。

### 注册（`Registry`）

- `Register` 对重复 `Kind` 返回 `connector_error/duplicate_connector_registered`；
  `Get`/`Open` 未知 Kind 返回 `connector_error/connector_not_found`。
- 进程级 `Default()` 与可注入的 `NewRegistry()` 并存；注册表内部用 `sync.RWMutex` 保护。

## 3. 检查点与幂等（`pkg/checkpoint`、`pkg/sink`）

### 3.1 每批次提交协议

```
1. Read(resume=已提交offset)
2. 过滤掉已在幂等索引中的 key（崩溃重放去重）
3. 有界队列并行 Sink.Write（失败则整批重试，位点不动）
4. 全部持久化成功后：Store.Commit(watermark, goodEvents, newBadOrdinals)
5. resume = watermark，继续
```

`watermark = 本批好记录与新坏记录中的最高行序号`。这样坏记录永远位于已提交水位或其之前，
重启时不会被反复"拒绝"；提交的坏记录序号持久化，**每条坏记录只上报一次**。

### 3.2 原子性

- 检查点：`写临时文件 → fsync → rename`，提交要么完整生效要么不生效。
- 位点必须**严格递增**：相等（重复提交）或更小（回退）一律返回
  `offset_error/offset_stale`，且在触碰文件之前拒绝（拒绝不留半成品）。
- 非法 offset 字符串 → `offset_error/offset_bad_format`。

### 3.3 崩溃窗口与"恰好一次"目标状态

| 崩溃时机 | 结果 |
| --- | --- |
| sink 写入前 | 该批未提交，重启整批重放 |
| 部分写入后、提交前 | 已写的 key 在重启重放时由 sink 去重；未写的被补写 |
| 提交之后 | 位点之后才会再读，无重放 |

`FileSink` 以 key 为文件名，`临时文件+rename` 落盘，**文件存在即"已投递"**这一判断是持久化的，
因此 sink 跨进程重开仍然幂等。引擎本身提供 at-least-once 投递，叠加幂等 sink 后
目标端呈现**exactly-once 的可观察状态**。

### 3.4 位点防护（可区分原因）

- 检查点文件的 `source_tag` 与当前打开的数据源不一致 → `offset_error/offset_source_mismatch`；
- resume offset 解析失败 → `offset_error/offset_bad_format`；
- resume offset 大于数据源当前末尾序号 → `offset_error/offset_out_of_range`；
- 提交非递增 offset → `offset_error/offset_stale`；
- 检查点文件损坏（非法 JSON / offset 不可解析）→ `checkpoint_error/checkpoint_corrupt`。

### 3.5 幂等索引剪枝

索引只保留"序号 ≥ 已提交水位"的 key 与位于水位上的坏记录序号；
严格早于水位的条目在提交时剪枝，因此稳态下检查点体积有界（通常为空索引）。

## 4. 并发、背压与关闭（`pkg/engine`）

- 长生命周期 worker 池，大小 `Workers`；任务经容量 `QueueSize` 的**有界 channel** 下发。
- 每个批次拥有独立的、容量等于批次大小的结果 channel：worker 发送结果永不阻塞，
  从根本上避免"结果无人读取导致 worker 永久挂住"。
- **背压**：下游变慢 → 任务队列填满 → 喂入协程在发送处阻塞，并累加
  `Stats.QueueFullDrops`（日志字段 `queue_blocks`）。内存占用恒为
  `O(QueueSize + Workers)`，不会无界堆积。
- **失败重试**：整批失败（任一事件写失败）则不提交位点，按指数退避重试整批；
  `MaxAttempts=0` 表示持续重试到成功或 ctx 取消。
- **取消/关闭**：
  - ctx 取消后：不再读取/提交新批次；正在投递的批次**不提交位点**直接返回；
  - worker 将同一个 ctx 传给 `Sink.Write`，**Sink 实现必须响应 ctx**（契约要求）；
  - 结果收集在取消后做有限排空以保留"已成功写入"的已知状态，随后退出；
  - 因为没有提交，重启时整批重放，幂等 sink 保证无重复、无丢失。
- 关闭 reader / sink 出错只记录日志，不影响已提交状态。

## 5. 扩展性：新增一种数据源

1. 实现 `connector.Connector` 与 `connector.SourceReader`（可复用 `connector.Normalize`、
   `connector.ParseResumeOrdinal`）；
2. 用稳定的 `Kind()` 与"kind:物理标识"的 `Tag()`；
3. 把所有底层错误用 `cerror.Wrap/New` 包成分类错误，不要把驱动 error 直接返回；
4. `reg.Register(yourConnector)` 即可被 engine 使用；
5. 若 offset 不是十进制序号，需要自行实现与 `checkpoint.CompareOffsets/ValidateResume`
   等价的比较与校验（当前内置实现共用十进制序号编码）。
