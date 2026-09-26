# incremental-ingestion-connectors-go

一套可扩展的本地数据摄取框架：为多种本地数据源提供统一的**拉取、检查点与投递**能力，
使增量同步在**重启、重复消费与下游异常**时的最终状态可预期（不丢、不重）。

## 架构

```
 Source(连接器) --Record--> Normalize --Event--> [有界队列] --worker 攒批--> Sink(幂等)
        ^                                                              |
        └────────────  CheckpointStore <== 提交器(连续前缀提交) <─────────┘
```

| 包 | 职责 |
|---|---|
| `internal/connector` | 数据源契约 `Source` 与插件注册表 `Registry` |
| `internal/event` | 原始记录 `Record` 归一化为内部事件 `Event` |
| `internal/checkpoint` | 位点 `Position` 校验与原子化文件存储 `FileStore` |
| `internal/sink` | 下游契约 `Sink` 与幂等文件下游 `FileSink` |
| `internal/pipeline` | 并发摄取、背压、按批投递与有序提交 |
| `connectors/jsonl` | JSONL 文件数据源 |
| `connectors/csv` | CSV 文件数据源（表头映射，payload 列为 JSON） |
| `cmd/ingest` | 演示命令 |

## 连接器契约

```go
type Source interface {
    Name() string                                              // 实例标识，用于位点匹配
    Open(ctx, from checkpoint.Position) error                  // 从位点续读；重复 Open 报 duplicate_init
    Next(ctx) (event.Record, checkpoint.Position, error)       // 拉一条；耗尽报 source_exhausted
    Close() error                                              // 幂等关闭
}
```

- 插件通过 `Registry.Register(kind, factory)` 注册；**标识冲突 / 空标识**返回
  `registration_conflict`，重复初始化返回 `duplicate_init`。
- 记录归一化要求必填字段 `key`、`kind`（字符串），可选 `payload`（对象）、`ts`（RFC3339）。
  **字段缺失**报 `missing_field`，**类型不匹配**报 `type_mismatch`；坏记录会被明确拒绝并中止
  该轮摄取，绝不静默跳过。
- 工厂或底层库的错误一律包装为框架 `*errors.Error`（未分类归为 `internal`），
  底层异常不会直接透出到对外接口。

新增数据源：实现 `Source`，写一个 `Register(*connector.Registry) error`，在 `cmd/ingest`
或自己的 main 中注册即可。

## 检查点与幂等语义

- 位点 `Position{Source, Offset, Seq}`：字节偏移 + 源内序号，随每批投递成功后提交。
- **按批提交**：提交器只推进 idx 连续覆盖的前缀，乱序完成的批次不会导致位点越过
  未完成的批次；位点单调前进。
- **拒绝非法位点且原因可区分**：
  - `position_regression`：Offset/Seq 回退；
  - `position_out_of_range`：Offset 超出当前源大小；
  - `position_source_mismatch`：位点属于其他数据源。
- **崩溃恢复**：已写入但未提交的事件在重启后会被重放，由 `FileSink` 按事件 ID
  （`source/seq/key`）去重 —— 至少一次拉取 + 幂等下游 = 不丢不重。
- 检查点文件采用 临时文件 + rename 原子写，避免崩溃产生半截文件。

## 错误分类

所有对外错误均为 `*internal/errors.Error`，用 `errors.ClassOf(err)` 取分类：

`registration_conflict` · `duplicate_init` · `missing_field` · `type_mismatch` ·
`position_regression` · `position_out_of_range` · `position_source_mismatch` ·
`source_exhausted` · `sink_failure` · `backpressure_timeout` · `canceled` · `internal`

日志中每条错误都带 `error_class` 字段，批次日志带 `batch`/`offset`/`seq`。

## 背压与关闭策略

- 队列容量 `QueueSize` 是背压边界：下游阻塞时生产者阻塞在入队上，并每 200ms 打印
  `backpressure: queue full, producer blocked` 警告 —— 行为确定且可观测，不丢数据。
- 吞吐随并发度提升：worker 并行攒批投递，在下游成为瓶颈前近似线性扩展
  （测试 `TestThroughputScalesWithWorkers` 实测 8 worker 约 7.5x）。
- **取消/关闭**：`ctx` 取消后，在途批次直接丢弃、不再投递也不再提交位点；
  重启后从最近提交位点重放，由 Sink 幂等去重。无数据竞争（`go test -race` 验证）。
- 下游写失败：该批次不提交位点，管线以 `sink_failure` 退出，重启后重放恢复。

## 本地复现与验证

```bash
# 全部测试（含竞态检测）
go test ./... -race

# 端到端演示
mkdir -p /tmp/demo && cd /tmp/demo
for i in $(seq 1 40); do echo "{\"key\":\"k$i\",\"kind\":\"upsert\",\"payload\":{\"n\":$i}}"; done > data.jsonl
go run <repo>/cmd/ingest -kind jsonl -path data.jsonl -out out.jsonl -ckpt ckpt.json -workers 4 -batch 8
# 再次运行 = 重启续传：produced=0 written=0，out.jsonl 行数不变
```

测试矩阵（`internal/pipeline/pipeline_test.go` 等）：

| 场景 | 测试 |
|---|---|
| 完整摄取 + 检查点推进 | `TestFullIngest` |
| 重启续传（不产生新写入） | `TestRestartResume` |
| 重复消费（位点丢失重放，下游去重） | `TestDuplicateConsumptionNoDuplicateWrites` |
| 下游写失败（错误分类 + 位点不越过失败批） | `TestSinkFailureClassified` |
| 并发取消（续传后不丢不重） | `TestConcurrentCancel` |
| 背压（慢下游 + 小队列不丢数据） | `TestBackpressureBlocking` |
| 吞吐随并发提升 | `TestThroughputScalesWithWorkers` |
| 坏记录明确拒绝 | `TestRecordLevelErrorAborts` |
| 位点回退/越界/源不匹配 | `internal/checkpoint`、`connectors/jsonl` 测试 |
| 注册冲突/重复初始化 | `internal/connector`、`connectors/*` 测试 |
