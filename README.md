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
| `internal/reject` | 坏记录拒绝存储：按类别留痕、跨重启去重、可汇总查看 |
| `internal/pipeline` | 并发摄取、背压、坏记录拒绝、按批投递与有序提交 |
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
  **字段缺失**报 `missing_field`，**类型不匹配**报 `type_mismatch`；
  **整行无法按格式解析**（坏 JSON、CSV 列数对不上表头）报 `parse_error`，
  连接器同时返回坏行之后的位点，调用方登记拒绝后从该位点继续。
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

## 坏记录与拒绝语义

脏数据不再让整轮同步卡死：**记录级问题按类别拒绝、留痕、然后继续**，
本轮剩下的好记录全部投递，位点最终推进到源末尾。

- **拒绝类别**（与错误分类一一对应）：
  - `parse_error`：整行/整文件无法按格式解析（坏 JSON 行、CSV 列数与表头不符）；
  - `missing_field`：记录缺少必填字段（`key`/`kind`）；
  - `type_mismatch`：记录字段类型不符（如 `key` 不是字符串、`ts` 不是 RFC3339）。
- **留痕与汇总**：每条拒绝以 `(source, offset)` 为身份追加到拒绝存储
  （`cmd/ingest` 默认 `out/rejects.jsonl`，可用 `-rejects` 指定），
  每条记录包含源内偏移、序号、类别与原因；`Summary()`/启动日志给出按类别聚合的汇总，
  直接 `cat rejects.jsonl` 即可逐条排查。
- **位点照常推进**：被拒记录的位点与已投递位点一样参与连续前缀提交，
  因此即使源末尾是坏记录，检查点也会推进到源末尾，下轮不会空转重扫。
- **去重语义**：同一条坏记录在一次运行里只登记一次；拒绝存储持久化，
  重启续传即使重扫旧区间（例如检查点丢失后从头重放）也**不重复登记、不重复上报**
  （日志只打 debug 级 skip）。好记录的重放仍由 Sink 幂等去重兜住。
- 非记录级错误（`internal`、`sink_failure` 等）仍然中止整轮同步，行为不变。

## 错误分类

所有对外错误均为 `*internal/errors.Error`，用 `errors.ClassOf(err)` 取分类，
日志中每条错误都带 `error_class` 字段。分类按"该改配置还是改数据"分组：

| 分组 | 分类 | 含义 / 处置 |
|---|---|---|
| 配置问题 | `config_missing` | 缺少必要配置（如 `path` 没给）——改配置 |
| | `config_invalid` | 配置值形态不对（如 `path` 不是字符串）——改配置 |
| | `unknown_connector` | 指定的数据源类型不存在（附可用类型列表）——改类型 |
| 记录问题 | `parse_error` | 整行/整文件无法按格式解析——改数据或换格式 |
| | `missing_field` | 记录缺少必填字段——改数据 |
| | `type_mismatch` | 记录字段类型不匹配——改数据 |
| 位点问题 | `position_regression` / `position_out_of_range` / `position_source_mismatch` | 位点回退 / 越界 / 源不匹配 |
| 运行问题 | `registration_conflict` · `duplicate_init` · `source_exhausted` · `sink_failure` · `backpressure_timeout` · `canceled` · `internal` | 注册/初始化/耗尽/下游/背压/取消/未分类 |

日志中每条错误都带 `error_class` 字段，批次日志带 `batch`/`offset`/`seq`，
拒绝日志带 `offset`/`error_class` 并可与 `rejects.jsonl` 对照。

## 背压与关闭策略

- 队列容量 `QueueSize` 是背压边界：下游阻塞时生产者阻塞在入队上，并每 200ms 打印
  `backpressure: queue full, producer blocked` 警告 —— 行为确定且可观测，不丢数据。
- 吞吐随并发度提升：worker 并行攒批投递，在下游成为瓶颈前近似线性扩展
  （测试 `TestThroughputScalesWithWorkers` 实测 8 worker 约 7.5x）。
- **取消/关闭**：外部 `ctx` 取消立即生效——worker 与提交器的运行上下文派生自外部
  `ctx`，取消后**不再投递、不再推进位点**，在途批次直接丢弃；提交器在每次提交前
  再检查一次取消信号，保证取消后提交数不再增长。取消那一刻已写出但未提交的部分，
  下次运行从最近提交位点重放，由 Sink 幂等去重兜住，不丢不重。
  取消/关闭结束后三者关系自洽：**位点覆盖的事件 ⊆ 已投递事件 ⊆ 已生产事件**
  （`checkpoint.Seq ≤ Written ≤ Produced`），`Run` 返回后所有 goroutine 已退出，
  统计与位点不再变化。并发取消无数据竞争（`go test -race` 验证）。
- 下游写失败：该批次不提交位点，管线以 `sink_failure` 退出，重启后重放恢复。

## 本地复现与验证

```bash
# 全部测试（含竞态检测）；用 -v 可看到每个用例覆盖的业务与结果
go test ./... -race -v

# 端到端演示（含脏数据）
mkdir -p /tmp/demo && cd /tmp/demo
for i in $(seq 1 40); do echo "{\"key\":\"k$i\",\"kind\":\"upsert\",\"payload\":{\"n\":$i}}"; done > data.jsonl
# 注入三类坏记录：缺必填字段、字段类型错、整行无法解析
sed -i '5i {"kind":"upsert"}' data.jsonl
sed -i '8i {"key":123,"kind":"upsert"}' data.jsonl
sed -i '12i not-a-json-line' data.jsonl
go run <repo>/cmd/ingest -kind jsonl -path data.jsonl -out out.jsonl -ckpt ckpt.json -rejects rejects.jsonl
# 预期：40 条好记录全部投递，3 条坏记录按类别拒绝（日志 WARN + reject summary），
#       检查点推进到源末尾
cat rejects.jsonl   # 逐条查看：第几条（offset/seq）、哪一类原因、具体报错
# 再次运行 = 重启续传：produced=0 written=0 rejected=0，输出与拒绝登记都不变
# 删掉 ckpt.json 再跑 = 强制重扫旧区间：好记录重放被下游去重（out.jsonl 仍 40 行），
#   坏记录不重复登记（rejects.jsonl 仍 3 行，rejected=0）
```

测试矩阵（`internal/pipeline/pipeline_test.go` 等）：

| 场景 | 测试 |
|---|---|
| 完整摄取 + 检查点推进 | `TestFullIngest` |
| 重启续传（不产生新写入） | `TestRestartResume` |
| 重复消费（位点丢失重放，下游去重） | `TestDuplicateConsumptionNoDuplicateWrites` |
| 坏记录分类拒绝后继续处理，位点推进到源末尾 | `TestBadRecordsRejectedAndContinue` |
| 重启重扫不重复登记拒绝、好记录不重复写入 | `TestRejectsNotDuplicatedAfterRestart` |
| 取消时在途批次丢弃、位点不推进，续传不丢不重 | `TestCancelStopsDeliveryAndCheckpoint` |
| 拒绝存储：运行内/跨重启去重、按类别汇总 | `internal/reject` 测试 |
| 下游写失败（错误分类 + 位点不越过失败批） | `TestSinkFailureClassified` |
| 并发取消（续传后不丢不重） | `TestConcurrentCancel` |
| 背压（慢下游 + 小队列不丢数据） | `TestBackpressureBlocking` |
| 吞吐随并发提升 | `TestThroughputScalesWithWorkers` |
| 配置缺失/配置形态不对/未知连接器类型可区分 | `connectors/*`、`internal/connector` 测试 |
| 整行无法解析归 `parse_error` 且可继续 | `connectors/jsonl`、`connectors/csv` 测试 |
| 位点回退/越界/源不匹配 | `internal/checkpoint`、`connectors/jsonl` 测试 |
| 注册冲突/重复初始化 | `internal/connector`、`connectors/*` 测试 |
