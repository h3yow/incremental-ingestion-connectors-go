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
| `internal/reject` | 坏记录拒绝登记、跨重启去重与分类汇总 |
| `internal/pipeline` | 并发摄取、背压、坏记录跳过、按批投递与有序提交 |
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
  坏记录不再中止整轮同步，而是**按类别拒掉、登记留痕后跳过**（见下节）。
- `Next` 遇到坏记录时返回记录级错误（`record_invalid` / `record_unparseable`）
  **连同该坏记录之后的位点**，调用方登记拒绝后可凭位点跳过并继续续读。
- 工厂或底层库的错误一律包装为框架 `*errors.Error`（未分类归为 `internal`），
  底层异常不会直接透出到对外接口。

新增数据源：实现 `Source`，写一个 `Register(*connector.Registry) error`，在 `cmd/ingest`
或自己的 main 中注册即可。

## 坏记录处理：拒绝、续跑与去重

- **分类拒绝**：坏记录按原因分为两类 —— `record_invalid`（必填信息缺失、字段形态不对）
  与 `record_unparseable`（整行/整文件无法按格式解析）。每条拒绝记录落盘到
  `rejects.jsonl`（JSONL，可直接查看），包含 `seq`（第几条）、`offset`、`class`、`reason`。
- **不阻塞本轮同步**：坏记录跳过之后，源中剩余的好记录全部正常投递；
  坏记录的位点与已投递事件位点一样进入连续前缀提交，因此**位点最终推进到源末尾**，
  即使文件末尾全是坏记录也不会卡住。
- **一次运行只登记一次**：拒绝记录按 `(source, offset)` 去重；登记（fsync）先于位点放行，
  崩溃窗口内重启重扫由存储去重兜住。
- **重启不重复登记、不重复上报**：`reject.FileStore` 打开时从 `rejects.jsonl` 重建去重
  集合，续传时即使旧区间被整体重扫（例如检查点丢失），已登记的坏记录只记 debug 日志，
  不再追加拒绝文件、不再发 WARN 上报。
- **可查看的汇总**：运行结束日志带 `reject_summary`（按类别计数），
  `cmd/ingest` 也会打印 `reject summary ... by_class=...`，便于判断该改配置还是改数据。

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

所有对外错误均为 `*internal/errors.Error`，用 `errors.ClassOf(err)` 取分类。
配置问题、数据问题、解析问题各自可区分，日志里按 `error_class` 分开统计：

| 分类 | 含义 | 该改什么 |
|---|---|---|
| `config_invalid` | 数据源配置写错、缺少必要配置（如缺 `path`） | 改配置 |
| `unknown_connector_kind` | 指定的数据源类型不存在（未注册） | 改配置/注册插件 |
| `record_invalid` | 记录内容不合规：必填字段缺失、字段形态不对 | 改数据 |
| `record_unparseable` | 整行/整文件无法按格式解析（JSON/CSV 语法错误） | 改数据 |
| `registration_conflict` | 连接器标识冲突或空标识 | 改代码 |
| `duplicate_init` | 数据源重复初始化 | 改代码 |
| `position_regression` / `position_out_of_range` / `position_source_mismatch` | 位点回退 / 越界 / 与源不匹配 | 检查位点 |
| `source_exhausted` | 数据源读取完毕（正常终止信号） | — |
| `sink_failure` | 下游写入失败 | 检查下游 |
| `canceled` | 上下文取消 | — |
| `internal` | 未分类内部错误（底层库错误一律包装成框架错误，不直接透出） | 排查 |

日志中每条错误都带 `error_class` 字段，批次日志带 `batch`/`offset`/`seq`，
运行结束日志带 `reject_summary` 分类计数。

## 背压与关闭策略

- 队列容量 `QueueSize` 是背压边界：下游阻塞时生产者阻塞在入队上，并每 200ms 打印
  `backpressure: queue full, producer blocked` 警告 —— 行为确定且可观测，不丢数据。
- 吞吐随并发度提升：worker 并行攒批投递，在下游成为瓶颈前近似线性扩展
  （测试 `TestThroughputScalesWithWorkers` 实测 8 worker 约 7.5x）。
- **取消/关闭的一致行为**：
  - 外部 `ctx` 取消立即生效：取消信号直接传播到 worker 与提交器，
    **不再投递、不再推进位点**，在途批次直接丢弃。
  - 取消那一刻已写出但尚未提交的部分，下次运行从最近提交位点重放，
    由 `FileSink` 幂等去重兜住 —— 不丢数据、不产生重复写入。
  - 结束后三者关系自洽：`committed <= batches`，`written <= produced`，
    已提交位点覆盖的事件一定是已投递事件的前缀；`written` 可能大于已提交覆盖量
    （已写未提交部分），这正是重放 + 去重语义所允许的。
  - `Pipeline.LastCommitted()` 返回本轮最近提交位点，`Run` 返回后提交数与位点
    不再变化（`TestCancelDoesNotAdvanceCheckpoint` 验证）；
    并发取消无数据竞争（`go test -race` 验证）。
- 下游写失败：该批次不提交位点，管线以 `sink_failure` 退出，重启后重放恢复。

## 本地复现与验证

```bash
# 全部测试（含竞态检测）
go test ./... -race

# 看每个用例覆盖的业务与结果
go test ./... -v | grep -E "覆盖:|结果:|^(=== RUN|--- (PASS|FAIL))"

# 端到端演示（含脏数据）
DEMO=$(mktemp -d)
for i in $(seq 1 20); do echo "{\"key\":\"k$i\",\"kind\":\"upsert\",\"payload\":{\"n\":$i}}"; done > $DEMO/data.jsonl
sed -i '5a {"kind":"upsert"}' $DEMO/data.jsonl          # 坏记录：缺 key -> record_invalid
sed -i '10a not-a-json-line' $DEMO/data.jsonl           # 坏记录：整行非 JSON -> record_unparseable
sed -i '15a {"key":"x","kind":"upsert","ts":"bad"}' $DEMO/data.jsonl  # 坏记录：ts 形态错 -> record_invalid

go run ./cmd/ingest -kind jsonl -path $DEMO/data.jsonl -out $DEMO/out.jsonl \
    -ckpt $DEMO/ckpt.json -rejects $DEMO/rejects.jsonl
# 预期：20 条好记录全部投递，3 条坏记录按类别 WARN 登记，位点推进到源末尾 seq=23

cat $DEMO/rejects.jsonl   # 可查看的拒绝记录：第几条、哪类原因、具体原因

go run ./cmd/ingest -kind jsonl -path $DEMO/data.jsonl -out $DEMO/out.jsonl \
    -ckpt $DEMO/ckpt.json -rejects $DEMO/rejects.jsonl
# 再次运行 = 重启续传：produced=0 written=0 rejected=0，输出与拒绝文件均不变

rm $DEMO/ckpt.json        # 模拟检查点丢失，强制重扫旧区间
go run ./cmd/ingest -kind jsonl -path $DEMO/data.jsonl -out $DEMO/out.jsonl \
    -ckpt $DEMO/ckpt.json -rejects $DEMO/rejects.jsonl
# 预期：rejected=0（坏记录不重复登记、不重复上报），out.jsonl 仍 20 行（下游幂等去重）
```

测试矩阵（`internal/pipeline/pipeline_test.go` 等，每个用例日志标注覆盖业务与结果）：

| 场景 | 测试 |
|---|---|
| 完整摄取 + 检查点推进 | `TestFullIngest` |
| 重启续传（不产生新写入） | `TestRestartResume` |
| 重复消费（位点丢失重放，下游去重） | `TestDuplicateConsumptionNoDuplicateWrites` |
| 坏记录按类别拒掉、好记录继续投递、位点到源末尾 | `TestBadRecordsRejectedAndSkipped` |
| 重启重扫旧区间不重复登记坏记录 | `TestBadRecordsNotReregisteredAfterRestart` |
| 拒绝记录登记/去重/汇总（含跨重启） | `internal/reject` 测试 |
| 坏行返回位点并可跳过续读 | `connectors/jsonl`、`connectors/csv` 测试 |
| 下游写失败（错误分类 + 位点不越过失败批） | `TestSinkFailureClassified` |
| 并发取消（续传后不丢不重） | `TestConcurrentCancel` |
| 取消后不再推进位点、统计自洽 | `TestCancelDoesNotAdvanceCheckpoint` |
| 背压（慢下游 + 小队列不丢数据） | `TestBackpressureBlocking` |
| 吞吐随并发提升 | `TestThroughputScalesWithWorkers` |
| 配置错误/未知类型/记录错误/解析错误分类可区分 | `connectors/*`、`internal/connector`、`internal/event` 测试 |
| 位点回退/越界/源不匹配 | `internal/checkpoint`、`connectors/jsonl` 测试 |
| 注册冲突/重复初始化 | `internal/connector`、`connectors/*` 测试 |
