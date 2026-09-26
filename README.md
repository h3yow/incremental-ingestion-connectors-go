# Incremental Ingestion Connectors (Go)

一个可扩展的**本地数据摄取框架**：为多种数据源提供统一的**拉取 → 归一化 → 检查点 → 幂等投递**能力，
保证增量同步在**重启、重复消费、下游故障**下最终状态可预期——不丢数据、目标端不产生重复写入。

零第三方依赖（仅用 Go 标准库），内置两类本地可复现数据源：**JSONL 文件** 与 **CSV/TSV 文件**。

## 特性一览

| 能力 | 说明 |
| --- | --- |
| 统一连接器契约 | `Connector`（工厂）+ `SourceReader`（运行句柄），插件化 `Registry` 注册 |
| 记录归一化 | 原始记录统一为带类型的内部 `event.Event`（key / offset / payload / source） |
| 显式拒绝、原因可区分 | 缺字段、类型不匹配、坏行、空 key、注册冲突、重复初始化……全部用 `Class/Kind` 分类，**绝不静默跳过** |
| 底层异常不透传 | OS / 驱动错误统一包装成 `*cerror.Error`，对外只暴露稳定的分类 |
| 增量位点 + 检查点 | 按批次原子提交（临时文件 + fsync + rename），按数据源标签绑定 |
| 重启续传 | 从已提交位点之后继续；崩溃窗口的批次重放，由幂等目标端去重 |
| 位点防护 | 回退、重复提交、越界、格式非法、与数据源不匹配的位点均被**拒绝且原因可区分** |
| 并发摄取 + 背压 | 多 worker 并行写下游；有界任务队列，满载时喂入方确定阻塞并计数（可观测） |
| 优雅关闭 | 取消时不再提交任何"状态未知"的批次；无数据丢失、无重复提交、`-race` 干净 |
| 幂等目标端 | 文件型 sink 以 key 为粒度"临时文件 + rename"，跨进程重开仍去重 |
| 吞吐随并发提升 | 下游成为瓶颈前，worker 数近似线性提速（测试中 8 worker ≈ 7.3x） |

## 目录结构

```
pkg/
  event/        归一化内部事件
  cerror/       对外错误分类（Class 粗类 + Kind 细分原因）
  connector/    连接器契约、Schema、RawRecord 类型化取值、Registry
  checkpoint/   位点/幂等索引、原子文件存储、位点校验
  sink/         下游投递契约 + 幂等文件 sink
  engine/       并发摄取、背压、重试、优雅关闭
  logging/      slog 适配器
connectors/
  jsonl/        JSONL 文件连接器（offset = 行序号，十进制字符串）
  csv/          CSV/TSV 文件连接器（encoding/csv，同样的序号 offset 语义）
cmd/ingest-demo 可本地复现全部场景的命令行
docs/           设计与语义文档
```

## 快速开始

```bash
go test -race ./...                 # 全部正常 + 异常链路测试
go run ./cmd/ingest-demo --help

# 1) 生成含坏记录的样本
go run ./cmd/ingest-demo generate --kind jsonl --out /tmp/demo/in.jsonl --rows 20

# 2) 首次摄取（跑到 EOF 自动停）
go run ./cmd/ingest-demo run --kind jsonl \
  --source /tmp/demo/in.jsonl --checkpoint /tmp/demo/ckpt.json \
  --sink-dir /tmp/demo/out --workers 4 --batch-size 10

# 3) 再次运行：从位点 20 续传，目标端零重复
go run ./cmd/ingest-demo run --kind jsonl \
  --source /tmp/demo/in.jsonl --checkpoint /tmp/demo/ckpt.json \
  --sink-dir /tmp/demo/out --workers 4 --batch-size 10
```

更多异常场景（中途崩溃、位点回退/越界/标签不匹配、并发取消）见
[docs/VERIFICATION.md](docs/VERIFICATION.md)。

## 语义速览

- **投递语义**：每批次 `读取 → 并行幂等写入 → 全部成功后才原子提交位点`。
  崩溃发生在"写入之后、提交之前"只会导致该批次重放；sink 幂等 → **恰好一次的目标端状态**（at-least-once 投递 + 幂等提交）。
- **坏记录**：既不投递也不静默丢弃。首次出现即分类上报（日志 + 回调），其行序号随检查点持久化，**重启后不会重复报警**。
- **背压**：`QueueSize` 有界；下游阻塞时喂入方在满队列上阻塞并累加 `queue_full_blocks`，内存有上界。
- **关闭**：`ctx` 取消立即停止喂入新批次，在途批次不提交位点；合规 sink 需响应 `ctx`。

完整设计见 [docs/DESIGN.md](docs/DESIGN.md)，错误分类见 [docs/ERRORS.md](docs/ERRORS.md)。
