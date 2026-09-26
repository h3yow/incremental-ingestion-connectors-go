# 本地复现与验证

环境：Go 1.25+（仓库在 Go 1.26 上验证），无外部服务、无第三方依赖。

## A. 一键自动化

```bash
go vet ./...
go test -race -count=1 ./...
```

覆盖矩阵（均在 `-race` 下运行）：

| 测试 | 覆盖链路 |
| --- | --- |
| `pkg/checkpoint` | 提交持久化、回退/重复提交拒绝、坏 offset 格式、标签不符、文件损坏、坏记录只报一次与剪枝、resume 校验四分支 |
| `pkg/connector` | 注册冲突/未找到、缺字段/空 key/类型错分类、错误不外泄底层类型 |
| `connectors/jsonl` | 归一化、三类坏记录分类、序号 resume、非法 offset、双开/双关、缺配置/文件错误包装 |
| `connectors/csv` | 归一化、类型错、空 key、缺列配置拒绝、TSV 分隔符、resume |
| `pkg/sink` | 批内与跨进程（重开目录）幂等、16 worker 并发零重复 |
| `pkg/engine` | 全量恰好一次、崩溃后续传无丢失无重复、重复运行幂等、下游故障重试后提交、并发取消（race）、背压有界可观测、8 worker ≈ 7.3x 提速 |

## B. 命令行手工复现

构建：

```bash
go build -o /tmp/ingest-demo ./cmd/ingest-demo
WORK=$(mktemp -d)
```

### B1. 首次摄取 + 坏记录显式拒绝

```bash
/tmp/ingest-demo generate --kind jsonl --out $WORK/in.jsonl --rows 20
/tmp/ingest-demo run --kind jsonl --source $WORK/in.jsonl \
  --checkpoint $WORK/ckpt.json --sink-dir $WORK/out --workers 4 --batch-size 10
```

日志可看到批次与位点：`batch committed ... offset=10 ...`；4 条坏记录分别打印
`missing_field` / `type_mismatch` / `malformed_record` 及**行序号与原因**。

### B2. 重启续传 + 重复消费不产生重复

```bash
/tmp/ingest-demo run --kind jsonl --source $WORK/in.jsonl \
  --checkpoint $WORK/ckpt.json --sink-dir $WORK/out --workers 4 --batch-size 10
ls $WORK/out | grep -c '^evt-'   # 始终等于好记录数 20
/tmp/ingest-demo inspect --checkpoint $WORK/ckpt.json
```

第二次运行从已提交 offset 续传，坏记录**不再重复报警**，目标端文件数不变。

### B3. 中途崩溃（下游故障）后续传

```bash
rm -f $WORK/ckpt.json; rm -f $WORK/out/evt-*
# 接受 7 条后下游持续失败，最多重试 2 次后中止（位点停在崩溃前最后一个完整批次）
/tmp/ingest-demo run --kind jsonl --source $WORK/in.jsonl \
  --checkpoint $WORK/ckpt.json --sink-dir $WORK/out \
  --workers 1 --batch-size 5 --fail-after 7 --max-attempts 2
ls $WORK/out | grep -c '^evt-'     # 崩溃窗口已落盘的文件数（>0）
# 恢复下游，不带 fail-after 重跑
/tmp/ingest-demo run --kind jsonl --source $WORK/in.jsonl \
  --checkpoint $WORK/ckpt.json --sink-dir $WORK/out --workers 4 --batch-size 5
ls $WORK/out | grep -c '^evt-'     # 必须恰好等于好记录总数，无重复
```

### B4. 位点异常被拒绝（原因可区分）

```bash
# 标签不符
python3 - <<PY
import json
d=json.load(open("$WORK/ckpt.json")); d["source_tag"]="jsonl-file:/other/file"
json.dump(d, open("$WORK/ckpt-tag.json","w"))
PY
/tmp/ingest-demo run --kind jsonl --source $WORK/in.jsonl \
  --checkpoint $WORK/ckpt-tag.json --sink-dir $WORK/out2
# -> offset_error / offset_source_mismatch

# 检查点损坏
echo '{nope' > $WORK/ckpt-bad.json
/tmp/ingest-demo run --kind jsonl --source $WORK/in.jsonl \
  --checkpoint $WORK/ckpt-bad.json --sink-dir $WORK/out3
# -> checkpoint_error / checkpoint_corrupt
```

回退（`offset_stale`）与越界（`offset_out_of_range`）由
`go test ./pkg/checkpoint/ -run TestCommitRejectsRollback -v` 与
`-run TestValidateResumeVariants -v` 直接断言。

### B5. 并发取消与背压

取消行为与有界背压由测试确定性复现：

```bash
go test -race ./pkg/engine/ -run TestConcurrentCancellation -v
go test -race ./pkg/engine/ -run TestQueueFullIsObservable -v
go test -race ./pkg/engine/ -run TestThroughputScalesWithWorkers -v
```

命令行侧可在摄取进行中按 `Ctrl-C`（demo 监听 SIGINT/SIGTERM）：进程会在当前批次
边界停止，日志停在最后一个 `batch committed` 的 offset；再次运行即从该位点续传。

### B6. CSV / TSV

```bash
/tmp/ingest-demo generate --kind csv --out $WORK/in.csv --rows 15
/tmp/ingest-demo run --kind csv --source $WORK/in.csv \
  --checkpoint $WORK/ckpt-csv.json --sink-dir $WORK/out-csv --workers 3 --batch-size 8
```

TSV 在库内通过 `delimiter: "\t"` 配置开启（见 `connectors/csv` 测试 `TestTSVDelimiter`）。

## C. 日志包含的关键信息

- 批次：`batch=N`、`events=`、`new_bad=`；
- 位点：启动 `checkpoint loaded offset=...`，提交 `batch committed offset=...`，
  结束 `source drained last_offset=...`；
- 归一化错误分类：`record rejected ordinal=.. class=record_error kind=missing_field|type_mismatch|malformed_record reason=.. raw=..`；
- 下游失败：`sink batch failed ... class=sink_error kind=sink_delivery_failed err=..`，
  并强调 `offset not committed`；
- 背压：结束行统计 `queue_blocks=N`（即 `Stats.QueueFullDrops`）。
