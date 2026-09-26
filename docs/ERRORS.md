# 错误分类

所有跨包边界返回的错误都是 `*cerror.Error`（`pkg/cerror`），底层 OS / 驱动错误只作为
`Cause` 被包装，不直接透出。调用方应基于稳定的 **Class（粗类）+ Kind（细分原因）**
做分支处理，而不是匹配底层错误文本。

日志中统一打印 `class` 与 `kind`（见 `record rejected`、`sink batch failed` 等事件）。

| Class | Kind | 触发场景 |
| --- | --- | --- |
| `connector_error` | `duplicate_connector_registered` | 同一 `Kind` 重复注册 |
| `connector_error` | `connector_not_found` | 打开未注册的数据源类型 |
| `connector_error` | `already_initialized` | 同一 Connector 实例未 Close 再次 Open |
| `connector_error` | `not_initialized` | 操作已 Close 的 reader / store |
| `connector_error` | `invalid_config` | 缺必填配置、配置类型错误、文件无法打开（OS 原因被包装）、Schema 列在表头缺失 |
| `record_error` | `missing_field` | 必填字段缺失、key 缺失或为空字符串 |
| `record_error` | `type_mismatch` | 字段值与声明类型不符（如 `"oops"` 给 int64、1.5 给 int64、`"yes"` 给 bool） |
| `record_error` | `malformed_record` | 非法 JSON、空行、畸形 CSV 行、无表头 |
| `offset_error` | `offset_stale` | 提交的 offset 等于或小于已提交 offset（重复提交/回退） |
| `offset_error` | `offset_out_of_range` | resume offset 超过数据源当前末尾 |
| `offset_error` | `offset_source_mismatch` | 检查点的 source_tag 与当前数据源不一致 |
| `offset_error` | `offset_bad_format` | offset 不是合法的非负十进制整数 |
| `checkpoint_error` | `checkpoint_corrupt` | 检查点文件无法读取/解析、原子写失败 |
| `sink_error` | `sink_delivery_failed` | 下游写入在重试预算内持续失败（含注入故障） |
| `internal_error` | `internal` | 取消、关闭底层文件失败等内部异常 |

## 程序内判断

```go
import "github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"

if ce, ok := cerror.As(err); ok {
    switch ce.Kind {
    case cerror.KindOffsetStale:
        // 回退/重复提交
    case cerror.KindMissingField, cerror.KindTypeMismatch:
        // 坏记录：计数 + 告警，不阻断整体进度
    }
}
if cerror.IsClass(err, cerror.ClassOffset) { /* 位点类问题 */ }
```

## 设计原则

1. **坏记录不静默**：每条坏记录至少经 `OnRecordError` 回调与结构化日志暴露一次；
   其序号进入检查点，重启后不重复报警。
2. **拒绝可区分**：回退、越界、标签不符、格式错是四种不同 `Kind`，不能笼统报"位点错"。
3. **底层不透传**：例如 `os.Open` 失败被包成 `connector_error/invalid_config`，
   原始 error 只出现在 `(cause: ...)` 中用于诊断。
4. **失败不留半成品**：位点回退在写文件前即被拒绝；sink 单条原子写，无半文件。
