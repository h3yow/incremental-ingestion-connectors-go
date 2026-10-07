// Package event 定义源端记录归一化后的内部事件。
package event

import (
	"fmt"
	"time"

	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
)

// Record 是连接器产出的原始记录（字段名 -> 值）。
type Record map[string]any

// 必填字段名。
const (
	FieldKey  = "key"
	FieldKind = "kind"
)

// 可选字段名。
const (
	FieldPayload = "payload"
	FieldTime    = "ts"
)

// Event 是归一化后的内部事件。
type Event struct {
	Source  string         // 数据源标识
	Seq     uint64         // 源内单调递增序号
	Key     string         // 业务主键，用于幂等去重
	Kind    string         // 事件类型
	Payload map[string]any // 业务负载
	Time    time.Time      // 事件时间
}

// ID 返回幂等键：source/seq 唯一标识一次摄取，key 标识业务实体。
func (e Event) ID() string {
	return fmt.Sprintf("%s/%d/%s", e.Source, e.Seq, e.Key)
}

// Normalize 把原始记录归一化为内部事件。
// 缺少必填字段返回 missing_field，字段类型不符返回 type_mismatch。
func Normalize(source string, seq uint64, rec Record) (Event, error) {
	const op = "event.Normalize"

	key, err := requireString(rec, FieldKey)
	if err != nil {
		return Event{}, err
	}
	kind, err := requireString(rec, FieldKind)
	if err != nil {
		return Event{}, err
	}

	ev := Event{Source: source, Seq: seq, Key: key, Kind: kind, Time: time.Now().UTC()}

	if v, ok := rec[FieldPayload]; ok {
		p, ok := v.(map[string]any)
		if !ok {
			return Event{}, ierr.Newf(ierr.ClassRecordInvalid, op,
				"field %q must be object, got %T", FieldPayload, v)
		}
		ev.Payload = p
	}
	if v, ok := rec[FieldTime]; ok {
		s, ok := v.(string)
		if !ok {
			return Event{}, ierr.Newf(ierr.ClassRecordInvalid, op,
				"field %q must be RFC3339 string, got %T", FieldTime, v)
		}
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return Event{}, ierr.Newf(ierr.ClassRecordInvalid, op,
				"field %q is not valid RFC3339: %q", FieldTime, s)
		}
		ev.Time = t.UTC()
	}
	return ev, nil
}

func requireString(rec Record, field string) (string, error) {
	const op = "event.Normalize"
	v, ok := rec[field]
	if !ok {
		return "", ierr.Newf(ierr.ClassRecordInvalid, op, "required field %q is missing", field)
	}
	s, ok := v.(string)
	if !ok {
		return "", ierr.Newf(ierr.ClassRecordInvalid, op, "field %q must be string, got %T", field, v)
	}
	if s == "" {
		return "", ierr.Newf(ierr.ClassRecordInvalid, op, "required field %q is empty", field)
	}
	return s, nil
}
