// Package connector defines the source connector contract.
package connector

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/event"
)

// FieldType enumerates the normalized value types a connector can declare.
type FieldType string

const (
	TypeString FieldType = "string"
	TypeInt64  FieldType = "int64"
	TypeFloat  FieldType = "float64"
	TypeBool   FieldType = "bool"
)

// Field describes one normalized field and its expected type.
type Field struct {
	Name     string
	Type     FieldType
	Required bool
}

// Schema is the ordered set of fields a connector emits.
type Schema struct {
	KeyField string
	Fields   []Field
}

// RawRecord is a record before normalization. Values are raw driver values;
// consumers must use the typed getters, which return classified errors
// instead of panicking on missing fields / type mismatches.
type RawRecord struct {
	Values map[string]any
}

func (r RawRecord) missingOrType(name string, cause error) *cerror.Error {
	if cause == nil {
		return cerror.New(cerror.ClassRecord, cerror.KindMissingField, fmt.Sprintf("field %q is absent", name))
	}
	return cerror.Wrap(cerror.ClassRecord, cerror.KindTypeMismatch,
		fmt.Sprintf("field %q has incompatible type", name), cause)
}

// String returns a required string value or a classified record error.
func (r RawRecord) String(name string) (string, *cerror.Error) {
	v, ok := r.Values[name]
	if !ok || v == nil {
		return "", r.missingOrType(name, nil)
	}
	s, ok := v.(string)
	if !ok {
		return "", r.missingOrType(name, fmt.Errorf("got %T", v))
	}
	return s, nil
}

// Int64 returns a required int64 value or a classified record error.
// JSON numbers decoded via encoding/json arrive as float64; integral values
// in the int64 range are accepted, non-integral ones are type mismatches.
// json.Number is accepted as well.
func (r RawRecord) Int64(name string) (int64, *cerror.Error) {
	v, ok := r.Values[name]
	if !ok || v == nil {
		return 0, r.missingOrType(name, nil)
	}
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case float64:
		if n != float64(int64(n)) {
			return 0, r.missingOrType(name, fmt.Errorf("non-integral number %v", n))
		}
		return int64(n), nil
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, r.missingOrType(name, err)
		}
		return i, nil
	default:
		return 0, r.missingOrType(name, fmt.Errorf("got %T", v))
	}
}

// Float64 returns a required float64 value or a classified record error.
func (r RawRecord) Float64(name string) (float64, *cerror.Error) {
	v, ok := r.Values[name]
	if !ok || v == nil {
		return 0, r.missingOrType(name, nil)
	}
	switch n := v.(type) {
	case float64:
		return n, nil
	case int64:
		return float64(n), nil
	case int:
		return float64(n), nil
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, r.missingOrType(name, err)
		}
		return f, nil
	default:
		return 0, r.missingOrType(name, fmt.Errorf("got %T", v))
	}
}

// Bool returns a required bool value or a classified record error.
func (r RawRecord) Bool(name string) (bool, *cerror.Error) {
	v, ok := r.Values[name]
	if !ok || v == nil {
		return false, r.missingOrType(name, nil)
	}
	b, ok := v.(bool)
	if !ok {
		return false, r.missingOrType(name, fmt.Errorf("got %T", v))
	}
	return b, nil
}

// Normalize validates rec against schema and produces an internal event.
// Missing required fields and type mismatches are rejected with distinct
// error kinds; declared optional fields that are absent are simply omitted.
// The key field must exist and be a non-empty string.
func Normalize(sch Schema, source, offset string, rec RawRecord) (event.Event, *cerror.Error) {
	key, cerr := rec.String(sch.KeyField)
	if cerr != nil {
		return event.Event{}, cerr
	}
	if key == "" {
		return event.Event{}, cerror.New(cerror.ClassRecord, cerror.KindMissingField,
			fmt.Sprintf("key field %q is empty", sch.KeyField))
	}
	payload := make(map[string]any, len(sch.Fields))
	for _, f := range sch.Fields {
		raw, present := rec.Values[f.Name]
		if !present || raw == nil {
			if f.Required {
				return event.Event{}, cerror.New(cerror.ClassRecord, cerror.KindMissingField,
					fmt.Sprintf("required field %q is absent", f.Name))
			}
			continue
		}
		var val any
		switch f.Type {
		case TypeString:
			s, e := rec.String(f.Name)
			if e != nil {
				return event.Event{}, e
			}
			val = s
		case TypeInt64:
			i, e := rec.Int64(f.Name)
			if e != nil {
				return event.Event{}, e
			}
			val = i
		case TypeFloat:
			x, e := rec.Float64(f.Name)
			if e != nil {
				return event.Event{}, e
			}
			val = x
		case TypeBool:
			b, e := rec.Bool(f.Name)
			if e != nil {
				return event.Event{}, e
			}
			val = b
		default:
			return event.Event{}, cerror.New(cerror.ClassInternal, cerror.KindInvalidConfig,
				fmt.Sprintf("schema field %q has unknown type %q", f.Name, f.Type))
		}
		payload[f.Name] = val
	}
	return event.Event{Key: key, Offset: offset, Payload: payload, Source: source}, nil
}

// BadRecord is one source record that was explicitly rejected during
// normalization. Bad records are never skipped silently: the engine invokes
// the configured error handler and logs them with their offset and the
// classified reason.
type BadRecord struct {
	// Ordinal is the 1-based source ordinal of the rejected record.
	Ordinal int64
	// Raw is the raw record text (truncated for logging safety).
	Raw string
	// Err is the classified rejection reason (KindMissingField,
	// KindTypeMismatch, KindMalformedRecord, ...).
	Err *cerror.Error
}

// ReadResult is one read batch.
type ReadResult struct {
	Events []event.Event
	Bad    []BadRecord
	// EndOrdinal is the 1-based ordinal of the last line examined (including
	// bad records), so resume validation can detect out-of-range offsets.
	EndOrdinal int64
	// EOF means the source was scanned to its physical end in this call.
	EOF bool
}

// CfgPath is the conventional config key for the source file path, shared
// by the built-in file-based connectors.
const CfgPathCommon = "path"

// Config is the free-form connector configuration.
type Config map[string]any

// SourceReader is the runtime, initialized handle to one data source.
type SourceReader interface {
	// Tag is a stable identifier of the concrete source (kind + physical
	// identity). A checkpoint whose tag differs is rejected on resume.
	Tag() string

	// Read returns up to maxRecords good events starting strictly after the
	// given resume offset (empty means the beginning). Rejected records are
	// returned in ReadResult.Bad with their classified reason, never skipped
	// silently. ReadResult.EndOrdinal always reflects the physical scan
	// position (good + bad records).
	Read(ctx context.Context, resumeOffset string, maxRecords int) (ReadResult, *cerror.Error)

	// Close releases source resources. Double close is an error.
	Close() *cerror.Error
}

// Connector is the reusable factory for one source kind.
type Connector interface {
	// Kind is the registration identifier.
	Kind() string

	// Schema declares the normalized shape.
	Schema() Schema

	// Open initializes a reader. Opening the same connector instance twice
	// without closing must fail with KindAlreadyInitialized.
	Open(ctx context.Context, cfg Config) (SourceReader, *cerror.Error)
}

// ParseResumeOrdinal parses a decimal-ordinal resume offset. Connectors
// using the built-in ordinal encoding delegate here so bad offsets are
// rejected uniformly with KindOffsetBadFormat.
func ParseResumeOrdinal(offset string) (int64, *cerror.Error) {
	if offset == "" {
		return 0, nil
	}
	return parseDecimalOffset(offset)
}
