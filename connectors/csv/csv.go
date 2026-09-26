// Package csv is a connector for delimiter-separated local files using the
// standard library encoding/csv reader. As with the JSONL connector the
// offset is the decimal 1-based data-line ordinal (excluding the header).
package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/connector"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/event"
)

// Kind is the registration identifier.
const Kind = "csv-file"

// Config keys.
const (
	CfgPath      = connector.CfgPathCommon // path to the CSV file (required)
	CfgDelimiter = "delimiter"
)

// Connector reads delimiter-separated files with a declared schema.
type Connector struct {
	schema connector.Schema

	mu   sync.Mutex
	open bool
}

// New creates a CSV connector. keyField names the field used as event key;
// the key column is always read as a non-empty string.
func New(keyField string, fields []connector.Field) *Connector {
	return &Connector{schema: connector.Schema{KeyField: keyField, Fields: fields}}
}

func (c *Connector) Kind() string             { return Kind }
func (c *Connector) Schema() connector.Schema { return c.schema }

// Open opens the configured file. Opening the same connector instance twice
// is rejected as KindAlreadyInitialized.
func (c *Connector) Open(ctx context.Context, cfg connector.Config) (connector.SourceReader, *cerror.Error) {
	if err := ctx.Err(); err != nil {
		return nil, cerror.Wrap(cerror.ClassConnector, cerror.KindInvalidConfig, "open canceled", err)
	}
	raw, ok := cfg[CfgPath]
	if !ok {
		return nil, cerror.New(cerror.ClassConnector, cerror.KindInvalidConfig,
			"missing required config key: "+CfgPath)
	}
	path, ok := raw.(string)
	if !ok || path == "" {
		return nil, cerror.New(cerror.ClassConnector, cerror.KindInvalidConfig,
			fmt.Sprintf("config %q must be a non-empty string, got %T", CfgPath, raw))
	}
	if c.schema.KeyField == "" {
		return nil, cerror.New(cerror.ClassConnector, cerror.KindInvalidConfig,
			"csv connector requires a non-empty key field")
	}

	delim := ','
	if d, present := cfg[CfgDelimiter]; present {
		ds, isStr := d.(string)
		if !isStr || len(ds) != 1 {
			return nil, cerror.New(cerror.ClassConnector, cerror.KindInvalidConfig,
				fmt.Sprintf("config %q must be a single-character string, got %T", CfgDelimiter, d))
		}
		delim = rune(ds[0])
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.open {
		return nil, cerror.New(cerror.ClassConnector, cerror.KindAlreadyInitialized,
			"csv connector instance is already open")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, cerror.Wrap(cerror.ClassConnector, cerror.KindInvalidConfig,
			"cannot open csv file: "+path, err)
	}
	c.open = true
	return &Reader{
		conn: c, file: f, path: path, delim: delim,
		schema: c.schema, tag: Kind + ":" + path,
	}, nil
}

// Reader is the CSV source reader.
type Reader struct {
	conn   *Connector
	file   *os.File
	path   string
	delim  rune
	schema connector.Schema
	tag    string

	closed bool
	mu     sync.Mutex
}

func (r *Reader) Tag() string { return r.tag }

// Read implements connector.SourceReader.
func (r *Reader) Read(ctx context.Context, resumeOffset string, maxRecords int) (connector.ReadResult, *cerror.Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return connector.ReadResult{}, cerror.New(cerror.ClassConnector, cerror.KindNotInitialized,
			"csv reader is closed")
	}
	if maxRecords <= 0 {
		return connector.ReadResult{}, cerror.New(cerror.ClassInternal, cerror.KindInvalidConfig,
			"maxRecords must be positive")
	}
	start, cerr := connector.ParseResumeOrdinal(resumeOffset)
	if cerr != nil {
		return connector.ReadResult{}, cerr
	}

	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return connector.ReadResult{}, cerror.Wrap(cerror.ClassInternal, cerror.KindInternal,
			"cannot seek source file", err)
	}
	cr := csv.NewReader(r.file)
	cr.Comma = r.delim
	cr.FieldsPerRecord = -1 // we validate column counts ourselves with clear errors

	header, err := cr.Read()
	if err != nil {
		if err == io.EOF {
			return connector.ReadResult{}, cerror.New(cerror.ClassRecord, cerror.KindMalformedRecord,
				"csv file has no header row: "+r.path)
		}
		return connector.ReadResult{}, cerror.Wrap(cerror.ClassRecord, cerror.KindMalformedRecord,
			"cannot read csv header", err)
	}
	index := make(map[string]int, len(header))
	for i, h := range header {
		index[h] = i
	}
	keyCol, ok := index[r.schema.KeyField]
	if !ok {
		return connector.ReadResult{}, cerror.New(cerror.ClassConnector, cerror.KindInvalidConfig,
			fmt.Sprintf("key column %q not found in csv header %v", r.schema.KeyField, header))
	}
	specs := make([]colSpec, 0, len(r.schema.Fields))
	for _, f := range r.schema.Fields {
		col, present := index[f.Name]
		if !present {
			return connector.ReadResult{}, cerror.New(cerror.ClassConnector, cerror.KindInvalidConfig,
				fmt.Sprintf("declared field %q not found in csv header %v", f.Name, header))
		}
		specs = append(specs, colSpec{field: f, col: col})
	}

	var ordinal int64 // data-line ordinal, 1-based
	res := connector.ReadResult{}
	goodReached := false
	for {
		if err := ctx.Err(); err != nil {
			return res, cerror.Wrap(cerror.ClassInternal, cerror.KindInternal, "read canceled", err)
		}
		row, rerr := cr.Read()
		if rerr == io.EOF {
			res.EOF = true
			return res, nil
		}
		if rerr != nil {
			// Malformed CSV (ragged quotes, bad number of fields when fixed,
			// etc.). Reported, not skipped.
			ordinal++
			res.EndOrdinal = ordinal
			res.Bad = append(res.Bad, connector.BadRecord{
				Ordinal: ordinal,
				Raw:     truncateJoin(row),
				Err: cerror.Wrap(cerror.ClassRecord, cerror.KindMalformedRecord,
					fmt.Sprintf("malformed csv row at data line %d", ordinal), rerr),
			})
			if len(res.Events) >= maxRecords {
				if goodReached {
					return res, nil
				}
				goodReached = true
			}
			continue
		}
		ordinal++
		res.EndOrdinal = ordinal
		if ordinal <= start {
			continue
		}

		ev, nerr := normalizeCSV(r.schema, specs, r.tag, ordinal, keyCol, row)
		if nerr != nil {
			res.Bad = append(res.Bad, connector.BadRecord{
				Ordinal: ordinal, Raw: truncateJoin(row), Err: nerr,
			})
		} else {
			res.Events = append(res.Events, ev)
		}
		if len(res.Events) >= maxRecords {
			if goodReached {
				return res, nil
			}
			goodReached = true
		}
	}
}

type colSpec struct {
	field connector.Field
	col   int
}

func normalizeCSV(sch connector.Schema, specs []colSpec, source string, ordinal int64, keyCol int, row []string) (event.Event, *cerror.Error) {
	if keyCol >= len(row) {
		return event.Event{}, cerror.New(cerror.ClassRecord, cerror.KindMissingField,
			fmt.Sprintf("row %d has %d columns, key column %q needs index %d",
				ordinal, len(row), sch.KeyField, keyCol))
	}
	key := row[keyCol]
	if key == "" {
		return event.Event{}, cerror.New(cerror.ClassRecord, cerror.KindMissingField,
			fmt.Sprintf("key field %q is empty at row %d", sch.KeyField, ordinal))
	}
	payload := make(map[string]any, len(specs))
	for _, sp := range specs {
		if sp.col >= len(row) {
			if sp.field.Required {
				return event.Event{}, cerror.New(cerror.ClassRecord, cerror.KindMissingField,
					fmt.Sprintf("row %d has %d columns, required field %q needs index %d",
						ordinal, len(row), sp.field.Name, sp.col))
			}
			continue
		}
		text := row[sp.col]
		if text == "" {
			if sp.field.Required {
				return event.Event{}, cerror.New(cerror.ClassRecord, cerror.KindMissingField,
					fmt.Sprintf("required field %q is empty at row %d", sp.field.Name, ordinal))
			}
			continue
		}
		v, perr := convertCSVValue(sp.field, text, ordinal)
		if perr != nil {
			return event.Event{}, perr
		}
		payload[sp.field.Name] = v
	}
	return event.Event{
		Key:     key,
		Offset:  strconv.FormatInt(ordinal, 10),
		Payload: payload,
		Source:  source,
	}, nil
}

func convertCSVValue(f connector.Field, text string, ordinal int64) (any, *cerror.Error) {
	switch f.Type {
	case connector.TypeString:
		return text, nil
	case connector.TypeInt64:
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, cerror.Wrap(cerror.ClassRecord, cerror.KindTypeMismatch,
				fmt.Sprintf("field %q at row %d is not an int64: %q", f.Name, ordinal, text), err)
		}
		return n, nil
	case connector.TypeFloat:
		x, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, cerror.Wrap(cerror.ClassRecord, cerror.KindTypeMismatch,
				fmt.Sprintf("field %q at row %d is not a float64: %q", f.Name, ordinal, text), err)
		}
		return x, nil
	case connector.TypeBool:
		b, err := strconv.ParseBool(text)
		if err != nil {
			return nil, cerror.Wrap(cerror.ClassRecord, cerror.KindTypeMismatch,
				fmt.Sprintf("field %q at row %d is not a bool: %q", f.Name, ordinal, text), err)
		}
		return b, nil
	default:
		return nil, cerror.New(cerror.ClassInternal, cerror.KindInvalidConfig,
			fmt.Sprintf("schema field %q has unknown type %q", f.Name, f.Type))
	}
}

// Close closes the underlying file and releases the connector for reopen.
func (r *Reader) Close() *cerror.Error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return cerror.New(cerror.ClassConnector, cerror.KindNotInitialized,
			"csv reader already closed")
	}
	r.closed = true
	err := r.file.Close()
	r.conn.mu.Lock()
	r.conn.open = false
	r.conn.mu.Unlock()
	if err != nil {
		return cerror.Wrap(cerror.ClassInternal, cerror.KindInternal, "close csv file", err)
	}
	return nil
}

func truncateJoin(row []string) string {
	out := ""
	for i, c := range row {
		if i > 0 {
			out += ","
		}
		if len(out) > 256 {
			return out + "...(truncated)"
		}
		out += c
	}
	if len(out) > 256 {
		return out[:256] + "...(truncated)"
	}
	return out
}
