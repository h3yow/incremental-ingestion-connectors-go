// Package jsonl is a connector for newline-delimited JSON files.
// Offset semantics: the offset is the decimal 1-based ordinal of the last
// consumed line ("0" / "" = nothing consumed). It is independent of byte
// positions, so the same file behaves identically across machines.
package jsonl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/connector"
)

// Kind is the registration identifier.
const Kind = "jsonl-file"

// CfgPath is the config key for the JSONL file (required).
const CfgPath = connector.CfgPathCommon

const maxRawForError = 256

// Connector reads JSONL files with a declared field schema.
type Connector struct {
	schema connector.Schema

	mu   sync.Mutex
	open bool
}

// New creates a JSONL connector. keyField names the string field used as the
// event key; fields declares the remaining typed fields.
func New(keyField string, fields []connector.Field) *Connector {
	return &Connector{schema: connector.Schema{KeyField: keyField, Fields: fields}}
}

func (c *Connector) Kind() string             { return Kind }
func (c *Connector) Schema() connector.Schema { return c.schema }

// Open opens the configured file. Opening the same connector instance twice
// is rejected as KindAlreadyInitialized.
func (c *Connector) Open(ctx context.Context, cfg connector.Config) (connector.SourceReader, *cerror.Error) {
	if err := ctx.Err(); err != nil {
		return nil, cerror.Wrap(cerror.ClassConnector, cerror.KindInvalidConfig,
			"open canceled", err)
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
			"jsonl connector requires a non-empty key field")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.open {
		return nil, cerror.New(cerror.ClassConnector, cerror.KindAlreadyInitialized,
			"jsonl connector instance is already open")
	}
	f, err := os.Open(path)
	if err != nil {
		// Underlying OS error is wrapped, not leaked.
		return nil, cerror.Wrap(cerror.ClassConnector, cerror.KindInvalidConfig,
			"cannot open jsonl file: "+path, err)
	}
	c.open = true
	return &Reader{
		conn:   c,
		file:   f,
		path:   path,
		schema: c.schema,
		tag:    Kind + ":" + path,
	}, nil
}

// Reader is the JSONL source reader.
type Reader struct {
	conn   *Connector
	file   *os.File
	path   string
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
			"jsonl reader is closed")
	}
	if maxRecords <= 0 {
		return connector.ReadResult{}, cerror.New(cerror.ClassInternal, cerror.KindInvalidConfig,
			"maxRecords must be positive")
	}

	start, cerr := connector.ParseResumeOrdinal(resumeOffset)
	if cerr != nil {
		return connector.ReadResult{}, cerr
	}

	// Seek to the beginning and scan to the resume point. We do not keep a
	// running byte cursor so that every Read call is self-contained and the
	// reader trivially survives repeated calls.
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return connector.ReadResult{}, cerror.Wrap(cerror.ClassConnector, cerror.KindMalformedRecord,
			"cannot seek source file", err)
	}
	sc := bufio.NewScanner(r.file)
	// Allow long lines (default 64KiB is too small for payloads).
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	var ordinal int64
	res := connector.ReadResult{}
	goodReached := false
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return res, cerror.Wrap(cerror.ClassInternal, cerror.KindSinkDelivery,
				"read canceled", err)
		}
		line := bytes.TrimSpace(sc.Bytes())
		ordinal++
		res.EndOrdinal = ordinal
		if ordinal <= start {
			continue // already consumed
		}
		if len(line) == 0 {
			// Blank lines are malformed records, not silent skips.
			res.Bad = append(res.Bad, connector.BadRecord{
				Ordinal: ordinal, Raw: "",
				Err: cerror.New(cerror.ClassRecord, cerror.KindMalformedRecord,
					"empty json line"),
			})
			if len(res.Events)+len(res.Bad) >= maxRecords {
				return res, nil
			}
			continue
		}
		var vals map[string]any
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&vals); err != nil {
			res.Bad = append(res.Bad, connector.BadRecord{
				Ordinal: ordinal, Raw: truncate(string(line)),
				Err: cerror.Wrap(cerror.ClassRecord, cerror.KindMalformedRecord,
					"line is not a valid JSON object", err),
			})
		} else {
			ev, nerr := connector.Normalize(r.schema, r.tag, fmt.Sprintf("%d", ordinal),
				connector.RawRecord{Values: vals})
			if nerr != nil {
				res.Bad = append(res.Bad, connector.BadRecord{
					Ordinal: ordinal, Raw: truncate(string(line)), Err: nerr,
				})
			} else {
				res.Events = append(res.Events, ev)
			}
		}
		// Stop only once the batch is full of GOOD events AND any bad
		// records immediately following them are also scanned, so the
		// committed watermark cannot strand bad lines past the last good one.
		if len(res.Events) >= maxRecords {
			if goodReached {
				return res, nil
			}
			goodReached = true
		}
	}
	if err := sc.Err(); err != nil {
		return res, cerror.Wrap(cerror.ClassRecord, cerror.KindMalformedRecord,
			"failed reading jsonl file: "+r.path, err)
	}
	res.EOF = true
	return res, nil
}

// Close closes the underlying file. Double close is an error and also marks
// the owning connector as available to reopen.
func (r *Reader) Close() *cerror.Error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return cerror.New(cerror.ClassConnector, cerror.KindNotInitialized,
			"jsonl reader already closed")
	}
	r.closed = true
	err := r.file.Close()
	r.conn.mu.Lock()
	r.conn.open = false
	r.conn.mu.Unlock()
	if err != nil {
		return cerror.Wrap(cerror.ClassInternal, cerror.KindInternal,
			"close jsonl file", err)
	}
	return nil
}

func truncate(s string) string {
	if len(s) <= maxRawForError {
		return s
	}
	return s[:maxRawForError] + "...(truncated)"
}
