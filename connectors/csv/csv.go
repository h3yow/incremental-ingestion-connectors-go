// Package csv 提供 CSV 文件数据源连接器。
// 配置：{"path": "数据文件路径", "name": "数据源实例标识"}。
// CSV 必须含表头，至少包含 key、kind 两列；可选 payload（JSON 对象字符串）与 ts（RFC3339）列。
package csv

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	stderrors "errors"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/h3yow/incremental-ingestion-connectors-go/internal/checkpoint"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/connector"
	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/event"
)

// Kind 是注册标识。
const Kind = "csv"

// Register 把连接器注册进注册表。
func Register(r *connector.Registry) error {
	return r.Register(Kind, New)
}

// New 是连接器工厂。
func New(cfg connector.Config) (connector.Source, error) {
	const op = "csv.New"
	path, ok := cfg["path"].(string)
	if !ok || path == "" {
		return nil, ierr.New(ierr.ClassConfigInvalid, op, `config field "path" (string) is required`)
	}
	name, _ := cfg["name"].(string)
	if name == "" {
		name = Kind + ":" + path
	}
	return &source{name: name, path: path}, nil
}

type source struct {
	name string
	path string

	mu     sync.Mutex
	file   *os.File
	reader *bufio.Reader
	header []string
	offset int64
	seq    uint64
	size   int64
	opened bool
	closed bool
}

func (s *source) Name() string { return s.name }

func (s *source) Open(ctx context.Context, from checkpoint.Position) error {
	const op = "csv.Open"
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.opened {
		return ierr.Newf(ierr.ClassDuplicateInit, op, "source %q is already opened", s.name)
	}
	if from.Source != "" && from.Source != s.name {
		return ierr.Newf(ierr.ClassPositionSourceMismatch, op,
			"position source %q does not match %q", from.Source, s.name)
	}
	f, err := os.Open(s.path)
	if err != nil {
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	s.size = fi.Size()
	if from.Offset > s.size {
		f.Close()
		return ierr.Newf(ierr.ClassPositionOutOfRange, op,
			"resume offset %d exceeds file size %d", from.Offset, s.size)
	}
	s.reader = bufio.NewReader(f)
	// 表头总是从文件头读取；续传时先读表头再跳到指定位点。
	hdr, err := s.reader.ReadBytes('\n')
	if err != nil && !stderrors.Is(err, io.EOF) {
		f.Close()
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	header, err := parseLine(hdr)
	if err != nil {
		f.Close()
		return ierr.Wrap(ierr.ClassRecordUnparseable, op, err)
	}
	s.header = header
	headerEnd := int64(len(hdr))
	resume := from.Offset
	if resume < headerEnd {
		resume = headerEnd
	}
	if _, err := f.Seek(resume, io.SeekStart); err != nil {
		f.Close()
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	s.reader = bufio.NewReader(f)
	s.file = f
	s.offset = resume
	s.seq = from.Seq
	s.opened = true
	return nil
}

func (s *source) Next(ctx context.Context) (event.Record, checkpoint.Position, error) {
	const op = "csv.Next"
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.opened {
		return nil, checkpoint.Position{}, ierr.New(ierr.ClassInternal, op, "source is not opened")
	}
	if s.closed {
		return nil, checkpoint.Position{}, ierr.New(ierr.ClassCanceled, op, "source is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, checkpoint.Position{}, ierr.Wrap(ierr.ClassCanceled, op, err)
	}
	for {
		line, err := s.reader.ReadBytes('\n')
		if len(line) > 0 {
			s.offset += int64(len(line))
		}
		if err != nil && !stderrors.Is(err, io.EOF) {
			return nil, checkpoint.Position{}, ierr.Wrap(ierr.ClassInternal, op, err)
		}
		if len(strings.TrimSpace(string(line))) == 0 {
			if stderrors.Is(err, io.EOF) || s.offset >= s.size {
				return nil, checkpoint.Position{}, ierr.New(ierr.ClassSourceExhausted, op, "no more records")
			}
			continue // 跳过空行
		}
		fields, err := parseLine(line)
		if err != nil {
			// 整行无法按 CSV 解析：坏记录占用一个序号并返回其后的位点，
			// 调用方登记拒绝后可凭该位点跳过本行继续续读。
			s.seq++
			return nil, checkpoint.Position{Source: s.name, Offset: s.offset, Seq: s.seq},
				ierr.Wrap(ierr.ClassRecordUnparseable, op, err)
		}
		rec, err := s.toRecord(fields)
		if err != nil {
			// 记录内容不合规：同样返回其后的位点，支持跳过续读。
			s.seq++
			return nil, checkpoint.Position{Source: s.name, Offset: s.offset, Seq: s.seq}, err
		}
		s.seq++
		return rec, checkpoint.Position{Source: s.name, Offset: s.offset, Seq: s.seq}, nil
	}
}

// toRecord 按表头把一行字段映射为原始记录。
func (s *source) toRecord(fields []string) (event.Record, error) {
	const op = "csv.Next"
	if len(fields) != len(s.header) {
		return nil, ierr.Newf(ierr.ClassRecordInvalid, op,
			"row has %d fields, header has %d", len(fields), len(s.header))
	}
	rec := event.Record{}
	for i, col := range s.header {
		v := fields[i]
		switch col {
		case "key", "kind":
			rec[col] = v
		case "payload":
			if v == "" {
				continue
			}
			var p map[string]any
			if err := json.Unmarshal([]byte(v), &p); err != nil {
				return nil, ierr.Newf(ierr.ClassRecordInvalid, op, "payload column is not a JSON object: %v", err)
			}
			rec["payload"] = p
		case "ts":
			if v != "" {
				rec["ts"] = v
			}
		default:
			// 未知列并入 payload
			if rec["payload"] == nil {
				rec["payload"] = map[string]any{}
			}
			rec["payload"].(map[string]any)[col] = v
		}
	}
	return rec, nil
}

func (s *source) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return ierr.Wrap(ierr.ClassInternal, "csv.Close", err)
		}
	}
	return nil
}

// parseLine 用 encoding/csv 解析单行。
func parseLine(line []byte) ([]string, error) {
	r := csv.NewReader(strings.NewReader(string(line)))
	rec, err := r.Read()
	if err != nil && !stderrors.Is(err, io.EOF) {
		return nil, err
	}
	return rec, nil
}
