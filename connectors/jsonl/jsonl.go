// Package jsonl 提供 JSONL 文件数据源连接器。
// 配置：{"path": "数据文件路径", "name": "数据源实例标识"}。
package jsonl

import (
	"bufio"
	"context"
	"encoding/json"
	stderrors "errors"
	"io"
	"os"
	"sync"

	"github.com/h3yow/incremental-ingestion-connectors-go/internal/checkpoint"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/connector"
	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/event"
)

// Kind 是注册标识。
const Kind = "jsonl"

// Register 把连接器注册进注册表。
func Register(r *connector.Registry) error {
	return r.Register(Kind, New)
}

// New 是连接器工厂。
func New(cfg connector.Config) (connector.Source, error) {
	const op = "jsonl.New"
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
	offset int64
	seq    uint64
	size   int64
	opened bool
	closed bool
}

func (s *source) Name() string { return s.name }

func (s *source) Open(ctx context.Context, from checkpoint.Position) error {
	const op = "jsonl.Open"
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
	if _, err := f.Seek(from.Offset, io.SeekStart); err != nil {
		f.Close()
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	s.file = f
	s.reader = bufio.NewReader(f)
	s.offset = from.Offset
	s.seq = from.Seq
	s.opened = true
	return nil
}

func (s *source) Next(ctx context.Context) (event.Record, checkpoint.Position, error) {
	const op = "jsonl.Next"
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
	return s.nextLocked(ctx)
}

// nextLocked 递归读取下一行（调用方已持有锁）。
func (s *source) nextLocked(ctx context.Context) (event.Record, checkpoint.Position, error) {
	line, err := s.reader.ReadBytes('\n')
	if len(line) > 0 {
		s.offset += int64(len(line))
	}
	if err != nil && !stderrors.Is(err, io.EOF) {
		return nil, checkpoint.Position{}, ierr.Wrap(ierr.ClassInternal, "jsonl.Next", err)
	}
	if len(trimSpace(line)) == 0 {
		if stderrors.Is(err, io.EOF) || s.offset >= s.size {
			return nil, checkpoint.Position{}, ierr.New(ierr.ClassSourceExhausted, "jsonl.Next", "no more records")
		}
		return s.nextLocked(ctx)
	}
	var rec event.Record
	if jerr := json.Unmarshal(trimSpace(line), &rec); jerr != nil {
		// 整行无法解析：坏记录同样占用一个序号，返回其后的位点，
		// 调用方登记拒绝后可凭该位点跳过本行继续续读。
		s.seq++
		return nil, checkpoint.Position{Source: s.name, Offset: s.offset, Seq: s.seq},
			ierr.Wrap(ierr.ClassRecordUnparseable, "jsonl.Next", jerr)
	}
	s.seq++
	return rec, checkpoint.Position{Source: s.name, Offset: s.offset, Seq: s.seq}, nil
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
			return ierr.Wrap(ierr.ClassInternal, "jsonl.Close", err)
		}
	}
	return nil
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\r' || b[0] == '\n') {
		b = b[1:]
	}
	for len(b) > 0 {
		c := b[len(b)-1]
		if c != ' ' && c != '\t' && c != '\r' && c != '\n' {
			break
		}
		b = b[:len(b)-1]
	}
	return b
}
