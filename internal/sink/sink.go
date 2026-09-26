// Package sink 定义下游投递契约与幂等实现。
package sink

import (
	"bufio"
	"context"
	"encoding/json"
	stderrors "errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/event"
)

// Sink 是下游投递契约。WriteBatch 必须幂等：重复写入同一批事件不得产生重复结果。
type Sink interface {
	WriteBatch(ctx context.Context, batch []event.Event) error
	Close() error
}

// FileSink 是幂等的本地文件下游：按事件 ID 去重后追加写 JSONL。
// 已写入的 ID 集合在打开时从输出文件重建，因此进程重启后重复投递仍被去重。
type FileSink struct {
	path string

	mu   sync.Mutex
	file *os.File
	seen map[string]struct{}
}

func NewFileSink(path string) *FileSink { return &FileSink{path: path} }

// open 惰性打开输出文件并重建去重集合。
func (s *FileSink) open() error {
	const op = "sink.FileSink.open"
	if s.file != nil {
		return nil
	}
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return ierr.Wrap(ierr.ClassSinkFailure, op, err)
		}
	}
	s.seen = map[string]struct{}{}
	// 重建已写入 ID 集合，保证重启后的幂等性。
	f, err := os.Open(s.path)
	switch {
	case err == nil:
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
		for sc.Scan() {
			var ev event.Event
			if json.Unmarshal(sc.Bytes(), &ev) == nil {
				s.seen[ev.ID()] = struct{}{}
			}
		}
		f.Close()
	case stderrors.Is(err, fs.ErrNotExist):
	default:
		return ierr.Wrap(ierr.ClassSinkFailure, op, err)
	}
	s.file, err = os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return ierr.Wrap(ierr.ClassSinkFailure, op, err)
	}
	return nil
}

// WriteBatch 幂等写入一批事件：已存在的事件 ID 被跳过，新事件追加并 fsync。
func (s *FileSink) WriteBatch(ctx context.Context, batch []event.Event) error {
	const op = "sink.FileSink.WriteBatch"
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.open(); err != nil {
		return err
	}
	w := bufio.NewWriter(s.file)
	for _, ev := range batch {
		if err := ctx.Err(); err != nil {
			return ierr.Wrap(ierr.ClassCanceled, op, err)
		}
		id := ev.ID()
		if _, dup := s.seen[id]; dup {
			continue // 幂等去重：重复消费不产生重复写入
		}
		data, err := json.Marshal(ev)
		if err != nil {
			return ierr.Wrap(ierr.ClassSinkFailure, op, err)
		}
		if _, err := w.Write(append(data, '\n')); err != nil {
			return ierr.Wrap(ierr.ClassSinkFailure, op, err)
		}
		s.seen[id] = struct{}{}
	}
	if err := w.Flush(); err != nil {
		return ierr.Wrap(ierr.ClassSinkFailure, op, err)
	}
	if err := s.file.Sync(); err != nil {
		return ierr.Wrap(ierr.ClassSinkFailure, op, err)
	}
	return nil
}

func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	if err != nil {
		return ierr.Wrap(ierr.ClassSinkFailure, "sink.FileSink.Close", err)
	}
	return nil
}
