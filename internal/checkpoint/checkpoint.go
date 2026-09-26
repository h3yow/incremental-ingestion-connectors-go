// Package checkpoint 提供增量位点与检查点管理。
package checkpoint

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
)

// Position 表示一个增量位点。
type Position struct {
	Source string `json:"source"` // 数据源标识
	Offset int64  `json:"offset"` // 源内偏移（如文件字节偏移）
	Seq    uint64 `json:"seq"`    // 已确认的源内序号
}

// ValidateAgainst 校验 p 是否可以作为 cur 的下一个提交位点。
// 回退、越界（由 maxOffset 限定，<0 表示不限制）或源不匹配都会被拒绝且原因可区分。
func (p Position) ValidateAgainst(cur Position, maxOffset int64) error {
	const op = "checkpoint.Validate"
	if p.Source != cur.Source {
		return ierr.Newf(ierr.ClassPositionSourceMismatch, op,
			"position source %q does not match current source %q", p.Source, cur.Source)
	}
	if p.Offset < cur.Offset || p.Seq < cur.Seq {
		return ierr.Newf(ierr.ClassPositionRegression, op,
			"position {offset:%d seq:%d} regresses from {offset:%d seq:%d}",
			p.Offset, p.Seq, cur.Offset, cur.Seq)
	}
	if maxOffset >= 0 && p.Offset > maxOffset {
		return ierr.Newf(ierr.ClassPositionOutOfRange, op,
			"offset %d exceeds source bound %d", p.Offset, maxOffset)
	}
	return nil
}

// Store 是检查点存储契约。
type Store interface {
	Load(ctx context.Context, source string) (Position, error)
	Commit(ctx context.Context, p Position) error
}

// FileStore 是基于本地文件的检查点存储（原子写：临时文件 + rename）。
type FileStore struct {
	path string
	mu   sync.Mutex
}

func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// Load 读取 source 的最近提交位点；文件不存在时返回零位点（从头开始）。
func (s *FileStore) Load(ctx context.Context, source string) (Position, error) {
	const op = "checkpoint.FileStore.Load"
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if stderrors.Is(err, fs.ErrNotExist) {
		return Position{Source: source}, nil
	}
	if err != nil {
		return Position{}, ierr.Wrap(ierr.ClassInternal, op, err)
	}
	var p Position
	if err := json.Unmarshal(data, &p); err != nil {
		return Position{}, ierr.Wrap(ierr.ClassInternal, op, err)
	}
	if p.Source != source {
		return Position{}, ierr.Newf(ierr.ClassPositionSourceMismatch, op,
			"stored checkpoint belongs to source %q, not %q", p.Source, source)
	}
	return p, nil
}

// Commit 原子提交位点；相对已提交位点回退或与已提交位点不属于同一源会被拒绝。
func (s *FileStore) Commit(ctx context.Context, p Position) error {
	const op = "checkpoint.FileStore.Commit"
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, err := s.loadLocked(p.Source)
	if err != nil {
		return err
	}
	if err := p.ValidateAgainst(cur, -1); err != nil {
		return err
	}

	data, err := json.Marshal(p)
	if err != nil {
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return ierr.Wrap(ierr.ClassInternal, op, err)
		}
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	return nil
}

func (s *FileStore) loadLocked(source string) (Position, error) {
	const op = "checkpoint.FileStore.load"
	data, err := os.ReadFile(s.path)
	if stderrors.Is(err, fs.ErrNotExist) {
		return Position{Source: source}, nil
	}
	if err != nil {
		return Position{}, ierr.Wrap(ierr.ClassInternal, op, err)
	}
	var p Position
	if err := json.Unmarshal(data, &p); err != nil {
		return Position{}, ierr.Wrap(ierr.ClassInternal, op, err)
	}
	return p, nil
}
