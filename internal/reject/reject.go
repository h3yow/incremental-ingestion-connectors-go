// Package reject 记录被明确拒绝的坏记录：按类别留痕、跨重启去重、可汇总查看。
//
// 语义：
//   - 每条坏记录以 (source, offset) 为身份，同一条只登记一次；
//   - 登记持久化到本地文件（JSONL 追加），重启后重建去重集，
//     续传时旧区间被重扫也不会重复登记、重复上报；
//   - Summary 提供按类别聚合的可查看汇总。
package reject

import (
	"bufio"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
)

// Entry 是一条被拒记录的痕迹。
type Entry struct {
	Source string `json:"source"` // 数据源标识
	Offset int64  `json:"offset"` // 坏记录之后的字节偏移（身份的一部分）
	Seq    uint64 `json:"seq"`    // 登记时源内已确认序号（信息性）
	Class  string `json:"class"`  // 拒绝类别（框架错误分类）
	Reason string `json:"reason"` // 具体原因
}

// key 返回去重身份：同一源内同一偏移的坏记录只登记一次。
func (e Entry) key() string {
	return fmt.Sprintf("%s/%d", e.Source, e.Offset)
}

// Summary 是按类别聚合的拒绝统计。
type Summary struct {
	Total   int            `json:"total"`
	ByClass map[string]int `json:"by_class"`
}

// Store 是拒绝记录存储：JSONL 文件追加 + 内存去重集，并发安全。
type Store struct {
	mu      sync.Mutex
	path    string
	seen    map[string]struct{}
	entries []Entry
}

// Open 打开（或创建）拒绝记录存储；path 为空时仅内存存储（不持久化）。
// 已有文件中的登记会被加载用于去重，保证重启续传不重复登记。
func Open(path string) (*Store, error) {
	const op = "reject.Open"
	s := &Store{path: path, seen: map[string]struct{}{}}
	if path == "" {
		return s, nil
	}
	f, err := os.Open(path)
	switch {
	case err == nil:
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
		for sc.Scan() {
			var e Entry
			if json.Unmarshal(sc.Bytes(), &e) == nil {
				s.entries = append(s.entries, e)
				s.seen[e.key()] = struct{}{}
			}
		}
		if err := sc.Err(); err != nil {
			return nil, ierr.Wrap(ierr.ClassInternal, op, err)
		}
	case stderrors.Is(err, fs.ErrNotExist):
	default:
		return nil, ierr.Wrap(ierr.ClassInternal, op, err)
	}
	return s, nil
}

// Add 登记一条拒绝；已登记过（含重启前）返回 false，不重复写入、不重复上报。
func (s *Store) Add(e Entry) (bool, error) {
	const op = "reject.Store.Add"
	s.mu.Lock()
	defer s.mu.Unlock()

	k := e.key()
	if _, dup := s.seen[k]; dup {
		return false, nil // 跨重启去重：同一条坏记录只登记一次
	}
	if s.path != "" {
		if err := s.appendLocked(e); err != nil {
			return false, err
		}
	}
	s.seen[k] = struct{}{}
	s.entries = append(s.entries, e)
	return true, nil
}

// appendLocked 追加写入一条登记（调用方已持有锁）。
func (s *Store) appendLocked(e Entry) error {
	const op = "reject.Store.append"
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return ierr.Wrap(ierr.ClassInternal, op, err)
		}
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	defer f.Close()
	data, err := json.Marshal(e)
	if err != nil {
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	if err := f.Sync(); err != nil {
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	return nil
}

// Entries 返回全部已登记记录（按登记顺序）。
func (s *Store) Entries() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Entry, len(s.entries))
	copy(out, s.entries)
	return out
}

// Summary 返回按类别聚合的统计，可直接打印或序列化查看。
func (s *Store) Summary() Summary {
	s.mu.Lock()
	defer s.mu.Unlock()
	sum := Summary{Total: len(s.entries), ByClass: map[string]int{}}
	for _, e := range s.entries {
		sum.ByClass[e.Class]++
	}
	return sum
}
