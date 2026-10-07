// Package reject 提供坏记录（被拒绝记录）的登记、去重与汇总能力。
//
// 语义：
//   - 每条坏记录按 (source, offset) 唯一标识，同一运行内只登记一次；
//   - FileStore 持久化到本地 JSONL 文件，重启续传时即使旧区间被重扫，
//     已登记的坏记录也不会重复登记、重复上报；
//   - 汇总（Summary）按错误分类统计，便于运维排查「改配置还是改数据」。
package reject

import (
	"context"
	"sync"
)

// Rejection 是一条被拒绝的坏记录。
type Rejection struct {
	Source string `json:"source"` // 数据源标识
	Seq    uint64 `json:"seq"`    // 源内序号（第几条记录）
	Offset int64  `json:"offset"` // 源内字节偏移（记录之后的位置）
	Class  string `json:"class"`  // 拒绝原因分类：record_invalid | record_unparseable
	Reason string `json:"reason"` // 具体原因描述
}

// Store 是拒绝记录存储契约。
type Store interface {
	// Add 登记一条拒绝记录；同 (source, offset) 已登记过时返回 added=false，不重复登记。
	Add(ctx context.Context, r Rejection) (added bool, err error)
	// List 返回 source 的全部拒绝记录（按 Offset 升序）。
	List(ctx context.Context, source string) ([]Rejection, error)
	// Summary 按分类统计 source 的拒绝记录数。
	Summary(ctx context.Context, source string) (map[string]int, error)
	// Close 释放资源，幂等。
	Close() error
}

// MemoryStore 是进程内拒绝记录存储（不持久化），用于测试与默认场景。
type MemoryStore struct {
	mu   sync.Mutex
	seen map[string]struct{}
	recs []Rejection
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{seen: map[string]struct{}{}} }

func (m *MemoryStore) Add(ctx context.Context, r Rejection) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key(r.Source, r.Offset)
	if _, dup := m.seen[k]; dup {
		return false, nil
	}
	m.seen[k] = struct{}{}
	m.recs = append(m.recs, r)
	return true, nil
}

func (m *MemoryStore) List(ctx context.Context, source string) ([]Rejection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Rejection
	for _, r := range m.recs {
		if r.Source == source {
			out = append(out, r)
		}
	}
	return out, nil
}

func (m *MemoryStore) Summary(ctx context.Context, source string) (map[string]int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return summaryLocked(m.recs, source), nil
}

func (m *MemoryStore) Close() error { return nil }

// FileStore 是基于本地 JSONL 文件的持久化拒绝记录存储（追加写 + fsync）。
// 打开时重建去重集合，因此重启后重扫旧区间不会重复登记。
type FileStore struct {
	path string

	mu   sync.Mutex
	seen map[string]struct{}
	recs []Rejection
}

func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

func (s *FileStore) Add(ctx context.Context, r Rejection) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addLocked(r)
}

func (s *FileStore) List(ctx context.Context, source string) ([]Rejection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return nil, err
	}
	var out []Rejection
	for _, r := range s.recs {
		if r.Source == source {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *FileStore) Summary(ctx context.Context, source string) (map[string]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return nil, err
	}
	return summaryLocked(s.recs, source), nil
}

func (s *FileStore) Close() error { return nil }

// key 返回拒绝记录的去重键：同数据源内字节偏移唯一标识一条记录。
func key(source string, offset int64) string {
	return source + "@" + itoa(offset)
}

func itoa(v int64) string {
	// 避免引入 strconv 之外的依赖；直接使用标准库。
	return strconvFormatInt(v)
}

func summaryLocked(recs []Rejection, source string) map[string]int {
	out := map[string]int{}
	for _, r := range recs {
		if r.Source == source {
			out[r.Class]++
		}
	}
	return out
}
