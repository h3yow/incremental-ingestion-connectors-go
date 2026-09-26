// Package connector 定义数据源连接器契约与插件注册机制。
package connector

import (
	"context"
	"sort"
	"sync"

	"github.com/h3yow/incremental-ingestion-connectors-go/internal/checkpoint"
	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/event"
)

// Config 是连接器配置（键值形式）。
type Config map[string]any

// Source 是数据源连接器契约。
type Source interface {
	// Name 返回数据源实例标识，用于位点匹配。
	Name() string
	// Open 从指定位点开始读取；重复初始化返回 duplicate_init。
	Open(ctx context.Context, from checkpoint.Position) error
	// Next 返回下一条原始记录及其后的位点；数据耗尽返回 source_exhausted 分类错误。
	Next(ctx context.Context) (event.Record, checkpoint.Position, error)
	// Close 释放资源，幂等。
	Close() error
}

// Factory 由插件提供，用于按配置构造 Source。
type Factory func(cfg Config) (Source, error)

// Registry 是连接器插件注册表，并发安全。
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

func NewRegistry() *Registry { return &Registry{factories: map[string]Factory{}} }

// Register 注册插件；标识冲突或空标识返回 registration_conflict。
func (r *Registry) Register(kind string, f Factory) error {
	const op = "connector.Registry.Register"
	if kind == "" {
		return ierr.New(ierr.ClassRegistrationConflict, op, "connector kind must not be empty")
	}
	if f == nil {
		return ierr.Newf(ierr.ClassRegistrationConflict, op, "factory for kind %q is nil", kind)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.factories[kind]; exists {
		return ierr.Newf(ierr.ClassRegistrationConflict, op, "connector kind %q is already registered", kind)
	}
	r.factories[kind] = f
	return nil
}

// New 按类型构造 Source；未知类型返回错误并列出可用类型。
func (r *Registry) New(kind string, cfg Config) (Source, error) {
	const op = "connector.Registry.New"
	r.mu.RLock()
	f, ok := r.factories[kind]
	r.mu.RUnlock()
	if !ok {
		return nil, ierr.Newf(ierr.ClassMissingField, op,
			"unknown connector kind %q (registered: %v)", kind, r.Kinds())
	}
	src, err := f(cfg)
	if err != nil {
		// 工厂返回的错误若未分类，归为 internal，避免底层错误直接透出。
		if _, isErr := err.(*ierr.Error); !isErr {
			return nil, ierr.Wrap(ierr.ClassInternal, op, err)
		}
		return nil, err
	}
	return src, nil
}

// Kinds 返回已注册的类型标识（排序后）。
func (r *Registry) Kinds() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	kinds := make([]string, 0, len(r.factories))
	for k := range r.factories {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	return kinds
}
