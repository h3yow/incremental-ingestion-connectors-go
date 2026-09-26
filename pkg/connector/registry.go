package connector

import (
	"context"
	"sort"
	"sync"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"
)

// Registry is a connector plugin registry.
type Registry struct {
	mu         sync.RWMutex
	connectors map[string]Connector
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{connectors: make(map[string]Connector)}
}

var (
	defaultOnce sync.Once
	defaultReg  *Registry
)

// Default returns the process-global registry.
func Default() *Registry {
	defaultOnce.Do(func() { defaultReg = NewRegistry() })
	return defaultReg
}

// Register adds a connector. A duplicate kind is rejected with
// KindDuplicateRegister.
func (r *Registry) Register(c Connector) *cerror.Error {
	if c == nil {
		return cerror.New(cerror.ClassConnector, cerror.KindInvalidConfig,
			"cannot register a nil connector")
	}
	kind := c.Kind()
	if kind == "" {
		return cerror.New(cerror.ClassConnector, cerror.KindInvalidConfig,
			"connector Kind() must be non-empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.connectors[kind]; exists {
		return cerror.New(cerror.ClassConnector, cerror.KindDuplicateRegister,
			"connector kind already registered: "+kind)
	}
	r.connectors[kind] = c
	return nil
}

// Get returns the connector for kind, or KindConnectorNotFound.
func (r *Registry) Get(kind string) (Connector, *cerror.Error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.connectors[kind]
	if !ok {
		return nil, cerror.New(cerror.ClassConnector, cerror.KindConnectorNotFound,
			"no connector registered for kind: "+kind)
	}
	return c, nil
}

// Kinds lists registered connector kinds (sorted).
func (r *Registry) Kinds() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.connectors))
	for k := range r.connectors {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Open is a convenience: resolve kind and open the connector.
func (r *Registry) Open(ctx context.Context, kind string, cfg Config) (SourceReader, *cerror.Error) {
	c, cerr := r.Get(kind)
	if cerr != nil {
		return nil, cerr
	}
	return c.Open(ctx, cfg)
}

func parseDecimalOffset(s string) (int64, *cerror.Error) {
	var n int64
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return 0, cerror.New(cerror.ClassOffset, cerror.KindOffsetBadFormat,
				"offset must be a non-negative base-10 integer, got: "+s)
		}
		n = n*10 + int64(ch-'0')
		if n < 0 {
			return 0, cerror.New(cerror.ClassOffset, cerror.KindOffsetBadFormat,
				"offset overflows int64: "+s)
		}
	}
	return n, nil
}
