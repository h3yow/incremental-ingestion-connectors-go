// Package logging bridges the standard library slog.Logger to engine.Logger.
package logging

import (
	"log/slog"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/engine"
)

// SlogAdapter adapts *slog.Logger to engine.Logger.
type SlogAdapter struct{ L *slog.Logger }

// New returns an adapter over l (nil -> slog.Default).
func New(l *slog.Logger) SlogAdapter {
	if l == nil {
		l = slog.Default()
	}
	return SlogAdapter{L: l}
}

func (a SlogAdapter) Info(msg string, kv ...any)  { a.L.Info(msg, kv...) }
func (a SlogAdapter) Error(msg string, kv ...any) { a.L.Error(msg, kv...) }

var _ engine.Logger = SlogAdapter{}
