// Package sink provides the downstream delivery contract.
package sink

import (
	"context"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/event"
)

// Result reports the outcome of one Write batch.
type Result struct {
	// Accepted is the number of records newly written.
	Accepted int
	// Deduped is the number of records already present (re-delivered).
	Deduped int
}

// Sink delivers normalized events. Implementations MUST be idempotent per
// event key: delivering the same key any number of times produces exactly
// one durable write.
type Sink interface {
	Write(ctx context.Context, events []event.Event) (Result, error)
	Close() error
}
