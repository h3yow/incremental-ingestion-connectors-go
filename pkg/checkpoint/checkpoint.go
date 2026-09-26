// Package checkpoint implements durable, atomic batch positions and the
// per-source idempotency index.
package checkpoint

import (
	"strconv"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/event"
)

// Checkpoint is the persisted state for one source.
type Checkpoint struct {
	// SourceTag identifies the concrete source this checkpoint belongs to.
	SourceTag string `json:"source_tag"`

	// Offset is the last fully committed source offset ("" = nothing).
	Offset string `json:"offset"`

	// AppliedKeys is the idempotency index: every event key that has been
	// delivered at offsets > Offset (retained for crash-dedup) plus the keys
	// delivered exactly at Offset.
	AppliedKeys []string `json:"applied_keys"`

	// BadOrdinals are source ordinals of records that were rejected during
	// normalization and already reported. Retaining them ensures each bad
	// record is surfaced exactly once even across restarts. Ordinals strictly
	// below Offset are pruned.
	BadOrdinals []int64 `json:"bad_ordinals"`
}

// CommitRequest is one atomic checkpoint advancement.
type CommitRequest struct {
	// Offset is the batch high-watermark.
	Offset string
	// Events are exactly the good records of the batch. Their keys are
	// merged into the idempotency index and keys belonging to earlier
	// offsets are pruned afterwards.
	Events []event.Event

	// BadOrdinals are bad-record ordinals that are committed together with
	// this watermark so they are reported at most once.
	BadOrdinals []int64
}

// Store loads and commits checkpoints for exactly one source.
//
// Implementations must make Commit atomic (temp file + fsync + rename) and
// must reject a commit whose offset is not strictly after the current
// offset (rollback protection).
type Store interface {
	// SourceTag returns the tag this store is bound to.
	SourceTag() string

	// Load returns the current checkpoint, or an empty checkpoint (empty
	// offset, empty index) on first run.
	Load() (Checkpoint, *cerror.Error)

	// Commit atomically persists the new offset and the merged idempotency
	// index. A stale or equal offset yields KindOffsetStale; an unparsable
	// offset yields KindOffsetBadFormat.
	Commit(req CommitRequest) *cerror.Error

	// Close flushes and releases resources.
	Close() *cerror.Error
}

// ParseOffset parses the common decimal-ordinal offset encoding used by the
// built-in connectors ("" and "0" both mean the beginning).
func ParseOffset(s string) (int64, *cerror.Error) {
	if s == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, cerror.Wrap(cerror.ClassOffset, cerror.KindOffsetBadFormat,
			"offset must be a non-negative base-10 integer, got: "+s, err)
	}
	return n, nil
}

// CompareOffsets compares two offsets for the same source.
// Returns -1/0/1. Empty and "0" are equal. Unparsable offsets yield a
// classified error.
func CompareOffsets(a, b string) (int, *cerror.Error) {
	na, ea := ParseOffset(a)
	if ea != nil {
		return 0, ea
	}
	nb, eb := ParseOffset(b)
	if eb != nil {
		return 0, eb
	}
	switch {
	case na < nb:
		return -1, nil
	case na > nb:
		return 1, nil
	default:
		return 0, nil
	}
}

// ValidateResume checks a resume offset against the bound source tag and the
// reader's declared end offset.
//   - empty offset always valid (full replay)
//   - tag mismatch      -> KindOffsetMismatch
//   - bad format        -> KindOffsetBadFormat
//   - offset > end      -> KindOffsetOutOfRange
func ValidateResume(storeTag, checkpointTag, resumeOffset, endOffset string) *cerror.Error {
	if checkpointTag != "" && storeTag != checkpointTag {
		return cerror.New(cerror.ClassOffset, cerror.KindOffsetMismatch,
			"checkpoint belongs to source "+checkpointTag+", but store is bound to "+storeTag)
	}
	n, cerr := ParseOffset(resumeOffset)
	if cerr != nil {
		return cerr
	}
	end, cerr := ParseOffset(endOffset)
	if cerr != nil {
		return cerr
	}
	if n > end {
		return cerror.New(cerror.ClassOffset, cerror.KindOffsetOutOfRange,
			"resume offset "+resumeOffset+" exceeds source end "+endOffset)
	}
	return nil
}
