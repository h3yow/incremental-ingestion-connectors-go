package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"
)

// FileStore is a JSON-file backed Store with atomic rename commits.
type FileStore struct {
	path      string
	sourceTag string

	mu     sync.Mutex
	loaded bool
	offset int64
	keyAt  map[string]int64   // key -> maximum ordinal at which it was delivered
	badAt  map[int64]struct{} // bad-record ordinals (>= committed offset)
	closed bool
}

// NewFileStore binds a store to path and sourceTag. On Load, an existing
// file whose source tag differs is rejected with KindOffsetMismatch and a
// malformed file with KindCheckpointCorrupt.
func NewFileStore(path, sourceTag string) *FileStore {
	return &FileStore{
		path:      path,
		sourceTag: sourceTag,
		keyAt:     make(map[string]int64),
		badAt:     make(map[int64]struct{}),
	}
}

func (s *FileStore) SourceTag() string { return s.sourceTag }

// Load reads and validates the checkpoint file. First run (no file yet) is a
// normal empty checkpoint, not an error.
func (s *FileStore) Load() (Checkpoint, *cerror.Error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			s.loaded = true
			return Checkpoint{SourceTag: s.sourceTag}, nil
		}
		return Checkpoint{}, cerror.Wrap(cerror.ClassCheckpoint, cerror.KindCheckpointCorrupt,
			"cannot read checkpoint file: "+s.path, err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return Checkpoint{}, cerror.Wrap(cerror.ClassCheckpoint, cerror.KindCheckpointCorrupt,
			"checkpoint file is not valid JSON: "+s.path, err)
	}
	if cp.SourceTag != "" && cp.SourceTag != s.sourceTag {
		return Checkpoint{}, cerror.New(cerror.ClassOffset, cerror.KindOffsetMismatch,
			fmt.Sprintf("checkpoint source tag %q does not match bound source %q",
				cp.SourceTag, s.sourceTag))
	}

	off, cerr := ParseOffset(cp.Offset)
	if cerr != nil {
		return Checkpoint{}, cerror.Wrap(cerror.ClassCheckpoint, cerror.KindCheckpointCorrupt,
			"checkpoint contains an unparsable offset", cerr)
	}

	// Re-hydrate the idempotency index. Persisted keys were retained at the
	// committed boundary; they stay dedupable until the first commit past it.
	s.keyAt = make(map[string]int64, len(cp.AppliedKeys))
	for _, k := range cp.AppliedKeys {
		s.keyAt[k] = off
	}
	// Re-hydrate already-reported bad records.
	s.badAt = make(map[int64]struct{}, len(cp.BadOrdinals))
	for _, n := range cp.BadOrdinals {
		if n > off {
			s.badAt[n] = struct{}{}
		}
	}
	s.offset = off
	s.loaded = true
	if cp.SourceTag == "" {
		cp.SourceTag = s.sourceTag
	}
	return cp, nil
}

// Commit atomically advances the checkpoint. The offset must parse and be
// strictly greater than the committed offset; otherwise the commit is
// rejected BEFORE any file is touched (no partial state on rejection).
func (s *FileStore) Commit(req CommitRequest) *cerror.Error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return cerror.New(cerror.ClassCheckpoint, cerror.KindNotInitialized,
			"checkpoint store is closed")
	}
	if !s.loaded {
		return cerror.New(cerror.ClassCheckpoint, cerror.KindNotInitialized,
			"checkpoint store must be loaded before commit")
	}

	newOff, cerr := ParseOffset(req.Offset)
	if cerr != nil {
		return cerr
	}
	if newOff <= s.offset {
		// Rollback / duplicate commit: explicitly refused.
		return cerror.New(cerror.ClassOffset, cerror.KindOffsetStale,
			fmt.Sprintf("refusing to roll back checkpoint: committed=%d requested=%d",
				s.offset, newOff))
	}

	// Merge this batch's keys at their exact ordinals.
	for _, ev := range req.Events {
		if ev.Key == "" {
			continue
		}
		n, perr := ParseOffset(ev.Offset)
		if perr != nil {
			return perr
		}
		if cur, ok := s.keyAt[ev.Key]; !ok || n > cur {
			s.keyAt[ev.Key] = n
		}
	}

	// Prune everything strictly before the new boundary. Keys at exactly the
	// boundary are retained to dedup an accidental boundary re-read.
	for k, n := range s.keyAt {
		if n < newOff {
			delete(s.keyAt, k)
		}
	}
	for _, n := range req.BadOrdinals {
		s.badAt[n] = struct{}{}
	}
	for n := range s.badAt {
		if n < newOff {
			delete(s.badAt, n)
		}
	}

	keys := make([]string, 0, len(s.keyAt))
	for k := range s.keyAt {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	bads := make([]int64, 0, len(s.badAt))
	for n := range s.badAt {
		bads = append(bads, n)
	}
	sort.Slice(bads, func(i, j int) bool { return bads[i] < bads[j] })

	cp := Checkpoint{SourceTag: s.sourceTag, Offset: req.Offset,
		AppliedKeys: keys, BadOrdinals: bads}
	if werr := s.writeAtomic(cp); werr != nil {
		return werr
	}
	s.offset = newOff
	return nil
}

// HasKey reports whether key is in the live idempotency index.
func (s *FileStore) HasKey(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.keyAt[key]
	return ok
}

// HasBad reports whether ordinal is an already-reported bad record.
func (s *FileStore) HasBad(ordinal int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.badAt[ordinal]
	return ok
}

func (s *FileStore) writeAtomic(cp Checkpoint) *cerror.Error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return cerror.Wrap(cerror.ClassCheckpoint, cerror.KindCheckpointCorrupt,
			"cannot create checkpoint directory", err)
	}
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return cerror.Wrap(cerror.ClassInternal, cerror.KindCheckpointCorrupt,
			"cannot encode checkpoint", err)
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".ckpt-*")
	if err != nil {
		return cerror.Wrap(cerror.ClassCheckpoint, cerror.KindCheckpointCorrupt,
			"cannot create temp checkpoint", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return cerror.Wrap(cerror.ClassCheckpoint, cerror.KindCheckpointCorrupt,
			"cannot write checkpoint", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return cerror.Wrap(cerror.ClassCheckpoint, cerror.KindCheckpointCorrupt,
			"cannot flush checkpoint", err)
	}
	if err := tmp.Close(); err != nil {
		return cerror.Wrap(cerror.ClassCheckpoint, cerror.KindCheckpointCorrupt,
			"cannot close checkpoint temp file", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return cerror.Wrap(cerror.ClassCheckpoint, cerror.KindCheckpointCorrupt,
			"cannot atomically rename checkpoint into place", err)
	}
	return nil
}

// Close releases the store. Double close is an error.
func (s *FileStore) Close() *cerror.Error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return cerror.New(cerror.ClassCheckpoint, cerror.KindNotInitialized,
			"checkpoint store already closed")
	}
	s.closed = true
	return nil
}
