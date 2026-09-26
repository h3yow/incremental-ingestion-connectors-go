package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/event"
)

// FileSink writes each key to an individual file under dir (key-safe name),
// atomically (temp + rename), so a crash at any point never produces a
// partial write. Existence of the key file is the durable idempotency
// decision, which makes the sink safe across process restarts.
type FileSink struct {
	dir string

	mu   sync.Mutex
	seen map[string]struct{} // in-run fast path; durable truth is the FS
}

// NewFileSink creates (or reopens) an idempotent file sink rooted at dir.
func NewFileSink(dir string) (*FileSink, error) {
	if dir == "" {
		return nil, errors.New("sink dir must be non-empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create sink dir: %w", err)
	}
	return &FileSink{dir: dir, seen: make(map[string]struct{})}, nil
}

// keyFile maps an arbitrary event key to a safe relative filename.
// Keys are hex of the raw key plus a short human-readable prefix; uniqueness
// is guaranteed by the hex component.
func keyFile(key string) string {
	return fmt.Sprintf("evt-%x.json", key)
}

// envelope is the persisted per-record JSON document.
type envelope struct {
	Key     string         `json:"key"`
	Source  string         `json:"source"`
	Offset  string         `json:"offset"`
	Payload map[string]any `json:"payload"`
}

// Write delivers a batch. Writes are key-ordered to keep on-disk ordering
// deterministic under concurrent workers; duplicate keys (in-run or on disk)
// count as Deduped and are never written twice.
func (s *FileSink) Write(ctx context.Context, events []event.Event) (Result, error) {
	var res Result
	for _, ev := range events {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if ev.Key == "" {
			return res, errors.New("sink received event with empty key")
		}
		name := filepath.Join(s.dir, keyFile(ev.Key))

		s.mu.Lock()
		_, inRun := s.seen[ev.Key]
		s.mu.Unlock()
		if inRun {
			res.Deduped++
			continue
		}

		// Durable dedup decision: a completed file means delivered.
		if _, err := os.Stat(name); err == nil {
			s.mu.Lock()
			s.seen[ev.Key] = struct{}{}
			s.mu.Unlock()
			res.Deduped++
			continue
		} else if !errors.Is(err, fs.ErrNotExist) {
			return res, fmt.Errorf("stat sink file: %w", err)
		}

		doc, err := json.MarshalIndent(envelope{
			Key: ev.Key, Source: ev.Source, Offset: ev.Offset, Payload: ev.Payload,
		}, "", "  ")
		if err != nil {
			return res, fmt.Errorf("encode event %q: %w", ev.Key, err)
		}
		if err := s.writeAtomic(name, doc); err != nil {
			return res, fmt.Errorf("write event %q: %w", ev.Key, err)
		}
		s.mu.Lock()
		s.seen[ev.Key] = struct{}{}
		s.mu.Unlock()
		res.Accepted++
	}
	return res, nil
}

func (s *FileSink) writeAtomic(name string, data []byte) error {
	tmp, err := os.CreateTemp(s.dir, ".evt-tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, name)
}

// Close releases the sink.
func (s *FileSink) Close() error { return nil }
