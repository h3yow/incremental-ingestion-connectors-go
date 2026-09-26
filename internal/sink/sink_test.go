package sink

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/h3yow/incremental-ingestion-connectors-go/internal/event"
)

func events(n int) []event.Event {
	out := make([]event.Event, n)
	for i := 0; i < n; i++ {
		out[i] = event.Event{Source: "s", Seq: uint64(i + 1), Key: "k", Kind: "u"}
	}
	return out
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, b := range data {
		if b == '\n' {
			n++
		}
	}
	return n
}

func TestIdempotentWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	s := NewFileSink(path)
	ctx := context.Background()

	batch := events(3)
	if err := s.WriteBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	// 重复写同一批：幂等去重
	if err := s.WriteBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if n := countLines(t, path); n != 3 {
		t.Fatalf("want 3 lines, got %d", n)
	}

	// 重启后（新实例）重复投递仍被去重
	s2 := NewFileSink(path)
	if err := s2.WriteBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := s2.WriteBatch(ctx, events(2)); err != nil { // Seq 1,2 重复
		t.Fatal(err)
	}
	s2.Close()
	if n := countLines(t, path); n != 3 {
		t.Fatalf("after restart want 3 lines, got %d", n)
	}
}

func TestContentValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	s := NewFileSink(path)
	if err := s.WriteBatch(context.Background(), events(1)); err != nil {
		t.Fatal(err)
	}
	s.Close()
	data, _ := os.ReadFile(path)
	var ev event.Event
	if err := json.Unmarshal(data[:len(data)-1], &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Seq != 1 || ev.Key != "k" {
		t.Fatalf("bad event %+v", ev)
	}
}
