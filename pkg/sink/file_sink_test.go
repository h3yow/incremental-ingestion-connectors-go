package sink_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/event"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/sink"
)

func ev(key string) event.Event {
	return event.Event{Key: key, Offset: "1", Source: "src", Payload: map[string]any{"v": key}}
}

func TestIdempotentWithinAndAcrossProcesses(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	s, err := sink.NewFileSink(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	res, err := s.Write(ctx, []event.Event{ev("a"), ev("b"), ev("c")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted != 3 || res.Deduped != 0 {
		t.Fatalf("first write: %+v", res)
	}

	// Redeliver the same batch (at-least-once replay): all deduped.
	res, err = s.Write(ctx, []event.Event{ev("a"), ev("b"), ev("c")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted != 0 || res.Deduped != 3 {
		t.Fatalf("redelivery: %+v", res)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// "Process restart": reopen the same directory; durable dedup survives.
	s2, err := sink.NewFileSink(dir)
	if err != nil {
		t.Fatal(err)
	}
	res, err = s2.Write(ctx, []event.Event{ev("a"), ev("d")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted != 1 || res.Deduped != 1 {
		t.Fatalf("after reopen: %+v", res)
	}
}

func TestConcurrentWritesNoDuplicate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	s, err := sink.NewFileSink(dir)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 16
	const perWorker = 50
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Every worker writes the SAME keys 0..perWorker-1 many times.
			batch := make([]event.Event, perWorker)
			for i := range batch {
				batch[i] = ev(fmt.Sprintf("shared-%d", i))
			}
			for round := 0; round < 5; round++ {
				if _, err := s.Write(context.Background(), batch); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// Exactly one file per unique key must exist.
	entries, _ := readDirNames(dir)
	count := 0
	for _, n := range entries {
		if len(n) > 5 && n[:4] == "evt-" {
			count++
		}
	}
	if count != perWorker {
		t.Fatalf("files=%d want %d (no duplicates)", count, perWorker)
	}
}

func readDirNames(dir string) ([]string, error) {
	f, err := openDir(dir)
	if err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	sort.Strings(names)
	return names, err
}
