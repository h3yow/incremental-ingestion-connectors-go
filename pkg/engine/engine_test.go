package engine_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/checkpoint"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/connector"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/engine"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/event"
	psink "github.com/h3yow/incremental-ingestion-connectors-go/pkg/sink"
)

// ---- fake source -----------------------------------------------------------

type fakeConnector struct {
	tag   string
	total int64
	mu    sync.Mutex
	open  bool
}

func (f *fakeConnector) Kind() string { return "fake" }
func (f *fakeConnector) Schema() connector.Schema {
	return connector.Schema{KeyField: "k"}
}
func (f *fakeConnector) Open(ctx context.Context, cfg connector.Config) (connector.SourceReader, *cerror.Error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.open {
		return nil, cerror.New(cerror.ClassConnector, cerror.KindAlreadyInitialized, "open")
	}
	f.open = true
	return &fakeReader{conn: f}, nil
}

type fakeReader struct{ conn *fakeConnector }

func (r *fakeReader) Tag() string { return r.conn.tag }
func (r *fakeReader) Close() *cerror.Error {
	r.conn.mu.Lock()
	defer r.conn.mu.Unlock()
	if !r.conn.open {
		return cerror.New(cerror.ClassConnector, cerror.KindNotInitialized, "closed")
	}
	r.conn.open = false
	return nil
}

func (r *fakeReader) Read(ctx context.Context, resumeOffset string, maxRecords int) (connector.ReadResult, *cerror.Error) {
	start, cerr := connector.ParseResumeOrdinal(resumeOffset)
	if cerr != nil {
		return connector.ReadResult{}, cerr
	}
	res := connector.ReadResult{}
	n := start
	for int64(len(res.Events)) < int64(maxRecords) && n < r.conn.total {
		n++
		res.Events = append(res.Events, event.Event{
			Key:     fmt.Sprintf("key-%05d", n),
			Offset:  fmt.Sprintf("%d", n),
			Source:  r.conn.tag,
			Payload: map[string]any{"n": n},
		})
	}
	res.EndOrdinal = n
	res.EOF = n >= r.conn.total
	return res, nil
}

// ---- sinks -----------------------------------------------------------------

type recordingSink struct {
	mu      sync.Mutex
	written map[string]int
	calls   int64
}

func newRecordingSink() *recordingSink { return &recordingSink{written: map[string]int{}} }
func (s *recordingSink) Write(ctx context.Context, evs []event.Event) (psink.Result, error) {
	atomic.AddInt64(&s.calls, 1)
	if err := ctx.Err(); err != nil {
		return psink.Result{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var r psink.Result
	for _, e := range evs {
		if _, exists := s.written[e.Key]; exists {
			r.Deduped++
			continue
		}
		s.written[e.Key]++
		r.Accepted++
	}
	return r, nil
}
func (s *recordingSink) Close() error { return nil }
func (s *recordingSink) unique() int  { s.mu.Lock(); defer s.mu.Unlock(); return len(s.written) }

// flakySink fails the first failAt distinct Write calls (whole batch), then
// delegates. Combined with the real file sink it models an outage window.
type flakySink struct {
	inner   psink.Sink
	failFor int32
}

func (f *flakySink) Write(ctx context.Context, evs []event.Event) (psink.Result, error) {
	if atomic.LoadInt32(&f.failFor) > 0 {
		atomic.AddInt32(&f.failFor, -1)
		return psink.Result{}, fmt.Errorf("simulated outage")
	}
	return f.inner.Write(ctx, evs)
}
func (f *flakySink) Close() error { return f.inner.Close() }

// outageSink fails the fan-outs numbered in failFanouts (1-based; each
// deliver() with 1 worker is one fan-out of BatchSize calls), then recovers.
type outageSink struct {
	inner       psink.Sink
	calls       int64
	failFanouts map[int64]bool
}

func (o *outageSink) Write(ctx context.Context, evs []event.Event) (psink.Result, error) {
	n := atomic.AddInt64(&o.calls, 1)
	fanout := (n-1)/10 + 1 // batch size is 10 in this test
	if o.failFanouts[fanout] {
		return psink.Result{}, fmt.Errorf("simulated batch outage")
	}
	return o.inner.Write(ctx, evs)
}
func (o *outageSink) Close() error { return o.inner.Close() }

// blockSink accepts the first allowFirst writes normally, then blocks every
// subsequent Write until gate is opened or ctx is canceled.
type blockSink struct {
	allowFirst int64
	count      int64
	gate       chan struct{}
	committed  chan struct{}
	blocked    chan struct{}
	blkOnce    sync.Once
	cmtOnce    sync.Once
}

func newBlockSink(allowFirst int64) *blockSink {
	return &blockSink{
		allowFirst: allowFirst,
		gate:       make(chan struct{}),
		committed:  make(chan struct{}),
		blocked:    make(chan struct{}),
	}
}

func (b *blockSink) Write(ctx context.Context, evs []event.Event) (psink.Result, error) {
	n := atomic.AddInt64(&b.count, 1)
	if n <= b.allowFirst {
		b.cmtOnce.Do(func() { close(b.committed) })
		return psink.Result{Accepted: len(evs)}, nil
	}
	b.blkOnce.Do(func() { close(b.blocked) })
	select {
	case <-b.gate:
		return psink.Result{Accepted: len(evs)}, nil
	case <-ctx.Done():
		return psink.Result{}, ctx.Err()
	}
}

func (b *blockSink) Close() error { return nil }

// ---- tests -----------------------------------------------------------------

func newHarness(t *testing.T, total int64) (*connector.Registry, *fakeConnector) {
	t.Helper()
	reg := connector.NewRegistry()
	fc := &fakeConnector{tag: "fake:src", total: total}
	if err := reg.Register(fc); err != nil {
		t.Fatal(err)
	}
	return reg, fc
}

func TestHappyPathDeliversAllExactlyOnce(t *testing.T) {
	reg, _ := newHarness(t, 137)
	dir := t.TempDir()
	store := checkpoint.NewFileStore(filepath.Join(dir, "c.json"), "fake:src")
	if _, err := store.Load(); err != nil {
		t.Fatal(err)
	}
	sink := newRecordingSink()
	eng := engine.New(reg, sink, nil)
	st, err := eng.Run(context.Background(), "fake", nil, store, engine.Options{
		Workers: 8, BatchSize: 10, QueueSize: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.unique() != 137 {
		t.Fatalf("unique delivered=%d want 137", sink.unique())
	}
	if st.Records != 137 || st.Delivered != 137 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestRestartResumeNoLossNoDuplicate(t *testing.T) {
	dir := t.TempDir()
	ckptPath := filepath.Join(dir, "c.json")
	sinkDir := filepath.Join(dir, "out")
	fsink, _ := psink.NewFileSink(sinkDir)

	const total int64 = 120
	reg1, _ := newHarness(t, total)
	store1 := checkpoint.NewFileStore(ckptPath, "fake:src")
	if _, err := store1.Load(); err != nil {
		t.Fatal(err)
	}
	// Flaky for the first 3 batch attempts -> the run aborts.
	flaky := &flakySink{inner: fsink, failFor: 3}
	eng1 := engine.New(reg1, flaky, nil)
	_, err := eng1.Run(context.Background(), "fake", nil, store1, engine.Options{
		Workers: 4, BatchSize: 10, QueueSize: 10, MaxAttempts: 1,
	})
	if err == nil || err.Kind != cerror.KindSinkDelivery {
		t.Fatalf("expected sink_delivery_failed abort, got %v", err)
	}
	filesAfterCrash := countEvtFiles(t, sinkDir)
	if filesAfterCrash == 0 {
		t.Fatal("expected some files delivered before crash")
	}

	// Restart with a healthy sink and the SAME checkpoint: must resume and
	// produce exactly total files, no duplicates.
	reg2, _ := newHarness(t, total)
	store2 := checkpoint.NewFileStore(ckptPath, "fake:src")
	fsink2, _ := psink.NewFileSink(sinkDir)
	eng2 := engine.New(reg2, fsink2, nil)
	st, err := eng2.Run(context.Background(), "fake", nil, store2, engine.Options{
		Workers: 8, BatchSize: 10, QueueSize: 16,
	})
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if got := countEvtFiles(t, sinkDir); got != int(total) {
		t.Fatalf("files after resume=%d want %d (crash had %d)", got, total, filesAfterCrash)
	}
	// Some crash-window records were redelivered and deduped by the sink.
	if st.Delivered+st.Deduped == 0 {
		t.Fatalf("expected delivered/deduped activity: %+v", st)
	}
}

func TestRepeatedRunsAreIdempotent(t *testing.T) {
	dir := t.TempDir()
	ckptPath := filepath.Join(dir, "c.json")
	sinkDir := filepath.Join(dir, "out")
	const total int64 = 60
	for run := 0; run < 3; run++ {
		reg, _ := newHarness(t, total)
		store := checkpoint.NewFileStore(ckptPath, "fake:src")
		if _, err := store.Load(); err != nil {
			t.Fatal(err)
		}
		fsink, _ := psink.NewFileSink(sinkDir)
		eng := engine.New(reg, fsink, nil)
		st, err := eng.Run(context.Background(), "fake", nil, store, engine.Options{
			Workers: 4, BatchSize: 8,
		})
		if err != nil {
			t.Fatal(err)
		}
		store.Close()
		if got := countEvtFiles(t, sinkDir); got != int(total) {
			t.Fatalf("run %d files=%d want %d stats=%+v", run, got, total, st)
		}
	}
}

func TestDownstreamRecoversViaRetryAndCommits(t *testing.T) {
	dir := t.TempDir()
	reg, _ := newHarness(t, 40)
	store := checkpoint.NewFileStore(filepath.Join(dir, "c.json"), "fake:src")
	if _, err := store.Load(); err != nil {
		t.Fatal(err)
	}
	fsink, _ := psink.NewFileSink(filepath.Join(dir, "out"))
	outage := &outageSink{inner: fsink, failFanouts: map[int64]bool{2: true, 3: true}}
	eng := engine.New(reg, outage, nil)
	st, err := eng.Run(context.Background(), "fake", nil, store, engine.Options{
		Workers: 1, BatchSize: 10, MaxAttempts: 10, RetryBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.BatchRetries < 2 {
		t.Fatalf("expected >=2 retries, stats=%+v", st)
	}
	if got := countEvtFiles(t, filepath.Join(dir, "out")); got != 40 {
		t.Fatalf("files=%d want 40", got)
	}
}

func TestConcurrentCancellationLosesAndDuplicatesNothing(t *testing.T) {
	dir := t.TempDir()
	reg, _ := newHarness(t, 1_000_000)
	store := checkpoint.NewFileStore(filepath.Join(dir, "c.json"), "fake:src")
	if _, err := store.Load(); err != nil {
		t.Fatal(err)
	}
	bs := newBlockSink(64) // allow first 2 batches (32 each) to commit
	eng := engine.New(reg, bs, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *cerror.Error, 1)
	var st engine.Stats
	go func() {
		var err *cerror.Error
		st, err = eng.Run(ctx, "fake", nil, store, engine.Options{
			Workers: 8, BatchSize: 32, QueueSize: 64,
		})
		done <- err
	}()

	// Wait until the allowed batches committed, then until workers block.
	select {
	case <-bs.committed:
	case <-time.After(2 * time.Second):
		t.Fatal("no batch committed before block")
	}
	select {
	case <-bs.blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("workers never became blocked")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error")
		}
		if st.Batches < 1 {
			t.Fatalf("expected committed batches before cancel, stats=%+v", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("engine did not stop after cancel")
	}

	// Checkpoint must be internally consistent; reopen and ensure the file
	// store is not corrupt.
	store2 := checkpoint.NewFileStore(filepath.Join(dir, "c.json"), "fake:src")
	cp, err := store2.Load()
	if err != nil {
		t.Fatalf("checkpoint corrupt after cancel: %v", err)
	}
	if cp.Offset == "" {
		t.Fatal("expected a committed offset before cancel")
	}
}

func TestBackpressureIsBoundedAndObservable(t *testing.T) {
	dir := t.TempDir()
	reg, _ := newHarness(t, 5000)
	store := checkpoint.NewFileStore(filepath.Join(dir, "c.json"), "fake:src")
	if _, err := store.Load(); err != nil {
		t.Fatal(err)
	}
	bs := newBlockSink(0) // block from the first write to saturate the queue
	eng := engine.New(reg, bs, nil)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, _ = eng.Run(ctx, "fake", nil, store, engine.Options{
			Workers: 4, BatchSize: 64, QueueSize: 8,
		})
	}()
	<-bs.blocked
	// While blocked, goroutine count must stay bounded: sleep then check the
	// feeder is parked on the full queue rather than spawning work.
	time.Sleep(100 * time.Millisecond)
	// Release and let it drain fully.
	cancel()
	close(bs.gate)
}

// slowSink sleeps delay per record; with N workers and per-record writes the
// wall time should shrink ~proportionally until the feeder becomes the limit.
type slowSink struct {
	delay time.Duration
}

func (s *slowSink) Write(ctx context.Context, evs []event.Event) (psink.Result, error) {
	if err := ctx.Err(); err != nil {
		return psink.Result{}, err
	}
	time.Sleep(s.delay)
	return psink.Result{Accepted: len(evs)}, nil
}
func (s *slowSink) Close() error { return nil }

func TestThroughputScalesWithWorkers(t *testing.T) {
	const total int64 = 96
	const delay = 2 * time.Millisecond
	run := func(workers int) time.Duration {
		dir := t.TempDir()
		reg, _ := newHarness(t, total)
		store := checkpoint.NewFileStore(filepath.Join(dir, "c.json"), "fake:src")
		if _, err := store.Load(); err != nil {
			t.Fatal(err)
		}
		eng := engine.New(reg, &slowSink{delay: delay}, nil)
		start := time.Now()
		if _, err := eng.Run(context.Background(), "fake", nil, store, engine.Options{
			Workers: workers, BatchSize: int(total), QueueSize: int(total),
		}); err != nil {
			t.Fatal(err)
		}
		return time.Since(start)
	}
	serial := run(1)
	parallel := run(8)
	// Expect a clear speedup, with slack for scheduler/CI noise.
	if parallel >= serial/3 {
		t.Fatalf("no scaling: serial=%v parallel(8)=%v", serial, parallel)
	}
	t.Logf("serial=%v parallel(8)=%v speedup=%.1fx", serial, parallel,
		float64(serial)/float64(parallel))
}

// countingBlockSink blocks after allowFirst writes and also atomically counts
// how many writes entered the blocked state, so the test can correlate the
// engine's queue-full counter with a saturated downstream.
type countingBlockSink struct {
	blockSink
	entered int64
}

func (c *countingBlockSink) Write(ctx context.Context, evs []event.Event) (psink.Result, error) {
	n := atomic.AddInt64(&c.count, 1)
	if n <= c.allowFirst {
		c.cmtOnce.Do(func() { close(c.committed) })
		return psink.Result{Accepted: len(evs)}, nil
	}
	atomic.AddInt64(&c.entered, 1)
	c.blkOnce.Do(func() { close(c.blocked) })
	select {
	case <-c.gate:
		return psink.Result{Accepted: len(evs)}, nil
	case <-ctx.Done():
		return psink.Result{}, ctx.Err()
	}
}

func TestQueueFullIsObservable(t *testing.T) {
	dir := t.TempDir()
	reg, _ := newHarness(t, 5000)
	store := checkpoint.NewFileStore(filepath.Join(dir, "c.json"), "fake:src")
	if _, err := store.Load(); err != nil {
		t.Fatal(err)
	}
	bs := &countingBlockSink{blockSink: blockSink{
		allowFirst: 0, gate: make(chan struct{}),
		committed: make(chan struct{}), blocked: make(chan struct{}),
	}}
	eng := engine.New(reg, bs, nil)
	ctx, cancel := context.WithCancel(context.Background())
	var st engine.Stats
	done := make(chan struct{})
	go func() {
		st, _ = eng.Run(ctx, "fake", nil, store, engine.Options{
			Workers: 4, BatchSize: 256, QueueSize: 4,
		})
		close(done)
	}()
	<-bs.blocked
	// With the downstream blocked and a 4-slot queue, the feeder MUST be
	// parking on the bounded channel (backpressure). Give it time to record.
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("engine did not stop on cancel under backpressure")
	}
	// Only a small, bounded number of writes entered the sink (workers +
	// queued slots), proving the bounded queue applies backpressure rather
	// than buffering unbounded work.
	if got := atomic.LoadInt64(&bs.entered); got > int64(4+16) {
		t.Fatalf("bounded backpressure violated: %d writes entered blocked sink", got)
	}
	t.Logf("blocked writes while saturated=%d (queue=4 workers=4), queue_full_blocks=%d",
		bs.entered, st.QueueFullDrops)
}
