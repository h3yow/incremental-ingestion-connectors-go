// Package engine runs concurrent, checkpointed, backpressured ingestion.
//
// Delivery model (per batch):
//
//	read batch -> parallel idempotent sink writes -> wait all OK -> atomic
//	checkpoint commit -> next batch
//
// The checkpoint commit happens only after every record of the batch is
// durably accepted by the sink. Both the sink write and the checkpoint
// commit are idempotent, so a crash between them simply re-delivers the
// batch after restart and the sink de-duplicates the overlap. At-least-once
// delivery with exactly-once observable state at the target is therefore
// guaranteed without a transactional sink.
package engine

import (
	"context"
	"sync"
	"time"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/checkpoint"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/connector"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/event"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/sink"
)

// Logger is the minimal structured-log surface used by the engine.
type Logger interface {
	Info(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// RecordError carries one rejected record with its classification and
// position for structured logging.
type RecordError struct {
	Ordinal int64
	Raw     string
	Class   cerror.Class
	Kind    cerror.Kind
	Msg     string
}

// Stats are observable counters. All counters are safe to read after Run
// returns.
type Stats struct {
	Batches        int64 `json:"batches"`
	Records        int64 `json:"records"`
	BadRecords     int64 `json:"bad_records"`
	Delivered      int64 `json:"delivered"`
	Deduped        int64 `json:"deduped"`
	BatchRetries   int64 `json:"batch_retries"`
	QueueFullDrops int64 `json:"queue_full_blocks"`
}

// Engine coordinates one source -> one sink ingestion run.
type Engine struct {
	registry *connector.Registry
	sink     sink.Sink
	log      Logger
}

// New builds an engine.
func New(reg *connector.Registry, s sink.Sink, log Logger) *Engine {
	if log == nil {
		log = nopLogger{}
	}
	return &Engine{registry: reg, sink: s, log: log}
}

// Options tunes a run.
type Options struct {
	Workers       int           // parallel sink writers per batch (>=1)
	BatchSize     int           // max good events per read batch (>=1)
	QueueSize     int           // bounded job queue for backpressure (>=1)
	PollInterval  time.Duration // wait between empty (non-EOF) polls
	MaxAttempts   int           // sink write attempts per batch (0 = retry until ctx)
	RetryBackoff  time.Duration // initial backoff, doubled each attempt
	OnRecordError func(RecordError)
}

func (o *Options) normalize() {
	if o.Workers <= 0 {
		o.Workers = 1
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 100
	}
	if o.QueueSize <= 0 {
		o.QueueSize = o.Workers * o.BatchSize
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 200 * time.Millisecond
	}
	if o.RetryBackoff <= 0 {
		o.RetryBackoff = 50 * time.Millisecond
	}
}

// Run opens kind with cfg, resumes from the checkpoint store, and ingests
// until the source reaches EOF (drained) or ctx is canceled. Bad records are
// reported via Options.OnRecordError and logged — never silently skipped;
// the checkpoint still advances past them because their ordinal is part of
// the source, but they are never delivered.
func (e *Engine) Run(
	ctx context.Context,
	kind string,
	cfg connector.Config,
	store checkpoint.Store,
	opts Options,
) (Stats, *cerror.Error) {
	opts.normalize()
	var st Stats

	// 1. Open and validate.
	cp, cerr := store.Load()
	if cerr != nil {
		return st, cerr
	}
	resume := cp.Offset
	e.log.Info("checkpoint loaded", "source", store.SourceTag(), "offset", resumeOrZero(resume),
		"applied_keys", len(cp.AppliedKeys))

	reader, cerr := e.registry.Open(ctx, kind, cfg)
	if cerr != nil {
		return st, cerr
	}
	defer func() {
		if clErr := reader.Close(); clErr != nil {
			e.log.Error("reader close failed", "err", clErr.Error())
		}
	}()

	// Tag mismatch: checkpoint for a different physical source is refused.
	if cp.SourceTag != "" && cp.SourceTag != reader.Tag() {
		return st, cerror.New(cerror.ClassOffset, cerror.KindOffsetMismatch,
			"checkpoint tag "+cp.SourceTag+" does not match opened source "+reader.Tag())
	}

	// Seed in-run idempotency index from the persisted checkpoint.
	known := make(map[string]struct{}, len(cp.AppliedKeys))
	for _, k := range cp.AppliedKeys {
		known[k] = struct{}{}
	}
	knownBad := make(map[int64]struct{}, len(cp.BadOrdinals))
	for _, n := range cp.BadOrdinals {
		knownBad[n] = struct{}{}
	}

	pool := newDeliveryPool(opts.Workers, opts.QueueSize, e.sink, e.log)
	defer pool.stop(ctx)

	// 2. Batch loop.
	for {
		if err := ctx.Err(); err != nil {
			return st, cerror.Wrap(cerror.ClassInternal, cerror.KindInternal,
				"ingestion canceled", err)
		}

		rr, rerr := reader.Read(ctx, resume, opts.BatchSize)
		if rerr != nil {
			return st, rerr
		}

		// Reject out-of-range resume offsets using the reader's physical end.
		if verr := checkpoint.ValidateResume(store.SourceTag(), reader.Tag(), resume,
			itoa(rr.EndOrdinal)); verr != nil {
			return st, verr
		}

		// Report bad records explicitly with their classified reasons.
		// Records already committed as bad are silently skipped at the
		// reporting level (they were surfaced on the previous run); every
		// NEW bad record is logged + dispatched and then checkpointed.
		var newBad []int64
		for _, b := range rr.Bad {
			if _, seen := knownBad[b.Ordinal]; seen {
				continue
			}
			knownBad[b.Ordinal] = struct{}{}
			newBad = append(newBad, b.Ordinal)
			st.BadRecords++
			re := RecordError{
				Ordinal: b.Ordinal, Raw: b.Raw,
				Class: b.Err.Class, Kind: b.Err.Kind, Msg: b.Err.Msg,
			}
			e.log.Error("record rejected",
				"ordinal", b.Ordinal, "class", b.Err.Class, "kind", b.Err.Kind,
				"reason", b.Err.Msg, "raw", b.Raw)
			if opts.OnRecordError != nil {
				opts.OnRecordError(re)
			}
		}

		// Batch watermark = highest ordinal among good AND bad records.
		batchWatermark := rr.EndOrdinal
		progress := len(rr.Events) > 0 || len(newBad) > 0

		if progress {
			// Split good events into already-known (persisted dedup index)
			// and pending delivery.
			pending := make([]event.Event, 0, len(rr.Events))
			for _, ev := range rr.Events {
				if _, ok := known[ev.Key]; ok {
					st.Deduped++
					continue
				}
				pending = append(pending, ev)
			}

			if len(pending) > 0 {
				// 3. Parallel delivery with bounded queue (backpressure).
				attempt := 0
				for {
					attempt++
					acc, ded, qf, derr := pool.deliver(ctx, pending)
					st.QueueFullDrops += qf
					if derr != nil {
						// Cancellation is not a downstream failure: stop at
						// once without retrying or committing this batch.
						if ctx.Err() != nil {
							return st, cerror.Wrap(cerror.ClassInternal, cerror.KindInternal,
								"ingestion canceled during sink delivery", ctx.Err())
						}
						if opts.MaxAttempts > 0 && attempt >= opts.MaxAttempts {
							return st, cerror.Wrap(cerror.ClassSink, cerror.KindSinkDelivery,
								"sink delivery permanently failed for batch ending at "+
									itoa(batchWatermark), derr)
						}
						st.BatchRetries++
						e.log.Error("sink batch failed; will retry (offset not committed)",
							"attempt", attempt, "class", cerror.ClassSink,
							"kind", cerror.KindSinkDelivery, "err", derr.Error())
						if werr := sleepCtx(ctx, backoff(opts.RetryBackoff, attempt)); werr != nil {
							return st, cerror.Wrap(cerror.ClassInternal, cerror.KindInternal,
								"ingestion canceled while backing off", werr)
						}
						continue
					}
					st.Delivered += int64(acc)
					st.Deduped += int64(ded)
					break
				}
			}

			// 4. Commit AFTER durable delivery. Crash before this point
			//    redelivers the batch; the sink dedups it.
			batchOffset := itoa(batchWatermark)
			if cerr := store.Commit(checkpoint.CommitRequest{
				Offset:      batchOffset,
				Events:      rr.Events,
				BadOrdinals: newBad,
			}); cerr != nil {
				// Rollback / stale / corrupt commit is fatal and explicit.
				return st, cerr
			}
			for _, ev := range rr.Events {
				known[ev.Key] = struct{}{}
			}
			st.Batches++
			st.Records += int64(len(rr.Events))
			resume = batchOffset
			e.log.Info("batch committed",
				"batch", st.Batches, "offset", batchOffset,
				"events", len(rr.Events), "new_bad", len(newBad),
				"delivered", st.Delivered, "deduped", st.Deduped,
				"bad_total", st.BadRecords)
		}

		if rr.EOF {
			e.log.Info("source drained (EOF)",
				"last_offset", resumeOrZero(resume), "batches", st.Batches,
				"records", st.Records, "bad", st.BadRecords,
				"delivered", st.Delivered, "deduped", st.Deduped)
			return st, nil
		}

		// Non-EOF empty batch (live source): wait before polling again.
		if werr := sleepCtx(ctx, opts.PollInterval); werr != nil {
			return st, cerror.Wrap(cerror.ClassInternal, cerror.KindInternal,
				"ingestion canceled while polling", werr)
		}
	}
}

func backoff(base time.Duration, attempt int) time.Duration {
	d := base
	for i := 1; i < attempt && d < 5*time.Second; i++ {
		d *= 2
	}
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func resumeOrZero(s string) string {
	if s == "" {
		return "0"
	}
	return s
}

func itoa(n int64) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = digits[n%10]
		n /= 10
	}
	return string(b[i:])
}

type nopLogger struct{}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// ---------------------------------------------------------------------------
// deliveryPool: parallel writes with a bounded feeder queue (backpressure)
// and per-batch result channels.
//
// Backpressure story:
//   - jobs is a bounded channel; a slow sink fills it and the feeder blocks,
//     which is counted as QueueFullDrops (the deterministic, observable
//     "queue full" behavior).
//   - Each batch gets its own buffered result channel sized to the batch, so
//     a worker can never block sending a result, and on cancellation the Run
//     loop simply exits: workers blocked in a context-respecting Sink.Write
//     unblock via ctx, drop into the per-batch buffer (which is GC'd), and
//     return to the shared job queue. No data is committed on cancel.
// ---------------------------------------------------------------------------

type job struct {
	ctx   context.Context
	stop  chan struct{}
	ev    event.Event
	resCh chan<- workerResult
}

type workerResult struct {
	accepted int
	deduped  int
	err      error
}

type deliveryPool struct {
	jobs chan job
	wg   sync.WaitGroup
	log  Logger
}

func newDeliveryPool(workers, queueSize int, s sink.Sink, log Logger) *deliveryPool {
	p := &deliveryPool{
		jobs: make(chan job, queueSize),
		log:  log,
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker(i, s)
	}
	return p
}

func (p *deliveryPool) worker(id int, s sink.Sink) {
	defer p.wg.Done()
	for j := range p.jobs {
		// Respect cancellation before doing the downstream call and while
		// waiting for a blocked sink. Sinks must honor ctx.
		if err := j.ctx.Err(); err != nil {
			select {
			case j.resCh <- workerResult{err: err}:
			case <-j.stop:
			}
			continue
		}
		res, err := s.Write(j.ctx, []event.Event{j.ev.Clone()})
		out := workerResult{accepted: res.Accepted, deduped: res.Deduped, err: err}
		// resCh is sized to the whole batch, so this send never blocks;
		// stop additionally guards a batch whose context died mid-send.
		select {
		case j.resCh <- out:
		case <-j.stop:
		}
	}
}

// deliver fans one batch out to the workers. It returns only after every
// event has either a result or a canceled context, so the caller never
// commits a checkpoint for a batch whose delivery state is unknown.
//
// On sink error the remaining events of THIS batch are still collected (the
// feeder stops enqueueing new ones), but the caller will NOT commit and will
// retry the full batch — the idempotent sink makes the redelivery safe.
func (p *deliveryPool) deliver(ctx context.Context, events []event.Event) (accepted, deduped, blocked int64, err error) {
	n := len(events)
	resCh := make(chan workerResult, n)
	stop := make(chan struct{})
	defer close(stop)

	sent := 0
	collected := 0
	var firstErr error
	batchFailed := false

	enqueue := func(ev event.Event) {
		j := job{ctx: ctx, stop: stop, ev: ev, resCh: resCh}
		// Bounded send with a non-blocking probe to record the observable
		// backpressure signal when the queue is saturated.
		for {
			select {
			case <-ctx.Done():
				return
			case p.jobs <- j:
				sent++
				return
			default:
				blocked++
				select {
				case <-ctx.Done():
					return
				case p.jobs <- j:
					sent++
					return
				case r := <-resCh:
					collect(r, &accepted, &deduped, &firstErr, &batchFailed)
					collected++
				}
			}
		}
	}

	for sent < n {
		if batchFailed {
			break // let in-flight writes finish; no point queueing more
		}
		if ctx.Err() != nil {
			break
		}
		enqueue(events[sent])
		// Opportunistically collect any ready result.
		select {
		case r := <-resCh:
			collect(r, &accepted, &deduped, &firstErr, &batchFailed)
			collected++
		default:
		}
	}

	// Drain results for everything that was sent. resCh has room for all n
	// results, so context-respecting workers never block: once ctx is done
	// they unblock from Write and deliver a terminal result. If a sink
	// ignores cancellation we do not block shutdown forever — the per-batch
	// stop channel (closed on return) lets those workers drop their result.
	drainDone := ctx.Done()
	for collected < sent {
		select {
		case r := <-resCh:
			collect(r, &accepted, &deduped, &firstErr, &batchFailed)
			collected++
		case <-drainDone:
			// Cancellation observed; collect immediately-ready results then
			// abandon the rest. Abandoned writes are not checkpointed and
			// are safely redelivered by the idempotent sink on restart.
			drainDone = nil
			for i := 0; i < 8 && collected < sent; i++ {
				select {
				case r := <-resCh:
					collect(r, &accepted, &deduped, &firstErr, &batchFailed)
					collected++
				default:
					i = 8
				}
			}
			if collected < sent {
				return accepted, deduped, blocked, ctx.Err()
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return accepted, deduped, blocked, err
	}
	if firstErr != nil {
		return accepted, deduped, blocked, firstErr
	}
	return accepted, deduped, blocked, nil
}

func collect(r workerResult, accepted, deduped *int64, firstErr *error, failed *bool) {
	if r.err != nil {
		if !*failed {
			*failed = true
			*firstErr = r.err
		}
		return
	}
	*accepted += int64(r.accepted)
	*deduped += int64(r.deduped)
}

// stop closes the job queue and waits for idle workers. It must be called
// after the engine has stopped issuing batches; workers stuck in a sink
// Write are released by that sink honoring the canceled context.
func (p *deliveryPool) stop(_ context.Context) {
	close(p.jobs)
	p.wg.Wait()
}
