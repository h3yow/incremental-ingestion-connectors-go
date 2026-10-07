// Package pipeline 提供并发摄取、背压与检查点提交编排。
//
// 语义：至少一次拉取 + 幂等下游 = 不丢不重。
//   - 生产者从 Source 逐条拉取并归一化，写入有界队列（背压边界）。
//   - 坏记录（解析失败/内容不合规）按类别登记到拒绝存储后继续处理，
//     其位点与好记录一样参与连续前缀提交，因此位点最终推进到源末尾；
//     拒绝登记跨重启去重，重扫旧区间不会重复登记。
//   - worker 从队列攒批，写 Sink 成功后把位点交给提交器。
//   - 提交器只按 seq 连续前缀提交检查点，崩溃后从最近提交位点续传，
//     已写入但未提交的事件由 Sink 幂等去重。
//   - 取消时不再投递、不再提交新位点，在途批次被丢弃，重启后重放，不产生重复。
package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h3yow/incremental-ingestion-connectors-go/internal/checkpoint"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/connector"
	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/event"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/reject"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/sink"
)

// Options 控制管线行为。
type Options struct {
	Workers   int // 并发 worker 数（<1 时取 1）
	QueueSize int // 队列容量（背压边界，<1 时取 1）
	BatchSize int // 每批提交/投递的事件数（<1 时取 1）
	// Rejects 是坏记录拒绝存储；nil 时使用纯内存存储（不持久化，重启去重失效）。
	Rejects *reject.Store
	Logger  *slog.Logger
}

func (o *Options) normalize() {
	if o.Workers < 1 {
		o.Workers = 1
	}
	if o.QueueSize < 1 {
		o.QueueSize = 1
	}
	if o.BatchSize < 1 {
		o.BatchSize = 1
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Rejects == nil {
		o.Rejects, _ = reject.Open("")
	}
}

// Stats 是管线运行统计，可用于观测与测试。
type Stats struct {
	Produced  atomic.Uint64 // 已归一化事件数
	Written   atomic.Uint64 // 已成功投递事件数
	Committed atomic.Uint64 // 已提交位点数
	Batches   atomic.Uint64 // 已投递批次数
	Rejected  atomic.Uint64 // 本轮新登记的拒绝记录数
}

type item struct {
	idx uint64 // 生产者分配的单调递增序号（从 0 开始）
	ev  event.Event
	pos checkpoint.Position
}

// posRef 是一条已投递事件的位点。
type posRef struct {
	idx uint64
	pos checkpoint.Position
}

// batchDone 是 worker 上报的已完成批次（idx 因队列分发可能不连续）。
type batchDone struct {
	items []posRef
}

// Pipeline 编排 拉取 -> 归一化 -> 投递 -> 提交位点。
type Pipeline struct {
	src   connector.Source
	sk    sink.Sink
	store checkpoint.Store
	opts  Options

	Stats Stats
}

func New(src connector.Source, sk sink.Sink, store checkpoint.Store, opts Options) *Pipeline {
	opts.normalize()
	return &Pipeline{src: src, sk: sk, store: store, opts: opts}
}

// Run 运行管线直到源耗尽或 ctx 取消。
// 源耗尽返回 nil；其余情况返回分类错误。
func (p *Pipeline) Run(ctx context.Context) error {
	log := p.opts.Logger
	queue := make(chan item, p.opts.QueueSize)
	done := make(chan batchDone, p.opts.Workers)

	// runCtx 派生自外部 ctx：外部取消立即传播到 worker 与提交器，
	// 取消后不再投递、不再提交位点；fail 时也会取消 runCtx 让全员退出。
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		firstErr  atomic.Value // 存 error
		workers   sync.WaitGroup
		committer sync.WaitGroup
	)
	fail := func(err error) {
		firstErr.CompareAndSwap(nil, err)
		cancel()
	}

	// 提交器：只提交 idx 连续前缀，保证位点单调前进。
	committer.Add(1)
	go func() {
		defer committer.Done()
		p.committer(runCtx, done, log)
	}()

	// worker：攒批 -> 投递 -> 上报位点。
	for w := 0; w < p.opts.Workers; w++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			p.worker(runCtx, id, queue, done, fail, log)
		}(w)
	}

	// 生产者：拉取 -> 归一化 -> 入队（受背压约束）；坏记录登记拒绝后继续。
	prodErr := p.produce(ctx, runCtx, queue, done, log)
	close(queue)
	workers.Wait()
	close(done)      // worker 全部退出后才关闭上报通道
	committer.Wait() // 等待提交器排空

	if prodErr != nil {
		return prodErr
	}
	if err, _ := firstErr.Load().(error); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return ierr.Wrap(ierr.ClassCanceled, "pipeline.Run", err)
	}
	return nil
}

// produce 从源拉取记录、归一化并写入队列。源耗尽返回 nil。
// 记录级问题（解析失败/内容不合规）登记拒绝后继续，被拒位点直接转交提交器，
// 与已投递位点一样参与连续前缀提交，因此位点最终能推进到源末尾。
func (p *Pipeline) produce(ctx, runCtx context.Context, queue chan<- item, done chan<- batchDone, log *slog.Logger) error {
	const op = "pipeline.produce"
	var idx uint64
	for {
		rec, pos, err := p.src.Next(ctx)
		if err != nil {
			if ierr.Is(err, ierr.ClassSourceExhausted) {
				log.Info("source exhausted", "source", p.src.Name(), "produced", p.Stats.Produced.Load())
				return nil
			}
			if ierr.Is(err, ierr.ClassCanceled) {
				return err
			}
			if isRecordLevel(err) {
				// 整行无法解析：明确拒绝、留痕、位点照常推进，然后继续。
				p.registerReject(pos, err, log)
				if !p.forwardRejected(runCtx, done, idx, pos) {
					return nil
				}
				idx++
				continue
			}
			return err
		}
		ev, err := event.Normalize(p.src.Name(), pos.Seq, rec)
		if err != nil {
			// 记录内容不合规：明确拒绝、留痕、位点照常推进，然后继续。
			p.registerReject(pos, err, log)
			if !p.forwardRejected(runCtx, done, idx, pos) {
				return nil
			}
			idx++
			continue
		}
		it := item{idx: idx, ev: ev, pos: pos}
		idx++
		// 背压：队列满时阻塞等待，定期打印可观测日志。
		timer := time.NewTimer(backpressureLogInterval)
		for {
			select {
			case queue <- it:
				timer.Stop()
				p.Stats.Produced.Add(1)
				goto next
			case <-timer.C:
				log.Warn("backpressure: queue full, producer blocked",
					"source", p.src.Name(), "seq", pos.Seq,
					"queue_size", p.opts.QueueSize)
				timer.Reset(backpressureLogInterval)
			case <-runCtx.Done():
				timer.Stop()
				return nil
			case <-ctx.Done():
				timer.Stop()
				return ierr.Wrap(ierr.ClassCanceled, op, ctx.Err())
			}
		}
	next:
	}
}

// isRecordLevel 报告错误是否为记录级问题（可拒绝后继续，而非中止整轮同步）。
func isRecordLevel(err error) bool {
	switch ierr.ClassOf(err) {
	case ierr.ClassParseError, ierr.ClassMissingField, ierr.ClassTypeMismatch:
		return true
	}
	return false
}

// registerReject 登记一条被拒记录；已登记过（含重启前）的不重复登记、不重复上报。
func (p *Pipeline) registerReject(pos checkpoint.Position, err error, log *slog.Logger) {
	entry := reject.Entry{
		Source: pos.Source,
		Offset: pos.Offset,
		Seq:    pos.Seq,
		Class:  string(ierr.ClassOf(err)),
		Reason: err.Error(),
	}
	added, aerr := p.opts.Rejects.Add(entry)
	if aerr != nil {
		log.Error("reject persist failed",
			"source", pos.Source, "offset", pos.Offset,
			"error_class", string(ierr.ClassOf(aerr)), "error", aerr)
		return
	}
	if !added {
		log.Debug("record already rejected, skip re-register",
			"source", pos.Source, "offset", pos.Offset)
		return
	}
	p.Stats.Rejected.Add(1)
	log.Warn("record rejected",
		"source", pos.Source, "offset", pos.Offset, "seq", pos.Seq,
		"error_class", string(ierr.ClassOf(err)), "error", err)
}

// forwardRejected 把被拒记录的位点转交提交器（idx 参与连续前缀，位点才能越过坏记录）。
// 取消时返回 false。
func (p *Pipeline) forwardRejected(runCtx context.Context, done chan<- batchDone, idx uint64, pos checkpoint.Position) bool {
	bd := batchDone{items: []posRef{{idx: idx, pos: pos}}}
	select {
	case done <- bd:
		return true
	case <-runCtx.Done():
		return false
	}
}

const backpressureLogInterval = 200 * time.Millisecond

// worker 从队列攒批，写 Sink 成功后上报批次位点。
func (p *Pipeline) worker(ctx context.Context, id int, queue <-chan item, done chan<- batchDone, fail func(error), log *slog.Logger) {
	const op = "pipeline.worker"
	batch := make([]item, 0, p.opts.BatchSize)
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		evs := make([]event.Event, len(batch))
		for i, it := range batch {
			evs[i] = it.ev
		}
		if err := p.sk.WriteBatch(ctx, evs); err != nil {
			if ierr.Is(err, ierr.ClassCanceled) {
				return false // 取消：丢弃在途批次，不提交位点
			}
			log.Error("sink write failed",
				"worker", id, "batch_size", len(evs),
				"error_class", string(ierr.ClassOf(err)), "error", err)
			fail(ierr.Wrap(ierr.ClassSinkFailure, op, err))
			return false
		}
		p.Stats.Written.Add(uint64(len(evs)))
		p.Stats.Batches.Add(1)
		bd := batchDone{items: make([]posRef, len(batch))}
		for i, it := range batch {
			bd.items[i] = posRef{idx: it.idx, pos: it.pos}
		}
		last := batch[len(batch)-1]
		log.Info("batch delivered",
			"worker", id, "batch", p.Stats.Batches.Load(),
			"batch_size", len(evs), "offset", last.pos.Offset, "seq", last.pos.Seq)
		select {
		case done <- bd:
		case <-ctx.Done():
			return false
		}
		batch = batch[:0]
		return true
	}

	for {
		select {
		case it, ok := <-queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, it)
			if len(batch) >= p.opts.BatchSize && !flush() {
				return
			}
		case <-ctx.Done():
			return // 取消：在途批次不投递不提交
		}
	}
}

// committer 只提交 idx 连续覆盖前缀的位点，保证位点单调前进且不越过未完成的批次。
// pending 的大小有界于在途事件窗口（队列容量 + worker 数 × 批大小）。
func (p *Pipeline) committer(ctx context.Context, done <-chan batchDone, log *slog.Logger) {
	pending := map[uint64]checkpoint.Position{} // idx -> 已投递事件的位点
	var next uint64                             // 下一个待覆盖的 idx
	for {
		select {
		case bd, ok := <-done:
			if !ok {
				return
			}
			for _, it := range bd.items {
				pending[it.idx] = it.pos
			}
			// 推进连续前缀，按批提交最后一个位点。
			var last checkpoint.Position
			advanced := false
			for {
				pos, ok := pending[next]
				if !ok {
					break
				}
				delete(pending, next)
				last = pos
				next++
				advanced = true
			}
			if !advanced {
				continue
			}
			if ctx.Err() != nil {
				return // 取消后不再推进位点：已写出未提交的部分下次运行重放，由 Sink 去重
			}
			if err := p.store.Commit(ctx, last); err != nil {
				log.Error("checkpoint commit failed",
					"offset", last.Offset, "seq", last.Seq,
					"error_class", string(ierr.ClassOf(err)), "error", err)
				// 提交失败不致命：重启后从旧位点重放，由 Sink 幂等去重。
			} else {
				p.Stats.Committed.Add(1)
				log.Info("checkpoint committed",
					"source", last.Source, "offset", last.Offset, "seq", last.Seq)
			}
		case <-ctx.Done():
			return
		}
	}
}
