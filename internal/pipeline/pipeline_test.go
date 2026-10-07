package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/incremental-ingestion-connectors-go/connectors/jsonl"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/checkpoint"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/connector"
	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/event"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/reject"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/sink"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// writeJSONL 生成 n 条合法记录的源文件。
func writeJSONL(t *testing.T, n int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.jsonl")
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, "{\"key\":\"k%d\",\"kind\":\"upsert\",\"payload\":{\"n\":%d}}\n", i, i)
	}
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
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

// runOnce 用同一检查点/输出文件跑一轮管线。
func runOnce(t *testing.T, srcPath, outPath, ckptPath string, workers int) (*Pipeline, error) {
	t.Helper()
	src, err := jsonl.New(connector.Config{"path": srcPath, "name": "test"})
	if err != nil {
		t.Fatal(err)
	}
	store := checkpoint.NewFileStore(ckptPath)
	pos, err := store.Load(context.Background(), src.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Open(context.Background(), pos); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	sk := sink.NewFileSink(outPath)
	defer sk.Close()
	p := New(src, sk, store, Options{
		Workers: workers, QueueSize: 8, BatchSize: 4, Logger: testLogger(),
	})
	err = p.Run(context.Background())
	return p, err
}

func TestFullIngest(t *testing.T) {
	dir := t.TempDir()
	srcPath := writeJSONL(t, 50)
	out := filepath.Join(dir, "out.jsonl")
	ckpt := filepath.Join(dir, "ckpt.json")

	p, err := runOnce(t, srcPath, out, ckpt, 4)
	if err != nil {
		t.Fatal(err)
	}
	if got := countLines(t, out); got != 50 {
		t.Fatalf("want 50 events, got %d", got)
	}
	if p.Stats.Written.Load() != 50 {
		t.Fatalf("stats written=%d", p.Stats.Written.Load())
	}
	// 检查点应推进到末尾
	pos, _ := checkpoint.NewFileStore(ckpt).Load(context.Background(), "test")
	if pos.Seq != 50 {
		t.Fatalf("checkpoint seq=%d", pos.Seq)
	}
}

func TestRestartResume(t *testing.T) {
	dir := t.TempDir()
	srcPath := writeJSONL(t, 30)
	out := filepath.Join(dir, "out.jsonl")
	ckpt := filepath.Join(dir, "ckpt.json")

	// 第一轮：完整摄取
	if _, err := runOnce(t, srcPath, out, ckpt, 2); err != nil {
		t.Fatal(err)
	}
	before := countLines(t, out)

	// 第二轮（模拟重启）：检查点已在末尾，不应再写入任何事件
	p, err := runOnce(t, srcPath, out, ckpt, 2)
	if err != nil {
		t.Fatal(err)
	}
	if p.Stats.Written.Load() != 0 {
		t.Fatalf("resume should write 0, wrote %d", p.Stats.Written.Load())
	}
	if got := countLines(t, out); got != before {
		t.Fatalf("output changed after resume: %d -> %d", before, got)
	}
}

func TestDuplicateConsumptionNoDuplicateWrites(t *testing.T) {
	dir := t.TempDir()
	srcPath := writeJSONL(t, 20)
	out := filepath.Join(dir, "out.jsonl")
	ckpt := filepath.Join(dir, "ckpt.json")

	// 第一轮正常摄取
	if _, err := runOnce(t, srcPath, out, ckpt, 3); err != nil {
		t.Fatal(err)
	}
	// 模拟崩溃后位点丢失：删掉检查点重放全部数据
	os.Remove(ckpt)
	if _, err := runOnce(t, srcPath, out, ckpt, 3); err != nil {
		t.Fatal(err)
	}
	// 下游幂等：重放不产生重复行
	if got := countLines(t, out); got != 20 {
		t.Fatalf("duplicate writes detected: %d lines", got)
	}
}

// failingSink 在第 failAfter 批之后持续失败。
type failingSink struct {
	mu        sync.Mutex
	calls     int
	failAfter int
}

func (f *failingSink) WriteBatch(ctx context.Context, batch []event.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls > f.failAfter {
		return ierr.New(ierr.ClassSinkFailure, "failingSink", "injected downstream failure")
	}
	return nil
}
func (f *failingSink) Close() error { return nil }

func TestSinkFailureClassified(t *testing.T) {
	srcPath := writeJSONL(t, 40)
	dir := t.TempDir()
	ckpt := filepath.Join(dir, "ckpt.json")

	src, _ := jsonl.New(connector.Config{"path": srcPath, "name": "test"})
	store := checkpoint.NewFileStore(ckpt)
	pos, _ := store.Load(context.Background(), "test")
	if err := src.Open(context.Background(), pos); err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	fs := &failingSink{failAfter: 2}
	p := New(src, fs, store, Options{Workers: 1, QueueSize: 4, BatchSize: 4, Logger: testLogger()})
	err := p.Run(context.Background())
	if !ierr.Is(err, ierr.ClassSinkFailure) {
		t.Fatalf("want sink_failure, got %v", err)
	}
	// 失败批次不得提交位点：检查点 seq 应 <= 8（前两批）
	pos, _ = store.Load(context.Background(), "test")
	if pos.Seq > 8 {
		t.Fatalf("checkpoint advanced past failed batch: seq=%d", pos.Seq)
	}
}

func TestConcurrentCancel(t *testing.T) {
	srcPath := writeJSONL(t, 2000)
	dir := t.TempDir()
	out := filepath.Join(dir, "out.jsonl")
	ckpt := filepath.Join(dir, "ckpt.json")

	// 第一轮：跑到一半取消
	func() {
		src, _ := jsonl.New(connector.Config{"path": srcPath, "name": "test"})
		store := checkpoint.NewFileStore(ckpt)
		pos, _ := store.Load(context.Background(), "test")
		src.Open(context.Background(), pos)
		defer src.Close()
		sk := sink.NewFileSink(out)
		defer sk.Close()

		ctx, cancel := context.WithCancel(context.Background())
		p := New(src, sk, store, Options{Workers: 8, QueueSize: 16, BatchSize: 8, Logger: testLogger()})
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()
		err := p.Run(ctx)
		if err != nil && !ierr.Is(err, ierr.ClassCanceled) {
			t.Fatalf("want canceled or nil, got %v", err)
		}
	}()

	// 取消后检查点不得越过已写入的数据（未提交的重放即可）
	pos, _ := checkpoint.NewFileStore(ckpt).Load(context.Background(), "test")
	t.Logf("after cancel: checkpoint seq=%d, written lines=%d", pos.Seq, countLines(t, out))

	// 第二轮：从检查点续传直到完成
	if _, err := runOnce(t, srcPath, out, ckpt, 8); err != nil {
		t.Fatal(err)
	}
	// 最终状态：不丢（10000 行全在）不重（没有重复行）
	if got := countLines(t, out); got != 2000 {
		t.Fatalf("after resume want 2000 lines, got %d", got)
	}
	assertNoDuplicateKeys(t, out)
}

// assertNoDuplicateKeys 校验输出中事件 ID 无重复。
func assertNoDuplicateKeys(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatal(err)
		}
		seen[ev.ID()]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Fatalf("duplicate event %s x%d", id, n)
		}
	}
}

func TestBackpressureBlocking(t *testing.T) {
	srcPath := writeJSONL(t, 30)
	dir := t.TempDir()
	ckpt := filepath.Join(dir, "ckpt.json")

	src, _ := jsonl.New(connector.Config{"path": srcPath, "name": "test"})
	store := checkpoint.NewFileStore(ckpt)
	pos, _ := store.Load(context.Background(), "test")
	src.Open(context.Background(), pos)
	defer src.Close()

	// 慢下游 + 小队列：生产者必须阻塞而不是丢数据
	slow := &slowSink{delay: 5 * time.Millisecond}
	p := New(src, slow, store, Options{Workers: 1, QueueSize: 2, BatchSize: 2, Logger: testLogger()})
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if slow.written.Load() != 30 {
		t.Fatalf("lost data under backpressure: wrote %d/30", slow.written.Load())
	}
}

type slowSink struct {
	delay   time.Duration
	written atomic.Uint64
}

func (s *slowSink) WriteBatch(ctx context.Context, batch []event.Event) error {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ierr.Wrap(ierr.ClassCanceled, "slowSink", ctx.Err())
	}
	s.written.Add(uint64(len(batch)))
	return nil
}
func (s *slowSink) Close() error { return nil }

func TestThroughputScalesWithWorkers(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	srcPath := writeJSONL(t, 2000)

	run := func(workers int) time.Duration {
		dir := t.TempDir()
		src, _ := jsonl.New(connector.Config{"path": srcPath, "name": "test"})
		store := checkpoint.NewFileStore(filepath.Join(dir, "ckpt.json"))
		pos, _ := store.Load(context.Background(), "test")
		src.Open(context.Background(), pos)
		defer src.Close()
		// 下游有固定成本，并发可摊薄
		sk := &slowSink{delay: 2 * time.Millisecond}
		p := New(src, sk, store, Options{
			Workers: workers, QueueSize: 64, BatchSize: 8, Logger: testLogger(),
		})
		start := time.Now()
		if err := p.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		return time.Since(start)
	}

	d1 := run(1)
	d8 := run(8)
	t.Logf("workers=1: %v, workers=8: %v (speedup %.1fx)", d1, d8, float64(d1)/float64(d8))
	if d8 >= d1 {
		t.Fatalf("throughput did not scale: 1 worker %v, 8 workers %v", d1, d8)
	}
}

// runOnceWithRejects 与 runOnce 相同，但使用持久化的拒绝存储（可跨运行去重）。
func runOnceWithRejects(t *testing.T, srcPath, outPath, ckptPath, rejectsPath string, workers int) (*Pipeline, error) {
	t.Helper()
	src, err := jsonl.New(connector.Config{"path": srcPath, "name": "test"})
	if err != nil {
		t.Fatal(err)
	}
	store := checkpoint.NewFileStore(ckptPath)
	pos, err := store.Load(context.Background(), src.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Open(context.Background(), pos); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	sk := sink.NewFileSink(outPath)
	defer sk.Close()
	rejects, err := reject.Open(rejectsPath)
	if err != nil {
		t.Fatal(err)
	}
	p := New(src, sk, store, Options{
		Workers: workers, QueueSize: 8, BatchSize: 4, Rejects: rejects, Logger: testLogger(),
	})
	err = p.Run(context.Background())
	return p, err
}

func TestBadRecordsRejectedAndContinue(t *testing.T) {
	t.Log("覆盖：缺字段/类型错/整行无法解析的坏记录被分类拒绝，好记录全部投递，位点推进到源末尾")
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.jsonl")
	// 好 5 条 + 坏 4 条（缺 key、key 类型错、整行非 JSON、末尾缺 kind）
	content := strings.Join([]string{
		`{"key":"a","kind":"u"}`,
		`{"kind":"u"}`, // missing_field
		`{"key":"b","kind":"u"}`,
		`{"key":1,"kind":"u"}`, // type_mismatch
		`not-json-at-all`,      // parse_error
		`{"key":"c","kind":"u"}`,
		`{"key":"d","kind":"u"}`,
		`{"kind":"u"}`, // missing_field（末尾坏记录，位点仍须到源末尾）
		`{"key":"e","kind":"u"}`,
	}, "\n") + "\n"
	os.WriteFile(srcPath, []byte(content), 0o644)
	out := filepath.Join(dir, "out.jsonl")
	ckpt := filepath.Join(dir, "ckpt.json")
	rejectsPath := filepath.Join(dir, "rejects.jsonl")

	p, err := runOnceWithRejects(t, srcPath, out, ckpt, rejectsPath, 2)
	if err != nil {
		t.Fatalf("bad records must not abort the run, got %v", err)
	}
	// 好记录全部投递
	if got := countLines(t, out); got != 5 {
		t.Fatalf("want 5 good events delivered, got %d", got)
	}
	if p.Stats.Rejected.Load() != 4 {
		t.Fatalf("want 4 rejects registered, got %d", p.Stats.Rejected.Load())
	}
	// 拒绝汇总可按类别查看
	rejects, _ := reject.Open(rejectsPath)
	sum := rejects.Summary()
	t.Logf("结果：delivered=5 rejected=%d by_class=%v", p.Stats.Rejected.Load(), sum.ByClass)
	if sum.Total != 4 || sum.ByClass["missing_field"] != 2 ||
		sum.ByClass["type_mismatch"] != 1 || sum.ByClass["parse_error"] != 1 {
		t.Fatalf("bad reject summary: %+v", sum)
	}
	// 每条拒绝都能看出是哪一条（位点）、因为哪一类原因
	for _, e := range rejects.Entries() {
		if e.Source == "" || e.Offset <= 0 || e.Class == "" || e.Reason == "" {
			t.Fatalf("reject entry incomplete: %+v", e)
		}
	}
	// 位点推进到源末尾（末尾是坏记录也要到末尾）
	fi, _ := os.Stat(srcPath)
	pos, _ := checkpoint.NewFileStore(ckpt).Load(context.Background(), "test")
	if pos.Offset != fi.Size() {
		t.Fatalf("checkpoint must reach source end: offset=%d size=%d", pos.Offset, fi.Size())
	}
	t.Logf("结果：checkpoint offset=%d == 源大小 %d（已推进到源末尾）", pos.Offset, fi.Size())
}

func TestRejectsNotDuplicatedAfterRestart(t *testing.T) {
	t.Log("覆盖：重启续传重扫旧区间时，坏记录不重复登记、不重复上报，好记录不重复写入")
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.jsonl")
	os.WriteFile(srcPath, []byte(
		"{\"key\":\"a\",\"kind\":\"u\"}\n{\"kind\":\"u\"}\n{\"key\":\"b\",\"kind\":\"u\"}\n"), 0o644)
	out := filepath.Join(dir, "out.jsonl")
	ckpt := filepath.Join(dir, "ckpt.json")
	rejectsPath := filepath.Join(dir, "rejects.jsonl")

	// 第一轮：完整摄取，1 条坏记录被登记
	p1, err := runOnceWithRejects(t, srcPath, out, ckpt, rejectsPath, 2)
	if err != nil {
		t.Fatal(err)
	}
	if p1.Stats.Rejected.Load() != 1 {
		t.Fatalf("run1: want 1 reject, got %d", p1.Stats.Rejected.Load())
	}

	// 模拟崩溃丢失检查点：第二轮从头重扫整个旧区间
	os.Remove(ckpt)
	p2, err := runOnceWithRejects(t, srcPath, out, ckpt, rejectsPath, 2)
	if err != nil {
		t.Fatal(err)
	}
	if p2.Stats.Rejected.Load() != 0 {
		t.Fatalf("run2: rescanned bad record must not re-register, got %d", p2.Stats.Rejected.Load())
	}
	rejects, _ := reject.Open(rejectsPath)
	if got := rejects.Summary().Total; got != 1 {
		t.Fatalf("reject store must stay at 1 entry, got %d", got)
	}
	// 好记录重放由下游幂等去重：输出不重复
	if got := countLines(t, out); got != 2 {
		t.Fatalf("want 2 lines after replay, got %d", got)
	}
	assertNoDuplicateKeys(t, out)
	t.Logf("结果：重扫后拒绝登记仍为 1 条，输出 2 行无重复")
}

func TestCancelStopsDeliveryAndCheckpoint(t *testing.T) {
	t.Log("覆盖：取消立即生效——在途批次被丢弃、不再投递、位点不再推进；续传后不丢不重")
	srcPath := writeJSONL(t, 200)
	dir := t.TempDir()
	out := filepath.Join(dir, "out.jsonl")
	ckpt := filepath.Join(dir, "ckpt.json")

	// gateSink：第一批进入后阻塞，直到测试取消并放行；取消后按约定返回 canceled
	gate := &gateSink{entered: make(chan struct{}), release: make(chan struct{})}

	src, _ := jsonl.New(connector.Config{"path": srcPath, "name": "test"})
	store := checkpoint.NewFileStore(ckpt)
	pos, _ := store.Load(context.Background(), "test")
	src.Open(context.Background(), pos)
	defer src.Close()

	ctx, cancel := context.WithCancel(context.Background())
	p := New(src, gate, store, Options{Workers: 1, QueueSize: 4, BatchSize: 4, Logger: testLogger()})
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(ctx) }()

	<-gate.entered // 一个批次正在投递中
	cancel()       // 外部取消
	close(gate.release)
	err := <-runDone
	if !ierr.Is(err, ierr.ClassCanceled) {
		t.Fatalf("want canceled, got %v", err)
	}
	// 取消后：在途批次被丢弃，投递数与提交数都为 0，位点没有推进
	if got := p.Stats.Written.Load(); got != 0 {
		t.Fatalf("in-flight batch must be dropped on cancel, written=%d", got)
	}
	if got := p.Stats.Committed.Load(); got != 0 {
		t.Fatalf("no commit allowed after cancel, committed=%d", got)
	}
	pos, _ = checkpoint.NewFileStore(ckpt).Load(context.Background(), "test")
	if pos.Seq != 0 || pos.Offset != 0 {
		t.Fatalf("checkpoint must not advance after cancel, got %+v", pos)
	}
	t.Logf("结果：取消时 written=%d committed=%d checkpoint=%+v（均未推进）",
		p.Stats.Written.Load(), p.Stats.Committed.Load(), pos)

	// 续传：全部记录投递，无丢失无重复
	if _, err := runOnce(t, srcPath, out, ckpt, 4); err != nil {
		t.Fatal(err)
	}
	if got := countLines(t, out); got != 200 {
		t.Fatalf("after resume want 200 lines, got %d", got)
	}
	assertNoDuplicateKeys(t, out)
	t.Log("结果：续传后 200 行齐全且无重复")
}

// gateSink 阻塞第一批投递，用于确定性地构造"取消时在途批次"场景。
type gateSink struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gateSink) WriteBatch(ctx context.Context, batch []event.Event) error {
	g.once.Do(func() { close(g.entered) })
	<-g.release
	if err := ctx.Err(); err != nil {
		return ierr.Wrap(ierr.ClassCanceled, "gateSink", err)
	}
	return nil
}
func (g *gateSink) Close() error { return nil }
