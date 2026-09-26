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
	mu       sync.Mutex
	calls    int
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

func TestRecordLevelErrorAborts(t *testing.T) {
	// 含一条缺 key 的坏记录：必须明确拒绝，不静默跳过
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.jsonl")
	os.WriteFile(srcPath, []byte("{\"key\":\"a\",\"kind\":\"u\"}\n{\"kind\":\"u\"}\n{\"key\":\"c\",\"kind\":\"u\"}\n"), 0o644)
	out := filepath.Join(dir, "out.jsonl")
	ckpt := filepath.Join(dir, "ckpt.json")

	_, err := runOnce(t, srcPath, out, ckpt, 2)
	if !ierr.Is(err, ierr.ClassMissingField) {
		t.Fatalf("want missing_field, got %v", err)
	}
}
