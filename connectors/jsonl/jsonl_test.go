package jsonl

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/h3yow/incremental-ingestion-connectors-go/internal/checkpoint"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/connector"
	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
)

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.jsonl")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func open(t *testing.T, path string, from checkpoint.Position) connector.Source {
	t.Helper()
	src, err := New(connector.Config{"path": path, "name": "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Open(context.Background(), from); err != nil {
		t.Fatal(err)
	}
	return src
}

func TestReadAll(t *testing.T) {
	path := writeFile(t, "{\"key\":\"a\",\"kind\":\"u\"}\n{\"key\":\"b\",\"kind\":\"u\"}\n")
	src := open(t, path, checkpoint.Position{Source: "test"})
	defer src.Close()

	for i, want := range []string{"a", "b"} {
		rec, pos, err := src.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if rec["key"] != want {
			t.Fatalf("rec %d: got %v", i, rec)
		}
		if pos.Seq != uint64(i+1) {
			t.Fatalf("pos %d: %+v", i, pos)
		}
	}
	_, _, err := src.Next(context.Background())
	if !ierr.Is(err, ierr.ClassSourceExhausted) {
		t.Fatalf("want exhausted, got %v", err)
	}
}

func TestResumeFromOffset(t *testing.T) {
	content := "{\"key\":\"a\",\"kind\":\"u\"}\n{\"key\":\"b\",\"kind\":\"u\"}\n"
	path := writeFile(t, content)
	src := open(t, path, checkpoint.Position{Source: "test"})
	_, pos, err := src.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	src.Close()

	// 重启续传：从第一条之后的位点继续
	src2 := open(t, path, pos)
	defer src2.Close()
	rec, pos2, err := src2.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rec["key"] != "b" || pos2.Seq != 2 {
		t.Fatalf("resume got %v %+v", rec, pos2)
	}
}

func TestDuplicateOpen(t *testing.T) {
	path := writeFile(t, "{\"key\":\"a\",\"kind\":\"u\"}\n")
	src := open(t, path, checkpoint.Position{Source: "test"})
	defer src.Close()
	err := src.Open(context.Background(), checkpoint.Position{Source: "test"})
	if !ierr.Is(err, ierr.ClassDuplicateInit) {
		t.Fatalf("want duplicate_init, got %v", err)
	}
}

func TestPositionMismatchAndOutOfRange(t *testing.T) {
	path := writeFile(t, "{\"key\":\"a\",\"kind\":\"u\"}\n")
	src, _ := New(connector.Config{"path": path, "name": "test"})
	err := src.Open(context.Background(), checkpoint.Position{Source: "other"})
	if !ierr.Is(err, ierr.ClassPositionSourceMismatch) {
		t.Fatalf("want mismatch, got %v", err)
	}
	err = src.Open(context.Background(), checkpoint.Position{Source: "test", Offset: 1 << 20})
	if !ierr.Is(err, ierr.ClassPositionOutOfRange) {
		t.Fatalf("want out_of_range, got %v", err)
	}
}

func TestBadJSONRejected(t *testing.T) {
	path := writeFile(t, "not-json\n")
	src := open(t, path, checkpoint.Position{Source: "test"})
	defer src.Close()
	_, _, err := src.Next(context.Background())
	if !ierr.Is(err, ierr.ClassRecordUnparseable) {
		t.Fatalf("want record_unparseable, got %v", err)
	}
}

// TestBadLineReturnsPositionAndContinues 覆盖：坏行返回 record_unparseable 及其后的
// 位点，调用方跳过后续读不受影响；凭该位点续传不会重读坏行。
func TestBadLineReturnsPositionAndContinues(t *testing.T) {
	t.Log("覆盖: 坏行返回位点并可跳过续读, 续传不重读坏行")
	path := writeFile(t, "{\"key\":\"a\",\"kind\":\"u\"}\nbad-line\n{\"key\":\"c\",\"kind\":\"u\"}\n")
	src := open(t, path, checkpoint.Position{Source: "test"})
	defer src.Close()
	ctx := context.Background()

	if _, _, err := src.Next(ctx); err != nil {
		t.Fatal(err)
	}
	// 坏行：返回分类错误 + 其后的位点
	_, pos, err := src.Next(ctx)
	if !ierr.Is(err, ierr.ClassRecordUnparseable) {
		t.Fatalf("want record_unparseable, got %v", err)
	}
	if pos.Seq != 2 || pos.Offset <= 0 {
		t.Fatalf("bad line should carry its position, got %+v", pos)
	}
	// 跳过后续读：下一条好记录正常返回
	rec, pos3, err := src.Next(ctx)
	if err != nil || rec["key"] != "c" || pos3.Seq != 3 {
		t.Fatalf("after skip got rec=%v pos=%+v err=%v", rec, pos3, err)
	}

	// 凭坏行位点续传：直接读到 c，不重读坏行
	src2 := open(t, path, pos)
	defer src2.Close()
	rec, pos4, err := src2.Next(ctx)
	if err != nil || rec["key"] != "c" || pos4.Seq != 3 {
		t.Fatalf("resume from bad-line position got rec=%v pos=%+v err=%v", rec, pos4, err)
	}
	t.Logf("结果: 坏行位点 %+v 可跳过续读, 续传直接读到 key=c", pos)
}

func TestMissingConfig(t *testing.T) {
	_, err := New(connector.Config{})
	if !ierr.Is(err, ierr.ClassConfigInvalid) {
		t.Fatalf("want config_invalid, got %v", err)
	}
}
