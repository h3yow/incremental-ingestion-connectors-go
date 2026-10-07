package csv

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
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadAllAndResume(t *testing.T) {
	path := writeFile(t, "key,kind,payload\na,upsert,\"{\"\"v\"\":1}\"\nb,delete,\n")
	src, err := New(connector.Config{"path": path, "name": "csvtest"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := src.Open(ctx, checkpoint.Position{Source: "csvtest"}); err != nil {
		t.Fatal(err)
	}

	rec, pos, err := src.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec["key"] != "a" || rec["kind"] != "upsert" {
		t.Fatalf("got %v", rec)
	}
	p, ok := rec["payload"].(map[string]any)
	if !ok || p["v"].(float64) != 1 {
		t.Fatalf("payload not parsed: %v", rec["payload"])
	}

	rec2, pos2, err := src.Next(ctx)
	if err != nil || rec2["key"] != "b" {
		t.Fatalf("got %v %v", rec2, err)
	}
	if _, _, err := src.Next(ctx); !ierr.Is(err, ierr.ClassSourceExhausted) {
		t.Fatalf("want exhausted, got %v", err)
	}
	src.Close()

	// 续传：从第二条之后的位点继续，应为耗尽
	src2, _ := New(connector.Config{"path": path, "name": "csvtest"})
	if err := src2.Open(ctx, pos2); err != nil {
		t.Fatal(err)
	}
	defer src2.Close()
	if _, _, err := src2.Next(ctx); !ierr.Is(err, ierr.ClassSourceExhausted) {
		t.Fatalf("want exhausted after resume, got %v", err)
	}
	_ = pos
}

func TestDuplicateOpen(t *testing.T) {
	path := writeFile(t, "key,kind\na,u\n")
	src, _ := New(connector.Config{"path": path, "name": "c"})
	ctx := context.Background()
	if err := src.Open(ctx, checkpoint.Position{Source: "c"}); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if err := src.Open(ctx, checkpoint.Position{Source: "c"}); !ierr.Is(err, ierr.ClassDuplicateInit) {
		t.Fatalf("want duplicate_init, got %v", err)
	}
}

func TestBadPayloadRejected(t *testing.T) {
	path := writeFile(t, "key,kind,payload\na,u,not-json\n")
	src, _ := New(connector.Config{"path": path, "name": "c"})
	ctx := context.Background()
	if err := src.Open(ctx, checkpoint.Position{Source: "c"}); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, _, err := src.Next(ctx); !ierr.Is(err, ierr.ClassRecordInvalid) {
		t.Fatalf("want record_invalid, got %v", err)
	}
}

// TestBadRowReturnsPositionAndContinues 覆盖：字段数不符的坏行返回 record_invalid 及
// 其后的位点，跳过后续读不受影响。
func TestBadRowReturnsPositionAndContinues(t *testing.T) {
	t.Log("覆盖: CSV 坏行返回位点并可跳过续读")
	path := writeFile(t, "key,kind\na,u\nonly-one-field\nb,u\n")
	src, _ := New(connector.Config{"path": path, "name": "c"})
	ctx := context.Background()
	if err := src.Open(ctx, checkpoint.Position{Source: "c"}); err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	if _, _, err := src.Next(ctx); err != nil {
		t.Fatal(err)
	}
	_, pos, err := src.Next(ctx)
	if !ierr.Is(err, ierr.ClassRecordInvalid) {
		t.Fatalf("want record_invalid, got %v", err)
	}
	if pos.Seq != 2 {
		t.Fatalf("bad row should carry its position, got %+v", pos)
	}
	rec, pos3, err := src.Next(ctx)
	if err != nil || rec["key"] != "b" || pos3.Seq != 3 {
		t.Fatalf("after skip got rec=%v pos=%+v err=%v", rec, pos3, err)
	}
	t.Logf("结果: 坏行位点 %+v 可跳过, 续读到 key=b", pos)
}

func TestMissingConfig(t *testing.T) {
	_, err := New(connector.Config{})
	if !ierr.Is(err, ierr.ClassConfigInvalid) {
		t.Fatalf("want config_invalid, got %v", err)
	}
}
