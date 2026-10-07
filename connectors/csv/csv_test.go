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
	// 字段内容不合规（payload 不是 JSON）：type_mismatch，返回坏行之后的位点
	path := writeFile(t, "key,kind,payload\na,u,not-json\nb,u,\n")
	src, _ := New(connector.Config{"path": path, "name": "c"})
	ctx := context.Background()
	if err := src.Open(ctx, checkpoint.Position{Source: "c"}); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	_, pos, err := src.Next(ctx)
	if !ierr.Is(err, ierr.ClassTypeMismatch) {
		t.Fatalf("want type_mismatch, got %v", err)
	}
	if pos.Offset <= 0 {
		t.Fatalf("reject must carry position past bad row, got %+v", pos)
	}
	// 坏行之后继续读
	rec, _, err := src.Next(ctx)
	if err != nil || rec["key"] != "b" {
		t.Fatalf("next after bad row got %v %v", rec, err)
	}
}

func TestBadRowShapeRejected(t *testing.T) {
	// 整行结构无法按表头解析：parse_error
	path := writeFile(t, "key,kind\na,u,extra\nb,u\n")
	src, _ := New(connector.Config{"path": path, "name": "c"})
	ctx := context.Background()
	if err := src.Open(ctx, checkpoint.Position{Source: "c"}); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, _, err := src.Next(ctx); !ierr.Is(err, ierr.ClassParseError) {
		t.Fatalf("want parse_error, got %v", err)
	}
	rec, _, err := src.Next(ctx)
	if err != nil || rec["key"] != "b" {
		t.Fatalf("next after bad row got %v %v", rec, err)
	}
}

func TestConfigClassified(t *testing.T) {
	// 缺少必要配置 vs 配置形态不对，两者可区分
	if _, err := New(connector.Config{}); !ierr.Is(err, ierr.ClassConfigMissing) {
		t.Fatalf("want config_missing, got %v", err)
	}
	if _, err := New(connector.Config{"path": 42}); !ierr.Is(err, ierr.ClassConfigInvalid) {
		t.Fatalf("want config_invalid, got %v", err)
	}
}
