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
	// 整行无法解析：归类 parse_error，返回坏行之后的位点，后续记录可继续读
	path := writeFile(t, "not-json\n{\"key\":\"b\",\"kind\":\"u\"}\n")
	src := open(t, path, checkpoint.Position{Source: "test"})
	defer src.Close()
	_, pos, err := src.Next(context.Background())
	if !ierr.Is(err, ierr.ClassParseError) {
		t.Fatalf("want parse_error, got %v", err)
	}
	if pos.Offset <= 0 {
		t.Fatalf("parse error must carry position past bad line, got %+v", pos)
	}
	rec, pos2, err := src.Next(context.Background())
	if err != nil || rec["key"] != "b" {
		t.Fatalf("next after bad line got %v %v", rec, err)
	}
	if pos2.Seq != 1 {
		t.Fatalf("bad line must not consume seq, got seq=%d", pos2.Seq)
	}
}

func TestMissingConfig(t *testing.T) {
	// 缺少必要配置：config_missing
	if _, err := New(connector.Config{}); !ierr.Is(err, ierr.ClassConfigMissing) {
		t.Fatalf("want config_missing, got %v", err)
	}
	// 配置形态不对：config_invalid
	if _, err := New(connector.Config{"path": 42}); !ierr.Is(err, ierr.ClassConfigInvalid) {
		t.Fatalf("want config_invalid, got %v", err)
	}
}
