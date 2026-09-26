package checkpoint

import (
	"context"
	"path/filepath"
	"testing"

	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
)

func TestValidateAgainst(t *testing.T) {
	cur := Position{Source: "s", Offset: 10, Seq: 5}
	cases := []struct {
		name string
		p    Position
		max  int64
		want ierr.Class
	}{
		{"ok", Position{Source: "s", Offset: 20, Seq: 6}, 100, ""},
		{"mismatch", Position{Source: "other", Offset: 20, Seq: 6}, 100, ierr.ClassPositionSourceMismatch},
		{"regress-offset", Position{Source: "s", Offset: 5, Seq: 6}, 100, ierr.ClassPositionRegression},
		{"regress-seq", Position{Source: "s", Offset: 20, Seq: 4}, 100, ierr.ClassPositionRegression},
		{"out-of-range", Position{Source: "s", Offset: 200, Seq: 6}, 100, ierr.ClassPositionOutOfRange},
	}
	for _, c := range cases {
		err := c.p.ValidateAgainst(cur, c.max)
		if c.want == "" {
			if err != nil {
				t.Fatalf("%s: unexpected %v", c.name, err)
			}
			continue
		}
		if !ierr.Is(err, c.want) {
			t.Fatalf("%s: want %s, got %v", c.name, c.want, err)
		}
	}
}

func TestFileStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt.json")
	s := NewFileStore(path)
	ctx := context.Background()

	// 无文件时返回零位点
	p, err := s.Load(ctx, "src-a")
	if err != nil || p.Source != "src-a" || p.Offset != 0 {
		t.Fatalf("load empty: %+v %v", p, err)
	}
	// 提交并读回
	if err := s.Commit(ctx, Position{Source: "src-a", Offset: 42, Seq: 7}); err != nil {
		t.Fatal(err)
	}
	p, err = s.Load(ctx, "src-a")
	if err != nil || p.Offset != 42 || p.Seq != 7 {
		t.Fatalf("load: %+v %v", p, err)
	}
	// 回退被拒绝
	err = s.Commit(ctx, Position{Source: "src-a", Offset: 10, Seq: 3})
	if !ierr.Is(err, ierr.ClassPositionRegression) {
		t.Fatalf("want regression, got %v", err)
	}
	// 源不匹配被拒绝
	err = s.Commit(ctx, Position{Source: "src-b", Offset: 99, Seq: 9})
	if !ierr.Is(err, ierr.ClassPositionSourceMismatch) {
		t.Fatalf("want mismatch, got %v", err)
	}
	// 加载其他源被拒绝
	_, err = s.Load(ctx, "src-b")
	if !ierr.Is(err, ierr.ClassPositionSourceMismatch) {
		t.Fatalf("want mismatch, got %v", err)
	}
}
