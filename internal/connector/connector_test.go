package connector

import (
	"context"
	"testing"

	"github.com/h3yow/incremental-ingestion-connectors-go/internal/checkpoint"
	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/event"
)

type fakeSource struct{ name string }

func (f *fakeSource) Name() string { return f.name }
func (f *fakeSource) Open(context.Context, checkpoint.Position) error {
	return nil
}
func (f *fakeSource) Next(context.Context) (event.Record, checkpoint.Position, error) {
	return nil, checkpoint.Position{}, ierr.New(ierr.ClassSourceExhausted, "fake.Next", "done")
}
func (f *fakeSource) Close() error { return nil }

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	factory := func(cfg Config) (Source, error) { return &fakeSource{name: "fake"}, nil }

	if err := r.Register("fake", factory); err != nil {
		t.Fatal(err)
	}
	// 重复注册：冲突
	if err := r.Register("fake", factory); !ierr.Is(err, ierr.ClassRegistrationConflict) {
		t.Fatalf("want registration_conflict, got %v", err)
	}
	// 空标识
	if err := r.Register("", factory); !ierr.Is(err, ierr.ClassRegistrationConflict) {
		t.Fatalf("want registration_conflict, got %v", err)
	}
	// 未知类型：与"缺少配置"区分开，便于运维判断是改配置还是换类型
	if _, err := r.New("nope", nil); !ierr.Is(err, ierr.ClassUnknownConnector) {
		t.Fatalf("want unknown_connector, got %v", err)
	}
	// 正常构造
	src, err := r.New("fake", nil)
	if err != nil || src.Name() != "fake" {
		t.Fatalf("got %v %v", src, err)
	}
	// 工厂底层错误不得直接透出
	r2 := NewRegistry()
	r2.Register("bad", func(Config) (Source, error) { return nil, context.DeadlineExceeded })
	if _, err := r2.New("bad", nil); !ierr.Is(err, ierr.ClassInternal) {
		t.Fatalf("want internal, got %v", err)
	}
}
