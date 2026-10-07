package reject

import (
	"context"
	"path/filepath"
	"testing"
)

// TestFileStoreAddAndSummary 覆盖：坏记录按类别登记、可查询汇总（第几条、因何被拒）。
func TestFileStoreAddAndSummary(t *testing.T) {
	t.Log("覆盖: 拒绝记录登记与按类别汇总")
	dir := t.TempDir()
	s := NewFileStore(filepath.Join(dir, "rejects.jsonl"))
	ctx := context.Background()

	recs := []Rejection{
		{Source: "src", Seq: 2, Offset: 40, Class: "record_invalid", Reason: "missing key"},
		{Source: "src", Seq: 5, Offset: 120, Class: "record_unparseable", Reason: "bad json"},
		{Source: "src", Seq: 7, Offset: 200, Class: "record_invalid", Reason: "bad ts"},
	}
	for _, r := range recs {
		added, err := s.Add(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if !added {
			t.Fatalf("first add of %+v should report added", r)
		}
	}

	sum, err := s.Summary(ctx, "src")
	if err != nil {
		t.Fatal(err)
	}
	if sum["record_invalid"] != 2 || sum["record_unparseable"] != 1 {
		t.Fatalf("summary=%v", sum)
	}
	list, err := s.List(ctx, "src")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 || list[0].Seq != 2 || list[2].Offset != 200 {
		t.Fatalf("list=%+v", list)
	}
	t.Logf("结果: 3 条拒绝记录登记成功, 汇总=%v", sum)
}

// TestFileStoreDedupeWithinRun 覆盖：同一条坏记录在一次运行里只登记一次。
func TestFileStoreDedupeWithinRun(t *testing.T) {
	t.Log("覆盖: 同一运行内同一条坏记录只登记一次")
	s := NewFileStore(filepath.Join(t.TempDir(), "rejects.jsonl"))
	ctx := context.Background()
	r := Rejection{Source: "src", Seq: 3, Offset: 64, Class: "record_invalid", Reason: "x"}

	if added, _ := s.Add(ctx, r); !added {
		t.Fatal("first add should report added")
	}
	if added, _ := s.Add(ctx, r); added {
		t.Fatal("duplicate add in same run must not be recorded again")
	}
	if n, _ := s.Summary(ctx, "src"); n["record_invalid"] != 1 {
		t.Fatalf("summary=%v, want exactly 1 record_invalid", n)
	}
	t.Log("结果: 重复登记被去重, 汇总保持 1 条")
}

// TestFileStoreDedupeAcrossRestart 覆盖：重启续传重扫旧区间时，不重复登记、不重复上报。
func TestFileStoreDedupeAcrossRestart(t *testing.T) {
	t.Log("覆盖: 重启后重扫旧区间, 已登记的坏记录不重复登记")
	path := filepath.Join(t.TempDir(), "rejects.jsonl")
	ctx := context.Background()
	r1 := Rejection{Source: "src", Seq: 2, Offset: 40, Class: "record_invalid", Reason: "missing key"}
	r2 := Rejection{Source: "src", Seq: 4, Offset: 96, Class: "record_unparseable", Reason: "bad json"}

	s1 := NewFileStore(path)
	for _, r := range []Rejection{r1, r2} {
		if added, err := s1.Add(ctx, r); err != nil || !added {
			t.Fatalf("first run add(%+v) added=%v err=%v", r, added, err)
		}
	}
	s1.Close()

	// 模拟重启：新建存储实例，旧区间被重扫，同样的坏记录再次到达
	s2 := NewFileStore(path)
	for _, r := range []Rejection{r1, r2} {
		added, err := s2.Add(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if added {
			t.Fatalf("after restart, rescanned rejection %+v must not be recorded again", r)
		}
	}
	// 新坏记录仍应正常登记
	r3 := Rejection{Source: "src", Seq: 9, Offset: 300, Class: "record_invalid", Reason: "empty kind"}
	if added, _ := s2.Add(ctx, r3); !added {
		t.Fatal("new rejection after restart should be recorded")
	}
	sum, _ := s2.Summary(ctx, "src")
	if sum["record_invalid"] != 2 || sum["record_unparseable"] != 1 {
		t.Fatalf("summary after restart=%v", sum)
	}
	t.Logf("结果: 重扫不重复登记, 新坏记录正常登记, 汇总=%v", sum)
}

// TestMemoryStore 覆盖：内存存储的登记/去重/汇总（管线默认行为）。
func TestMemoryStore(t *testing.T) {
	t.Log("覆盖: 内存拒绝存储的登记与去重")
	m := NewMemoryStore()
	ctx := context.Background()
	r := Rejection{Source: "s", Seq: 1, Offset: 10, Class: "record_invalid", Reason: "x"}
	if added, _ := m.Add(ctx, r); !added {
		t.Fatal("first add should report added")
	}
	if added, _ := m.Add(ctx, r); added {
		t.Fatal("duplicate add must be deduped")
	}
	list, _ := m.List(ctx, "s")
	if len(list) != 1 {
		t.Fatalf("list=%+v", list)
	}
	t.Log("结果: 内存存储去重正确")
}
