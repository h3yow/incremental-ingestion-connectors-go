package reject

import (
	"path/filepath"
	"testing"
)

func TestAddAndSummary(t *testing.T) {
	t.Log("覆盖：坏记录按类别登记留痕，汇总可按类别查看")
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Add(Entry{Source: "s", Offset: 10, Class: "parse_error", Reason: "bad json"}); !ok {
		t.Fatal("first add must register")
	}
	if ok, _ := s.Add(Entry{Source: "s", Offset: 20, Class: "missing_field", Reason: "no key"}); !ok {
		t.Fatal("second add must register")
	}
	sum := s.Summary()
	if sum.Total != 2 || sum.ByClass["parse_error"] != 1 || sum.ByClass["missing_field"] != 1 {
		t.Fatalf("bad summary: %+v", sum)
	}
	t.Logf("结果：total=%d by_class=%v", sum.Total, sum.ByClass)
}

func TestDedupWithinRun(t *testing.T) {
	t.Log("覆盖：同一条坏记录在一次运行里只登记一次")
	s, _ := Open("")
	e := Entry{Source: "s", Offset: 10, Class: "parse_error"}
	if ok, _ := s.Add(e); !ok {
		t.Fatal("first add must register")
	}
	if ok, _ := s.Add(e); ok {
		t.Fatal("duplicate add must be rejected")
	}
	if got := s.Summary().Total; got != 1 {
		t.Fatalf("want 1 entry, got %d", got)
	}
}

func TestDedupAcrossRestart(t *testing.T) {
	t.Log("覆盖：重启后旧区间被重扫，已登记的坏记录不重复登记、不重复上报")
	path := filepath.Join(t.TempDir(), "rejects.jsonl")

	s1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s1.Add(Entry{Source: "s", Offset: 10, Class: "parse_error"})
	s1.Add(Entry{Source: "s", Offset: 30, Class: "missing_field"})

	// 模拟重启：重新打开同一文件，重扫旧区间
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := s2.Add(Entry{Source: "s", Offset: 10, Class: "parse_error"}); ok {
		t.Fatal("rescanned record must not be re-registered")
	}
	if ok, _ := s2.Add(Entry{Source: "s", Offset: 30, Class: "missing_field"}); ok {
		t.Fatal("rescanned record must not be re-registered")
	}
	// 新坏记录仍要登记
	if ok, _ := s2.Add(Entry{Source: "s", Offset: 55, Class: "type_mismatch"}); !ok {
		t.Fatal("new bad record must register")
	}
	if got := s2.Summary().Total; got != 3 {
		t.Fatalf("want 3 entries after restart, got %d", got)
	}

	// 再次重启：持久化的登记仍然完整
	s3, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := s3.Summary().Total; got != 3 {
		t.Fatalf("want 3 persisted entries, got %d", got)
	}
	t.Logf("结果：重扫不重复登记，持久化条目=%d", s3.Summary().Total)
}
