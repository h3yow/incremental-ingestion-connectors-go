package checkpoint_test

import (
	"path/filepath"
	"testing"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/checkpoint"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/event"
)

func TestCommitAdvancesAndPersists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ckpt.json")
	s := checkpoint.NewFileStore(path, "src:A")
	if _, err := s.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	evs := []event.Event{{Key: "k1", Offset: "1"}, {Key: "k2", Offset: "2"}}
	if err := s.Commit(checkpoint.CommitRequest{Offset: "2", Events: evs}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: state must survive restart.
	s2 := checkpoint.NewFileStore(path, "src:A")
	cp, err := s2.Load()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if cp.Offset != "2" {
		t.Fatalf("offset = %q, want 2", cp.Offset)
	}
	// k1 (ordinal 1) was pruned as strictly below the watermark 2; only
	// keys at/after the boundary are retained for crash-dedup.
	if s2.HasKey("k1") {
		t.Fatalf("k1 should have been pruned: %v", cp.AppliedKeys)
	}
	if !s2.HasKey("k2") {
		t.Fatalf("boundary key k2 not rehydrated: %v", cp.AppliedKeys)
	}
}

func TestCommitRejectsRollbackAndDuplicate(t *testing.T) {
	s := checkpoint.NewFileStore(filepath.Join(t.TempDir(), "c.json"), "src:A")
	if _, err := s.Load(); err != nil {
		t.Fatal(err)
	}
	evs := []event.Event{{Key: "k1", Offset: "5"}}
	if err := s.Commit(checkpoint.CommitRequest{Offset: "5", Events: evs}); err != nil {
		t.Fatal(err)
	}
	// Equal offset: duplicate commit must be refused as stale.
	err := s.Commit(checkpoint.CommitRequest{Offset: "5", Events: evs})
	if err == nil || err.Kind != cerror.KindOffsetStale {
		t.Fatalf("equal commit: got %v, want offset_stale", err)
	}
	// Smaller offset: rollback must be refused.
	err = s.Commit(checkpoint.CommitRequest{Offset: "1", Events: nil})
	if err == nil || err.Kind != cerror.KindOffsetStale {
		t.Fatalf("rollback commit: got %v, want offset_stale", err)
	}
}

func TestCommitRejectsBadOffsetFormat(t *testing.T) {
	s := checkpoint.NewFileStore(filepath.Join(t.TempDir(), "c.json"), "src:A")
	if _, err := s.Load(); err != nil {
		t.Fatal(err)
	}
	err := s.Commit(checkpoint.CommitRequest{Offset: "-3", Events: nil})
	if err == nil || err.Kind != cerror.KindOffsetBadFormat {
		t.Fatalf("got %v, want offset_bad_format", err)
	}
	err = s.Commit(checkpoint.CommitRequest{Offset: "xyz", Events: nil})
	if err == nil || err.Kind != cerror.KindOffsetBadFormat {
		t.Fatalf("got %v, want offset_bad_format", err)
	}
}

func TestLoadRejectsTagMismatchAndCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	s := checkpoint.NewFileStore(path, "src:A")
	if _, err := s.Load(); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(checkpoint.CommitRequest{Offset: "1",
		Events: []event.Event{{Key: "k", Offset: "1"}}}); err != nil {
		t.Fatal(err)
	}

	// Different binding on the same file -> source mismatch.
	other := checkpoint.NewFileStore(path, "src:B")
	_, err := other.Load()
	if err == nil || err.Kind != cerror.KindOffsetMismatch {
		t.Fatalf("tag mismatch: got %v, want offset_source_mismatch", err)
	}

	// Corrupt JSON file.
	cpath := filepath.Join(dir, "corrupt.json")
	if err := writeFile(cpath, []byte("{nope")); err != nil {
		t.Fatal(err)
	}
	cs := checkpoint.NewFileStore(cpath, "src:A")
	if _, err := cs.Load(); err == nil || err.Kind != cerror.KindCheckpointCorrupt {
		t.Fatalf("corrupt: got %v, want checkpoint_corrupt", err)
	}
}

func TestBadOrdinalsReportedOnceAndPruned(t *testing.T) {
	s := checkpoint.NewFileStore(filepath.Join(t.TempDir(), "c.json"), "src:A")
	if _, err := s.Load(); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(checkpoint.CommitRequest{
		Offset:      "6",
		Events:      []event.Event{{Key: "k1", Offset: "5"}},
		BadOrdinals: []int64{6},
	}); err != nil {
		t.Fatal(err)
	}
	if !s.HasBad(6) {
		t.Fatal("bad ordinal 6 not tracked")
	}
	// Advance past it: it must be pruned from the live index.
	if err := s.Commit(checkpoint.CommitRequest{
		Offset: "8", Events: []event.Event{{Key: "k2", Offset: "7"}, {Key: "k3", Offset: "8"}},
	}); err != nil {
		t.Fatal(err)
	}
	if s.HasBad(6) {
		t.Fatal("bad ordinal 6 should have been pruned")
	}
}

func TestValidateResumeVariants(t *testing.T) {
	if err := checkpoint.ValidateResume("A", "A", "5", "10"); err != nil {
		t.Fatalf("valid resume rejected: %v", err)
	}
	if err := checkpoint.ValidateResume("A", "B", "1", "10"); err == nil ||
		err.Kind != cerror.KindOffsetMismatch {
		t.Fatalf("tag: got %v", err)
	}
	if err := checkpoint.ValidateResume("A", "A", "11", "10"); err == nil ||
		err.Kind != cerror.KindOffsetOutOfRange {
		t.Fatalf("range: got %v", err)
	}
	if err := checkpoint.ValidateResume("A", "A", "xx", "10"); err == nil ||
		err.Kind != cerror.KindOffsetBadFormat {
		t.Fatalf("format: got %v", err)
	}
	// Empty resume always valid, even against end=0.
	if err := checkpoint.ValidateResume("A", "", "", "0"); err != nil {
		t.Fatalf("empty resume: %v", err)
	}
}
