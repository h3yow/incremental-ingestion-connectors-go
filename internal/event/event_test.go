package event

import (
	"testing"

	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
)

func TestNormalizeOK(t *testing.T) {
	ev, err := Normalize("src", 7, Record{
		"key":     "k1",
		"kind":    "upsert",
		"payload": map[string]any{"v": 1},
		"ts":      "2026-09-26T10:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.Key != "k1" || ev.Kind != "upsert" || ev.Seq != 7 || ev.Source != "src" {
		t.Fatalf("bad event: %+v", ev)
	}
	if ev.Payload["v"] != 1 {
		t.Fatal("payload lost")
	}
	if ev.Time.IsZero() {
		t.Fatal("ts not parsed")
	}
	if ev.ID() != "src/7/k1" {
		t.Fatalf("bad id %q", ev.ID())
	}
}

func TestNormalizeMissingField(t *testing.T) {
	_, err := Normalize("src", 1, Record{"kind": "upsert"})
	if !ierr.Is(err, ierr.ClassMissingField) {
		t.Fatalf("want missing_field, got %v", err)
	}
	_, err = Normalize("src", 1, Record{"key": "k"})
	if !ierr.Is(err, ierr.ClassMissingField) {
		t.Fatalf("want missing_field, got %v", err)
	}
}

func TestNormalizeTypeMismatch(t *testing.T) {
	_, err := Normalize("src", 1, Record{"key": 123, "kind": "upsert"})
	if !ierr.Is(err, ierr.ClassTypeMismatch) {
		t.Fatalf("want type_mismatch, got %v", err)
	}
	_, err = Normalize("src", 1, Record{"key": "k", "kind": "u", "payload": "not-a-map"})
	if !ierr.Is(err, ierr.ClassTypeMismatch) {
		t.Fatalf("want type_mismatch, got %v", err)
	}
	_, err = Normalize("src", 1, Record{"key": "k", "kind": "u", "ts": "not-a-time"})
	if !ierr.Is(err, ierr.ClassTypeMismatch) {
		t.Fatalf("want type_mismatch, got %v", err)
	}
}
