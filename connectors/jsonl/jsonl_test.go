package jsonl_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/h3yow/incremental-ingestion-connectors-go/connectors/jsonl"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/connector"
)

func testSchema() (string, []connector.Field) {
	return "id", []connector.Field{
		{Name: "name", Type: connector.TypeString, Required: true},
		{Name: "qty", Type: connector.TypeInt64, Required: true},
	}
}

func writeJSONL(t *testing.T, lines ...string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "in.jsonl")
	data := ""
	for _, l := range lines {
		data += l + "\n"
	}
	if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestJSONLReadNormalizesAndClassifies(t *testing.T) {
	path := writeJSONL(t,
		`{"id":"k1","name":"a","qty":1}`,
		`{"id":"k2","qty":2}`,              // missing name
		`{"id":"k3","name":"c","qty":"x"}`, // type mismatch
		`{bad`,                             // malformed
		`{"id":"k4","name":"d","qty":4}`,
	)
	c := jsonl.New(testSchema())
	r, err := c.Open(context.Background(), connector.Config{connector.CfgPathCommon: path})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	res, err := r.Read(context.Background(), "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Events) != 2 {
		t.Fatalf("events = %d, want 2", len(res.Events))
	}
	if res.EndOrdinal != 5 || !res.EOF {
		t.Fatalf("end=%d eof=%v, want 5/true", res.EndOrdinal, res.EOF)
	}
	wantKinds := []cerror.Kind{
		cerror.KindMissingField, cerror.KindTypeMismatch, cerror.KindMalformedRecord,
	}
	if len(res.Bad) != 3 {
		t.Fatalf("bad = %d, want 3", len(res.Bad))
	}
	for i, want := range wantKinds {
		if res.Bad[i].Err.Kind != want {
			t.Fatalf("bad[%d] kind=%s want %s", i, res.Bad[i].Err.Kind, want)
		}
	}
}

func TestJSONLResumeOffset(t *testing.T) {
	path := writeJSONL(t,
		`{"id":"k1","name":"a","qty":1}`,
		`{"id":"k2","name":"b","qty":2}`,
		`{"id":"k3","name":"c","qty":3}`,
	)
	c := jsonl.New(testSchema())
	r, _ := c.Open(context.Background(), connector.Config{connector.CfgPathCommon: path})
	defer r.Close()

	res, err := r.Read(context.Background(), "2", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Events) != 1 || res.Events[0].Key != "k3" {
		t.Fatalf("resume after 2 should only return k3, got %+v", res.Events)
	}

	// Bad offset format rejected at the connector boundary.
	if _, err := r.Read(context.Background(), "not-a-number", 10); err == nil ||
		err.Kind != cerror.KindOffsetBadFormat {
		t.Fatalf("bad offset: got %v", err)
	}
}

func TestJSONLDoubleOpenAndDoubleCloseRejected(t *testing.T) {
	path := writeJSONL(t, `{"id":"k1","name":"a","qty":1}`)
	c := jsonl.New(testSchema())
	r, err := c.Open(context.Background(), connector.Config{connector.CfgPathCommon: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open(context.Background(), connector.Config{connector.CfgPathCommon: path}); err == nil ||
		err.Kind != cerror.KindAlreadyInitialized {
		t.Fatalf("second open: got %v, want already_initialized", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err == nil || err.Kind != cerror.KindNotInitialized {
		t.Fatalf("double close: got %v, want not_initialized", err)
	}
	// After close, reopen is allowed again.
	if _, err := c.Open(context.Background(), connector.Config{connector.CfgPathCommon: path}); err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
}

func TestJSONLOpenMissingConfigAndFileWrapped(t *testing.T) {
	c := jsonl.New(testSchema())
	if _, err := c.Open(context.Background(), connector.Config{}); err == nil ||
		err.Kind != cerror.KindInvalidConfig {
		t.Fatalf("missing config: got %v", err)
	}
	// OS-level file error must be wrapped, not leaked.
	_, err := c.Open(context.Background(), connector.Config{connector.CfgPathCommon: "/no/such/file.jsonl"})
	if err == nil || err.Class != cerror.ClassConnector {
		t.Fatalf("missing file: got %v", err)
	}
}
