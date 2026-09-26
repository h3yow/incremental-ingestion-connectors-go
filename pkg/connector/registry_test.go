package connector_test

import (
	"context"
	"strings"
	"testing"

	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/connector"
)

type stubConnector struct{ kind string }

func (s *stubConnector) Kind() string { return s.kind }
func (s *stubConnector) Schema() connector.Schema {
	return connector.Schema{}
}
func (s *stubConnector) Open(context.Context, connector.Config) (connector.SourceReader, *cerror.Error) {
	return nil, nil
}

func TestRegisterDuplicateAndNotFound(t *testing.T) {
	r := connector.NewRegistry()
	if err := r.Register(&stubConnector{kind: "a"}); err != nil {
		t.Fatal(err)
	}
	err := r.Register(&stubConnector{kind: "a"})
	if err == nil || err.Kind != cerror.KindDuplicateRegister {
		t.Fatalf("duplicate register: got %v, want %s", err, cerror.KindDuplicateRegister)
	}
	if _, err := r.Get("missing"); err == nil || err.Kind != cerror.KindConnectorNotFound {
		t.Fatalf("get missing: got %v", err)
	}
}

func TestNormalizeClassifiesFieldErrors(t *testing.T) {
	sch := connector.Schema{
		KeyField: "id",
		Fields: []connector.Field{
			{Name: "name", Type: connector.TypeString, Required: true},
			{Name: "qty", Type: connector.TypeInt64, Required: true},
			{Name: "active", Type: connector.TypeBool, Required: false},
		},
	}
	cases := []struct {
		name string
		vals map[string]any
		want cerror.Kind
	}{
		{"missing key", map[string]any{"name": "n", "qty": int64(1)}, cerror.KindMissingField},
		{"empty key", map[string]any{"id": "", "name": "n", "qty": int64(1)}, cerror.KindMissingField},
		{"missing required", map[string]any{"id": "x", "qty": int64(1)}, cerror.KindMissingField},
		{"string as int", map[string]any{"id": "x", "name": "n", "qty": "oops"}, cerror.KindTypeMismatch},
		{"fractional int", map[string]any{"id": "x", "name": "n", "qty": 1.5}, cerror.KindTypeMismatch},
		{"string as bool", map[string]any{"id": "x", "name": "n", "qty": 1, "active": "yes"}, cerror.KindTypeMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := connector.Normalize(sch, "src", "1", connector.RawRecord{Values: tc.vals})
			if err == nil {
				t.Fatal("expected rejection, got nil")
			}
			if err.Class != cerror.ClassRecord {
				t.Fatalf("class = %s, want record_error", err.Class)
			}
			if err.Kind != tc.want {
				t.Fatalf("kind = %s, want %s (msg: %s)", err.Kind, tc.want, err.Msg)
			}
		})
	}

	// Happy path: integral float64 accepted as int64.
	ev, err := connector.Normalize(sch, "src", "7", connector.RawRecord{Values: map[string]any{
		"id": "k", "name": "n", "qty": float64(42), "active": true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if ev.Key != "k" || ev.Offset != "7" || ev.Payload["qty"] != int64(42) || ev.Payload["active"] != true {
		t.Fatalf("unexpected event: %+v", ev)
	}
}

func TestErrorDoesNotLeakUnderlyingTypeAtBoundary(t *testing.T) {
	// External callers can only branch on the stable Class/Kind taxonomy;
	// the driver cause is wrapped, not returned as an unwrapped error.
	r := connector.NewRegistry()
	_, err := r.Open(context.Background(), "nope", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), string(cerror.KindConnectorNotFound)) {
		t.Fatalf("error string should carry the stable kind, got: %v", err)
	}
	if ce, ok := cerror.As(err); !ok || ce.Class != cerror.ClassConnector {
		t.Fatalf("error not classified: %v", err)
	}
}
