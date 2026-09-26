package csv_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/h3yow/incremental-ingestion-connectors-go/connectors/csv"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/cerror"
	"github.com/h3yow/incremental-ingestion-connectors-go/pkg/connector"
)

func schema() (string, []connector.Field) {
	return "id", []connector.Field{
		{Name: "name", Type: connector.TypeString, Required: true},
		{Name: "qty", Type: connector.TypeInt64, Required: true},
	}
}

func writeCSV(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "in.csv")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCSVNormalizeAndClassify(t *testing.T) {
	path := writeCSV(t,
		"id,name,qty\n"+
			"k1,a,10\n"+
			"k2,,10\n"+ // missing required name
			"k3,c,nope\n"+ // qty not int
			",no-key,1\n"+ // empty key
			"k5,e,50\n")
	c := csv.New(schema())
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
		t.Fatalf("events=%d want 2: %+v", len(res.Events), res.Events)
	}
	want := []cerror.Kind{
		cerror.KindMissingField, cerror.KindTypeMismatch, cerror.KindMissingField,
	}
	if len(res.Bad) != 3 {
		t.Fatalf("bad=%d want 3", len(res.Bad))
	}
	for i, w := range want {
		if res.Bad[i].Err.Kind != w {
			t.Fatalf("bad[%d]=%s want %s", i, res.Bad[i].Err.Kind, w)
		}
	}
	if !res.EOF || res.EndOrdinal != 5 {
		t.Fatalf("eof=%v end=%d", res.EOF, res.EndOrdinal)
	}
}

func TestCSVResume(t *testing.T) {
	path := writeCSV(t, "id,name,qty\nk1,a,1\nk2,b,2\nk3,c,3\n")
	c := csv.New(schema())
	r, _ := c.Open(context.Background(), connector.Config{connector.CfgPathCommon: path})
	defer r.Close()
	res, err := r.Read(context.Background(), "2", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Events) != 1 || res.Events[0].Key != "k3" {
		t.Fatalf("got %+v", res.Events)
	}
}

func TestCSVMissingHeaderColumnRejected(t *testing.T) {
	path := writeCSV(t, "other,name\nk1,a\n")
	c := csv.New(schema())
	r, err := c.Open(context.Background(), connector.Config{connector.CfgPathCommon: path})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, err = r.Read(context.Background(), "", 10)
	if err == nil || err.Kind != cerror.KindInvalidConfig {
		t.Fatalf("missing key column: got %v, want invalid_config", err)
	}
}

func TestTSVDelimiter(t *testing.T) {
	path := writeCSV(t, "id\tname\tqty\nk1\ta\t1\nk2\tb\t2\n")
	c := csv.New(schema())
	r, err := c.Open(context.Background(), connector.Config{
		connector.CfgPathCommon: path,
		csv.CfgDelimiter:        "\t",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	res, err := r.Read(context.Background(), "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Events) != 2 {
		t.Fatalf("events=%d want 2", len(res.Events))
	}
}
