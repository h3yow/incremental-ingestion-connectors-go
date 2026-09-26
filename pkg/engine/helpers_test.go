package engine_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func countEvtFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "evt-") {
			n++
		}
	}
	return n
}

var _ = filepath.Join
