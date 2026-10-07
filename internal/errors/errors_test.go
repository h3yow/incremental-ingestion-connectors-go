package errors

import (
	"fmt"
	"testing"
)

func TestClassOf(t *testing.T) {
	err := New(ClassRecordInvalid, "op", "msg")
	if ClassOf(err) != ClassRecordInvalid {
		t.Fatalf("got %s", ClassOf(err))
	}
	if ClassOf(fmt.Errorf("boom")) != ClassInternal {
		t.Fatal("non-framework error must be internal")
	}
	wrapped := fmt.Errorf("outer: %w", err)
	if !Is(wrapped, ClassRecordInvalid) {
		t.Fatal("Is must see through wrapping")
	}
}

func TestErrorMessage(t *testing.T) {
	e := Wrap(ClassSinkFailure, "op", fmt.Errorf("disk full"))
	if got := e.Error(); got == "" {
		t.Fatal("empty message")
	}
}
