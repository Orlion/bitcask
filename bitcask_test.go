package bitcask

import "testing"

func TestNew(t *testing.T) {
	b := New()
	if b == nil {
		t.Fatalf("b is nil")
	}
}
