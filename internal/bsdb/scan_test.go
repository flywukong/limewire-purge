package bsdb

import (
	"strings"
	"testing"
)

func TestReconcile(t *testing.T) {
	if err := Reconcile(1125, 1125, 1125); err != nil {
		t.Fatalf("complete scan rejected: %v", err)
	}
	// a read that stopped early (e.g. connection lost mid-batch)
	if err := Reconcile(1125, 1125, 1); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("short read accepted: %v", err)
	}
	// objects added or removed while scanning
	if err := Reconcile(1125, 1126, 1125); err == nil || !strings.Contains(err.Error(), "not stable") {
		t.Fatalf("moving bucket accepted: %v", err)
	}
}

func TestTableFor(t *testing.T) {
	// mainnet limewire lives in objects_43
	if got := TableFor("limewire"); got != "objects_43" {
		t.Fatalf("TableFor(limewire) = %s", got)
	}
}
