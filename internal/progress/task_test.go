package progress

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestFinishScanRejectsEmptyBeforeDatabaseAccess(t *testing.T) {
	// No connection: empty input must be rejected without touching/finalizing DB.
	err := (&DB{}).FinishScan(context.Background(), 0)
	if !errors.Is(err, ErrEmptyScan) || !strings.Contains(err.Error(), "BSDB") || !strings.Contains(err.Error(), "--bucket-id") {
		t.Fatalf("empty scan should fail with actionable guidance: %v", err)
	}
}

func TestBindingDiff(t *testing.T) {
	stored := Binding{ChainID: "greenfield_9000-121", SPOperator: "0xA", Storage: "aws:us-east-1/sp-a",
		SpDB: "127.0.0.1:3306/sp_0", Bucket: "limewire", BucketID: 10}
	if d := stored.Diff(stored); len(d) != 0 {
		t.Fatalf("identical bindings differ: %v", d)
	}

	// sp1 pointed at sp0's database: operator, storage and SpDB all differ
	sp1 := stored
	sp1.SPOperator, sp1.Storage, sp1.SpDB = "0xB", "aws:us-east-1/sp-b", "127.0.0.1:3306/sp_1"
	d := stored.Diff(sp1)
	if len(d) != 3 {
		t.Fatalf("want 3 differences, got %v", d)
	}
	for i, field := range []string{"SP operator", "object storage", "SpDB"} {
		if !strings.HasPrefix(d[i], field) {
			t.Fatalf("difference %d = %q, want %s", i, d[i], field)
		}
	}

	other := stored
	other.BucketID = 42268
	if d := stored.Diff(other); len(d) != 1 || !strings.Contains(d[0], "42268") {
		t.Fatalf("bucket id change: %v", d)
	}
}
