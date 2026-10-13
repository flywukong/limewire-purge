package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"limewire-purge/internal/progress"
)

func TestCompletionRequiresNonemptyScanAndCompletedTask(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    string
		rows      sql.NullInt64
		completed int
		want      bool
	}{
		{"normal", progress.ScanComplete, sql.NullInt64{Int64: 3, Valid: true}, 1, true},
		{"legacy empty", progress.ScanComplete, sql.NullInt64{Valid: true}, 0, false},
		{"empty with task", progress.ScanComplete, sql.NullInt64{Valid: true}, 1, false},
		{"missing scan count", progress.ScanComplete, sql.NullInt64{}, 1, false},
		{"no task", progress.ScanComplete, sql.NullInt64{Int64: 3, Valid: true}, 0, false},
		{"incomplete scan", progress.ScanRunning, sql.NullInt64{Int64: 3, Valid: true}, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := completedNonEmptyScan(progress.Task{ScanStatus: tc.status, ScanRows: tc.rows}, tc.completed)
			if got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestTaskLogIncludesSummaryAndWorkerContext(t *testing.T) {
	var out bytes.Buffer
	writer, flags, prefix := log.Writer(), log.Flags(), log.Prefix()
	t.Cleanup(func() { log.SetOutput(writer); log.SetFlags(flags); log.SetPrefix(prefix) })
	log.SetOutput(&out)
	log.SetFlags(0)
	log.SetPrefix("test ")
	end := beginTaskLog("purge", progress.Subtask{ID: 7, ObjectCount: 23, LogicalBytes: 1 << 30})
	log.Printf("oid=42 deletion failed")
	end()
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "task=7 command=purge objects=23 logical_bytes=1073741824") || !strings.Contains(lines[1], "task=7 oid=42") {
		t.Fatal(out.String())
	}
	if log.Prefix() != "test " {
		t.Fatal("prefix not restored")
	}
}

func TestTaskErrorRetainsIdentityAndCause(t *testing.T) {
	cause := errors.New("request failed")
	err := withTaskError(7, cause)
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "task=7") {
		t.Fatal(err)
	}
	if withTaskError(7, nil) != nil {
		t.Fatal("success became an error")
	}
}

func TestPurgeRequiresExplicitExecute(t *testing.T) {
	err := runPurge(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "--execute") {
		t.Fatal(err)
	}
}

func TestCommandsAcceptVersioningOptIn(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.toml")
	for _, run := range []struct {
		name string
		fn   func(context.Context, []string) error
		args []string
	}{
		{"purge", runPurge, []string{"--execute"}},
		{"verify", runVerify, nil},
	} {
		t.Run(run.name, func(t *testing.T) {
			args := append(run.args, "--allow-versioned-bucket", "--config", missing)
			err := run.fn(context.Background(), args)
			// Reach config loading without opening any real services. A removed or
			// rejected opt-in flag must fail this regression test.
			if err == nil || !strings.Contains(err.Error(), missing) {
				t.Fatalf("did not reach config loading: %v", err)
			}
		})
	}
}

func TestReportPreservesDeferredAndEscapesDetails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.tsv")
	if err := writeReport(path, []progress.ReportRow{{OID: 3, Kind: "DEFERRED", Detail: "operator reason\nsecond line"}, {OID: 4, Kind: "ERROR", Detail: "denied"}}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "3\tDEFERRED\t\"operator reason\nsecond line\"") || !strings.Contains(string(b), "4\tERROR\tdenied") {
		t.Fatal(string(b))
	}
}
