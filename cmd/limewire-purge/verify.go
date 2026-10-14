package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"limewire-purge/internal/progress"
	"limewire-purge/internal/s3store"
	"limewire-purge/internal/spconfig"
	"limewire-purge/internal/spdb"
	verifier "limewire-purge/internal/verify"
)

func runVerify(ctx context.Context, args []string) (retErr error) {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	var c common
	addCommon(fs, &c)
	id := fs.Uint64("task-id", 0, "subtask id; default: oldest deletion-complete task awaiting verification (one task per invocation)")
	restart := fs.Bool("restart", false, "start a fresh verification generation; recheck every nondeferred OID")
	allowVersioned := fs.Bool("allow-versioned-bucket", false, "explicitly allow Enabled/Suspended buckets; a clean result does not prove historical versions or delete markers are removed")
	out := fs.String("out", "", "report path (default: residue-task-ID.tsv); overwritten atomically")
	qps := fs.Float64("qps", 50, "verification requests per second")
	retries := fs.Int("max-retry", 3, "additional attempts per failed check")
	timeout := fs.Duration("s3-timeout", 2*time.Minute, "HTTP timeout; also bounds one verification check")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *qps <= 0 || *retries < 0 || *timeout <= 0 {
		return fmt.Errorf("invalid QPS/retry/timeout")
	}
	cfg, err := spconfig.Load(c.config)
	if err != nil {
		return err
	}
	pg, ctx, err := c.openBound(ctx, cfg)
	if err != nil {
		return err
	}
	defer pg.Close()
	s, err := pg.ResolveVerifySubtask(ctx, *id)
	if err != nil {
		if errors.Is(err, progress.ErrNoVerifyTask) {
			return fmt.Errorf("没有待验收的任务 (no task awaiting verification); to recheck history use --task-id N --restart: %w", err)
		}
		return err
	}
	defer beginTaskLog("verify", s)()
	selectedID := s.ID
	defer func() { retErr = withTaskError(selectedID, retErr) }()
	if err := pg.CheckManifest(ctx, s); err != nil {
		return holdGlobal(ctx, pg, s, err)
	}
	store, err := s3store.New(ctx, cfg.PieceStore.Store, *timeout)
	if err != nil {
		return err
	}
	if err := store.EnsureVersioningDisabled(ctx, *allowVersioned); err != nil {
		return err
	}
	meta, err := spdb.Open(cfg.SpDB.DSN())
	if err != nil {
		return err
	}
	defer meta.Close()
	s, err = pg.BeginVerify(ctx, s, *restart)
	if err != nil {
		return err
	}
	if *out == "" {
		*out = fmt.Sprintf("residue-task-%d.tsv", s.ID)
	}
	if !s.DoneAt.Valid {
		r := verifier.Runner{Store: store, Meta: meta, Progress: pg, Task: s, Check: pg.CheckLock, QPS: *qps, MaxRetry: *retries, Timeout: *timeout}
		if err := r.Run(ctx); err != nil {
			return err
		}
	} else {
		log.Printf("task=%d already completed; exporting stored results only (use --restart to recheck)", s.ID)
	}
	rows, err := pg.VerifyReport(ctx, s)
	if err != nil {
		return err
	}
	if err := writeReport(*out, rows); err != nil {
		return err
	}
	if !s.DoneAt.Valid {
		if err := pg.CompleteVerify(ctx, s); err != nil {
			return err
		}
	}
	s, err = pg.Subtask(ctx, s.ID)
	if err != nil {
		return err
	}
	log.Printf("verification report: %s (DEFERRED is not PASS)", *out)
	return printSubtask(ctx, pg, s, false)
}

// A failed export never replaces the previous report with a partial file.
func writeReport(path string, rows []progress.ReportRow) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".purge-report-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	b := bufio.NewWriter(f)
	w := csv.NewWriter(b)
	w.Comma = '\t'
	if err := w.Write([]string{"oid", "result", "detail"}); err != nil {
		return err
	}
	for _, r := range rows {
		if err := w.Write([]string{fmt.Sprint(r.OID), r.Kind, r.Detail}); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	if err := b.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
