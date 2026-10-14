// limewire-purge: per-SP tool that removes one Greenfield bucket's pieces from the
// SP's physical object storage without touching chain state.
//
//	scan    pull the bucket's object ids out of the SP's bsdb into scan_objects
//	task    allocate capacity-bounded subtasks; explicitly defer/resume
//	purge   process the selected subtask (chain check → prefix delete → metadata)
//	verify  independently check the subtask, persist verification checkpoints
//	status  print counts
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"limewire-purge/internal/bsdb"
	"limewire-purge/internal/chain"
	"limewire-purge/internal/progress"
	"limewire-purge/internal/purge"
	"limewire-purge/internal/s3store"
	"limewire-purge/internal/spconfig"
	"limewire-purge/internal/spdb"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime)
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "scan":
		err = runScan(ctx, os.Args[2:])
	case "purge":
		err = runPurge(ctx, os.Args[2:])
	case "verify":
		err = runVerify(ctx, os.Args[2:])
	case "status":
		err = runStatus(ctx, os.Args[2:])
	case "task":
		err = runTask(ctx, os.Args[2:])
	default:
		usage()
	}
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		log.Fatalf("%s: %v", os.Args[1], err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: limewire-purge <scan|task|purge|verify|status> [flags]")
	os.Exit(2)
}

type common struct {
	config, progressDSN, bucket string
	bucketID                    uint64
}

func addCommon(fs *flag.FlagSet, c *common) {
	fs.StringVar(&c.config, "config", "", "SP TOML (reads [PieceStore.Store], [SpDB], [BsDB])")
	fs.StringVar(&c.progressDSN, "progress-dsn", "", "MySQL DSN of the tool's own database, e.g. user:pass@tcp(host:3306)/limewire_purge")
	fs.StringVar(&c.bucket, "bucket", "limewire", "Greenfield bucket name")
	fs.Uint64Var(&c.bucketID, "bucket-id", 42268, "Greenfield bucket id (decimal)")
}

func (c *common) open(ctx context.Context) (*progress.DB, error) {
	if c.progressDSN == "" {
		return nil, fmt.Errorf("--progress-dsn is required")
	}
	return progress.Open(c.progressDSN)
}

func (c *common) openLocked(ctx context.Context, operation string) (*progress.DB, context.Context, error) {
	pg, err := c.open(ctx)
	if err != nil {
		return nil, ctx, err
	}
	l, err := pg.Acquire(ctx, operation)
	if err != nil {
		pg.Close()
		return nil, ctx, err
	}
	ctx = l.Context()
	if err := pg.EnsureSchema(ctx); err != nil {
		pg.Close()
		return nil, ctx, err
	}
	return pg, ctx, nil
}

// binding is the task identity this run works for, taken from the SP config and
// the bucket flags; the progress database must be bound to exactly this.
func (c *common) binding(cfg *spconfig.Config) (progress.Binding, error) {
	if cfg.SpAccount.SpOperatorAddress == "" {
		return progress.Binding{}, fmt.Errorf("%s: [SpAccount] SpOperatorAddress is empty", c.config)
	}
	t, err := spconfig.ParseBucketURL(cfg.PieceStore.Store.BucketURL)
	if err != nil {
		return progress.Binding{}, fmt.Errorf("BucketURL: %w", err)
	}
	return progress.Binding{
		ChainID:    cfg.Chain.ChainID,
		SPOperator: cfg.SpAccount.SpOperatorAddress,
		Storage:    t.Identity(),
		SpDB:       cfg.SpDB.Address + "/" + cfg.SpDB.Database,
		Bucket:     c.bucket,
		BucketID:   c.bucketID,
	}, nil
}

// openBound opens the progress database for purge/verify: it must belong to this
// task and hold a list from a scan that completed.
func (c *common) openBound(ctx context.Context, cfg *spconfig.Config) (*progress.DB, context.Context, error) {
	b, err := c.binding(cfg)
	if err != nil {
		return nil, ctx, err
	}
	pg, ctx, err := c.openLocked(ctx, "task operation")
	if err != nil {
		return nil, ctx, err
	}
	if err := pg.CheckBinding(ctx, b); err != nil {
		pg.Close()
		return nil, ctx, err
	}
	if err := pg.RequireCompleteScan(ctx); err != nil {
		pg.Close()
		return nil, ctx, err
	}
	return pg, ctx, nil
}

// ---------------------------------------------------------------- scan

func runScan(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	var c common
	addCommon(fs, &c)
	batch := fs.Int("batch", 5000, "rows per bsdb query")
	countOnly := fs.Bool("count-only", false, "only print COUNT(*) from bsdb, write nothing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}

	cfg, err := spconfig.Load(c.config)
	if err != nil {
		return err
	}
	sc, err := bsdb.Open(cfg.BsDB.DSN(), c.bucket)
	if err != nil {
		return err
	}
	defer sc.Close()
	log.Printf("bsdb table for bucket %s: %s", c.bucket, sc.Table())

	n, err := sc.Count(ctx, c.bucketID, c.bucket)
	if err != nil {
		return err
	}
	log.Printf("bsdb says bucket %s (id %d) has %d live objects", c.bucket, c.bucketID, n)
	if *countOnly {
		return nil
	}
	if n == 0 {
		return progress.ErrEmptyScan
	}

	b, err := c.binding(cfg)
	if err != nil {
		return err
	}
	pg, ctx, err := c.openLocked(ctx, "scan")
	if err != nil {
		return err
	}
	defer pg.Close()
	if err := pg.Bind(ctx, b); err != nil {
		return err
	}
	log.Printf("progress database bound to chain %s, SP %s, storage %s, SpDB %s, bucket %s (id %d)",
		b.ChainID, b.SPOperator, b.Storage, b.SpDB, b.Bucket, b.BucketID)
	// purge and verify refuse the list from here until this scan reconciles
	if err := pg.BeginScan(ctx, n); err != nil {
		return err
	}

	var written uint64
	total, err := sc.Scan(ctx, c.bucketID, c.bucket, *batch, func(rows []progress.ScanRow) error {
		if err := pg.InsertScan(ctx, rows); err != nil {
			return err
		}
		written += uint64(len(rows))
		log.Printf("scan: %d rows written (last oid %d)", written, rows[len(rows)-1].ObjectID)
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w; the list is marked incomplete, re-run scan", err)
	}
	after, err := sc.Count(ctx, c.bucketID, c.bucket)
	if err != nil {
		return fmt.Errorf("recount after scan: %w; the list is marked incomplete, re-run scan", err)
	}
	if err := bsdb.Reconcile(n, after, total); err != nil {
		return err
	}
	if err := pg.FinishScan(ctx, total); err != nil {
		return err
	}
	cnt, err := pg.Counts(ctx)
	if err != nil {
		return err
	}
	log.Printf("scan complete: %d rows read from bsdb = %d live objects; scan_objects holds %d objects / %d bytes", total, n, cnt.Total, cnt.ScanBytes)
	return nil
}

// ---------------------------------------------------------------- purge

type chainAdapter struct{ c *chain.Client }

func (a chainAdapter) HeadObject(ctx context.Context, oid uint64) (purge.ObjectHead, error) {
	h, err := a.c.HeadObjectByID(ctx, oid)
	if err != nil {
		return purge.ObjectHead{}, err
	}
	return purge.ObjectHead{ID: h.ID, Bucket: h.BucketName, Status: h.Status, Version: h.Version, PayloadSize: h.PayloadSize}, nil
}

// progressAdapter maps the purge package's Progress interface onto the progress DB.
type progressAdapter struct {
	db     *progress.DB
	taskID uint64
}

func (a progressAdapter) Claim(ctx context.Context, after uint64, limit int, retryFailed bool) ([]purge.Claimed, error) {
	if err := a.db.CheckLock(ctx); err != nil {
		return nil, err
	}
	rows, err := a.db.ClaimSubtask(ctx, a.taskID, after, limit)
	if err != nil {
		return nil, err
	}
	out := make([]purge.Claimed, len(rows))
	for i, r := range rows {
		out[i] = purge.Claimed{OID: r.ObjectID, PayloadSize: r.PayloadSize, Status: r.Status, Version: r.Version}
	}
	return out, nil
}

func (a progressAdapter) Done(ctx context.Context, oid uint64, keys, bytes uint64, check purge.SizeCheck) error {
	var expected *uint64
	if check.Known {
		e := check.Expected
		expected = &e
	}
	return a.db.DoneSubtask(ctx, a.taskID, oid, keys, bytes, check.Role, expected)
}

func (a progressAdapter) Fail(ctx context.Context, oid uint64, keys, bytes uint64, reason string) error {
	if err := a.db.CheckMember(ctx, a.taskID, oid); err != nil {
		return err
	}
	return a.db.Fail(ctx, oid, keys, bytes, reason)
}

func runPurge(ctx context.Context, args []string) (retErr error) {
	fs := flag.NewFlagSet("purge", flag.ContinueOnError)
	var c common
	addCommon(fs, &c)
	taskID := fs.Uint64("task-id", 0, "default: current deletion task; explicit ID may also repair a deletion-complete, unverified task")
	execute := fs.Bool("execute", false, "confirm real deletion within the selected task")
	chainRPC := fs.String("chain-rpc", "https://greenfield-chain.bnbchain.org:443", "chain CometBFT RPC URL (with scheme); local e.g. http://localhost:26657")
	chainID := fs.String("chain-id", "greenfield_1017-1", "chain id, e.g. greenfield_1017-1 (mainnet) or greenfield_9000-121 (local)")
	dryRun := fs.Bool("dry-run", false, "list only; delete nothing; write no progress")
	conc := fs.Int("concurrency", 8, "objects processed in parallel")
	qps := fs.Float64("qps", 50, "object-storage requests per second (list + delete)")
	maxRetry := fs.Int("max-retry", 10, "retries per piece (list errors counted separately); an object still failing stops this run, fix the cause and rerun")
	retryFailed := fs.Bool("retry-failed", false, "deprecated: current-task failed objects are now included automatically")
	allowVersioned := fs.Bool("allow-versioned-bucket", false, "explicitly allow Enabled/Suspended buckets; deletes current keys only, leaving historical versions and delete markers outside verification")
	dataChunks := fs.Uint("data-chunks", 0, "EC data chunks for the secondary size check; 0 = read current value from chain")
	segmentSize := fs.Uint64("segment-size", 0, "max segment size for the secondary size check; 0 = read current value from chain")
	chainTimeout := fs.Duration("chain-timeout", 30*time.Second, "HTTP timeout of one chain query; transient network errors and HTTP 429/5xx are tried up to 3 times")
	s3Timeout := fs.Duration("s3-timeout", 2*time.Minute, "timeout of one object-storage HTTP attempt, response body included; the AWS SDK retries a timed-out attempt")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if !*execute && !*dryRun {
		return fmt.Errorf("real deletion requires --execute; optional --dry-run previews only")
	}
	if *execute && *dryRun {
		return fmt.Errorf("choose --execute or --dry-run, not both")
	}
	if *conc <= 0 || *qps <= 0 || *maxRetry < 0 || *chainTimeout <= 0 || *s3Timeout <= 0 {
		return fmt.Errorf("invalid concurrency/QPS/retry/timeout")
	}

	cfg, err := spconfig.Load(c.config)
	if err != nil {
		return err
	}
	if *chainID != cfg.Chain.ChainID {
		return fmt.Errorf("--chain-id %s does not match the SP config's chain %s; refusing to run", *chainID, cfg.Chain.ChainID)
	}
	pg, ctx, err := c.openBound(ctx, cfg)
	if err != nil {
		return err
	}
	defer pg.Close()
	task, err := pg.ResolvePurgeSubtask(ctx, *taskID)
	if err != nil {
		return err
	}
	defer beginTaskLog("purge", task)()
	defer func() { retErr = withTaskError(task.ID, retErr) }()
	if task.DoneAt.Valid || task.HoldReason != "" {
		return fmt.Errorf("task %d is completed or on SAFETY_HOLD: %s", task.ID, task.HoldReason)
	}
	if err := pg.CheckManifest(ctx, task); err != nil {
		return holdGlobal(ctx, pg, task, err)
	}

	store, err := s3store.New(ctx, cfg.PieceStore.Store, *s3Timeout)
	if err != nil {
		return err
	}
	log.Printf("physical bucket: %s (from BucketURL %s, IAMType %s)", store.Bucket, cfg.PieceStore.Store.BucketURL, cfg.PieceStore.Store.IAMType)
	if err := store.EnsureVersioningDisabled(ctx, *allowVersioned); err != nil {
		return err
	}

	ch, err := chain.New(*chainID, *chainRPC, *chainTimeout)
	if err != nil {
		return err
	}
	id, err := ch.HeadBucketID(ctx, c.bucket)
	if err != nil {
		return err
	}
	if id != c.bucketID {
		return holdGlobal(ctx, pg, task, fmt.Errorf("chain says bucket %s has id %d, flag --bucket-id is %d; refusing to run", c.bucket, id, c.bucketID))
	}

	// EC layout for the size check. The chain value is the *current* one; objects
	// written under different historical params would show as mismatches, which is
	// a warning only. limewire's params never changed during its lifetime.
	dc, seg := uint32(*dataChunks), *segmentSize
	if dc == 0 || seg == 0 {
		cdc, cseg, perr := ch.RedundancyParams(ctx)
		if perr != nil {
			log.Printf("WARNING: cannot read EC params from chain (%v); secondary size check disabled unless --data-chunks/--segment-size are given", perr)
		}
		if dc == 0 {
			dc = cdc
		}
		if seg == 0 {
			seg = cseg
		}
	}
	log.Printf("size check: data-chunks=%d segment-size=%d (primary expects payload_size, secondary expects sum of ceil(segment/data-chunks))", dc, seg)

	meta, err := spdb.Open(cfg.SpDB.DSN())
	if err != nil {
		return err
	}
	defer meta.Close()

	cnt, err := pg.SubCounts(ctx, task.ID)
	if err != nil {
		return err
	}
	if cnt.Total == 0 {
		return fmt.Errorf("scan_objects is empty; run scan first")
	}
	log.Printf("task=%d total=%d processed=%d failed=%d pending=%d deferred=%d (dry-run=%v)",
		task.ID, cnt.Total, cnt.Processed, cnt.Failed, cnt.Pending, cnt.Deferred, *dryRun)
	if *retryFailed {
		log.Print("--retry-failed is no longer required: this task automatically includes failed objects")
	}
	if !*dryRun {
		if err := pg.BeginPurge(ctx, task); err != nil {
			return err
		}
	}

	r := &purge.Runner{
		Store: store, Chain: chainAdapter{ch}, Meta: meta, Progress: progressAdapter{pg, task.ID},
		Check:    pg.CheckLock,
		OnSafety: func(cctx context.Context, oid uint64, e error) error { return pg.Hold(cctx, task.ID, &oid, e.Error()) },
		Opt: purge.Options{Bucket: c.bucket, Concurrency: *conc, QPS: *qps, MaxRetry: *maxRetry, DryRun: *dryRun,
			DataChunks: dc, SegmentSize: seg},
	}
	if err := r.Run(ctx, cnt.Total-cnt.Deferred); err != nil {
		return err
	}
	if *dryRun {
		return nil
	}
	left, err := pg.SubCounts(ctx, task.ID)
	if err != nil {
		return err
	}
	if left.Pending+left.Failed > 0 {
		return fmt.Errorf("task=%d still has %d pending and %d failed objects; rerun purge", task.ID, left.Pending, left.Failed)
	}
	if err := pg.CompletePurge(ctx, task); err != nil {
		return err
	}
	log.Printf("task=%d deletion pass finished; next task may now be created; verification remains pending (verify --task-id %d)", task.ID, task.ID)
	return nil
}

// ---------------------------------------------------------------- status

func runStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	var c common
	addCommon(fs, &c)
	failures := fs.Int("failures", 20, "how many recent failures to print")
	taskID := fs.Uint64("task-id", 0, "show one task; default: all tasks and outstanding pool")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *failures < 0 {
		return fmt.Errorf("--failures must be nonnegative")
	}

	pg, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer pg.Close()
	if c.config != "" {
		cfg, err := spconfig.Load(c.config)
		if err != nil {
			return err
		}
		b, err := c.binding(cfg)
		if err != nil {
			return err
		}
		if err := pg.CheckBinding(ctx, b); err != nil {
			return err
		}
	}
	t, bound, err := pg.Task(ctx)
	if err != nil {
		return err
	}
	if bound {
		fmt.Printf("task      chain=%s sp=%s storage=%s spdb=%s bucket=%s id=%d\n", t.ChainID, t.SPOperator, t.Storage, t.SpDB, t.Bucket, t.BucketID)
		fmt.Printf("scan      status=%s bsdb_count=%s rows=%s finished=%s\n", t.ScanStatus,
			nullInt(t.ScanBsdbCount), nullInt(t.ScanRows), nullStr(t.ScanFinishedAt))
	} else {
		fmt.Println("task      not bound (no scan has run against this database)")
	}
	cnt, err := pg.Counts(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("objects   total=%d done=%d failed=%d remaining=%d\n", cnt.Total, cnt.Done, cnt.Failed, cnt.Remaining())
	fmt.Printf("deleted   keys=%d bytes=%d\n", cnt.DeletedKeys, cnt.DeletedBytes)
	fmt.Printf("scan      Σ payload_size=%d bytes\n", cnt.ScanBytes)
	fmt.Printf("size      checked=%d ok=%d mismatch=%d unchecked=%d (done objects: deleted_bytes vs size expected from chain payload)\n",
		cnt.SizeChecked, cnt.SizeChecked-cnt.SizeMismatch, cnt.SizeMismatch, cnt.Done-cnt.SizeChecked)
	if cnt.SizeMismatch > 0 {
		mm, err := pg.Mismatches(ctx, *failures)
		if err != nil {
			return err
		}
		fmt.Println("size mismatches (oid\trole\tdeleted\texpected):")
		for _, l := range mm {
			fmt.Println("  " + l)
		}
	}
	if cnt.Failed > 0 {
		fails, err := pg.Failures(ctx, *failures)
		if err != nil {
			return err
		}
		fmt.Println("recent failures (oid\treason):")
		for _, l := range fails {
			fmt.Println("  " + l)
		}
	}
	owner, err := pg.LockOwner(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("execution_lock connection=%s\n", nullInt(owner))
	tasks, err := pg.Subtasks(ctx)
	if err != nil {
		return err
	}
	var unfinished, completed, exceptions, deletionComplete, deletionPending, awaitingVerify int
	for _, s := range tasks {
		if s.PurgedAt.Valid {
			deletionComplete++
			if !s.DoneAt.Valid {
				awaitingVerify++
			}
		} else if !s.DoneAt.Valid {
			deletionPending++
		}
		if s.DoneAt.Valid {
			completed++
			c, err := pg.SubCounts(ctx, s.ID)
			if err != nil {
				return err
			}
			if c.Deferred > 0 {
				exceptions++
			}
		} else {
			unfinished++
		}
	}
	fmt.Printf("subtasks  completed=%d (with_exceptions=%d) unfinished=%d\n", completed, exceptions, unfinished)
	fmt.Printf("stages    deletion_completed=%d deletion_pending=%d verification_completed=%d verification_pending=%d\n", deletionComplete, deletionPending, completed, awaitingVerify)
	if *taskID != 0 {
		s, err := pg.Subtask(ctx, *taskID)
		if err != nil {
			return err
		}
		tasks = []progress.Subtask{s}
	}
	if *taskID == 0 && deletionPending == 0 {
		fmt.Println("current   none (no pending deletion task; verification may still be pending)")
	}
	for _, s := range tasks {
		if *taskID == 0 && s.DoneAt.Valid {
			continue
		}
		if !s.DoneAt.Valid && !s.PurgedAt.Valid {
			fmt.Printf("current   task=%d\n", s.ID)
		}
		if err := printSubtask(ctx, pg, s, s.LockedBy(owner)); err != nil {
			return err
		}
		rows, err := pg.VerifyReport(ctx, s)
		if err != nil {
			return err
		}
		for i, r := range rows {
			if i >= *failures {
				break
			}
			fmt.Printf("  oid=%d %s %s\n", r.OID, r.Kind, r.Detail)
		}
	}
	unassigned, deferred, err := pg.Outstanding(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("pool      unassigned=%d deferred_unresolved=%d\n", unassigned, deferred)
	if *taskID == 0 && bound && allDone(t, cnt.Total, completed, unassigned, deferred, unfinished, owner.Valid) {
		fmt.Println("ALL_DONE: every assigned object verified; no unassigned or deferred objects")
	}
	if bound && t.ScanStatus == progress.ScanComplete && (!t.ScanRows.Valid || t.ScanRows.Int64 <= 0 || cnt.Total == 0) {
		fmt.Println("WARNING: empty or invalid sealed scan; not ALL_DONE; check bucket ID and BSDB synchronization before creating a new scan database")
	}
	return nil
}

// Old progress databases may contain an empty COMPLETE scan. Neither that nor
// a manifest without any completed subtask is evidence of completed cleanup.
func completedNonEmptyScan(t progress.Task, completed int) bool {
	return t.ScanStatus == progress.ScanComplete && t.ScanRows.Valid && t.ScanRows.Int64 > 0 && completed > 0
}

func allDone(t progress.Task, total uint64, verified int, unassigned, deferred uint64, unfinished int, locked bool) bool {
	return completedNonEmptyScan(t, verified) && total > 0 && unassigned == 0 && deferred == 0 && unfinished == 0 && !locked
}

func nullInt(v sql.NullInt64) string {
	if !v.Valid {
		return "-"
	}
	return fmt.Sprint(v.Int64)
}

func nullStr(v sql.NullString) string {
	if !v.Valid {
		return "-"
	}
	return v.String
}
