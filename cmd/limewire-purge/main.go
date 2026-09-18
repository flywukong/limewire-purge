// limewire-purge: per-SP tool that removes one Greenfield bucket's pieces from the
// SP's physical object storage without touching chain state.
//
//	scan    pull the bucket's object ids out of the SP's bsdb into scan_objects
//	purge   process every unprocessed object (chain check → prefix delete → metadata)
//	verify  ignore progress, re-scan everything read-only, report residue
//	status  print counts
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/time/rate"

	"limewire-purge/internal/bsdb"
	"limewire-purge/internal/chain"
	"limewire-purge/internal/pieceop"
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
	default:
		usage()
	}
	if err != nil {
		log.Fatalf("%s: %v", os.Args[1], err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: limewire-purge <scan|purge|verify|status> [flags]")
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
	pg, err := progress.Open(c.progressDSN)
	if err != nil {
		return nil, err
	}
	return pg, pg.EnsureSchema(ctx)
}

// ---------------------------------------------------------------- scan

func runScan(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	var c common
	addCommon(fs, &c)
	batch := fs.Int("batch", 5000, "rows per bsdb query")
	countOnly := fs.Bool("count-only", false, "only print COUNT(*) from bsdb, write nothing")
	fs.Parse(args)

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

	pg, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer pg.Close()

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
		return err
	}
	cnt, err := pg.Counts(ctx)
	if err != nil {
		return err
	}
	log.Printf("scan done: %d rows read from bsdb, scan_objects now holds %d objects / %d bytes", total, cnt.Total, cnt.ScanBytes)
	return nil
}

// ---------------------------------------------------------------- purge

type chainAdapter struct{ c *chain.Client }

func (a chainAdapter) HeadObjectBucket(ctx context.Context, oid uint64) (string, error) {
	h, err := a.c.HeadObjectByID(ctx, oid)
	if err != nil {
		return "", err
	}
	return h.BucketName, nil
}

func runPurge(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("purge", flag.ExitOnError)
	var c common
	addCommon(fs, &c)
	chainGRPC := fs.String("chain-grpc", "greenfield-chain.bnbchain.org:443", "chain gRPC endpoint")
	chainID := fs.String("chain-id", "greenfield_1017-1", "chain id, e.g. greenfield_1017-1 (mainnet) or greenfield_9000-121 (local)")
	dryRun := fs.Bool("dry-run", false, "list only; delete nothing; write no progress")
	conc := fs.Int("concurrency", 8, "objects processed in parallel")
	qps := fs.Float64("qps", 50, "object-storage requests per second (list + delete)")
	maxRetry := fs.Int("max-retry", 5, "rounds with errors and no progress before an object is marked failed")
	retryFailed := fs.Bool("retry-failed", false, "re-process objects previously marked failed")
	fs.Parse(args)

	cfg, err := spconfig.Load(c.config)
	if err != nil {
		return err
	}
	store, err := s3store.New(ctx, cfg.PieceStore.Store)
	if err != nil {
		return err
	}
	log.Printf("physical bucket: %s (from BucketURL %s, IAMType %s)", store.Bucket, cfg.PieceStore.Store.BucketURL, cfg.PieceStore.Store.IAMType)

	ch, err := chain.New(*chainID, *chainGRPC)
	if err != nil {
		return err
	}
	id, err := ch.HeadBucketID(ctx, c.bucket)
	if err != nil {
		return err
	}
	if id != c.bucketID {
		return fmt.Errorf("chain says bucket %s has id %d, flag --bucket-id is %d; refusing to run", c.bucket, id, c.bucketID)
	}

	meta, err := spdb.Open(cfg.SpDB.DSN())
	if err != nil {
		return err
	}
	defer meta.Close()

	pg, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer pg.Close()
	cnt, err := pg.Counts(ctx)
	if err != nil {
		return err
	}
	if cnt.Total == 0 {
		return fmt.Errorf("scan_objects is empty; run scan first")
	}
	log.Printf("progress: total=%d done=%d failed=%d remaining=%d (dry-run=%v retry-failed=%v)",
		cnt.Total, cnt.Done, cnt.Failed, cnt.Remaining(), *dryRun, *retryFailed)

	r := &purge.Runner{
		Store: store, Chain: chainAdapter{ch}, Meta: meta, Progress: pg,
		Opt: purge.Options{Bucket: c.bucket, Concurrency: *conc, QPS: *qps, MaxRetry: *maxRetry, DryRun: *dryRun, RetryFailed: *retryFailed},
	}
	return r.Run(ctx, cnt.Total)
}

// ---------------------------------------------------------------- verify

func runVerify(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	var c common
	addCommon(fs, &c)
	out := fs.String("out", "./residue.tsv", "residue report (oid<TAB>where<TAB>count)")
	qps := fs.Float64("qps", 50, "object-storage requests per second")
	fs.Parse(args)

	cfg, err := spconfig.Load(c.config)
	if err != nil {
		return err
	}
	store, err := s3store.New(ctx, cfg.PieceStore.Store)
	if err != nil {
		return err
	}
	meta, err := spdb.Open(cfg.SpDB.DSN())
	if err != nil {
		return err
	}
	defer meta.Close()
	pg, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer pg.Close()

	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	lim := rate.NewLimiter(rate.Limit(*qps), 1)
	var checked, residue uint64
	var after uint64
	for {
		rows, err := pg.ScanIDs(ctx, after, 1000)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			after = row.ObjectID
			checked++
			for _, prefix := range pieceop.Prefixes(row.ObjectID) {
				var n int
				if err := store.ListAll(ctx, prefix, func(objs []s3store.Object) error { n += len(objs); return lim.Wait(ctx) }); err != nil {
					return fmt.Errorf("oid %d %s: %w", row.ObjectID, prefix, err)
				}
				if n > 0 {
					residue++
					fmt.Fprintf(w, "%d\t%s\t%d\n", row.ObjectID, prefix, n)
				}
			}
			exists, err := meta.Exists(ctx, row.ObjectID)
			if err != nil {
				return err
			}
			if exists {
				residue++
				fmt.Fprintf(w, "%d\tmetadata\t1\n", row.ObjectID)
			}
			if checked%1000 == 0 {
				log.Printf("verify: checked=%d residue=%d (last oid %d)", checked, residue, row.ObjectID)
			}
		}
	}
	log.Printf("verify done: checked=%d residue=%d → %s", checked, residue, *out)
	if residue > 0 {
		return fmt.Errorf("%d residue entries", residue)
	}
	return nil
}

// ---------------------------------------------------------------- status

func runStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	var c common
	addCommon(fs, &c)
	failures := fs.Int("failures", 20, "how many recent failures to print")
	fs.Parse(args)

	pg, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer pg.Close()
	cnt, err := pg.Counts(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("objects   total=%d done=%d failed=%d remaining=%d\n", cnt.Total, cnt.Done, cnt.Failed, cnt.Remaining())
	fmt.Printf("deleted   keys=%d bytes=%d\n", cnt.DeletedKeys, cnt.DeletedBytes)
	fmt.Printf("scan      Σ payload_size=%d bytes\n", cnt.ScanBytes)
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
	return nil
}
