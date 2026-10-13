package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"

	"limewire-purge/internal/progress"
	"limewire-purge/internal/spconfig"
)

func holdGlobal(ctx context.Context, pg *progress.DB, s progress.Subtask, cause error) error {
	return errors.Join(cause, pg.Hold(ctx, s.ID, nil, cause.Error()))
}

// The CLI executes one command per process. Prefix its standard logger so all
// worker/SDK-adapter logs carry the selected task without changing each caller.
// Restore it for in-process tests; command errors separately retain their ID.
func beginTaskLog(command string, s progress.Subtask) func() {
	previous := log.Prefix()
	log.SetPrefix(previous + fmt.Sprintf("task=%d ", s.ID))
	log.Printf("command=%s objects=%d logical_bytes=%d (%.3f GiB) target_bytes=%d digest=%s", command, s.ObjectCount, s.LogicalBytes, float64(s.LogicalBytes)/(1<<30), s.TargetBytes, s.Digest)
	return func() { log.SetPrefix(previous) }
}

func withTaskError(id uint64, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("task=%d: %w", id, err)
}

func runTask(ctx context.Context, args []string) (retErr error) {
	if len(args) == 0 {
		return errors.New("usage: task <create|defer|exclude|resume> [flags]")
	}
	action := args[0]
	if action == "exclude" {
		action = "defer"
	} // same audited deferral; retry via a new task
	if action != "create" && action != "defer" && action != "resume" {
		return fmt.Errorf("unknown task action %q", action)
	}
	fs := flag.NewFlagSet("task "+action, flag.ContinueOnError)
	var c common
	addCommon(fs, &c)
	var id, oid uint64
	var reason, size string
	var max int
	var deferred bool
	switch action {
	case "create":
		fs.StringVar(&size, "target-size", "500GiB", "approximate logical payload size (B/KiB/MiB/GiB/TiB)")
		fs.IntVar(&max, "max-objects", 10000, "maximum OIDs in a task (1..100000)")
		fs.BoolVar(&deferred, "retry-deferred", false, "explicitly allocate from the deferred pool, retaining history")
	default:
		fs.Uint64Var(&id, "task-id", 0, "optional expectation: must match the current unfinished task")
		fs.StringVar(&reason, "reason", "", "required operator explanation, recorded in task_events")
		if action == "defer" {
			fs.Uint64Var(&oid, "oid", 0, "object to defer; not marked PASS")
		}
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
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
	var s progress.Subtask
	if action == "create" {
		n, err := progress.ParseCapacity(size)
		if err != nil {
			return err
		}
		s, err = pg.CreateSubtask(ctx, n, max, deferred)
		if err != nil {
			return err
		}
		defer beginTaskLog("task create", s)()
		defer func() { retErr = withTaskError(s.ID, retErr) }()
	} else {
		s, err = pg.ResolveCurrentSubtask(ctx, id)
		if err != nil {
			return err
		}
		defer beginTaskLog("task "+action, s)()
		selectedID := s.ID
		defer func() { retErr = withTaskError(selectedID, retErr) }()
		switch action {
		case "defer":
			if oid == 0 {
				return errors.New("--oid is required")
			}
			err = pg.DeferObject(ctx, s, oid, reason)
		case "resume":
			err = pg.ResumeSafety(ctx, s, reason)
		}
		if err != nil {
			return err
		}
		s, err = pg.Subtask(ctx, s.ID)
		if err != nil {
			return err
		}
	}
	return printSubtask(ctx, pg, s, false)
}

func printSubtask(ctx context.Context, pg *progress.DB, s progress.Subtask, locked bool) error {
	c, err := pg.SubCounts(ctx, s.ID)
	if err != nil {
		return err
	}
	fmt.Printf("task=%d state=%s objects=%d logical_bytes=%d processed=%d pending=%d failed=%d deferred=%d verify_pass=%d residue=%d verify_errors=%d generation=%d\n",
		s.ID, s.State(c, locked), s.ObjectCount, s.LogicalBytes, c.Processed, c.Pending, c.Failed, c.Deferred, c.Verified, c.Residue, c.VerifyErrors, s.Generation)
	if s.HoldReason != "" {
		fmt.Printf("  safety_hold oid=%s reason=%s\n", nullInt(s.HoldOID), s.HoldReason)
	}
	return nil
}
