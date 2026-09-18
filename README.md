# limewire-purge

Per-SP tool that removes one Greenfield bucket's pieces from the SP's physical
object storage (S3) without touching chain state. Design and runbook live in
`../report/limewire-s3-purge-plan.html` and `../report/limewire-purge-runbook.html`.

Run one instance per SP, inside that SP's pod (or anything carrying the same
ServiceAccount): the bucket, endpoint and credentials come from the SP's own TOML.

```
limewire-purge scan   --config sp.toml --progress-dsn 'u:p@tcp(host:3306)/limewire_purge'
limewire-purge purge  --config sp.toml --progress-dsn ... --dry-run          # list only
limewire-purge purge  --config sp.toml --progress-dsn ... --concurrency 8 --qps 50
limewire-purge purge  --config sp.toml --progress-dsn ... --retry-failed
limewire-purge verify --config sp.toml --progress-dsn ... --out residue.tsv
limewire-purge status --progress-dsn ...
```

| step | what happens |
|---|---|
| `scan` | reads `objects_NN` (NN = murmur3(bucket) % 64) from the SP's **bsdb** by primary-key batches and fills `scan_objects(object_id, payload_size, status, version)` |
| `purge` | for every object not yet in `purge_progress`: chain `head_object_by_id` must say the target bucket → list `s<oid>_` then `e<oid>_`, delete in batches of ≤1000 until the listing is empty → re-check both prefixes empty → `DELETE FROM integrity_meta_NN / piece_hash WHERE object_id=?` → write one row: status 1 (done) or 2 (failed) plus deleted key/byte counts |
| `verify` | ignores progress, re-lists both prefixes and re-checks metadata for every scanned object, writes residue |
| `status` | counts and recent failure reasons |

Nothing below the object is persisted: a failed batch leaves its keys in S3, and the
next attempt simply lists the prefix again. Counters accumulate across attempts.

Deliberately not reused from the SP code base: its `DeleteObjectsByPrefix` (keeps
appending to one identifier slice, loops forever past 1000 keys) and the
`math.MaxInt32` metadata branch.

```
go test ./...        # offline: prefix boundary, 2500-key batching, partial failure, resume, dry-run
go build ./cmd/limewire-purge
```
