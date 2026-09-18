# limewire-purge

按 SP 部署的工具，把某个 Greenfield 桶的 piece 从该 SP 的底层对象存储（S3）里删掉，
不改动任何链上状态。方案与操作细节见
`../report/limewire-s3-purge-plan.html`（行为与原理）和
`../report/limewire-purge-runbook.html`（命令与配置）；本地测试步骤见 `TESTING.md`。

每个 SP 跑一份，运行在该 SP 的 pod 内（或挂同一 ServiceAccount 的环境里）：
桶名、endpoint、凭证都来自该 SP 自己的 TOML。

```
limewire-purge scan   --config sp.toml --progress-dsn 'u:p@tcp(host:3306)/limewire_purge'
limewire-purge purge  --config sp.toml --progress-dsn ... --dry-run          # 只列举
limewire-purge purge  --config sp.toml --progress-dsn ... --concurrency 8 --qps 50
limewire-purge purge  --config sp.toml --progress-dsn ... --retry-failed
limewire-purge verify --config sp.toml --progress-dsn ... --out residue.tsv
limewire-purge status --progress-dsn ...
```

| 子命令 | 做什么 |
|---|---|
| `scan` | 从该 SP 的 **bsdb** 读 `objects_NN`（NN = murmur3(桶名) % 64），按主键分批捞出，写入 `scan_objects(object_id, payload_size, status, version)` |
| `purge` | 对每个还不在 `purge_progress` 里的对象：先查链 `head_object_by_id` 确认属于目标桶 → 列举 `s<oid>_` 与 `e<oid>_`，每批 ≤1000 删除，直到列举为空 → 复查两个前缀确实为空 → `DELETE FROM integrity_meta_NN / piece_hash WHERE object_id=?` → 写一行进度：status 1（完成）或 2（失败），附删除的 key 数与字节数 |
| `verify` | 忽略进度，对 `scan_objects` 里每个对象重新只读列举两个前缀、复查元数据，输出残留 |
| `status` | 打印计数与最近的失败原因 |

对象层级以下不落任何状态：某一批删除失败，失败的 key 仍在 S3 里，下一次直接重新列举
前缀即可，不需要记到 piece 级。删除量在多次尝试间累加。

有意不复用 SP 仓库里的两处实现：它的 `DeleteObjectsByPrefix`（key 列表声明在循环外、
只追加不清空，超过 1000 个 key 会死循环），以及元数据删除的 `math.MaxInt32` 分支。

```
go test ./...        # 离线单测：前缀边界 / 2500 key 分批 / 部分失败 / 续跑 / dry-run
go build ./cmd/limewire-purge
```
