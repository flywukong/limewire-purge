# limewire-purge

按 SP 部署的物理清理工具：先全量扫描指定 Greenfield bucket 的 OID，封存到工具 MySQL；再按逻辑容量分配子任务，逐对象查链、批删 piece、清理本地元数据，最后独立验收该子任务。

**不可逆物理删除，不是链上 DeleteObject / DiscontinueObject。** 不发送交易、不停止计费、不自动停用 challenge。仅用于已授权废弃 bucket；链上仍可能是 SEALED，但物理文件已无法读取。

## 工作流程

```text
scan 全量落库 → 数量对账 → COMPLETE 封存
    ↓
task create：按约 500 GiB（可配置）固定本批 OID
    ↓
purge --execute：仅当前任务，逐 OID 查链 → 批删 → 清本地元数据 → 记进度
    ↓
verify：独立检查当前任务两个前缀 + 本地元数据，逐 OID 保存验收结果
    ├─ ERROR：重跑 verify，跳过本代次已 PASS 的对象
    ├─ RESIDUE：重跑 purge 清残留，再 verify 整批
    └─ 全部 PASS：DONE，可以 task create 下一批

处理不了的 OID → 人工 task defer --oid … --reason …
    → 不计 PASS，保留在延期池
    → 其余对象验收通过：DONE_WITH_EXCEPTIONS，可继续下一批
    → 后续 task create --retry-deferred 显式重试延期池
```

无需人工记录 task ID：`purge / verify / task defer / task resume` 默认选择唯一未完成任务。重复 `task create` 返回现有未完成任务，不会另开一批。没有当前任务时，`purge --execute` 报错并提示先运行 `task create`，绝不自动创建。

正式删除脚本建议带 `--task-id N`，固定为人工检查过的任务 ID：对 `purge / task defer / task resume`，这个参数必须等于当前任务 ID，否则拒绝执行，而不是切换到另一批。`task exclude` 是 `task defer` 的别名，同样记原因、留在延期池、不算 PASS。

`task create / purge / verify` 选定任务后的首条日志打印 `task=N`、对象数和逻辑容量，后续处理日志也带任务 ID。默认 `verify` 在没有待验收任务时明确报错；历史查询用 `status --task-id N`，历史重新验收用 `verify --task-id N --restart`（不得有其他未完成任务）。

本版本不自动连续执行。试运行后可把任务调大，例如 `--target-size 5TiB --max-objects 100000`（仍可能先达到对象数上限）；以后可用外部脚本串联，但必须检查验收结果、安全暂停和延期项，不能只写无限循环。

## 操作命令

**回到之前的 QA / SP 测试环境**：先按 [现有环境回归步骤](TESTING.md#现有-qa--sp-环境回归优先走这里) 更新工具，在同一测试环境用新的一轮独立进度库和小任务验证。不要照搬后面的本地 MinIO 初始化步骤重置现有集群；测试 RPC、chain ID、bucket ID 必须来自该环境。若物理桶为 Suspended/Enabled 且接受历史版本不在清理范围内，purge 和 verify 都要加 `--allow-versioned-bucket`。

下面仅为示例，不会自动执行。替换所有测试参数，并检查创建任务时打印的范围及容量。

```bash
# Bash / Zsh；DSN 用现有安全机制注入，不在仓库中保存密码。
: "${PURGE_PROGRESS_DSN:?请设置本 SP 的独立进度库 DSN}"
purge_common=(
  --config /path/to/sp0/config.toml
  --progress-dsn "$PURGE_PROGRESS_DSN"
  --bucket purge-test
  --bucket-id 123
)
purge_chain=(
  --chain-rpc http://127.0.0.1:26657
  --chain-id greenfield_9000-121
)

# 每个 SP 全量扫描一次，成功后名单封存。
./limewire-purge scan "${purge_common[@]}" --batch 5000

# 先小规模真实试运行；create 不会删除任何业务数据。
./limewire-purge task create "${purge_common[@]}" --target-size 1GiB --max-objects 100
./limewire-purge status "${purge_common[@]}"
./limewire-purge purge "${purge_common[@]}" "${purge_chain[@]}" --execute --concurrency 1 --qps 5
./limewire-purge verify "${purge_common[@]}" --qps 5

# 本批完成后再选下一批；容量指逻辑 payload，不是本 SP 释放空间。
./limewire-purge task create "${purge_common[@]}" --target-size 500GiB
./limewire-purge purge "${purge_common[@]}" "${purge_chain[@]}" --execute --concurrency 2 --qps 10
./limewire-purge verify "${purge_common[@]}" --qps 10
./limewire-purge status "${purge_common[@]}"
```

`purge` 必须提供 `--execute` 才真实删除；可选 `--dry-run` 仅预览当前任务待处理项，发现异常非零退出，但继续列出其他对象的问题。dry-run 不写对象处理结果，不是正式删除的前置条件；仍会取得工具锁并检查/初始化进度表。

### 失败和中断恢复

| 情况 | 操作 |
|---|---|
| 删除中断、网络错误、piece 重试耗尽 | 原 `purge … --execute` 重跑；自动领取当前任务未处理及失败项 |
| 验收中断或 ERROR | 原 `verify …` 重跑，续验本代次未 PASS 的 OID |
| 验收发现 RESIDUE | 原 `purge … --execute` 重清理，再 `verify …`；旧验收代次整体失效 |
| SAFETY_HOLD | 查清原因后 `task resume … --reason '处理依据'`，再运行；不能绕过身份/名单/key 校验 |
| 个别对象长期无法处理 | `task defer … --oid 123 --reason '原因与后续安排'`，继续处理其余对象，再 verify |
| 后续处理延期项 | 当前任务完成后 `task create … --retry-deferred --target-size 500GiB` |
| 重新完整验收历史任务 | 无其他未完成任务时 `verify … --task-id N --restart` |

普通失败不用 `task resume`；旧 `--retry-failed` 参数仅兼容，已不再改变领取范围。
`task defer` 只改变工具调度，不删数据、不更新冻结快照、不计 PASS；记录原因、时间并追加 `task_events`。
某 OID 引发的安全暂停可通过明确延期该 OID 解除；全局或其他 OID 的安全暂停不能这样绕过。
若冻结快照本身已改变，resume 仍会再次拦截；应先延期留账，再单独审核新的清理范围，不能手改库来“通过”校验。

已完成任务禁止再次 purge。默认 verify 没有未完成任务时会提示指定历史 ID；显式查看已完成任务不会重跑检查，仅导出既有报告，重新检查必须加 `--restart`。

## 命令和数据边界

| 命令 | BSDB | 链 | S3 | SpDB | 工具进度库 |
|---|---|---|---|---|---|
| scan | 读全量存活对象 | — | — | — | 写名单、绑定、封存 |
| task create/defer/resume | — | — | — | — | 分配任务 / 延期 / 解除暂停 |
| purge | — | 查 bucket ID 和逐对象身份 | 列举、批删、复查 | 按 OID 删除、复查 | 读任务名单，写逐对象结果 |
| verify | — | — | 只读检查两前缀 | 只读检查 | 保存验收代次和断点，完成任务 |
| status | — | — | — | — | 只读汇总，不建表、不拿锁 |

一个 OID 对应一个 GNFD 链上 object，不是一个 S3 key。工具进度库是 `--progress-dsn` 指定的 MySQL，不是自动创建的本地 SQLite；同一个 SP 的各子任务共用该库的不同任务记录，**不需要每批一个物理子库**。

### scan 全量导入

读取本 SP BSDB 的 `objects_NN`，分表由应用层 `murmur3(bucketName) % 64` 算出，不是 MySQL 内置路由。
筛选 `bucket_id + bucket_name + removed=0`，按源表主键分页。默认 `--batch 5000` 只是 SQL 每页行数，仍会扫完整名单。

完整性要求：扫描前源表计数 = 扫描后计数 = 实际读到行数 = 进度库唯一 OID 数；检查 `rows.Err()`。
任何一步失败不封存。中断后 scan 从头重扫、INSERT IGNORE 去重；已有快照不刷新。
一旦 COMPLETE，禁止再 scan 追加或覆盖名单。计数不同会报错，不再只给 WARNING。

计数相等不能证明源库已追上链，也不能排除扫描期间等量替换；必须先确认 BSDB 同步及目标数据冻结。
`--count-only` 只读源表，不连接进度库，可以正常显示 0。正式扫描计数为 0 时拒绝继续，封存层也拒绝 0 行名单；请检查 bucket 名称/ID 和 BSDB 同步情况。这里检查的是对象数，不是 payload 大小，零字节对象仍可正常入库。

历史空名单即使已标记 COMPLETE，status 也只给出警告，不输出 ALL_DONE。确认配置和数据源后使用新的独立进度库重新 scan，保留旧库用于排查。

### task create 容量选择

按 OID 升序选未分配对象，累计 `payload_size` 达到目标或数量上限即停止；默认 `500GiB`、`10000` 个 OID。
允许最后一个普通对象略超容量。超大对象独立成批，不拆 OID；零字节对象仍占数量额度。支持正整数 `B/KiB/MiB/GiB/TiB`，不接受含糊的 `500G`。

创建在事务中同时写任务、名单和 SHA-256 摘要（OID、payload、状态、版本）。写失败整体回滚；提交响应丢失后重跑返回已存在任务。
默认排除所有已分配 OID；只有显式 `--retry-deferred` 才从已完成任务的未解决延期项重新选取，旧记录保留。

### purge 删除和重试

1. 启动核对环境绑定、封存状态、任务摘要、S3 版本控制状态、链上 bucket ID；Enabled/Suspended 默认拒绝，可显式放行（见下文）。
2. 每个 OID 删前查链，核对返回 OID、bucket、状态、版本、payload 与冻结名单一致。查不到是普通失败，不退回仅信本地记录；身份不符是安全异常。
3. 扫本 SP 配置物理桶的 `s<oid>_`、`e<oid>_`，不根据当前 primary/secondary 角色跳过。
4. 严格校验完整 key 格式及 OID，删除入口再次校验；每轮最多 1000 个 key，用 `DeleteObjects` 批删并检查逐 key 响应。
5. 从前缀开头重新列举剩余 key，直到两个前缀为空并复查；再按 OID 清理本地 `integrity_meta_NN`、`piece_hash` 并复查。
6. 每个 OID 完成或失败后写进度，不是每个 piece 写一次 DB。完成写入和残留标记清除处于同一事务。

2500 个 key 分为最多 1000 + 1000 + 500 三批，不是 2500 个单删请求。
每个失败 piece 独立计数；整批请求失败则本批未成功 key 各加一次。默认 `--max-retry 10` 是首次尝试加最多 10 次重试。
某 key 耗尽预算则停止该 OID，保留元数据；不保证该对象其他 key 全部删完。
列举报错另计连续次数；成功列举清零。批次间按失败数退避，最多 30 秒。

一个对象内部重试耗尽仍失败，就打印醒目的 `!!! STOP`（日志含 task、OID、错误原因），停止派发新对象；队列里尚未开始的对象留待下次，已开始的对象继续处理至完成或自身失败、尽量记录结果后非零退出。不再自动补跑对象、不继续遍历整批。链查询或元数据操作最终报错、进度读写失败同样停止新对象。收尾没有固定几分钟上限，大对象可能更久；自身失败仍可能留下部分残留。
链查询的超时、连接拒绝/重置、HTTP 429/5xx 等临时错误，以及 SpDB 删除/复查的临时连接故障、死锁等，各最多 3 次应用层尝试，间隔 1 秒、2 秒。SpDB 每次尝试限时 30 秒；两张表按 OID 删除可安全重试。权限错误、对象不存在、表不存在等不盲目重试；身份/快照不符不重试。驱动内部重试不计入这 3 次。
普通失败不写 SAFETY_HOLD：根据日志排障后重跑原 purge 命令即可，默认领取失败和未完成对象，不需要 task resume。恢复重新列实际残留，不依赖 segment 游标。dry-run 仍仅预览，不因单个对象报错提前停止。
删除已完成但结果未落库也会停止；重跑复查后补写。统计可能漏记“删完未落库”的字节，不能用于精确账单核算。此规则仅修改 purge，verify 的逐对象验收/重试行为不变。

身份不符、未知 key 或删除入口安全拒绝仍立即停止派发、取消在途任务、持久化 SAFETY_HOLD，且不自动补跑。失锁、Ctrl+C 也立即取消，不等待普通失败的收尾。
已发出的请求无法撤回。安全暂停落库失败也会明确报错，必须人工检查后再运行。

**大小对账**：每个 oid 删完（dry-run 时是列举完）后，按哪个前缀有数据推断本 SP 的角色，再和 bsdb 里的链上 `payload_size` 比对：

| 角色 | 判定 | 预期字节 |
|---|---|---|
| primary | `s<oid>_` 有数据 | 等于 `payload_size` |
| secondary | 只有 `e<oid>_` 有数据 | 每段一个分片，累加 ⌈段大小 ÷ 数据分片数⌉，最后一段按余数算 |
| none | 两个前缀都没数据 | 不比对（未封存对象，或之前已删掉） |
| mixed | 两个前缀都有数据 | 异常，直接告警 |

secondary 公式可逐字节复现 SP 的实际存储（本地 3+3 实测：5 MiB 对象 1,747,627 B，50 MiB 对象 17,476,269 B）。数据分片数和段大小默认启动时从链上读当前值，也可用 `--data-chunks`、`--segment-size` 指定。

对不上只打 `WARNING` 并记入进度表，**不影响 DONE 判定**：字节已经删掉，拦截也不可逆。`status` 的 `size` 一行给出 checked / ok / mismatch / unchecked，并列出不符的 oid。比对以进度表里的累计 `deleted_bytes` 为准，所以中断重试后的对象也能正确比对；但中断前已删、未落库的字节无从统计，这类对象可能显示为 mismatch。另外链上参数若在对象写入后变过，历史对象也会显示为 mismatch（limewire 存续期间参数未变）。


### verify 独立验收

仅验收本任务全部未延期 OID，不扫描其他任务。要求对象已处理成功；有未处理/失败项须先 purge 或人工 defer。
对每个 OID 检查两个前缀是否为空（只需至多返回一个 key），以及本地元数据是否存在；无需再次列出数千个已删 key。

逐 OID 记录 PASS / RESIDUE / ERROR 与验收代次。单个查询失败默认额外重试 3 次，仍失败记录 ERROR 并继续其他对象；不会被当成空。
进度落库失败或锁丢失立即停止。中断重跑跳过本代次 PASS，删除再次启动则旧 PASS 整批失效。

报告默认 `residue-task-ID.tsv`，字段为 `oid / result / detail`（带表头、TSV 引号转义），包含当前代次残留/错误及本任务延期项。先写临时文件再原子替换，导出失败不完成任务。
未检查的数量看 status，报告不是唯一进度依据。

全部未延期对象 PASS 才完成；有延期项显示 DONE_WITH_EXCEPTIONS，不能宣称全量清空。
verify 通过只说明配置范围内目标数据无残留，**不能证明未误删其他对象**，也不证明云端历史版本或账单已清除。

### 状态和锁

| 状态 | 含义 / 恢复 |
|---|---|
| READY | 新任务未开始 |
| RUNNING / VERIFYING | 当前有进程持执行锁，阶段为删除 / 验收 |
| RETRY_PENDING | 无运行进程，仍有未处理或失败 OID；重跑 purge |
| AWAITING_VERIFY | 已处理，尚未验收完成；运行 verify |
| VERIFY_FAILED | 有残留或查询错误；按结果重跑 purge / verify |
| SAFETY_HOLD | 持久化安全暂停；人工查明后 resume 或合法 defer |
| DONE | 当前代次全 PASS |
| DONE_WITH_EXCEPTIONS | 其余对象全 PASS，但有单独留账的延期项 |

RUNNING/VERIFYING 由执行锁结合阶段推导，不把崩溃前的运行标记当作永久占用。
所有会写库的命令（含 verify、可选 dry-run 的 schema 检查）使用同一命名锁；status 不加锁。

锁名包含进度库名摘要；不同 SP 的不同库不互相阻塞。通过专用 `sql.Conn` 持有 MySQL GET_LOCK，每 2 秒检查会话仍持锁，请求边界再次检查；失锁取消操作，不自动换连接重获。退出显式 RELEASE_LOCK。
必须连接同一个 MySQL 写实例；它不是跨实例分布式锁，也不能撤回已发出的 S3 请求。不要在线复制进度库后并行清同一 SP。

status 默认显示当前任务详情及总体汇总：已完成批数（含有延期项的批数）、未完成批数、未分配对象数、未解决延期数和当前锁连接；历史任务详情需传 `--task-id N`。只有 scan COMPLETE、扫描行数及名单对象数大于 0、至少一个已完成任务、无未分配/延期对象、无未完成任务且无执行锁时输出 ALL_DONE。
全局 `purge_progress done` 只是“处理成功”，不是“验收通过”；剩余数为零也不能单独证明成功。

### 任务绑定

进度表只按 object_id 记录，而同一个 oid 在 7 个 SP 上都有数据。如果 sp1 误用了 sp0 的进度库，
就会把 sp0 已完成的对象当成自己已完成而跳过。为此，进度库第一次 `scan` 时写入一行 `task_meta`，
把这个库绑定到一个任务：

| 字段 | 来源 |
|---|---|
| 链 ID | SP TOML `[Chain] ChainID`（为空时同 SP 一样取 `greenfield_9000-121`） |
| SP operator 地址 | SP TOML `[SpAccount] SpOperatorAddress` |
| 物理桶 | `BucketURL`（`BUCKET_URL` 优先）解析出的 endpoint（AWS 默认 endpoint 记为 `aws:<region>`）加桶名，path / virtual-hosted 写法视为同一个桶 |
| SpDB | `[SpDB]` 的 `Address/Database`（环境变量优先） |
| 目标 bucket | `--bucket`、`--bucket-id` |

之后 `scan`、`purge`、`verify` 每次启动都按本次的配置和参数重新计算这些字段并与库中比对，任何一项不同都拒绝运行，
并列出不同的字段。若确实需要更换（例如 SP 迁移了物理桶），应新建进度库重新开始，不要修改 `task_meta`。

旧版本（无绑定）用过的进度库不会被自动接管：库里没有 `task_meta`、但 `scan_objects` 或 `purge_progress` 已有数据时，
`scan` 拒绝绑定，因为无法判断这些记录属于哪个 SP，接管后其中的 DONE 会让本 SP 跳过对象。此时请换一个新的空库。


### 核心表格式

每个 SP 一套六张表，均需 InnoDB；工具自动建表，不自动创建数据库。准确 DDL 见 `internal/progress/progress.go`。

| 表 | 主键 / 关键字段 | 用途 |
|---|---|---|
| task_meta | id=1；chain_id、sp_operator、storage、spdb、bucket_name、bucket_id；scan_status、scan_bsdb_count、scan_rows、scan_finished_at | 固定范围及全量扫描封存 |
| scan_objects | object_id BIGINT UNSIGNED PK；payload_size BIGINT UNSIGNED、status VARCHAR(16)、version INT | 冻结 OID 快照，status 是链对象状态 |
| purge_tasks | id BIGINT UNSIGNED 自增 PK；target_bytes、object_count、logical_bytes；digest CHAR(64)、phase；hold_reason、hold_oid；verify_generation、done_at | 子任务范围、阶段、安全暂停、验收代次、完成时间 |
| task_objects | 联合 PK(task_id, object_id)，object_id 索引；deferred、defer_reason、deferred_at；verify_generation、verify_status、verify_detail、verified_at | 固定成员、人工延期、逐对象验收断点 |
| purge_progress | object_id PK；status TINYINT（1处理成功/2失败）、deleted_keys INT UNSIGNED、deleted_bytes BIGINT UNSIGNED、fail_reason、role、expected_bytes、updated_at | 每 OID 最新处理结果及累计删除量 |
| task_events | id 自增 PK；task_id、object_id（可空）、action、reason、created_at | DEFER / RESUME 人工决定留账；不是逐 key 删除审计 |

成员不能静默移除。延期重新分配允许同一 OID 出现在不同历史任务，但只有最后一次分配参与延期池。
分配互斥由同库执行锁和创建事务保证；人工直接改表会破坏这些约束，不受支持。
处理结果按 OID 累计，历史任务显示的删除统计可能随显式延期重试增加，不是逐任务尝试审计。

旧库只有完整扫描、没有删除结果时可直接创建子任务；已有未分配到任务的旧删除结果时拒绝自动接管，使用新进度库或先单独审核迁移方案。不要通过手改 DONE/hold 字段绕过检查。

## 2. 运行前提

- 每个 SP 使用自己的配置、云身份、物理桶、SpDB 和进度库。可以运行在该 SP 的 pod 内，
  或具备相同存储访问身份和网络可达性的环境中；不需要链上交易私钥。
- 清理前由运维确认 challenge/attestation 风险已在所需范围内处置；只停本地一个进程不能视为全网已停。
  链上仍保留对象时，不能因本工具完成就恢复相关抽检。工具不会执行这些运维操作。
- 停止目标数据的上传、更新、迁移和恢复等可能重新写入 piece 的流程，确保扫描及删除期间目标集合稳定。
  不影响其他租户的读写是执行前提，需要先在测试环境验证，再低并发、小流量试运行。
- 当前仅支持 S3 / MinIO，`PieceStore.Shards > 1` 会直接拒绝，不会遍历多个物理分片桶。
- `purge`（含 dry-run）和 `verify` 默认拒绝 Enabled/Suspended 桶；两条命令都可加 `--allow-versioned-bucket` 显式放行，并打印警告。版本控制查询失败仍拒绝，参数不能绕过权限或查询错误。
  放行后仍按 key 删除，不按 `VersionId` 清理；verify/PASS/DONE 仅表示当前可见 key 和本地元数据通过检查，不代表历史版本、删除标记或计费空间已清除。

  已确认接受上述限制时，在原命令末尾分别添加参数：

  ```bash
  ./limewire-purge purge "${purge_common[@]}" --execute --allow-versioned-bucket
  ./limewire-purge verify "${purge_common[@]}" --allow-versioned-bucket
  ```

  示例沿用前文配置；测试环境的 purge 仍需填写对应 `--chain-id` 和 `--chain-rpc`。
- BSDB 需 SELECT 权限；SpDB 正式清理需 SELECT、DELETE；进度库需 SELECT、INSERT、UPDATE、CREATE（旧表迁移还需 ALTER）；
  S3 需列举及查询版本控制的权限，正式清理另需删除权限。权限应限定到授权目标。

## 3. 构建

当前 Makefile 固定 Go `1.21.13`，SDK 的 BLS 依赖需要 CGO 和 C 编译器。
推荐直接在目标 Linux amd64 构建环境编译：

```bash
make build       # 输出 ./limewire-purge
make test        # go vet + 全量单元测试
make test-logic  # 仅 pieceop / purge / spconfig，不覆盖 SDK 和真实存储集成
```

`make linux` 输出 `limewire-purge-linux-amd64`，但跨平台执行需要配置 Linux C 交叉工具链。
不能使用 `CGO_ENABLED=0` 构建当前 SDK 依赖。构建完成后还需在目标运行环境验证二进制可启动。

## 4. 配置

`--config` 指向本 SP 的现有 TOML。工具只读取所需字段，不启动 SP 服务。
以下仅为本地测试示意，不应覆盖正在运行的 SP 配置：

```toml
[Chain]
ChainID = "greenfield_9000-121"

[SpAccount]
SpOperatorAddress = "REPLACE_WITH_THIS_SP_OPERATOR"

[BsDB]
User = "bs_reader"
Passwd = "REPLACE_ME"
Address = "127.0.0.1:3306"
Database = "bsdb_sp0"

[SpDB]
User = "sp_purger"
Passwd = "REPLACE_ME"
Address = "127.0.0.1:3306"
Database = "spdb_sp0"

[PieceStore]
Shards = 0

[PieceStore.Store]
Storage = "minio"
BucketURL = "http://127.0.0.1:9000/sp0"
IAMType = "AKSK"
```

环境变量优先于 TOML 中对应字段：

| 用途 | 环境变量 |
|---|---|
| BSDB 连接 | `BS_DB_USER`、`BS_DB_PASSWORD`、`BS_DB_ADDRESS`、`BS_DB_DATABASE` |
| SpDB 连接 | `SP_DB_USER`、`SP_DB_PASSWORD`、`SP_DB_ADDRESS`、`SP_DB_DATABASE` |
| 物理桶 URL | `BUCKET_URL` |
| AKSK 凭证 | `AWS_ACCESS_KEY`、`AWS_SECRET_KEY`，可选 `AWS_SESSION_TOKEN` |
| SA / IRSA 凭证 | 使用 AWS SDK 默认凭证链，例如 `AWS_ROLE_ARN`、`AWS_WEB_IDENTITY_TOKEN_FILE` |

云凭证不从上述 TOML 读取。使用现有凭证注入机制，不要将真实密码或 access key 提交到仓库。
进度 DSN 含密码，传入命令行也可能被进程列表或终端记录暴露，应使用受控运行环境。


## 参数速查

| 参数 | 命令 | 默认 / 说明 |
|---|---|---|
| --config | scan/task/purge/verify | 本 SP TOML；status 可省略，提供时核对绑定 |
| --progress-dsn | 除 scan --count-only 外 | 独立 MySQL 库，必须带库名 |
| --bucket / --bucket-id | 有 config 的命令 | limewire / 42268；建议显式填写 |
| --batch | scan | 5000，SQL 页大小 |
| --target-size / --max-objects | task create | 500GiB / 10000，最大对象数允许 1..100000 |
| --retry-deferred | task create | 默认关闭；显式处理延期池 |
| --task-id | purge/verify/status/task defer/task resume | 默认当前任务；purge/defer/resume 传值必须匹配当前 ID；verify/status 可显式查历史 |
| --oid / --reason | task defer | 必填；reason 为人工原因 |
| --reason | task resume | 必填；不会修改冻结快照 |
| --execute / --dry-run | purge | 二选一；真实删除必须 execute |
| --allow-versioned-bucket | purge / verify | 默认 false；显式放行 Enabled/Suspended，历史版本和删除标记不在清理及验收范围内 |
| --chain-rpc / --chain-id | purge | 主网 RPC / greenfield_1017-1；测试环境务必改 |
| --concurrency / --qps | purge | 8 / 50；按其他租户负载调整，QPS 不含 SDK 内部重试 |
| --max-retry | purge / verify | 10 / 3，额外重试数 |
| --chain-timeout / --s3-timeout | purge | 30s / 2m；verify 同样有 s3-timeout |
| --data-chunks / --segment-size | purge | 0：从链读，仅用于大小对账 |
| --restart | verify | 默认 false；新验收代次，全部重新检查 |
| --out | verify | residue-task-ID.tsv，原子覆盖 |
| --failures | status | 20，显示失败及各任务报告样本条数 |

## 限制和测试

- 不自动连续跨子任务；不做逐 key 审计或云端日志对账。
- 不支持物理分片桶和云历史版本永久清理；使用 --allow-versioned-bucket 不改变这一限制。
- verify 仅覆盖导入名单和当前配置物理桶，不代表链上已删除、停止计费或全部 SP 副本都消失。
- 冻结目标数据是断点验收有效的前提；有回写可能时必须先停回写，再 --restart 完整复验。
- 大小对账不是防误删证明，现有失败记录可能缺少角色/期望量，统计也可能因中断少计。
- 使用官方 Greenfield 链客户端、AWS SDK；保留独立的受限批删及按 OID 清元数据，不直接调用 SP 完整 GC。

单元测试覆盖容量分配、状态、会话锁互斥/失锁、piece 重试、安全停机、验收断点/代次及错误恢复。MySQL 真实生命周期测试为显式选用：

```bash
# 管理员准备一个全新的空测试库，名字必须以 limewire_purge_test_ 开头。
# 只向该库写测试表/数据，不连链、S3、BSDB、SpDB；运行后保留数据供检查。
LIMEWIRE_TEST_MYSQL_DSN='user:password@tcp(test-host:3306)/limewire_purge_test_001' \
  go test ./internal/progress -run TestMySQLSubtaskLifecycle -v
```

没有测试 DSN 时该项 SKIP。上线前还需真实 MySQL、测试链和对象存储联调，不能用离线测试代替。
详见 [子任务设计](docs/SUBTASK_DESIGN.md)、[本地测试](docs/LOCAL_TEST.md)。工作区配套方案在父目录 report，不随单独 clone 提供。
