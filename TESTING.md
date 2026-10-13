# 清理工具回归测试

## 现有 QA / SP 环境回归（优先走这里）

用于回到之前已经部署好的测试环境，不重建链、不切换 S3/MinIO 后端、不重置 SP。本文命令是操作示例，需确认环境和授权后人工分阶段执行。

### 1. 更新工具和确认范围

- 在现有代码目录检查 `git status --short`、`git remote -v` 和 `git branch -vv`，确认仓库/分支无误且没有未保存修改后，再 `git pull --ff-only`。
- 在测试环境对应的 Linux 构建机执行 `make build`、`make test`，记录 `git rev-parse HEAD`；替换二进制前停止旧的清理工具进程，避免测试时混用版本。编译要求见后面的本地环境章节。
- 确认本 SP 的 config、BSDB/SpDB、物理桶、链 RPC/chain ID、目标 bucket 名称/ID。先处置测试范围的 challenge/attestation 风险，停止目标对象的回写，核对 BSDB 已同步。
- 使用本轮新建的空 MySQL 进度库，**每个 SP 一个库**。旧库保留；不要删除旧记录、复制其他 SP 的库或手改 COMPLETE/DONE 状态。若只是本轮中断续跑，则继续用本轮同一个库，不再创建新库。
- 准备新测试对象：小对象、多段对象，以及另一个逻辑 bucket 的对照对象。记录对照对象的内容摘要，清理前后都验证可读且摘要一致。只有要覆盖超过 1000 key 的分批路径时，才需要按实际 segment 参数准备足够大的对象。

### 2. 参数、全量扫描和小任务

以下环境变量由操作者填写；DSN/凭证通过现有安全机制提供，不写入仓库。这里不预设 QA 地址、chain ID 或 bucket ID。

```bash
# Bash / Zsh
: "${PURGE_CONFIG:?本 SP config.toml 路径}"
: "${PURGE_PROGRESS_DSN:?本轮本 SP 独立进度库 DSN}"
: "${PURGE_BUCKET:?本轮授权测试 bucket 名称}"
: "${PURGE_BUCKET_ID:?该测试 bucket 的链上 ID}"
: "${PURGE_CHAIN_RPC:?该测试环境的 CometBFT RPC URL}"
: "${PURGE_CHAIN_ID:?该测试环境 chain ID，与 SP config 一致}"
purge_common=(--config "$PURGE_CONFIG" --progress-dsn "$PURGE_PROGRESS_DSN"
  --bucket "$PURGE_BUCKET" --bucket-id "$PURGE_BUCKET_ID")
purge_chain=(--chain-rpc "$PURGE_CHAIN_RPC" --chain-id "$PURGE_CHAIN_ID")
purge_storage=()
# 仅当物理桶为 Enabled/Suspended，且已确认接受历史版本不清理时使用：
# purge_storage=(--allow-versioned-bucket)

./limewire-purge scan "${purge_common[@]}" --count-only
# 先确认计数与测试对象名单相符，再逐条执行：
./limewire-purge scan "${purge_common[@]}"
./limewire-purge task create "${purge_common[@]}" --target-size 1GiB --max-objects 2
./limewire-purge status "${purge_common[@]}"
```

如果物理桶处于 Suspended，不能只给 purge 加参数而漏掉 verify。放行后 PASS/DONE 不证明历史版本、删除标记或账单空间已清除。

### 3. 人工检查摘要后再执行

核对当前 task ID、OID 数量、逻辑容量及环境绑定，再运行。下面不自动创建下一批；一次只处理已检查的小任务。

```bash
set -o pipefail
mkdir -p ./test-logs
./limewire-purge purge "${purge_common[@]}" "${purge_chain[@]}" "${purge_storage[@]}" \
  --execute --concurrency 1 --qps 5 --max-retry 10 2>&1 | tee ./test-logs/purge.log
```

确认 purge 成功退出后，**单独**执行验收：

```bash
./limewire-purge verify "${purge_common[@]}" "${purge_storage[@]}" \
  --qps 5 --out ./test-logs/residue.tsv 2>&1 | tee ./test-logs/verify.log
./limewire-purge status "${purge_common[@]}"
```

每轮保留日志（下轮前换路径，避免覆盖）。正式脚本可给 purge 加 `--task-id N` 固定到已检查的任务；手动操作默认当前任务。只有本批通过后才再次 `task create`。检查对照对象仍可读且摘要相同后，再扩大容量/并发，不直接开始全量清理。

### 4. 这版必须回归的项目

| 场景 | 预期结果 |
|---|---|
| 空扫描/错误 bucket ID | count-only 可显示 0；正式 scan 报错、不封存；历史空库不显示 ALL_DONE |
| 正常小任务 | 链上身份检查 → 按前缀批删 → 清本地元数据 → 独立 verify → DONE；其他 bucket 对照数据不变 |
| 无任务/错误 task ID | purge 不自动创建任务；传入与当前任务不同的 ID 时拒绝删除 |
| 短暂 RPC/SpDB 故障 | 临时错误最多 3 次应用层尝试，间隔 1 秒/2 秒；恢复后继续，永久错误不盲目重试 |
| piece 重试耗尽，并发设为 2 | 打印 !!! STOP 和 task/OID/原因；不开始新对象，在途对象完成或自身失败并尽量落库后退出；不自动补跑、不产生普通失败的 SAFETY_HOLD |
| 普通失败排障后重跑 | 原 purge 命令继续失败和未完成对象，保留已完成进度，不需 task resume；部分残留重新列举 |
| Ctrl+C/失锁/安全异常 | 立即取消，不等待普通失败收尾；安全异常持久化 SAFETY_HOLD |
| verify 中断/残留 | 重跑续验；RESIDUE 要先 purge 再整批验收。verify 的 ERROR 目前仍记录后继续检查，不采用 purge 的停止策略 |
| Suspended/Enabled 桶 | 默认拒绝；purge 和 verify 显式放行后可运行，日志说明历史版本不在范围内 |

故障注入只能作用于授权测试对象/隔离测试服务，不要修改整个共享桶的策略或中断其他租户的数据库。收尾时间取决于在途对象大小和请求耗时，不承诺几分钟内结束。验收证据保留：代码 SHA、测试环境/任务摘要、日志、verify 报告、对照对象前后摘要；不要收集凭证。

---

## 新建本地 Greenfield / MinIO 环境（已有 QA 环境无需执行）

前提：本地集群已起（validators + sp0..sp6 + challenger + MySQL），SP 存储后端为
MinIO（见下文），链 CometBFT RPC 在 `http://127.0.0.1:26657`（本地验证器的 RPC 端口，非 REST 的 1317）。测试桶名假设为 `purge-test`。

## 1. 装工具

在部署机（有 Go 即可）上：

```bash
git clone git@github.com:flywukong/limewire-purge.git
cd limewire-purge
make build          # 产出 ./limewire-purge
make test           # 离线单测：前缀边界 / 2500 key 分批 / 部分失败 / 续跑
```

工具依赖 go-sdk 里的 BLS（C 库），编译需要 CGO/gcc，且工具链固定 Go 1.21.13。
最简单是直接在部署机上 `make build`。若一定要在别处交叉编译，需装好 linux
的 C 交叉工具链再 `make linux CC=<cross-gcc>`，产物 `limewire-purge-linux-amd64`
拷到部署机；纯 `GOOS=linux` 交叉（无 CGO）会链接失败。

## 2. SP 换成 MinIO 后端（一次性）

装并起 MinIO：

```bash
curl -fsSL https://dl.min.io/server/minio/release/linux-amd64/minio -o /usr/local/bin/minio && chmod +x /usr/local/bin/minio
curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc     -o /usr/local/bin/mc    && chmod +x /usr/local/bin/mc
mkdir -p /data/minio
MINIO_ROOT_USER=gnfd MINIO_ROOT_PASSWORD=gnfd-local-secret \
  nohup minio server /data/minio --address :9000 --console-address :9001 >/var/log/minio.log 2>&1 &
mc alias set local http://127.0.0.1:9000 gnfd gnfd-local-secret
for i in 0 1 2 3 4 5 6; do mc mb -p local/sp$i; done
```

每个 SP 的 `config.toml` 里 `[PieceStore.Store]` 改成（spN 对应各自的桶）：

```toml
[PieceStore.Store]
Storage   = "minio"
BucketURL = "http://127.0.0.1:9000/sp0"
IAMType   = "AKSK"
```

启动每个 SP 进程前导出凭证（工具运行时也要有这两个变量）：

```bash
export AWS_ACCESS_KEY=gnfd AWS_SECRET_KEY=gnfd-local-secret
```

换后端相当于换空盘，需 reset 集群重新上传。建议同时把 genesis 里
`max_segment_size` 调成 1 MiB，这样一个 ~1 GB 的对象就有上千段，能覆盖
分批（>1000 key）路径。

## 3. 造测试数据

用 `gnfd-cmd` 建桶并上传几个对象：一个小对象、一个大对象（跨多段）、
一个「对照对象」放在**别的桶**（验证工具不会误删）。记下大对象的
objectID 与桶 ID（`head-bucket` / `head-object`）。

上传后先肉眼确认 key 格式：

```bash
mc ls local/sp0 | head        # 应看到 s<oid>_s0 这类文件名
```

## 4. 跑工具

准备进度库（MySQL 新库，和 SP 用同一个实例即可）：

```bash
mysql -uroot -p -e "CREATE DATABASE limewire_purge_sp0"
```

```bash
PDSN='root:PASS@tcp(127.0.0.1:3306)/limewire_purge_sp0'
CONF=~/.local/sp0/config.toml

# ① 先只数一下 bsdb，不写任何东西
./limewire-purge scan --config $CONF --bucket purge-test --bucket-id <ID> --count-only

# ② 捞名单进 scan_objects
./limewire-purge scan --config $CONF --progress-dsn "$PDSN" --bucket purge-test --bucket-id <ID>

# ③ 创建小任务（全量名单仍在库中）
./limewire-purge task create --config $CONF --progress-dsn "$PDSN" \
    --bucket purge-test --bucket-id <ID> --target-size 1GiB --max-objects 2

# 可选 dry-run：只列举当前任务，不删
./limewire-purge purge --config $CONF --progress-dsn "$PDSN" \
    --bucket purge-test --bucket-id <ID> --chain-rpc http://127.0.0.1:26657 --chain-id greenfield_9000-121 --dry-run

# ④ 正式删（sp0）
./limewire-purge purge --config $CONF --progress-dsn "$PDSN" \
    --bucket purge-test --bucket-id <ID> --chain-rpc http://127.0.0.1:26657 --chain-id greenfield_9000-121 --execute

# ⑤ 其余 SP：各自 scan，必须换 --config 和独立进度库，例如 limewire_purge_sp1。
# 不共用进度库，也不复制另一个 SP 的 DONE 结果。

# ⑥ 验收 + 汇总
./limewire-purge verify --config $CONF --progress-dsn "$PDSN" --bucket purge-test --bucket-id <ID>
./limewire-purge status --progress-dsn "$PDSN"

# 本任务验收通过后，重复 task create → purge --execute → verify，直到 ALL_DONE。
# 中断后直接重跑 purge / verify，无需记 task ID。
```

## 5. 要观察的点

- `scan --count-only` 的数与已知逻辑对象清单一致（不能与 S3 key 数直接比较）→ bsdb 那条 SQL（含
  BINARY(32) 取右 64 位、分表号）在本地 schema 上成立。
- dry-run 命中 key 数 = 对象段数（primary 桶）；secondary 桶为 EC 分片数。
- 删完后 `mc ls local/sp0 | grep s<oid>_` 为空；对照对象所在桶不受影响。
- `status` 的子任务成员、处理状态和验收状态对得上；deleted_bytes 与对象大小成比例
  （primary ≈ payload，secondary ≈ payload/data_chunks）。
- SpDB 里 `integrity_meta_NN` / `piece_hash` 中该 oid 的行已清。
- 验证默认 500GiB、超大对象独立成批、--max-objects 截断；重复 create 返回原任务。
- 删除中途结束进程后重跑，只清实际残留；verify 中断后续验，--restart 重新检查全批。
- 在测试桶对一个 key 制造拒删：失败保留当前任务；task defer --oid ... --reason ... 后，不计 PASS，验收其余项得到 DONE_WITH_EXCEPTIONS；再 task create --retry-deferred 显式回收该项。
- 人为在测试 OID 前缀放未知格式 key，确认 SAFETY_HOLD 持久化且普通 purge 拒绝；人工处理并 task resume 后仍重新校验。
- 两个命令同时运行，后者被执行锁拒绝；测试库中断持锁连接后，当前进程停止派发。不同 SP 的独立库可以并行。

## 6. 注意

- **先停 challenger**，否则删 piece 后随机抽检会 slash 本地 SP：
  `pkill -f gnfd-challenger`（或按部署脚本停 challenger0..5）。
- 每个 SP 用各自的 `--config`；MinIO root 账号能删所有桶，本地没有 IRSA
  的桶级隔离，靠 config 指对 sp。
- 反复全流程测试使用新的空进度库，保留旧记录便于核对；不要直接改任务状态绕过验收。
