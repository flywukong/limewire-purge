# 在本地 Greenfield 环境测试 limewire-purge

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
mysql -uroot -p -e "CREATE DATABASE IF NOT EXISTS limewire_purge"
```

```bash
PDSN='root:PASS@tcp(127.0.0.1:3306)/limewire_purge'
CONF=~/.local/sp0/config.toml

# ① 先只数一下 bsdb，不写任何东西
./limewire-purge scan --config $CONF --bucket purge-test --bucket-id <ID> --count-only

# ② 捞名单进 scan_objects
./limewire-purge scan --config $CONF --progress-dsn "$PDSN" --bucket purge-test --bucket-id <ID>

# ③ dry-run：只列举不删，看命中 key 数
./limewire-purge purge --config $CONF --progress-dsn "$PDSN" \
    --bucket purge-test --bucket-id <ID> --chain-rpc http://127.0.0.1:26657 --chain-id greenfield_9000-121 --dry-run

# ④ 正式删（sp0）
./limewire-purge purge --config $CONF --progress-dsn "$PDSN" \
    --bucket purge-test --bucket-id <ID> --chain-rpc http://127.0.0.1:26657 --chain-id greenfield_9000-121

# ⑤ 其余 SP：换 --config 指向 spN。进度库用同一个 DSN（scan_objects 共用，
#    但 purge_progress 会互相覆盖）——多 SP 一起测时，每个 SP 用各自的库/表前缀，
#    第一版最简单的做法是一个 SP 一个 progress 库：/limewire_purge_sp0 等。

# ⑥ 验收 + 汇总
./limewire-purge verify --config $CONF --progress-dsn "$PDSN" --bucket purge-test --bucket-id <ID>
./limewire-purge status --progress-dsn "$PDSN"
```

## 5. 要观察的点

- `scan --count-only` 的数与 `mc ls` / 链上对象数一致 → bsdb 那条 SQL（含
  BINARY(32) 取右 64 位、分表号）在本地 schema 上成立。
- dry-run 命中 key 数 = 对象段数（primary 桶）；secondary 桶为 EC 分片数。
- 删完后 `mc ls local/sp0 | grep s<oid>_` 为空；对照对象所在桶不受影响。
- `status` 的 done/failed/remaining 对得上；deleted_bytes 与对象大小成比例
  （primary ≈ payload，secondary ≈ payload/data_chunks）。
- SpDB 里 `integrity_meta_NN` / `piece_hash` 中该 oid 的行已清。

## 6. 注意

- **先停 challenger**，否则删 piece 后随机抽检会 slash 本地 SP：
  `pkill -f gnfd-challenger`（或按部署脚本停 challenger0..5）。
- 每个 SP 用各自的 `--config`；MinIO root 账号能删所有桶，本地没有 IRSA
  的桶级隔离，靠 config 指对 sp。
- 反复测试时 `DROP DATABASE limewire_purge` 重来；或换新 progress 库。
