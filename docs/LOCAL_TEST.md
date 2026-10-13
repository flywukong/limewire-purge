# 本地环境测试步骤

若是回到之前已经部署好的 QA / SP 环境，优先使用 [TESTING.md 的现有环境回归步骤](../TESTING.md#现有-qa--sp-环境回归优先走这里)，不要重新初始化集群或切换存储。该步骤包含更新版本、小任务运行、Suspended 桶参数和本轮故障回归项目。

适用于 greenfield-deploy 起的本地集群（sp0…sp6、MySQL、链 REST 在 :1317），SP 存储后端已按 README 说明切到 MinIO。

## 0. 前提

- SP 的 `config.toml` 里 `[PieceStore.Store]` 为 `Storage = "minio"`，`BucketURL = "http://127.0.0.1:9000/spN"`，`IAMType = "AKSK"`
- 环境变量 `AWS_ACCESS_KEY` / `AWS_SECRET_KEY`（MinIO 的账号密码）对 SP 进程和工具都可见
- 测试期间停掉 challenger0…5，否则删完 piece 后本地 SP 会被 slash

## 1. 安装

在开发机交叉编译再拷到主机，主机不需要 Go：

```bash
git clone git@github.com:flywukong/limewire-purge.git && cd limewire-purge && make linux
scp limewire-purge-linux-amd64 root@<host>:/usr/local/bin/limewire-purge
```

或者在部署机上按固定 Go 1.21.13 + CGO/C 编译器要求直接 `make build`。跨平台构建还需要对应的 C 交叉工具链，不能仅设置 GOOS。

## 2. 建工具自己的库

```sql
CREATE DATABASE limewire_purge;
```

六张表由工具首次写命令自动建立，每个 SP 使用独立库。完整命令与故障演练见 [TESTING.md](../TESTING.md)，不要多个 SP 共用同一进度库。

## 3. 造测试数据

```bash
gnfd-cmd bucket create gnfd://purge-test
gnfd-cmd object put ./small.bin gnfd://purge-test/small          # 1 段
gnfd-cmd object put ./large.bin gnfd://purge-test/large          # > max_segment_size，多段
gnfd-cmd object put ./keep.bin  gnfd://keep-bucket/keep          # 另一个桶，对照，必须活着
gnfd-cmd bucket head gnfd://purge-test                            # 记下 bucket id
```

上传完先到 MinIO 里确认 key 格式（这一步等价于手册第 4 节「验证 key 假设」）：

```bash
mc ls local/sp0 | head        # 应看到 s<oid>_s0 …
mc ls local/sp1 | head        # 应看到 e<oid>_s0_p<r> …
```

## 4. 按子任务运行

每个 SP 使用自己的 config 和独立进度库，按 [README](../README.md) 的公共参数示例执行：

```text
scan 全量落库、对账、封存（一次）
task create --target-size 1GiB --max-objects 2
purge --execute
verify
status
```

以上为命令顺序，实际须提供公共参数。任务完成后再创建下一批；purge/verify 默认续跑唯一未完成任务，不需人工记录 ID。可选 dry-run 不是前置要求。

至少覆盖中断恢复、失败重试、验收残留重排、人工延期及延期重试、SAFETY_HOLD、执行锁失联；核对其他桶对照对象仍完整。详见根目录测试说明。
