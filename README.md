# 本地 Raft 一致性与协调服务

一个**只依赖本地环境**（Python 3.14 + FastAPI，无任何外部服务）的分布式一致性实现。
可以用多个线程（进程内）或多个真实 OS 进程（HTTP）模拟一个 3/5 节点集群，
在节点掉线、崩溃重启、网络分区和请求重试的情况下，对状态变更达成多数派一致，
并且**每一次拒绝都带可区分的错误码与原因**，结论可解释。

## 目录结构

```
raft/
  errors.py        可区分的一致性错误（错误码 + 原因 + 任期）
  types.py         LogEntry / Snapshot / RPC 请求响应 / 节点状态等数据结构
  storage.py       持久化：InMemoryStorage 与 FileStorage（原子落盘 + fsync），
                   快照偏移日志（压缩后旧条目从磁盘消失）
  statemachine.py  确定性 KV 状态机（set/delete/noop），位点必须连续，可快照/恢复
  transport.py     LoopbackTransport（进程内，可注入分区/宕机）与 HttpTransport
  node.py          RaftNode：选举、任期、复制、提交、应用、幂等、鉴权、崩溃恢复、
                   日志压缩（快照）与快照追赶
  cluster.py       InProcessCluster：测试用集群编排与故障注入
  server.py        单节点 FastAPI 服务（客户端 API / Raft RPC / 管理 API）
scripts/
  run_local_cluster.py   一键拉起 3 个 uvicorn 进程
tests/               选举、复制、崩溃重启、分区恢复、幂等、并发、权限、
                     日志压缩与快照追赶、多进程 E2E
```

## 一致性语义与规则

### 1. 主节点选举与任期管理

- 时间轴被划分为单调递增的**任期（term）**。节点因选举超时自增任期成为候选者，
  获得多数派投票后成为主。
- **同一任期最多一个主（选举安全）**：
  - 一个节点在一个任期内只能投一票（先到先得），投票结果随任期持久化；
  - 候选者日志必须至少与投票人一样新（比较最后日志任期、再比较最后日志下标），
    落后节点拿不到选票；
  - 同一任期内若收到另一个自称主的 AppendEntries，会以
    `duplicate_leader_same_term` 拒绝并记录错误日志。
- **被隔离/落后的节点不能写入**：写请求只在主节点处理；一个被隔离的节点即使
  误成候选也凑不齐多数派，复制在超时后以 `not_replicated_timeout` 失败，写入不生效。
- **旧任期请求必须被显式拒绝**（不会被静默忽略或覆盖）：
  - 旧任期 RequestVote / AppendEntries → `stale_term`，响应里带回当前任期帮助纠偏；
  - 节点看到更高任期的请求 → 立即转为 follower 并持久化新任期（日志打印 `TERM-CHANGE`）。

### 2. 复制顺序与提交

- 写请求在主节点上追加为一条日志（下标严格连续），并行复制给 follower。
- **只有多数节点确认持久化后才提交**（N/2+1）。主节点统计各 follower 的
  matchIndex，推进 commitIndex。
- **只直接提交当前任期的条目**（Raft 安全约束）；上一任期的条目通过新主当选时
  立刻追加的一条当前任期 **no-op** 日志被间接提交，保证已提交日志不丢、不被覆盖。
- follower 按 `prevLogIndex/prevLogTerm` 做一致性检查：
  - 缺前序条目 → `missing_prev_entries`，主节点回退 nextIndex 重试；
  - 同下标任期冲突 → 截断该下标及之后的冲突后缀再追加（`log_inconsistent`）；
  - 已存在且任期相同 → 幂等跳过，绝不重复应用。
- commitIndex 只能前进：存储层对任何回退直接抛 `invalid_commit_position`。

### 3. 幂等、重试与客户端顺序

每个写请求带 `client_id` + `request_id` + 单调递增的 `client_seq`：

- **重复提交不产生重复结果**：相同 `(client_id, request_id)` 命中同一条日志、
  返回同一个下标和首次结果（日志打印 `IDEMPOTENT-HIT`）；
- 同一 `request_id` 被复用于**不同命令** → `duplicate_request`（409），防止误用；
- 同一客户端 `client_seq` 回退（过期/乱序/重放请求）→ `stale_client_seq`（409），
  错误体带 `got_seq` / `max_seq`；
- 超时返回 `not_replicated_timeout`，并提示“该条目稍后可能仍会提交，请用同一
  request_id 重试”——重试因此是安全且幂等的。

### 4. 恢复边界（崩溃 / 重启 / 落盘）

- 任期、投票、全部日志、commitIndex 都持久化。FileStorage 用
  “临时文件 + fsync + 原子 rename”整体落盘，崩溃不会产生半写状态。
- **重启恢复**：节点以 follower 身份从磁盘重新加载；状态机从空白开始，把
  `last_applied+1 .. commitIndex` 的已提交日志按序重放（日志打印 `replay`），
  之后再上线追主。重放下标必须严格连续，任何回退/跳变都被
  `invalid_commit_position` 拒绝——**不能从中途继续**。
- **代际（epoch）隔离**：每次崩溃递增代际号，上一代在途的选举/复制/RPC 线程
  发现代际不匹配会自行退出，且重启会重建全新状态机对象，旧线程无法污染新生命周期。
- 空盘节点重启后，通过日志一致性协议从多数派完整追赶，而不是从某个中间态继续。
- 分区恢复后，少数派侧未提交的日志会被多数派的已提交日志截断覆盖，
  各节点日志与状态机最终完全一致。

### 5. 日志压缩（快照）与快照追赶

集群不会无限堆积历史：**已提交且已在本地应用完**的旧日志会被压缩成一份紧凑的
状态快照，压缩后磁盘上不再保留这些旧记录。

- **触发时机**：每当状态机应用推进后检查一次，`last_applied - snapshot_index`
  达到阈值即压缩。阈值通过 `RaftNode(snapshot_threshold=...)` 配置，
  HTTP 进程用环境变量 `RAFT_SNAPSHOT_THRESHOLD`（**默认 100**，`<=0` 关闭自动压缩）。
  压缩只覆盖“已提交 + 已应用”的条目（存储层对覆盖未提交历史的快照直接拒绝），
  且与 apply 串行、临界区短——**压缩期间新写入照常追加与提交**，不会被卡住。
- **快照里装了什么**：
  - 边界 `(last_included_index, last_included_term)`——压缩点；
  - 状态机在压缩点的完整 KV 状态；
  - 幂等去重水位 `(client_id, request_id) -> (index, seq, command, result, term)` 与
    各客户端 `client_seq` 高水位——保证压缩/重启/快照安装之后，
    旧请求重试仍然幂等、过期序列号仍然被拒。
  - FileStorage 把快照单独写到 `{node_id}.snapshot.json`（同样的
    “临时文件 + fsync + 原子 rename”），状态文件里的旧日志条目随即消失。
- **落后节点怎么判断走快照还是普通复制**：由主节点驱动——
  主节点发现某 follower 的 `next_index <= 自己的 snapshot_index`
  （它需要的旧历史本地已压缩掉）时，改发 `InstallSnapshot` RPC
  （带 `snapshot_id` 请求标识）；否则走普通 AppendEntries。
  follower 侧收到 AppendEntries 时若 `prev_log_index` 落在本地快照区内，
  以快照边界为一致性锚点、只追加快照之后的条目；若主的前序点与本地
  **快照边界本身**冲突，则以 `snapshot_boundary_conflict` 明确拒绝且
  **绝不截断快照覆盖区**，等主用快照对齐。
- **与已提交边界的兼容范围**：快照只承载多数派已提交历史，安装快照只会
  让 commitIndex / lastApplied **前进**（状态机拒绝位点回退）；已提交结论在
  压缩与追赶过程中不会被回退或覆盖。重启时节点直接从本地快照恢复状态机与
  幂等水位（日志打印 `RESTORE-SNAPSHOT`），只重放快照边界之后的已提交日志，
  不再从最早那条历史开始。
- **可解释性**：`COMPACT`（压缩边界、丢弃条数）、`SNAPSHOT-SEND`（来源、目标、
  snapshot_id、边界）、`SNAPSHOT-INSTALL` / `SNAPSHOT-SKIP`（接收方、来源、
  snapshot_id、落点）、`RESTORE-SNAPSHOT`（重启恢复边界）都有日志，
  `/status` 输出 `snapshot_index` / `snapshot_term`。

### 6. 并发与权限边界

- 所有易变/持久状态由节点内锁保护，**持锁期间不做网络 IO**；每个 follower
  最多一个在途 AppendEntries 工作线程，避免请求乱序互相覆盖。
- 多客户端并发写：日志下标全局唯一、同一客户端严格按 seq 顺序，结果不重复、
  不乱序、不丢失。
- **内部协调操作三重校验**（以 `POST /admin/step-down` 为例）：
  1. 调用方身份必须是集群成员，否则 `unauthorized`（403）；
  2. 必须带正确的 `X-RAFT-AUTH` 令牌，否则 `unauthorized`（401）；
  3. 请求任期不得旧于当前任期，否则 `stale_term`（409）并回带当前任期。
- 未知节点发来的 Raft RPC 同样以 `unauthorized` 拒绝。

### 错误码一览

| code | 含义 |
| --- | --- |
| `not_leader` | 请求发给了非主/无主（响应含 `leader_id` 提示） |
| `stale_term` | 请求任期比节点当前任期旧 |
| `stale_client_seq` | 同客户端序列号回退（过期/重放请求） |
| `duplicate_request` | request_id 被不同命令复用 |
| `log_inconsistent` / `missing_prev_entries` | 日志一致性检查失败（触发回退/截断） |
| `snapshot_boundary_conflict` | 主的前序点与本地已提交快照边界冲突（等主发快照对齐） |
| `invalid_commit_position` | 提交/应用位点回退或不连续 |
| `not_replicated_timeout` | 超时未获多数派确认（用同一 request_id 重试） |
| `unauthorized` | 身份非成员或令牌错误 |
| `node_down` | 节点处于崩溃态 |

## HTTP API

客户端：

```bash
# 写（幂等）
curl -sX POST localhost:8101/client/command -H 'Content-Type: application/json' -d '{
  "client_id": "c1", "request_id": "r1", "client_seq": 1,
  "command": {"op": "set", "key": "k", "value": 42}}'

# 线性一致读（仅主节点；follower 返回 not_leader + leader_id）
curl -s localhost:8101/client/state/k

# 节点状态（含 snapshot_index / snapshot_term）
curl -s localhost:8101/status
```

内部 Raft RPC 新增 `POST /raft/install-snapshot`（主节点向落后过久的
follower 推送快照，body 带 `snapshot_id` 请求标识与快照边界/数据）。

管理（需令牌）：`/admin/step-down`、`/admin/partition`、`/admin/heal`、
`/admin/crash`、`/admin/restart`，请求头加 `-H 'X-RAFT-AUTH: local-cluster-token'`。

## 本地验证方法

一键启动 3 进程集群并跑一轮演示（选举 → 写入复制 → 主读/follower 拒绝）：

```bash
uv sync
uv run python scripts/run_local_cluster.py --client
# 仅启动集群（前台运行）：
uv run python scripts/run_local_cluster.py
```

节点数据在 `./.raft-data/`，删除即可重置；`--keep-data` 可保留上次数据验证重启恢复。

运行测试（全部为本地、无需外部服务）：

```bash
uv run pytest                       # 全部 30 个用例（含真实多进程 HTTP E2E）
```

测试会通过 `raft` logger 打印**任期变化、提交/应用位点、来源节点与请求标识**，
想看详细决策过程：

```bash
RAFT_TEST_LOG=INFO uv run pytest -k crash        # 崩溃重启
RAFT_TEST_LOG=INFO uv run pytest -k partition    # 分区恢复
RAFT_TEST_LOG=INFO uv run pytest -k snapshot -s  # 压缩边界 / 快照追赶来源与请求标识
RAFT_TEST_LOG=INFO uv run pytest tests/test_http_cluster.py   # 真实多进程
```

复现“节点落后到只能靠快照才追得上”的完整场景（进程内，秒级）：

```bash
RAFT_TEST_LOG=INFO uv run pytest tests/test_snapshot.py::test_lagging_node_catches_up_via_snapshot -s
# 日志里能看到：COMPACT 压缩边界 -> SNAPSHOT-SEND（来源/snapshot_id）->
# SNAPSHOT-INSTALL（落点）-> CATCH-UP-DONE（进度对齐）
```

用真实多进程集群手动复现：把压缩阈值调小再启动，写一批数据触发压缩，
然后 crash 一个 follower、再写一批、restart 它，观察 `/status` 里
`snapshot_index` 从 0 变为与主对齐：

```bash
RAFT_SNAPSHOT_THRESHOLD=5 uv run python scripts/run_local_cluster.py
# 另开终端写 20 个键 -> curl /status 看 snapshot_index 增长
# curl -X POST localhost:8102/admin/crash -H 'X-RAFT-AUTH: local-cluster-token'
# 再写 10 个键 -> restart -> node2 的 snapshot_index 被快照追赶拉齐
```

测试覆盖：

- 选举：单主、旧任期拒绝、非成员拒绝、隔离无法选主/提交；
- 复制：多数派提交、无多数派失败码、幂等重试、request_id 复用冲突、
  序列号回退、位点不可回退；
- 恢复：follower 崩溃重启（FileStorage）、空盘追赶、分区恢复后日志一致；
- 压缩与快照追赶：阈值触发压缩且磁盘旧记录消失、压缩期间写入不中断、
  落后过久节点靠 InstallSnapshot 追平并恢复一致、空盘节点快照追赶、
  重启直接走本地快照恢复、压缩+重启后幂等去重仍生效；
- 并发：8 客户端×10 写不丢不重不乱序、同 request_id 并发只产生一条、
  主节点崩溃切换期间持续写入；
- 权限：令牌/身份/任期三种拒绝原因可区分；
- E2E：3 个 uvicorn 进程，崩溃重启 + 分区恢复全链路。
