# 本地 Raft 一致性与协调服务

一个**只依赖本地环境**（Python 3.14 + FastAPI，无任何外部服务）的分布式一致性实现。
可以用多个线程（进程内）或多个真实 OS 进程（HTTP）模拟一个 3/5 节点集群，
在节点掉线、崩溃重启、网络分区和请求重试的情况下，对状态变更达成多数派一致，
并且**每一次拒绝都带可区分的错误码与原因**，结论可解释。

## 目录结构

```
raft/
  errors.py        可区分的一致性错误（错误码 + 原因 + 任期）
  types.py         LogEntry / RPC 请求响应（含 InstallSnapshot）/ 节点状态等数据结构
  storage.py       持久化：InMemoryStorage 与 FileStorage（原子落盘 + fsync），
                   日志压缩（base index/term）与快照存取
  statemachine.py  确定性 KV 状态机（set/delete/noop），位点必须连续，可安装快照
  transport.py     LoopbackTransport（进程内，可注入分区/宕机）与 HttpTransport
  node.py          RaftNode：选举、任期、复制、提交、应用、幂等、鉴权、崩溃恢复、
                   日志压缩与快照追赶
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

### 5. 日志压缩与快照追赶

**什么时候触发压缩**：节点每应用完一批已提交日志就检查一次，
`last_applied - snapshot_index >= snapshot_threshold` 时把截至 `last_applied`
的历史压缩成一份快照。阈值可配置：

- 进程内/单测：`RaftNode(..., snapshot_threshold=N)`（默认 100，<=0 关闭）；
- 多进程 HTTP：环境变量 `RAFT_SNAPSHOT_THRESHOLD`（默认 100）。

压缩只覆盖**已提交且已应用**的边界，因此已生效结果不会丢、不重、不回退；
压缩在应用循环之外瞬时完成，新写入照常提交，节点不会被卡住。

**快照里装了什么**（FileStorage 落 `{node_id}.snapshot.json`，原子写入）：

- `last_included_index` / `last_included_term`：快照覆盖的日志边界（含该条）；
- `snapshot_id`：本次快照的唯一标识，用于全链路日志追踪；
- 状态机全量数据（KV）；
- 幂等元数据：`(client_id, request_id) -> (index, command, client_seq, term)`、
  各客户端 `client_seq` 上限、已应用结果——保证压缩与重启之后重复请求
  仍命中首次结果，已提交结论不可被回退或覆盖。

压缩后，边界及之前的日志条目从存储中删除（FileStorage 主状态文件只保留
边界之后的条目），磁盘占用不再随历史无限增长；日志逻辑起点变为
`(snapshot_index, snapshot_term)`，位点比较、追加连续性检查都以它为基准。

**落后节点怎么判断走快照还是普通复制**：由主节点按每个 follower 的
`nextIndex` 决定——

- `nextIndex > snapshot_index`：需要的条目还在日志里，走普通 AppendEntries；
- `nextIndex <= snapshot_index`：需要的旧历史已被压缩，改发
  `InstallSnapshot`（整份快照 + 边界 + snapshot_id）。

follower 收到快照时：旧任期拒绝；不比本地快照新的幂等跳过
（`already_covered`）；否则先推进持久化的 commitIndex、落快照、压缩日志，
再安装状态机状态（位点只前进不后退）。若快照边界条目与本地日志任期冲突，
边界之后的本地日志整段作废——那些条目必然未被提交。装完快照后，
边界之后的新历史仍走普通 AppendEntries 补齐。follower 收到与本地快照
边界重叠的 AppendEntries 时，会先裁掉被覆盖的前缀再做一致性检查，
不会接错中间某一段。

**本地重启恢复**：节点重启时先加载本地快照直接恢复状态机与幂等元数据
（日志打印 `SNAPSHOT-RESTORE`），只重放快照边界之后的已提交日志，
不再从最早那条历史开始。

**可解释性**：压缩边界（`SNAPSHOT-COMPACT boundary=(i,t) threshold=N`）、
追赶来源与请求标识（`SNAPSHOT-SEND to=X snapshot_id=...` /
`SNAPSHOT-INSTALL source=Y snapshot_id=...`）、本地恢复
（`SNAPSHOT-RESTORE`）都能在日志里直接看出某个节点为什么停在这个进度上；
`/status` 也暴露 `snapshot_index` / `snapshot_term`。

**与已提交边界的兼容范围**：快照只覆盖已提交位点及其之前的历史；
commitIndex、last_applied 在压缩与快照追赶全程只前进不回退；
存储层依然对任何回退抛 `invalid_commit_position`；
被截断的未提交条目的幂等记录会同步清除，已提交条目的幂等记录随快照永久保留。

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

# 节点状态
curl -s localhost:8101/status
```

管理（需令牌）：`/admin/step-down`、`/admin/partition`、`/admin/heal`、
`/admin/crash`、`/admin/restart`，请求头加 `-H 'X-RAFT-AUTH: local-cluster-token'`。

节点间 Raft RPC 新增 `POST /raft/install-snapshot`（主节点向落后过多的
follower 发送整份快照）；`/status` 响应含 `snapshot_index` / `snapshot_term`。

## 本地验证方法

一键启动 3 进程集群并跑一轮演示（选举 → 写入复制 → 主读/follower 拒绝）：

```bash
uv sync
uv run python scripts/run_local_cluster.py --client
# 仅启动集群（前台运行）：
uv run python scripts/run_local_cluster.py
```

节点数据在 `./.raft-data/`，删除即可重置；`--keep-data` 可保留上次数据验证重启恢复。

复现日志压缩与快照追赶（多进程）：把阈值调小，写够数量后观察
`.raft-data/` 下出现 `*.snapshot.json`、主状态文件中的 `log` 被截短，
节点日志出现 `SNAPSHOT-COMPACT`；再清空某个节点的数据文件并重启，
观察 `SNAPSHOT-SEND` / `SNAPSHOT-INSTALL` 完成快照追赶：

```bash
RAFT_SNAPSHOT_THRESHOLD=5 uv run python scripts/run_local_cluster.py --client
```

运行测试（全部为本地、无需外部服务）：

```bash
uv run pytest                       # 全部 33 个用例（含真实多进程 HTTP E2E）
```

测试会通过 `raft` logger 打印**任期变化、提交/应用位点、来源节点与请求标识**，
想看详细决策过程：

```bash
RAFT_TEST_LOG=INFO uv run pytest -k crash        # 崩溃重启
RAFT_TEST_LOG=INFO uv run pytest -k partition    # 分区恢复
RAFT_TEST_LOG=INFO uv run pytest -k snapshot     # 日志压缩与快照追赶
RAFT_TEST_LOG=INFO uv run pytest tests/test_http_cluster.py   # 真实多进程
```

测试覆盖：

- 选举：单主、旧任期拒绝、非成员拒绝、隔离无法选主/提交；
- 复制：多数派提交、无多数派失败码、幂等重试、request_id 复用冲突、
  序列号回退、位点不可回退；
- 恢复：follower 崩溃重启（FileStorage）、空盘追赶、分区恢复后日志一致；
- 日志压缩：阈值触发、旧条目落盘移除、压缩期间写入不丢不重、
  压缩后幂等重试仍命中首次结果；
- 快照追赶：离线过久/空盘节点只能靠快照追上并断言最终一致、
  轻微落后仍走普通复制、已提交结论不被快照回退、本地快照重启恢复、
  幂等元数据随快照跨重启保留；
- 并发：8 客户端×10 写不丢不重不乱序、同 request_id 并发只产生一条、
  主节点崩溃切换期间持续写入；
- 权限：令牌/身份/任期三种拒绝原因可区分；
- E2E：3 个 uvicorn 进程，崩溃重启 + 分区恢复全链路。
