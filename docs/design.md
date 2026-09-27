# 本地 Raft 一致性与协调服务 — 设计说明

本项目在**单进程内用多个节点实例**模拟一个分布式集群，提供主节点选举、任期
管理、日志复制、崩溃恢复、网络分区、幂等重试与线性一致读写。仅依赖本地环境
（Python 3.14 + FastAPI/pytest），不连接任何外部服务。

- 节点：`raft_consensus/node.py` 中的 `RaftNode`
- 持久化：`raft_consensus/storage.py`（`state.json` + `log.jsonl`，fsync）
- 复制日志：`raft_consensus/log.py`
- 状态机：`raft_consensus/statemachine.py`（确定性 KV）
- 网络/时钟：`raft_consensus/network.py`、`raft_consensus/simulation.py`
- 集群门面：`raft_consensus/cluster.py`（后台单线程事件泵 + 每节点任务队列）
- 端到端演示：`raft_consensus/demo.py`

---

## 1. 选举规则与任期管理

- 每个节点持久化保存 `current_term`（当前任期）与 `voted_for`（本任期已投给谁）。
- 节点角色为 `follower / candidate / leader`。选举超时
  （默认 150–300ms 随机，可在 `NodeConfig` 配置）未收到合法主节点消息，即自增
  任期、转为候选人、先投自己，并向其余节点发 `RequestVote`。
- 获得**多数派**选票（`N//2+1`）后成为主节点，同一任期立即提交一条
  `noop` 空日志并周期性（默认 50ms）广播心跳/`AppendEntries`。
- **同一任期最多一个主节点**：一个节点在同一任期只投一张票，且候选人日志必须
  "至少和投票者一样新"（先比最后任期，再比最后位点，论文 §5.4.1）才可能得票。
- 被隔离或落后的节点：
  - 处于少数派分区的领导者无法获得多数确认，**不能提交新写入**；选举超时后
    退回 follower，此后写入被 `NotLeaderError` 明确拒绝。
  - 任何节点收到比自己 `current_term` 更高的任期请求，立即更新任期并退回
    follower；**任期更旧的请求一律拒绝**，抛 `StaleTermError`，原因中携带
    `request_term < current_term`，绝不被静默忽略或覆盖。
- 选举/任期变化都有日志，例如：
  `[term] 3 -> 4 (become follower, leader=n2)`、
  `[leader] n4 ELECTED leader for term=4`。

## 2. 复制顺序与多数派提交

- 客户端写只由主节点处理：追加一条 `LogEntry(term, index, client_id,
  request_id, command)` 到本地日志并持久化，再并行发 `AppendEntries`。
- **只有多数节点确认持久化后才算生效**：主节点统计各跟随者 `match_index`，
  对当前任期的日志，一旦某个位点获得多数匹配，就推进 `commit_index` 并按
  **严格位点顺序** `last_applied+1 … commit_index` 应用到状态机（论文 §5.4.2，
  不会直接提交旧任期间接复制的条目）。
- 跟随者收到 `AppendEntries` 先做前序一致性检查，再把新日志**先 fsync 落盘**
  然后才回复成功；随后按 `leader_commit` 推进并应用。
- **冲突回退（论文 §5.3 的 XTerm/XIndex/XLen 快速回退）**：
  - 跟随者缺少前序位点 → 返回 `follower_last_index`，主节点跳到其后；
  - 同位点任期冲突 → 返回冲突任期 `conflict_term` 与其在跟随者处的首条位点
    `conflict_first_index`；主节点若有该任期则退到该任期最后位点之后，否则
    退到冲突任期首条位点之前。每轮 `next_index` **严格单调下降**，有限步收敛。
- **位点回退保护（恢复边界）**：任何试图删除/覆盖 `index ≤ commit_index`
  已提交条目的操作，都会抛 `CommittedRollbackError` 被明确拒绝，
  不允许"从中途继续"式地静默改写已决历史。
- 应用日志时打印决策位点、来源与请求标识：
  `[apply] node=n3 term=2 decision_index=5 req=req-002 src_client=client-1 ...`

## 3. 幂等与重复请求

- 以 `(client_id, request_id)` 作为客户端请求的唯一标识，跟随日志复制并持久化。
- 已提交的重复请求：命中 `applied_requests`，直接返回**首次决策位点与结果**，
  不会再次执行，也不会再产生日志条目。
- 在途的重复请求：复用同一个 `Proposal`，多个等待者共享一个结果。
- 因此请求重试 / 网络重传（同一 request_id）**至多生效一次**，不会重复扣款式
  地产生多个结果；不同 request_id 即使内容相同也被视为不同写。

## 4. 崩溃恢复边界

- 落盘文件（每个节点一个目录）：
  - `state.json`：`{term, voted_for, commit_index}`，临时文件 + 原子 rename；
  - `log.jsonl`：每行一条日志，追加后 `fsync`；冲突截断/整写同样原子替换；
    目录项也 `fsync`，崩溃后不留半成品。
- 重启 = 用同目录**重新构造一个全新节点实例**：
  1. 读取 `term/voted_for/commit_index` 与全部日志；
  2. 重放 `1 … commit_index` 到 KV 状态机，并重建幂等结果表；
  3. 未提交的尾日志保留在本地，由新主节点的复制对账处理（冲突则被覆盖，
     但**已提交区间永不回退**）。
- 多数派之外的未提交写入：既不会推进 `commit_index`，也不会出现在任何节点
  的已应用状态中（测试 `test_uncommitted_tail_not_visible_after_partial_crash`
  专门覆盖）。

## 5. 并发与权限边界

- **并发模型**：所有 Raft 事件（定时器、RPC、客户端任务）都在集群的一条后台
  事件泵线程上串行执行；多客户端线程通过"每节点任务队列"提交。节点内部因此
  无需加锁，日志追加/提交/应用天然串行，结果**不乱序、不重复、不丢失**。
- 提交推进时只统计**当前网络可达**的跟随者（分区/离线节点的旧 `match_index`
  不计入多数），从机制上杜绝"隔离主用过期票数误提交"。
- **内部协调类操作的身份与任期校验**（`dispatch` 入口）：
  - 消息来源 `src` 必须是集群成员，否则回 `UnknownCallerError`；
  - 载荷声明的 `candidate_id / leader_id` 必须与消息来源一致，防止一个节点
    冒充另一个节点发协调请求；
  - 请求任期旧于本地 → `StaleTermError`；同任期重复投票 →
    `AlreadyVotedError`；日志不一致 → `LogCheckMismatchError`（带回退提示）；
    覆盖已提交 → `CommittedRollbackError`。
  - 所有拒绝都携带**接收方当前任期**与**文字原因**，调用方可以区分失败种类，
    而不是靠字符串匹配或面对静默失败。
- 客户端身份：写/读必须带非空 `client_id`，否则 `UnknownCallerError`；
  非主节点写/读返回 `NotLeaderError(term, leader_id)` 提示当前主。
- 线性一致读：主节点用 ReadIndex 轻量实现——向跟随者发一轮空心跳确认自己仍与
  多数派保持联系，再返回本地已应用状态，不会读到陈旧/未提交数据。

## 6. 失败原因一览（可区分）

| 异常 | 触发场景 |
| --- | --- |
| `NotLeaderError` | 向非主节点写/读，或旧主退位后在途提案被终结（带 leader 提示） |
| `StaleTermError` | 请求携带的任期旧于接收方当前任期 |
| `AlreadyVotedError` | 同一任期已投给其他候选人 |
| `LogCheckMismatchError` | 前序位点/任期不一致（带 XTerm/XIndex/XLen 回退提示） |
| `CommittedRollbackError` | 试图覆盖/回退已提交日志区间 |
| `DuplicateRequestError` | 显式重复检测冲突（正常重试走幂等命中，返回首次结果） |
| `UnknownCallerError` | 内部 RPC 来源不在集群/与载荷身份不符，或客户端身份为空 |
| `NodeOfflineError` | 目标节点崩溃离线，其排队任务被失败终结 |

## 7. 本地验证方法

```bash
# 环境（已提供 uv.lock）
uv sync

# 1) 端到端演示：选举 / 多数派写入 / 幂等重试 / 崩溃重启 / 网络分区愈合 / 并发写
uv run python main.py --demo

# 2) 单元测试（默认会打印任期变化、决策位点、来源节点、请求标识）
uv run pytest                      # 全部
uv run pytest -s -o log_cli=true -o log_cli_level=INFO   # 实时查看 Raft 日志
uv run pytest tests/test_replication.py                 # 只跑复制/幂等
uv run pytest tests/test_recovery.py                    # 崩溃/分区/恢复
uv run pytest tests/test_concurrency.py                 # 并发
uv run pytest tests/test_permissions.py                 # 身份与任期权限

# 3) HTTP 健康检查（可选）
uv run python main.py             # 然后访问 http://127.0.0.1:8000/health
```

数据目录默认使用临时目录（演示）或 pytest 的 `tmp_path`，不会污染工作区。

## 8. 范围与取舍

- 本实现用于在本地清晰、可解释地演示和测试 Raft 一致性，采用单进程多实例 +
  虚拟时钟事件循环；网络分区、消息丢弃、节点崩溃都通过模拟器确定性注入。
- 未实现成员变更、快照/日志压缩、PreVote 与真实多机网络（TCP）。这些不影响
  对"单一主、多数派提交、恢复一致、权限与幂等"核心语义的验证；如需扩展，
  `Network` 抽象可替换为真实传输，`PersistentStore` 已隔离落盘细节。
