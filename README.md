# 本地分布式一致性与协调服务（Python Raft）

在**单进程内用多个节点实例**模拟一个 Raft 集群，不依赖任何外部服务，即可在本地
验证：主节点选举、任期管理、多数派复制、节点崩溃重启、网络分区与愈合、请求幂等
重试、多客户端并发写入。各节点对状态变更最终达成一致，且所有失败都有**可区分、
可解释的原因**。

## 能力一览

- **选举与任期**：随机超时发起选举，同任期至多一个主；旧任期请求被
  `StaleTermError` 拒绝并说明原因；隔离/落后节点不能提交写入。
- **复制与恢复**：多数节点 fsync 确认后才提交，严格按位点顺序应用；
  `(client_id, request_id)` 幂等去重；重启从 `state.json` + `log.jsonl`
  重放恢复；覆盖已提交区间抛 `CommittedRollbackError`。
- **冲突协商**：实现论文 XTerm/XIndex/XLen 快速回退，分区愈合后落后节点
  有限步内追平。
- **并发与权限**：单线程事件泵 + 每节点任务队列，多客户端并发不乱序/不重复/
  不丢失；内部 RPC 校验来源身份与载荷身份、当前任期；读走 ReadIndex 线性一致。
- **可观测**：日志打印任期变化、决策位点、来源节点与请求标识。

## 快速开始

```bash
uv sync

# 端到端演示（约几秒，虚拟时钟）
uv run python main.py --demo

# 测试
uv run pytest
uv run pytest -s -o log_cli=true -o log_cli_level=INFO   # 带实时 Raft 日志

# HTTP 健康检查（可选）
uv run python main.py            # GET http://127.0.0.1:8000/health
```

`--demo` 依次演示：选举唯一主 → 多数派写入 → 同请求幂等重试 → 主崩溃后重选并
重启追平 → 网络分区下少数派写入被拒绝、愈合后收敛 → 多客户端并发写无丢失无重复。

## 目录结构

```
raft_consensus/
  errors.py         可区分的失败原因（旧任期/非主/重复投票/日志冲突/已提交回退/越权...）
  log.py            复制日志纯逻辑（一致性检查、冲突截断、已提交保护）
  storage.py        JSON/JSONL 持久化（原子写 + fsync），重启恢复
  statemachine.py   确定性 KV 状态机
  messages.py       RequestVote / AppendEntries 等 RPC 消息
  network.py        崩溃丢弃、网络分区模拟
  simulation.py     离散事件循环（虚拟/实时时钟、崩溃代际失效）
  node.py           RaftNode：选举、复制、提交、幂等、权限校验
  cluster.py        LocalCluster：多实例集群门面 + 后台事件泵 + 客户端 API
  demo.py           端到端演示场景
tests/              选举 / 复制 / 恢复 / 并发 / 权限 / 日志可解释性
docs/design.md      选举规则、复制顺序、恢复边界、权限约束、验证方法
```

## 在代码中使用

```python
from raft_consensus.cluster import LocalCluster

cluster = LocalCluster(3, data_root="./.cluster-data")  # 3 节点
cluster.start()
leader = cluster.wait_leader()

r = cluster.write("client-1", "req-001", "account/a", 100)
# r = {"index": 2, "term": 1, "leader": "n3", "result": {...}}

cluster.write("client-1", "req-001", "account/a", 100)   # 同请求重试 -> 复用同一位点

cluster.crash(leader)        # 故障注入：崩溃
cluster.restart(leader)      # 从磁盘恢复
cluster.partition([{"n1"}, {"n2", "n3"}])  # 网络分区
cluster.partition(None)      # 愈合
cluster.state_on_each()      # 各节点角色/任期/位点/状态快照
```

失败都以 `raft_consensus.errors` 中的具名异常表达，详见
[`docs/design.md`](docs/design.md)。
