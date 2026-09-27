"""复制与幂等测试。"""

from __future__ import annotations

import pytest

from raft_consensus.errors import CommittedRollbackError
from raft_consensus.log import LogEntry
from raft_consensus.messages import AppendEntries


def test_write_committed_on_majority(cluster3):
    cluster3.wait_leader()
    r = cluster3.write("c1", "w1", "k", "v1")
    assert r["index"] == 2  # index1 是 leader 上任 noop
    assert r["result"]["old"] is None
    # 多数确认后，所有存活节点最终收敛
    assert cluster3.run_until(
        lambda: all(n.commit_index >= r["index"]
                    for n in cluster3.sim.nodes.values()), timeout=3)
    for nid, st in cluster3.state_on_each().items():
        assert st["state"] == {"k": "v1"}, nid


def test_duplicate_submission_no_duplicate_effect(cluster3):
    leader = cluster3.wait_leader()
    node = cluster3.node(leader)
    r1 = cluster3.write("cA", "req-x", "key1", 1)
    # 完全相同的 (client_id, request_id) 重试
    r2 = cluster3.write("cA", "req-x", "key1", 1)
    assert r1["index"] == r2["index"], "重试必须复用首次决策位点"
    assert r2["result"] == r1["result"]
    # 日志中该请求只有一条
    entries = [e for e in node.log.entries if e.client_id == "cA"]
    assert len(entries) == 1
    assert entries[0].request_id == "req-x"
    # 幂等表只登记一次
    assert ("cA", "req-x") in node.applied_requests


def test_different_request_ids_are_distinct(cluster3):
    cluster3.wait_leader()
    a = cluster3.write("cA", "r1", "k", 1)
    b = cluster3.write("cA", "r2", "k", 2)
    assert a["index"] != b["index"]
    assert cluster3.read("k") == 2


def test_committed_rollback_explicitly_rejected(cluster3):
    leader = cluster3.wait_leader()
    follower = next(n for n in cluster3.ids if n != leader)
    cluster3.write("c", "w1", "a", 1)
    cluster3.run_until(
        lambda: cluster3.node(follower).commit_index >= 2, timeout=3)
    fn = cluster3.node(follower)
    # 试图用旧任期日志覆盖已提交位点 1：必须抛 CommittedRollbackError
    bad = [LogEntry(term=99, index=1, client_id="z",
                    request_id="forged", command={"op": "set",
                                                  "key": "a", "value": 666})]
    with pytest.raises(CommittedRollbackError) as ei:
        fn.log.match_and_truncate(prev_index=0, prev_term=0,
                                  new_entries=bad, commit_index=fn.commit_index)
    assert ei.value.commit_index >= 1
    # 状态未被污染
    assert fn.sm.get("a") == 1


def test_follower_catches_up_after_lag(cluster3):
    """落后节点通过一致性检查 + 回退提示快速补齐，不允许从中途乱续。"""
    leader = cluster3.wait_leader()
    laggard = cluster3.ids[0] if cluster3.ids[0] != leader else cluster3.ids[1]
    cluster3.crash(laggard)
    cluster3.write("c", "w1", "k1", 1)
    cluster3.write("c", "w2", "k2", 2)
    cluster3.restart(laggard)
    cluster3.run_for(1.5)
    assert cluster3.run_until(
        lambda: cluster3.node(laggard).sm.snapshot()
               == {"k1": 1, "k2": 2}, timeout=3)
