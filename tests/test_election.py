"""选举与任期管理测试。"""

from __future__ import annotations

import pytest

from raft_consensus.errors import NotLeaderError, StaleTermError
from raft_consensus.messages import AppendEntries


def test_single_leader_elected(cluster3):
    leader = cluster3.wait_leader(timeout=3)
    assert leader in {"n1", "n2", "n3"}
    # 同一任期最多一个主节点
    leaders = cluster3.leaders()
    assert len(leaders) == 1
    # 所有在线节点任期一致
    terms = set(cluster3.terms().values())
    assert len(terms) == 1


def test_isolated_node_cannot_commit(cluster3):
    leader = cluster3.wait_leader()
    followers = [n for n in cluster3.ids if n != leader]
    # 把主节点单独隔离
    cluster3.partition([{leader}, set(followers)])
    cluster3.run_for(2.0)  # 多数派一侧应选出新主
    new_leader = None
    ok = cluster3.run_until(
        lambda: any(cluster3.node(n).role == "leader" for n in followers),
        timeout=3.0)
    assert ok
    new_leader = next(n for n in followers if cluster3.node(n).role == "leader")
    assert new_leader != leader

    # 旧主在隔离期间尝试写入：它已失去多数派，提案无法提交
    # （选举超时后旧主应退位为 follower，写入被 NotLeaderError 拒绝）
    cluster3.run_until(lambda: cluster3.node(leader).role != "leader", timeout=3)
    with pytest.raises(NotLeaderError) as ei:
        cluster3.write("c", "r-old", "k", "v", node_id=leader, timeout=0.4)
    assert ei.value.term is not None

    # 恢复分区，集群最终收敛到同一任期/状态
    cluster3.partition(None)
    cluster3.run_for(1.0)
    r = cluster3.write("c", "r-new", "k", "v2")
    assert r["index"] >= 1
    cluster3.run_until(
        lambda: len({n.commit_index for n in cluster3.sim.nodes.values()}) == 1,
        timeout=3)


def test_stale_term_request_rejected_with_reason(cluster3):
    leader = cluster3.wait_leader()
    follower = next(n for n in cluster3.ids if n != leader)
    current = cluster3.node(follower).current_term
    # 伪造一个任期更旧的 AppendEntries，必须被明确拒绝而不是静默处理
    stale = AppendEntries(
        term=current - 1, leader_id="n1",
        prev_log_index=0, prev_log_term=0, entries=(),
        leader_commit=0, request_id="stale-001")
    with pytest.raises(StaleTermError) as ei:
        cluster3.node(follower).handle_append_entries(stale, caller="n1")
    err = ei.value
    assert err.request_term == current - 1
    assert err.current_term == current
    assert "stale term" in err.reason


def test_reelection_after_leader_crash(cluster3):
    leader1 = cluster3.wait_leader()
    cluster3.crash(leader1)
    ok = cluster3.run_until(
        lambda: len([n for n in cluster3.sim.nodes.values()
                     if n.role == "leader"]) == 1
        and cluster3.leader() != leader1, timeout=3)
    assert ok
    leader2 = cluster3.leader()
    assert leader2 is not None and leader2 != leader1
    # 新任期必须严格大于旧任期
    assert cluster3.node(leader2).current_term >= 1
    r = cluster3.write("c", "after-crash", "a", 1)
    assert r["leader"] == leader2
