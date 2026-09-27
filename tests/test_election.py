"""选举与任期管理测试。"""

from __future__ import annotations

import time

from raft.types import RequestVoteRequest


def test_single_leader_per_term(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    assert leader is not None
    term = leader.status().term
    assert term >= 1
    leaders = [n for n in c.nodes().values() if n.status().role.value == "leader"
               and n.status().term == term]
    assert len(leaders) == 1


def test_stale_term_request_vote_is_rejected(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    current_term = leader.status().term

    # 伪造一个旧任期的 RequestVote，来源为不存在的节点
    resp = leader.handle_request_vote(
        RequestVoteRequest(term=current_term - 1, candidate_id="n1",
                           last_log_index=0, last_log_term=0)
    )
    assert resp.vote_granted is False
    assert resp.reason == "stale_term"
    assert resp.term == current_term  # 返回当前任期，帮助调用方纠偏


def test_unknown_caller_is_unauthorized(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    import pytest
    from raft.errors import UnauthorizedError
    with pytest.raises(UnauthorizedError):
        leader.handle_request_vote(
            RequestVoteRequest(term=1, candidate_id="attacker",
                               last_log_index=99, last_log_term=99)
        )


def test_split_vote_then_new_term_leader(cluster_factory):
    # 5 节点：隔离两个节点，多数派 3 个仍能选主
    c = cluster_factory(5)
    c.isolate("n4")
    c.isolate("n5")
    leader = c.wait_for_leader(timeout=3)
    assert leader is not None
    isolated = c.get("n4")
    # 被隔离节点不应成为主
    time.sleep(0.3)
    assert isolated.status().role.value != "leader"
    c.heal()
    time.sleep(0.5)
    leaders = [n for n in c.nodes().values() if n.is_leader()]
    assert len(leaders) == 1


def test_isolated_node_cannot_commit_writes(cluster_factory):
    """被隔离/落后的节点在隔离期间不得接受写入。"""
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)
    follower = [n for n in c.nodes().values() if not n.is_leader()][0]
    c.isolate(follower.node_id)
    time.sleep(0.3)

    # 直接向被隔离的 follower 提交 -> not_leader
    resp = follower.submit_command("c", "r1", 1, {"op": "set", "key": "k", "value": 1})
    assert resp.success is False
    assert resp.code == "not_leader"

    # 隔离期间它也可能自举为候选，但无法形成多数派；
    # 即便它误以为自己是主，多数派复制也会超时失败
    if follower.is_leader():
        resp2 = follower.submit_command("c", "r2", 2, {"op": "set", "key": "k", "value": 2},
                                        timeout=0.6)
        assert resp2.success is False
        assert resp2.code == "not_replicated_timeout"

    c.heal()
    new_leader = c.wait_for_leader(timeout=3)
    assert new_leader is not None
