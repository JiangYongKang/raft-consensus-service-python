"""内部协调操作的身份 / 令牌 / 任期校验测试。"""

from __future__ import annotations

import time

import pytest

from raft.errors import StaleTermError, UnauthorizedError


def _step_down(node, caller, term, token):
    return node.internal_step_down(caller, term, token)


def test_step_down_requires_correct_token(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    term = leader.status().term
    with pytest.raises(UnauthorizedError) as ei:
        _step_down(leader, "n2", term, "wrong-token")
    assert ei.value.code == "unauthorized"
    # 被拒绝后仍然是主
    assert leader.is_leader()


def test_step_down_requires_known_caller(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    term = leader.status().term
    with pytest.raises(UnauthorizedError) as ei:
        _step_down(leader, "stranger-node", term, "local-cluster-token")
    assert ei.value.code == "unauthorized"
    assert leader.is_leader()


def test_step_down_rejects_stale_term(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    term = leader.status().term
    with pytest.raises(StaleTermError) as ei:
        _step_down(leader, "n2", max(0, term - 1), "local-cluster-token")
    assert ei.value.code == "stale_term"
    assert ei.value.term == term
    assert leader.is_leader()


def test_step_down_valid_then_reelection(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    term = leader.status().term
    out = _step_down(leader, "n2", term, "local-cluster-token")
    assert out["accepted"] is True
    assert out["role"] == "follower"
    assert not leader.is_leader()
    # 集群应在更高任期重新选出主
    new_leader = c.wait_for_leader(timeout=3)
    assert new_leader.status().term >= term
