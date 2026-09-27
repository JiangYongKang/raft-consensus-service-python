"""复制、提交位点、幂等重试、崩溃恢复测试。"""

from __future__ import annotations

import time

import pytest

from raft.errors import (
    DuplicateRequestError,
    InvalidCommitError,
    StaleClientRequestError,
)


def _write(leader, client_id, request_id, seq, command, timeout=2.0):
    return leader.submit_command(client_id, request_id, seq, command, timeout=timeout)


def test_write_committed_by_majority(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)  # 等 noop

    r = _write(leader, "c1", "w1", 1, {"op": "set", "key": "x", "value": 1})
    assert r.success
    c.wait_until_applied(r.index)
    for n in c.nodes().values():
        assert n.sm.get("x") == 1
        assert n.status().commit_index >= r.index


def test_no_majority_write_fails_with_distinguishable_code(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    lid = leader.node_id
    others = [nid for nid in c.nodes() if nid != lid]
    time.sleep(0.2)

    # 隔离主节点与全部 follower（主所在组只有 1 票，无法达成多数）
    c.partition([[lid], others])
    time.sleep(0.3)

    if leader.is_leader():
        r = _write(leader, "c1", "w-isolated", 1,
                   {"op": "set", "key": "y", "value": 1}, timeout=0.6)
        assert r.success is False
        assert r.code == "not_replicated_timeout"

    c.heal()
    new_leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)
    # 分区期间未提交的写不能在恢复后冒出来
    assert new_leader.sm.get("y") is None


def test_idempotent_retry_does_not_duplicate(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)

    cmd = {"op": "set", "key": "a", "value": 100}
    r1 = _write(leader, "c1", "dup-1", 1, cmd)
    assert r1.success
    # 相同 client_id+request_id 重复提交：必须返回同一 index，不产生第二条
    r2 = _write(leader, "c1", "dup-1", 1, cmd)
    assert r2.success and r2.index == r1.index
    r3 = _write(leader, "c1", "dup-1", 1, cmd)
    assert r3.index == r1.index

    c.wait_until_committed(r1.index)
    entries = [e for e in leader.storage.all_entries() if e.request_id == "dup-1"]
    assert len(entries) == 1
    # 序列号前进的不同请求正常追加
    r4 = _write(leader, "c1", "dup-2", 2, {"op": "set", "key": "b", "value": 2})
    assert r4.success and r4.index > r1.index


def test_request_id_reuse_with_different_command_is_rejected(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)
    assert _write(leader, "c1", "same-id", 1, {"op": "set", "key": "k", "value": 1}).success
    with pytest.raises(DuplicateRequestError) as ei:
        _write(leader, "c1", "same-id", 2, {"op": "set", "key": "k", "value": 2})
    assert ei.value.code == "duplicate_request"


def test_stale_client_sequence_rejected(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)
    assert _write(leader, "c9", "s5", 5, {"op": "set", "key": "k", "value": 1}).success
    with pytest.raises(StaleClientRequestError) as ei:
        _write(leader, "c9", "s4", 4, {"op": "set", "key": "k", "value": 2})
    assert ei.value.code == "stale_client_seq"
    assert ei.value.got_seq == 4 and ei.value.max_seq == 5
    # 新的更大序列号正常
    assert _write(leader, "c9", "s6", 6, {"op": "set", "key": "k", "value": 3}).success


def test_commit_position_cannot_go_backwards():
    from raft.storage import InMemoryStorage
    s = InMemoryStorage()
    s.save_commit_index(3)
    with pytest.raises(InvalidCommitError):
        s.save_commit_index(2)


def test_follower_crash_and_restart_recovers_state(cluster_factory, tmp_path):
    """节点崩溃重启后恢复一致状态（FileStorage 持久化）。"""
    from raft.cluster import InProcessCluster
    c = InProcessCluster(
        3,
        storage_factory=lambda nid: __import__(
            "raft.storage", fromlist=["FileStorage"]
        ).FileStorage(str(tmp_path), nid),
    )
    c.start()
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)
    follower_id = [nid for nid in c.nodes() if nid != leader.node_id][0]

    r1 = _write(leader, "c1", "p1", 1, {"op": "set", "key": "x", "value": "v1"})
    c.wait_until_committed(r1.index, nodes=[follower_id])

    # 崩溃该 follower
    c.crash_node(follower_id)
    time.sleep(0.1)
    # 集群其余节点继续服务
    r2 = _write(leader, "c1", "p2", 2, {"op": "set", "key": "y", "value": "v2"})
    assert r2.success

    # 重启：从磁盘恢复，并追赶上崩溃期间的新日志
    c.restart_node(follower_id)
    c.wait_until_applied(r2.index, nodes=[follower_id], timeout=3)
    fn = c.get(follower_id)
    assert fn.sm.get("x") == "v1"
    assert fn.sm.get("y") == "v2"
    assert fn.status().last_log_index == leader.status().last_log_index


def test_restarted_node_with_empty_disk_catches_up(cluster_factory):
    """数据盘清空的节点重启后必须从多数派完整追赶，而不是从中途继续。"""
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)
    follower_id = [nid for nid in c.nodes() if nid != leader.node_id][0]

    r1 = _write(leader, "c1", "m1", 1, {"op": "set", "key": "a", "value": 1})
    c.wait_until_committed(r1.index)

    c.crash_node(follower_id)
    c.restart_node(follower_id, fresh_storage=True)
    c.wait_until_applied(r1.index, nodes=[follower_id], timeout=3)
    fn = c.get(follower_id)
    assert fn.sm.get("a") == 1
    assert fn.status().last_log_index == leader.status().last_log_index


def test_partition_recovery_converges(cluster_factory):
    """分区恢复后，少数派侧未提交的写入被覆盖，集群最终一致。"""
    c = cluster_factory(5)
    leader = c.wait_for_leader(timeout=3)
    lid = leader.node_id
    time.sleep(0.2)

    followers = [nid for nid in c.nodes() if nid != lid]
    side_b = followers[:2]
    # 主 + 2 follower 一侧（多数派）；2 follower 另一侧（少数派）
    majority_side = [lid] + followers[2:]
    c.partition([majority_side, side_b])
    time.sleep(0.4)

    # 多数派侧继续提交
    r = _write(leader, "c1", "major", 1, {"op": "set", "key": "color", "value": "blue"})
    assert r.success
    c.wait_until_committed(r.index, nodes=majority_side)

    # 少数派可能选出一个新主（没有多数派，无法提交）
    for nid in side_b:
        n = c.get(nid)
        if n.is_leader():
            rr = _write(n, "c2", "minor", 1,
                        {"op": "set", "key": "color", "value": "red"}, timeout=0.6)
            assert rr.success is False

    c.heal()
    final_leader = c.wait_for_leader(timeout=3)
    time.sleep(0.5)
    for n in c.nodes().values():
        assert n.sm.get("color") in (None, "blue")
    assert final_leader.sm.get("color") == "blue"
    # 所有节点日志完全一致
    logs = {nid: [(e.index, e.term) for e in n.storage.all_entries()]
            for nid, n in c.nodes().items()}
    vals = set(tuple(v) for v in logs.values())
    assert len(vals) == 1
    c.stop()
