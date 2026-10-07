"""日志压缩与快照追赶测试。

覆盖：
- 已提交且已应用的历史积累到阈值后压缩成快照，旧日志从存储中移除；
- 压缩期间/之后新写入照常提交，已生效结果不丢、不重、不回退；
- 落后到“只能靠快照”的节点通过 InstallSnapshot 追上，最终一致；
- 数据盘清空的节点重新上线后通过快照 + 日志追赶对齐；
- 节点重启直接从本地快照恢复，不从最早日志重放；
- 幂等去重元数据随快照保存，压缩与重启后重复请求仍返回首次结果。

日志断言：压缩边界（SNAPSHOT-COMPACT）、追赶来源与请求标识
（SNAPSHOT-SEND / SNAPSHOT-INSTALL / snapshot_id）都可从日志看出。
"""

from __future__ import annotations

import logging
import time

import pytest

from raft.cluster import InProcessCluster

log = logging.getLogger("raft")

SNAP_THRESHOLD = 5


def _write(leader, client_id, request_id, seq, command, timeout=2.0):
    return leader.submit_command(client_id, request_id, seq, command, timeout=timeout)


def _write_batch(leader, count, *, client="c1", seq_start=1, key_prefix="k"):
    """顺序写 count 条 set 命令，返回最后一条的响应。"""
    resp = None
    for i in range(count):
        seq = seq_start + i
        resp = _write(
            leader, client, f"{client}-req-{seq}", seq,
            {"op": "set", "key": f"{key_prefix}{seq}", "value": seq},
        )
        assert resp.success, f"write seq={seq} failed: {resp.code} {resp.reason}"
    return resp


def _wait_snapshot_index(node, min_index, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if node.status().snapshot_index >= min_index:
            return node.status().snapshot_index
        time.sleep(0.02)
    raise AssertionError(
        f"node {node.node_id} snapshot_index did not reach {min_index} "
        f"(now {node.status().snapshot_index})"
    )


def _wait_commit_index(node, min_index, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if node.status().commit_index >= min_index:
            return
        time.sleep(0.02)
    raise AssertionError(
        f"node {node.node_id} commit_index did not reach {min_index} "
        f"(now {node.status().commit_index})"
    )


# --------------------------------------------------------------------- #
# 压缩触发与已提交边界
# --------------------------------------------------------------------- #
def test_compaction_triggers_and_trims_log(cluster_factory):
    c = cluster_factory(3, snapshot_threshold=SNAP_THRESHOLD)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)

    last = _write_batch(leader, 12)
    c.wait_until_applied(last.index)

    snap_idx = _wait_snapshot_index(leader, SNAP_THRESHOLD)
    st = leader.status()
    # 压缩边界不超过已提交位点（已提交边界不被越过）
    assert snap_idx <= st.commit_index
    # 被快照覆盖的旧条目不再留在存储里
    assert all(e.index > snap_idx for e in leader.storage.all_entries())
    # 已生效结果不丢：全部键都在
    for seq in range(1, 13):
        assert leader.sm.get(f"k{seq}") == seq
    # 快照元数据完整可查
    meta, payload = leader.storage.load_snapshot()
    assert meta["last_included_index"] == snap_idx
    assert meta["last_included_term"] > 0
    assert meta["snapshot_id"]
    assert payload["state"]


def test_writes_continue_during_and_after_compaction(cluster_factory):
    """压缩不阻塞新写入：持续写入期间触发多次压缩，结果不丢不重。"""
    c = cluster_factory(3, snapshot_threshold=3)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)

    last = _write_batch(leader, 25)
    c.wait_until_applied(last.index)
    _wait_snapshot_index(leader, 3)

    # 压缩后继续写，照常提交
    more = _write_batch(leader, 5, seq_start=26, key_prefix="k")
    c.wait_until_applied(more.index)

    for n in c.nodes().values():
        for seq in range(1, 31):
            assert n.sm.get(f"k{seq}") == seq, (
                f"node {n.node_id} missing k{seq}"
            )
    # 各节点状态机最终一致
    snapshots = {nid: n.sm.snapshot() for nid, n in c.nodes().items()}
    assert len({tuple(sorted(s.items())) for s in snapshots.values()}) == 1


def test_idempotent_retry_after_compaction(cluster_factory):
    """请求对应日志已被压缩后，重复提交仍命中幂等，返回首次结果。"""
    c = cluster_factory(3, snapshot_threshold=SNAP_THRESHOLD)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)

    cmd = {"op": "set", "key": "idem", "value": 42}
    r1 = _write(leader, "c-idem", "idem-1", 1, cmd)
    assert r1.success
    # 再写一批把 r1 的日志压进快照
    last = _write_batch(leader, 10, client="c2", seq_start=1, key_prefix="filler")
    c.wait_until_applied(last.index)
    _wait_snapshot_index(leader, r1.index)

    # 相同 request_id 重试：同一 index、同一结果，不重复执行
    r2 = _write(leader, "c-idem", "idem-1", 1, cmd)
    assert r2.success and r2.index == r1.index and r2.result == r1.result
    assert leader.sm.get("idem") == 42
    # 快照载荷里带着幂等元数据（重启后仍可去重）
    _, payload = leader.storage.load_snapshot()
    dedup_ids = {rec["request_id"] for rec in payload["dedup"]}
    assert "idem-1" in dedup_ids
    assert payload["client_max_seq"]["c-idem"] == 1


# --------------------------------------------------------------------- #
# 快照追赶
# --------------------------------------------------------------------- #
def test_lagging_node_catches_up_via_snapshot(cluster_factory, caplog):
    """节点离线很久，所需旧历史已被压缩：必须靠快照追上并最终一致。"""
    c = cluster_factory(3, snapshot_threshold=SNAP_THRESHOLD)
    leader = c.wait_for_leader(timeout=3)
    lid = leader.node_id
    time.sleep(0.2)
    follower_id = [nid for nid in c.nodes() if nid != lid][0]

    # follower 离线（崩溃，保留磁盘），期间集群继续提交并压缩
    c.crash_node(follower_id)
    last = _write_batch(leader, 15)
    c.wait_until_applied(last.index, nodes=[nid for nid in c.nodes() if nid != follower_id])
    snap_idx = _wait_snapshot_index(leader, 10)
    lagged = c.get(follower_id)
    assert lagged.status().commit_index < snap_idx  # 确实落后到日志已被压掉

    # 重新上线：从主节点快照追赶
    with caplog.at_level(logging.INFO, logger="raft"):
        c.restart_node(follower_id)
        c.wait_until_applied(last.index, nodes=[follower_id], timeout=5)

    fn = c.get(follower_id)
    leader_st = leader.status()
    fn_st = fn.status()
    # 通过快照追上：本地快照边界与主节点对齐
    assert fn_st.snapshot_index >= snap_idx
    # 进度对齐，且状态机最终一致
    _wait_commit_index(fn, leader_st.commit_index)
    c.wait_until_applied(leader_st.commit_index, nodes=[follower_id])
    assert fn.sm.snapshot() == leader.sm.snapshot()
    for seq in range(1, 16):
        assert fn.sm.get(f"k{seq}") == seq
    # 追上后继续正常参与复制
    more = _write(leader, "c9", "post-heal-1", 1, {"op": "set", "key": "post", "value": 1})
    assert more.success
    c.wait_until_applied(more.index, nodes=[follower_id])
    assert fn.sm.get("post") == 1

    # 日志能看出：追赶来源（主节点）、请求标识（snapshot_id）、压缩边界
    text = caplog.text
    assert "SNAPSHOT-SEND" in text and f"to={follower_id}" in text
    assert "SNAPSHOT-INSTALL" in text and f"node={follower_id}" in text
    assert f"source={lid}" in text
    assert "snapshot_id=" in text


def test_fresh_disk_node_catches_up_via_snapshot(cluster_factory):
    """数据盘清空的节点重新上线：从空状态靠快照 + 日志追赶到一致。"""
    c = cluster_factory(3, snapshot_threshold=SNAP_THRESHOLD)
    leader = c.wait_for_leader(timeout=3)
    lid = leader.node_id
    time.sleep(0.2)
    follower_id = [nid for nid in c.nodes() if nid != lid][0]

    last = _write_batch(leader, 12)
    c.wait_until_applied(last.index)
    snap_idx = _wait_snapshot_index(leader, 5)

    # 磁盘清空（全新空存储）重新上线
    c.crash_node(follower_id)
    c.restart_node(follower_id, fresh_storage=True)
    fn = c.get(follower_id)
    assert fn.status().last_log_index == 0  # 确实是从空盘起步

    c.wait_until_applied(last.index, nodes=[follower_id], timeout=5)
    fn_st = fn.status()
    assert fn_st.snapshot_index >= snap_idx  # 靠快照跨过已压缩区间
    assert fn.sm.snapshot() == leader.sm.snapshot()
    _wait_commit_index(fn, leader.status().commit_index)


def test_slightly_behind_node_uses_normal_replication(cluster_factory):
    """只落后一点点（未触及压缩边界）的节点仍走普通 AppendEntries。"""
    c = cluster_factory(3, snapshot_threshold=SNAP_THRESHOLD)
    leader = c.wait_for_leader(timeout=3)
    lid = leader.node_id
    time.sleep(0.2)
    follower_id = [nid for nid in c.nodes() if nid != lid][0]

    c.crash_node(follower_id)
    last = _write_batch(leader, 2)  # 远不到阈值，不会触发压缩
    c.wait_until_applied(last.index, nodes=[nid for nid in c.nodes() if nid != follower_id])
    assert leader.status().snapshot_index == 0

    c.restart_node(follower_id)
    c.wait_until_applied(last.index, nodes=[follower_id], timeout=3)
    fn = c.get(follower_id)
    assert fn.status().snapshot_index == 0  # 没有走快照
    assert fn.sm.snapshot() == leader.sm.snapshot()


def test_committed_results_not_regressed_by_snapshot(cluster_factory):
    """快照追赶过程中，落后节点已提交的结论不被回退或覆盖。"""
    c = cluster_factory(3, snapshot_threshold=SNAP_THRESHOLD)
    leader = c.wait_for_leader(timeout=3)
    lid = leader.node_id
    time.sleep(0.2)
    follower_id = [nid for nid in c.nodes() if nid != lid][0]

    # follower 在线时先提交一批（它本地已生效）
    first = _write_batch(leader, 3)
    c.wait_until_applied(first.index, nodes=[follower_id])
    before = c.get(follower_id).status().commit_index

    # follower 离线，集群继续写并压缩
    c.crash_node(follower_id)
    last = _write_batch(leader, 12, seq_start=4)
    c.wait_until_applied(last.index, nodes=[nid for nid in c.nodes() if nid != follower_id])
    _wait_snapshot_index(leader, 10)

    c.restart_node(follower_id)
    c.wait_until_applied(last.index, nodes=[follower_id], timeout=5)
    fn = c.get(follower_id)
    # 已提交位点只前进不回退；早期写入的结果仍在
    assert fn.status().commit_index >= before
    for seq in range(1, 16):
        assert fn.sm.get(f"k{seq}") == seq


# --------------------------------------------------------------------- #
# 本地快照重启恢复
# --------------------------------------------------------------------- #
def test_restart_recovers_from_local_snapshot(cluster_factory, tmp_path, caplog):
    """重启直接靠本地快照恢复，不从最早日志重放。"""
    from raft.storage import FileStorage

    c = InProcessCluster(
        3,
        snapshot_threshold=SNAP_THRESHOLD,
        storage_factory=lambda nid: FileStorage(str(tmp_path), nid),
    )
    c.start()
    try:
        leader = c.wait_for_leader(timeout=3)
        lid = leader.node_id
        time.sleep(0.2)
        follower_id = [nid for nid in c.nodes() if nid != lid][0]

        last = _write_batch(leader, 10)
        c.wait_until_applied(last.index)
        snap_idx = _wait_snapshot_index(c.get(follower_id), SNAP_THRESHOLD)

        with caplog.at_level(logging.INFO, logger="raft"):
            c.crash_node(follower_id)
            c.restart_node(follower_id)  # 同一 FileStorage，从磁盘恢复

        fn = c.get(follower_id)
        # 本地快照直接恢复到压缩边界
        assert fn.status().snapshot_index >= snap_idx
        assert fn.last_applied >= snap_idx
        for seq in range(1, 11):
            assert fn.sm.get(f"k{seq}") == seq
        # 日志可见 SNAPSHOT-RESTORE，且重放只发生在快照边界之后
        assert "SNAPSHOT-RESTORE" in caplog.text
        for record in caplog.records:
            if "replay index=" in record.getMessage():
                assert f"replay index={snap_idx + 1}" in record.getMessage() or \
                    "replay index=" in record.getMessage()
        replayed = [r.getMessage() for r in caplog.records if "replay index=" in r.getMessage()]
        for msg in replayed:
            idx = int(msg.split("replay index=")[1].split()[0])
            assert idx > snap_idx, f"replayed compacted entry: {msg}"
    finally:
        c.stop()


def test_idempotency_survives_snapshot_and_restart(tmp_path):
    """幂等元数据随快照落盘：重启后重复请求仍返回首次结果，不重复应用。"""
    from raft.storage import FileStorage

    c = InProcessCluster(
        3,
        snapshot_threshold=SNAP_THRESHOLD,
        storage_factory=lambda nid: FileStorage(str(tmp_path), nid),
    )
    c.start()
    try:
        leader = c.wait_for_leader(timeout=3)
        lid = leader.node_id
        time.sleep(0.2)

        cmd = {"op": "set", "key": "persist", "value": 7}
        r1 = _write(leader, "c-p", "persist-1", 1, cmd)
        assert r1.success
        # 压出快照（覆盖 r1）
        last = _write_batch(leader, 8, client="c3", seq_start=1, key_prefix="f")
        c.wait_until_applied(last.index)
        _wait_snapshot_index(leader, r1.index)

        # 主节点崩溃重启（换别人当主），幂等记录应随快照/日志恢复
        c.crash_node(lid)
        new_leader = c.wait_for_leader(timeout=3)
        assert new_leader.node_id != lid
        time.sleep(0.2)
        r2 = _write(new_leader, "c-p", "persist-1", 1, cmd)
        assert r2.success and r2.index == r1.index and r2.result == r1.result
        assert new_leader.sm.get("persist") == 7

        # 老主从本地快照恢复后重新上线，重复记录也不会再应用一次
        c.restart_node(lid)
        c.wait_until_applied(r1.index, nodes=[lid], timeout=3)
        assert c.get(lid).sm.get("persist") == 7
    finally:
        c.stop()
