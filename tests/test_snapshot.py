"""日志压缩与快照追赶测试。

覆盖：
- 已提交且已应用的旧历史达到阈值后被压缩成快照，磁盘上不再保留旧记录；
- 压缩期间/之后新写入照常提交，已生效结果不回退、不重复；
- 离线过久 / 磁盘清空的节点通过 InstallSnapshot 被带回一致状态；
- 节点重启直接靠本地快照恢复，不从最早日志重放；
- 压缩与快照追赶之后，幂等去重仍然生效。

日志（RAFT_TEST_LOG=INFO）里能看到 COMPACT 的压缩边界、SNAPSHOT-SEND 的
来源与 snapshot_id（请求标识）、SNAPSHOT-INSTALL 的接收方落点。
"""

from __future__ import annotations

import json
import logging
import time

from raft.storage import FileStorage

log = logging.getLogger("raft")


def _wait_until(cond, timeout: float = 5.0, what: str = "condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.02)
    raise AssertionError(f"timeout waiting for {what}")


def _write_n(leader, client_id: str, n: int, start: int = 1, timeout: float = 2.0):
    last_index = 0
    for i in range(start, start + n):
        r = leader.submit_command(
            client_id, f"{client_id}-w{i}", i,
            {"op": "set", "key": f"k{i}", "value": i}, timeout=timeout,
        )
        assert r.success, f"write {i} failed: {r.code} {r.reason}"
        last_index = r.index
    return last_index


def test_compaction_discards_applied_history_and_writes_continue(cluster_factory):
    """已应用历史超过阈值即压缩：旧条目从存储中消失，已生效结果不动，新写照常提交。"""
    c = cluster_factory(3, snapshot_threshold=5)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)
    last = _write_n(leader, "c1", 15)
    c.wait_until_applied(last)  # 等全部节点应用完再断言状态机

    for nid, n in c.nodes().items():
        _wait_until(lambda n=n: n.storage.snapshot_index() >= 10,
                    what=f"compaction on {nid}")
        snap_idx = n.storage.snapshot_index()
        # 磁盘/内存日志里不再保留快照边界及之前的旧记录
        assert all(e.index > snap_idx for e in n.storage.all_entries())
        assert n.storage.first_index() == snap_idx + 1
        # 已提交位点不回退
        assert n.status().commit_index >= last - 1
        # 已生效结果不丢、不回退
        for i in range(1, 16):
            assert n.sm.get(f"k{i}") == i

    # 压缩之后新写入照常提交
    leader = c.wait_for_leader(timeout=3)
    last2 = _write_n(leader, "c1", 5, start=16)
    c.wait_until_applied(last2)
    for n in c.nodes().values():
        for i in range(16, 21):
            assert n.sm.get(f"k{i}") == i


def test_lagging_node_catches_up_via_snapshot(cluster_factory):
    """节点离线过久，所需旧历史已被压缩：必须靠快照追赶，追上后全集群一致。"""
    c = cluster_factory(3, snapshot_threshold=5)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)
    lid = leader.node_id
    lagging = min(nid for nid in c.nodes() if nid != lid)

    c.isolate(lagging)
    time.sleep(0.2)
    _write_n(leader, "c1", 15)

    # 多数派把落后者需要的历史压缩掉
    _wait_until(lambda: leader.storage.snapshot_index() >= 10,
                what="leader compaction beyond lagging node")
    snap_idx = leader.storage.snapshot_index()
    lag = c.get(lagging)
    assert lag.status().commit_index < snap_idx  # 确实落后到快照边界之内

    # 愈合：落后者需要的旧历史在别人盘上已不存在，只能走 InstallSnapshot
    c.heal()
    new_leader = c.wait_for_leader(timeout=3)
    _wait_until(
        lambda: c.get(lagging).storage.snapshot_index() > 0
        and c.get(lagging).status().commit_index >= new_leader.status().commit_index,
        timeout=8, what=f"snapshot catch-up on {lagging}",
    )
    st = c.get(lagging).status()
    lst = new_leader.status()
    log.info(
        "CATCH-UP-DONE node=%s snapshot_index=%d commit=%d applied=%d leader=%s commit=%d",
        lagging, st.snapshot_index, st.commit_index, st.last_applied,
        lst.node_id, lst.commit_index,
    )
    # 进度对齐，状态一致
    assert st.commit_index == lst.commit_index
    assert st.last_applied == lst.last_applied
    for i in range(1, 16):
        assert c.get(lagging).sm.get(f"k{i}") == i
    snaps = {nid: n.sm.snapshot() for nid, n in c.nodes().items()}
    assert len({json.dumps(s, sort_keys=True) for s in snaps.values()}) == 1

    # 追上之后继续正常参与复制
    r = new_leader.submit_command("c2", "after-catchup", 1,
                                  {"op": "set", "key": "post", "value": "ok"}, timeout=2.0)
    assert r.success
    c.wait_until_applied(r.index)
    assert c.get(lagging).sm.get("post") == "ok"


def test_fresh_disk_node_catches_up_via_snapshot(cluster_factory):
    """磁盘被清空后重新上线的节点：靠快照 + 后续日志被带回一致状态。"""
    c = cluster_factory(3, snapshot_threshold=5)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)
    victim = min(nid for nid in c.nodes() if nid != leader.node_id)

    last = _write_n(leader, "c1", 12)
    _wait_until(lambda: leader.storage.snapshot_index() >= 8, what="compaction")

    # 崩溃 + 清空磁盘重启
    c.crash_node(victim)
    c.restart_node(victim, fresh_storage=True)
    node = c.get(victim)
    assert node.storage.last_index() == 0  # 确实是空盘

    _wait_until(lambda: c.get(victim).storage.snapshot_index() > 0,
                timeout=8, what=f"install-snapshot on fresh node {victim}")
    _wait_until(lambda: c.get(victim).status().last_applied >= last,
                timeout=8, what=f"fresh node applied to {last}")
    for i in range(1, 13):
        assert c.get(victim).sm.get(f"k{i}") == i
    # 与其他节点状态机完全一致
    other = c.get(leader.node_id)
    assert c.get(victim).sm.snapshot() == other.sm.snapshot()


def test_restart_restores_from_local_snapshot(cluster_factory, tmp_path):
    """节点重启直接靠本地快照恢复：不从最早日志重放，状态与进度对齐。"""
    def make_storage(nid: str) -> FileStorage:
        return FileStorage(str(tmp_path), nid)

    c = cluster_factory(3, snapshot_threshold=5, storage_factory=make_storage)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)
    victim = min(nid for nid in c.nodes() if nid != leader.node_id)
    last = _write_n(leader, "c1", 12)
    _wait_until(lambda: c.get(victim).storage.snapshot_index() >= 8,
                what=f"compaction on {victim}")
    snap_idx = c.get(victim).storage.snapshot_index()

    # 磁盘上：快照文件存在，状态文件里不再有被压缩的旧记录
    snap_file = tmp_path / f"{victim}.snapshot.json"
    state_file = tmp_path / f"{victim}.state.json"
    assert snap_file.exists()
    state = json.loads(state_file.read_text())
    assert all(e["index"] > snap_idx for e in state["log"])

    c.crash_node(victim)
    c.restart_node(victim)  # 同一 FileStorage：从本地快照 + 日志恢复
    node = c.get(victim)
    assert node.storage.snapshot_index() == snap_idx
    assert node.storage.first_index() == snap_idx + 1  # 不可能从最早那条重放
    _wait_until(lambda: c.get(victim).status().last_applied >= last,
                what=f"{victim} applied after restart")
    for i in range(1, 13):
        assert node.sm.get(f"k{i}") == i
    # 重启后进度与集群对齐并继续复制
    leader = c.wait_for_leader(timeout=3)
    r = leader.submit_command("c3", "post-restart", 1,
                              {"op": "set", "key": "after", "value": 1}, timeout=2.0)
    assert r.success
    c.wait_until_applied(r.index)
    assert node.sm.get("after") == 1


def test_idempotency_survives_compaction_and_restart(cluster_factory, tmp_path):
    """压缩与快照恢复之后，旧请求的幂等去重仍然生效（不重复执行）。"""
    def make_storage(nid: str) -> FileStorage:
        return FileStorage(str(tmp_path), nid)

    c = cluster_factory(3, snapshot_threshold=5, storage_factory=make_storage)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)
    lid = leader.node_id
    cmd = {"op": "set", "key": "idem", "value": 1}
    r1 = leader.submit_command("c1", "idem-1", 1, cmd, timeout=2.0)
    assert r1.success
    _write_n(leader, "c1", 10, start=2)  # 把 idem-1 推进压缩区
    _wait_until(lambda: c.get(lid).storage.snapshot_index() > r1.index,
                what="compaction past idempotent entry")

    # 压缩后重试同一请求：命中去重，返回首次结果，不产生新条目
    r2 = c.get(lid).submit_command("c1", "idem-1", 1, cmd, timeout=2.0)
    assert r2.success and r2.index == r1.index
    assert r2.result == r1.result

    # 崩溃重启（从本地快照恢复幂等水位）后，去重信息仍在
    c.crash_node(lid)
    c.restart_node(lid)
    node = c.get(lid)
    assert ("c1", "idem-1") in node._dedup
    assert node._client_max_seq.get("c1", 0) >= 11
    # 序列号水位同样恢复：过期序列号必须被拒绝
    from raft.errors import StaleClientRequestError
    leader2 = c.wait_for_leader(timeout=3)
    if leader2.node_id == lid:
        try:
            node.submit_command("c1", "idem-old", 2, cmd, timeout=1.0)
            raise AssertionError("stale seq should be rejected")
        except StaleClientRequestError:
            pass
