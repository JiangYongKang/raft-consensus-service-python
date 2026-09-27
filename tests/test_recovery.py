"""节点崩溃重启与状态恢复测试。"""

from __future__ import annotations

from raft_consensus.storage import PersistentStore


def test_restart_restores_committed_state(cluster3, tmp_data):
    leader = cluster3.wait_leader()
    cluster3.write("c", "w1", "x", 1)
    cluster3.write("c", "w2", "y", 2)
    cluster3.run_until(
        lambda: all(n.commit_index >= 3 for n in cluster3.sim.nodes.values()),
        timeout=3)

    victim = leader
    cluster3.crash(victim)
    # 重启：新实例从磁盘日志重放恢复
    cluster3.restart(victim)
    cluster3.run_for(1.0)
    restored = cluster3.node(victim)
    assert restored.sm.snapshot() == {"x": 1, "y": 2}
    # 幂等结果表也恢复，重启后重复请求仍命中首次结果
    assert ("c", "w1") in restored.applied_requests
    idx, _ = restored.applied_requests[("c", "w1")]
    assert idx == 2


def test_disk_files_persist_across_process_reopen(tmp_data):
    """不经过模拟器，直接验证落盘文件可被全新存储对象读取。"""
    store = PersistentStore(tmp_data, "n9")
    store.init()
    store.save_state(5, "n3")
    store.save_commit_index(4)
    store2 = PersistentStore(tmp_data, "n9")
    assert store2.load_all() == (5, "n3", 4)


def test_all_nodes_crash_and_full_restart_converge(cluster3):
    cluster3.wait_leader()
    cluster3.write("c", "w1", "k", "v")
    cluster3.run_until(
        lambda: all(n.commit_index >= 2 for n in cluster3.sim.nodes.values()),
        timeout=3)
    # 全体崩溃
    for nid in cluster3.ids:
        cluster3.crash(nid)
    # 依次重启（独立新实例），应能重新选出主并保留已提交数据
    for nid in cluster3.ids:
        cluster3.restart(nid)
    cluster3.run_for(2.0)
    leader = cluster3.wait_leader(timeout=3)
    assert leader is not None
    for nid in cluster3.ids:
        assert cluster3.node(nid).sm.get("k") == "v"
    # 恢复后还能继续写入
    r = cluster3.write("c", "w2", "k2", "v2")
    cluster3.run_until(
        lambda: all(n.commit_index >= r["index"]
                    for n in cluster3.sim.nodes.values()), timeout=3)


def test_uncommitted_tail_not_visible_after_partial_crash(cluster3):
    """只在少数派持久化、未达成多数提交的日志，不能变成已应用状态。"""
    from raft_consensus.errors import NotLeaderError
    from raft_consensus.log import LogEntry

    leader = cluster3.wait_leader()
    followers = [n for n in cluster3.ids if n != leader]
    # 把主单独隔离：多数派（两个跟随者）一侧选出新主
    cluster3.partition([{leader}, set(followers)])
    assert cluster3.run_until(
        lambda: any(cluster3.node(n).role == "leader" for n in followers),
        timeout=3)

    # 旧主在隔离侧：它无法获得多数派。尝试提交必须失败——
    # 要么已退位 -> NotLeaderError；要么短暂仍为主 -> 提案无法提交。
    prop_node = cluster3.node(leader)
    cluster3.run_for(0.8)  # 让选举超时把旧主淘汰为 follower
    before = prop_node.commit_index
    rejected_or_uncommitted = False
    try:
        p = prop_node.submit_write(
            "iso", "iso1", {"op": "set", "key": "ghost", "value": 1})
        cluster3.run_for(1.0)
        rejected_or_uncommitted = (not p.done) and \
            prop_node.commit_index == before
    except NotLeaderError:
        rejected_or_uncommitted = True
    assert rejected_or_uncommitted, "隔离主的写必须被拒绝或无法提交"
    assert prop_node.sm.get("ghost") is None

    # 即使强行在旧主本地追加一条"少数派尾日志"，它也永远无法被提交/应用
    if not any(e.request_id == "iso1" for e in prop_node.log.entries):
        ghost = LogEntry(term=prop_node.current_term,
                         index=prop_node.log.last_index + 1,
                         client_id="iso", request_id="iso1",
                         command={"op": "set", "key": "ghost", "value": 1})
        prop_node.log.append([ghost])
        prop_node.store.append_entries([ghost])
    before = prop_node.commit_index
    cluster3.run_for(1.0)
    assert prop_node.commit_index == before, "少数派尾日志不得越过多数派提交"
    assert prop_node.sm.get("ghost") is None

    # 多数派一侧对 "ghost" 完全不可见
    for nid in cluster3.ids:
        n = cluster3.node(nid)
        committed_entries = n.log.entries[1:n.commit_index + 1]
        assert all(e.request_id != "iso1" for e in committed_entries)
        assert n.sm.get("ghost") is None

    # 愈合后集群继续可用并收敛（旧未提交尾日志由 Raft 正常对账处理）
    cluster3.partition(None)
    cluster3.run_for(1.0)
    r = cluster3.write("c", "post-heal", "k", "ok")
    cluster3.run_until(
        lambda: all(n.commit_index >= r["index"]
                    for n in cluster3.sim.nodes.values()), timeout=3)
