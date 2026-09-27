"""并发与请求重试测试。"""

from __future__ import annotations

import threading

import pytest

from raft_consensus.errors import NotLeaderError


def test_concurrent_writes_no_loss_or_duplication(cluster3):
    """多个客户端线程同时写不同 key：全部成功、位点唯一连续、状态完整。"""
    cluster3.wait_leader()
    n_clients, n_each = 8, 5
    errors: list[Exception] = []
    results: list[dict] = []
    lock = threading.Lock()

    def worker(ci: int):
        for j in range(n_each):
            for attempt in range(10):
                try:
                    rid = f"c{ci}-w{j}"
                    r = cluster3.write(f"client{ci}", rid,
                                       f"k-{ci}-{j}", j)
                    with lock:
                        results.append(r)
                    break
                except NotLeaderError:
                    cluster3.run_until(
                        lambda: cluster3.leader() is not None, timeout=3)
            else:
                errors.append(RuntimeError(f"client {ci} req {j} failed"))

    threads = [threading.Thread(target=worker, args=(ci,))
               for ci in range(n_clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors, errors
    assert len(results) == n_clients * n_each
    # 决策位点唯一，无重复无空洞（扣除每个 leader 的 noop）
    indices = sorted(r["index"] for r in results)
    assert len(set(indices)) == len(indices)
    assert indices == list(range(min(indices), max(indices) + 1))
    # 最终所有节点状态一致
    expected = {f"k-{ci}-{j}": j
                for ci in range(n_clients) for j in range(n_each)}
    cluster3.run_until(
        lambda: all(n.sm.snapshot() == expected
                    for n in cluster3.sim.nodes.values()), timeout=5)
    for nid, st in cluster3.state_on_each().items():
        assert st["state"] == expected, nid


def test_concurrent_retries_same_request_single_effect(cluster3):
    """多个线程用同一个 request_id 并发重试：只产生一条日志、一个结果。"""
    leader = cluster3.wait_leader()
    node = cluster3.node(leader)
    outcomes: list = []
    barrier = threading.Barrier(4)

    def retry():
        barrier.wait()
        try:
            outcomes.append(cluster3.write("dup-client", "same-req",
                                           "counter", 1))
        except Exception as e:  # noqa: BLE001
            outcomes.append(e)

    ts = [threading.Thread(target=retry) for _ in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=15)

    ok = [o for o in outcomes if not isinstance(o, Exception)]
    assert len(ok) == 4
    assert len({o["index"] for o in ok}) == 1
    entries = [e for e in node.log.entries
               if e.client_id == "dup-client"]
    assert len(entries) == 1
    assert cluster3.read("counter") == 1


def test_failed_over_writes_still_linearizable(cluster3):
    """主节点崩溃后客户端带新请求重试到新主：不丢、不重、最终一致。"""
    leader1 = cluster3.wait_leader()
    cluster3.write("c", "w0", "k0", 0)
    cluster3.crash(leader1)
    cluster3.run_until(lambda: cluster3.leader() is not None
                       and cluster3.leader() != leader1, timeout=3)
    leader2 = cluster3.leader()
    r = cluster3.write("c", "w1", "k1", 1)
    assert r["leader"] == leader2
    cluster3.restart(leader1)
    cluster3.run_until(
        lambda: all(n.sm.get("k1") == 1 for n in cluster3.sim.nodes.values()),
        timeout=5)


def test_writes_apply_in_log_order(cluster3):
    """同一 key 顺序写，所有节点观察到的值序列与提交顺序一致。"""
    cluster3.wait_leader()
    leader = cluster3.leader()
    for i in range(1, 6):
        cluster3.write("seq", f"s{i}", "seq-key", i)
    cluster3.run_until(
        lambda: len({n.commit_index for n in cluster3.sim.nodes.values()}) == 1
        and all(n.sm.get("seq-key") == 5
                for n in cluster3.sim.nodes.values()), timeout=3)
    leader_node = cluster3.node(leader)
    vals = [e.command["value"]
            for e in leader_node.log.entries
            if e.client_id == "seq"]
    assert vals == [1, 2, 3, 4, 5]
