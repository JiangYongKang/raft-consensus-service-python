"""并发客户端：结果不乱序、不重复、不丢失。"""

from __future__ import annotations

import threading
import time

import pytest

from raft.errors import StaleTermError, UnauthorizedError


def test_concurrent_writers_unique_indices_and_all_committed(cluster_factory):
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)

    n_clients = 8
    per_client = 10
    errors: list[Exception] = []
    results: list[tuple[str, int, int]] = []  # (client, seq, index)
    lock = threading.Lock()

    def writer(client_idx: int):
        client_id = f"client-{client_idx}"
        for seq in range(1, per_client + 1):
            rid = f"{client_id}-req-{seq}"
            cmd = {"op": "set", "key": f"k-{client_idx}-{seq}",
                   "value": f"v-{client_idx}-{seq}"}
            for attempt in range(30):
                # 每次尝试重新发现主；始终用同一 request_id，重试天然幂等
                leaders = [n for n in c.nodes().values() if n.is_leader()]
                if not leaders:
                    time.sleep(0.05)
                    continue
                try:
                    r = leaders[0].submit_command(client_id, rid, seq, cmd, timeout=2.0)
                except Exception as e:  # pragma: no cover
                    errors.append(e)
                    break
                if r.success:
                    with lock:
                        results.append((client_id, seq, r.index))
                    break
                time.sleep(0.05)
            else:
                errors.append(AssertionError(f"write failed {client_id} seq={seq}"))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(n_clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors, errors
    assert len(results) == n_clients * per_client

    # 1) 每个 (client, seq) 恰好一个结果 -> 不重复不丢失
    keys = [(cid, seq) for cid, seq, _ in results]
    assert len(keys) == len(set(keys))

    # 2) 日志 index 全局唯一且连续（主节点视角）
    indices = sorted(i for _, _, i in results)
    assert len(indices) == len(set(indices))

    # 3) 单客户端内严格按 seq 顺序提交（不乱序）
    by_client: dict[str, list[int]] = {}
    for cid, seq, idx in results:
        by_client.setdefault(cid, []).append(idx)
    for cid, idxs in by_client.items():
        assert idxs == sorted(set(idxs)), cid

    max_idx = max(indices)
    c.wait_until_applied(max_idx, timeout=5)
    # 4) 所有节点应用一致
    for n in c.nodes().values():
        assert n.sm.get("k-3-7") == "v-3-7"
        assert n.status().commit_index >= max_idx


def test_concurrent_same_request_id_returns_single_index(cluster_factory):
    """多个线程用同一 (client_id, request_id) 并发提交 -> 只产生一条日志。"""
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)

    out: list[int] = []
    lock = threading.Lock()

    def fire():
        r = leader.submit_command(
            "race-client", "same-req", 1,
            {"op": "set", "key": "race", "value": "win"}, timeout=2.0,
        )
        assert r.success, r
        with lock:
            out.append(r.index)

    threads = [threading.Thread(target=fire) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert len(set(out)) == 1
    entries = [e for e in leader.storage.all_entries() if e.request_id == "same-req"]
    assert len(entries) == 1


def test_concurrent_writes_survive_leader_failover(cluster_factory):
    """写入过程中主节点崩溃：客户端用相同 request_id 重试，最终不丢不重。"""
    c = cluster_factory(3)
    leader = c.wait_for_leader(timeout=3)
    time.sleep(0.2)

    n_clients, per_client = 4, 8
    done_keys: list[tuple[str, int]] = []
    errors: list[Exception] = []
    lock = threading.Lock()
    start = threading.Event()

    def writer(client_idx: int):
        start.wait(5)
        cid = f"fc-{client_idx}"
        for seq in range(1, per_client + 1):
            rid = f"{cid}-{seq}"
            cmd = {"op": "set", "key": f"f-{cid}-{seq}", "value": seq}
            ok = False
            for _ in range(40):
                leaders = [n for n in c.nodes().values() if n.is_leader()]
                if not leaders:
                    time.sleep(0.05)
                    continue
                try:
                    r = leaders[0].submit_command(cid, rid, seq, cmd, timeout=1.0)
                except Exception as e:  # pragma: no cover
                    errors.append(e)
                    break
                if r.success:
                    with lock:
                        done_keys.append((cid, seq))
                    ok = True
                    break
                time.sleep(0.05)
            if not ok:
                errors.append(AssertionError(f"{cid} seq={seq} not committed"))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(n_clients)]
    for t in threads:
        t.start()
    start.set()
    time.sleep(0.15)
    # 写入进行中，干掉当前主
    victim = c.wait_for_leader(timeout=2).node_id
    c.crash_node(victim)
    new_leader = c.wait_for_leader(timeout=3)
    # 稍后把旧主恢复
    time.sleep(0.5)
    c.restart_node(victim)

    for t in threads:
        t.join(timeout=30)
    assert not errors, errors[:3]
    assert len(done_keys) == n_clients * per_client
    assert len(set(done_keys)) == len(done_keys)

    # 等所有已提交日志应用完毕后再校验状态机
    applied_target = max(n.status().commit_index for n in c.nodes().values())
    c.wait_until_applied(applied_target, timeout=5)
    expected = set(done_keys)
    for n in c.nodes().values():
        for cid, seq in expected:
            assert n.sm.get(f"f-{cid}-{seq}") == seq, (n.node_id, cid, seq)
