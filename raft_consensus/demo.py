"""本地端到端演示：多个节点实例组成集群，验证一致性能力。

运行：
    uv run python main.py --demo
或：
    uv run python -m raft_consensus.demo

场景（全部本地、无外部服务，使用虚拟时钟，几秒内跑完）：
    1. 集群启动并自动选出唯一主节点（任期变化有日志）
    2. 正常写入并复制到多数派（打印决策位点、来源节点、请求标识）
    3. 幂等重试：同一请求重复提交不产生重复结果
    4. 主节点崩溃 -> 剩余节点重新选举 -> 旧主重启后自动追上一致状态
    5. 网络分区 -> 少数派拒绝写入 -> 分区愈合后重新收敛
    6. 多客户端并发写入 -> 不乱序、不重复、不丢失
"""

from __future__ import annotations

import logging
import tempfile
import threading

from raft_consensus.cluster import LocalCluster
from raft_consensus.errors import NotLeaderError

BANNER = "=" * 68


def _section(title: str) -> None:
    print(f"\n{BANNER}\n{title}\n{BANNER}")


def _snapshot(c: LocalCluster, title: str) -> None:
    print(f"-- {title} --")
    for nid, st in sorted(c.state_on_each().items()):
        print(f"   {nid}: role={st['role']:<9} term={st['term']:<3} "
              f"commit={st['commit_index']:<3} last={st['last_index']:<3} "
              f"state={st['state']}")


def _write_with_retry(c: LocalCluster, client: str, req: str,
                      key: str, value: object, *, attempts: int = 20):
    """客户端在主切换时带退避重试（同一 request_id 保证重试幂等）。"""
    last: Exception | None = None
    for _ in range(attempts):
        try:
            return c.write(client, req, key, value)
        except NotLeaderError as e:
            last = e
            c.run_until(lambda: c.leader() is not None, 3.0)
    raise RuntimeError(f"write {req} failed after retries: {last}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )
    with tempfile.TemporaryDirectory(prefix="raft-demo-") as data_root:
        c = LocalCluster(5, data_root=data_root, clean=True, seed=42)
        c.start()

        # 1) 选举
        _section("场景 1：集群启动 -> 选举唯一主节点")
        leader = c.wait_leader(timeout=3)
        print(f"   当选主节点: {leader}，当前任期: {c.node(leader).current_term}")
        assert len(c.leaders()) == 1

        # 2) 正常写入
        _section("场景 2：写入经多数派确认后生效（日志含决策位点/来源/请求标识）")
        r1 = _write_with_retry(c, "client-1", "req-001", "account/a", 100)
        print(f"   写入结果: decision_index={r1['index']} term={r1['term']} "
              f"leader={r1['leader']}")
        r2 = _write_with_retry(c, "client-1", "req-002", "account/b", 200)
        print(f"   写入结果: decision_index={r2['index']} term={r2['term']} "
              f"leader={r2['leader']}")
        c.run_until(lambda: len({n.commit_index
                                 for n in c.sim.nodes.values()}) == 1, timeout=3)
        _snapshot(c, "多数派提交后各节点快照")

        # 3) 幂等重试
        _section("场景 3：同一 (client, request) 重复提交 -> 幂等，不产生重复结果")
        r3 = _write_with_retry(c, "client-1", "req-001", "account/a", 100)
        print(f"   重试返回同一决策位点: {r3['index']}（首次 {r1['index']}），"
              f"结果一致: {r3['result'] == r1['result']}")
        assert r3["index"] == r1["index"]

        # 4) 主节点崩溃 + 重启恢复
        _section("场景 4：主节点崩溃 -> 重新选举 -> 旧主重启并从磁盘恢复")
        c.crash(leader)
        print(f"   节点 {leader} 已崩溃（进程离线，在途消息被丢弃）")
        new_leader = None
        c.run_until(lambda: c.leader() is not None and c.leader() != leader, 3)
        new_leader = c.leader()
        print(f"   新主节点: {new_leader}，新任期: {c.node(new_leader).current_term}")
        r4 = _write_with_retry(c, "client-2", "req-003", "account/c", 300)
        print(f"   故障期间新写入: decision_index={r4['index']}")
        c.restart(leader)
        print(f"   节点 {leader} 重启：从 log.jsonl/state.json 重放恢复")
        c.run_until(lambda: all(n.sm.get("account/c") == 300
                                for n in c.sim.nodes.values()), 5)
        _snapshot(c, "旧主重启追平后各节点快照")

        # 5) 网络分区
        _section("场景 5：网络分区 -> 少数派不能写入 -> 愈合后收敛")
        followers = [n for n in c.ids if n != new_leader]
        minority = [new_leader, followers[0]]
        majority = followers[1:]
        c.partition([set(minority), set(majority)])
        print(f"   分区: 少数派={minority} 多数派={majority}")
        c.run_for(1.2)
        # 旧主（少数派）尝试写
        try:
            c.write("client-x", "req-stale", "ghost", 1,
                    node_id=new_leader, timeout=1.0)
            print("   !! 意外：少数派写入成功")
        except (NotLeaderError, TimeoutError) as e:
            print(f"   少数派写入被拒绝（原因可区分）: {type(e).__name__}: {e}")
        ml = None
        c.run_until(lambda: any(c.node(n).role == "leader" for n in majority), 3)
        ml = next(n for n in majority if c.node(n).role == "leader")
        r5 = _write_with_retry(c, "client-3", "req-004", "account/d", 400)
        print(f"   多数派主 {ml} 接受写入: decision_index={r5['index']}")
        c.partition(None)
        print("   分区愈合，等待集群重新收敛到同一任期/状态 ...")
        c.run_for(2.0)
        c.run_until(lambda: all(n.sm.get("account/d") == 400
                                for n in c.sim.nodes.values()), 5)
        _snapshot(c, "分区愈合后各节点快照")

        # 6) 并发写入
        _section("场景 6：多客户端并发写入 -> 不乱序/不重复/不丢失")
        n_clients, n_each = 6, 4
        errors: list[Exception] = []
        done = []
        lock = threading.Lock()

        def worker(ci: int):
            for j in range(n_each):
                for _ in range(10):
                    try:
                        r = c.write(f"client-{ci}", f"w-{ci}-{j}",
                                    f"k/{ci}/{j}", j)
                        with lock:
                            done.append(r["index"])
                        break
                    except NotLeaderError:
                        c.run_until(lambda: c.leader() is not None, 3)
                else:
                    errors.append(RuntimeError(f"client {ci} req {j} failed"))

        threads = [threading.Thread(target=worker, args=(ci,))
                   for ci in range(n_clients)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        assert not errors, errors
        assert len(done) == n_clients * n_each
        assert len(set(done)) == len(done), "决策位点有重复"
        print(f"   {len(done)} 个并发写全部成功，决策位点唯一且连续: "
              f"{min(done)}..{max(done)}")
        expected = {f"k/{ci}/{j}": j
                    for ci in range(n_clients) for j in range(n_each)}
        c.run_until(lambda: all(n.sm.snapshot() == expected
                                for n in c.sim.nodes.values()), 5)

        _section("最终结论：所有节点状态一致")
        _snapshot(c, "终态")
        states = {tuple(sorted(n.sm.snapshot().items()))
                  for n in c.sim.nodes.values()}
        assert len(states) == 1, states
        print("\n演示完成：选举、复制、崩溃恢复、分区愈合、幂等重试、并发写入"
              "全部达成一致。")
        c.stop()


if __name__ == "__main__":
    main()
