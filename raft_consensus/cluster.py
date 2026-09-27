"""本地集群门面：在单进程内组装多个 RaftNode 实例与持久化目录。

客户端调用通过模拟器事件队列派发到目标节点线程（串行执行），
因此"并发多客户端"由模拟器事件循环天然串行化、不会乱序。
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from raft_consensus.errors import NotLeaderError
from raft_consensus.node import NodeConfig, RaftNode
from raft_consensus.simulation import Simulator

logger = logging.getLogger("raft.cluster")


class LocalCluster:
    def __init__(self, size: int = 5, *, data_root: str | Path = "./.cluster-data",
                 realtime: bool = False, seed: int = 1,
                 time_unit: float = 0.01, clean: bool = True) -> None:
        assert size >= 1 and size % 2 == 1, "集群大小应为奇数"
        self.size = size
        self.data_root = Path(data_root)
        if clean and self.data_root.exists():
            shutil.rmtree(self.data_root)
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.ids = tuple(f"n{i+1}" for i in range(size))
        self.sim = Simulator(self.ids, realtime=realtime, time_unit=time_unit,
                             seed=seed)
        self.configs: dict[str, NodeConfig] = {}
        for nid in self.ids:
            cfg = NodeConfig(
                node_id=nid, peers=self.ids, data_dir=self.data_root,
                election_timeout_min=0.15, election_timeout_max=0.30,
                heartbeat_interval=0.05, seed=seed * 1000 + hash(nid) % 1000,
            )
            self.configs[nid] = cfg
            self.sim.register_builder(nid, self._make_builder(nid))
            node = self._make(nid)
            self.sim.add_node(node)

    def _make_builder(self, nid: str):
        def build() -> RaftNode:
            node = self._make(nid)
            self.sim.add_node(node)
            return node
        return build

    def _make(self, nid: str) -> RaftNode:
        cfg = self.configs[nid]
        return RaftNode(
            cfg,
            send=self.sim.send,
            schedule=self.sim.make_scheduler(nid),
            now=self.sim.make_clock(),
            new_msg_id=self.sim.new_msg_id,
        )

    # ---- 生命周期 ----
    def start(self) -> None:
        for nid in self.ids:
            self.sim.nodes[nid].start()

    def stop(self) -> None:
        for node in list(self.sim.nodes.values()):
            node.shutdown()

    def reset_data(self) -> None:
        if self.data_root.exists():
            shutil.rmtree(self.data_root)

    # ---- 故障注入 ----
    def crash(self, node_id: str) -> None:
        self.sim.crash(node_id)

    def restart(self, node_id: str) -> None:
        self.sim.restart(node_id)

    def partition(self, groups: list[set[str]] | None) -> None:
        self.sim.network.partition(groups)

    def run_until(self, predicate, timeout: float = 5.0) -> bool:
        return self.sim.run_until(predicate, timeout=timeout)

    def run_for(self, seconds: float) -> None:
        self.sim.run_for(seconds)

    # ---- 观测 ----
    def leader(self) -> str | None:
        leaders = [n.node_id for n in self.sim.nodes.values()
                   if n.role == "leader"]
        # 同一任期最多一个领导者；若瞬时出现两个，交由测试断言发现
        return leaders[0] if leaders else None

    def leaders(self) -> list[str]:
        return [n.node_id for n in self.sim.nodes.values()
                if n.role == "leader"]

    def node(self, node_id: str) -> RaftNode:
        return self.sim.nodes[node_id]

    def terms(self) -> dict[str, int]:
        return {n.node_id: n.current_term for n in self.sim.nodes.values()}

    def state_on_each(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for nid, node in self.sim.nodes.items():
            out[nid] = {
                "role": node.role,
                "term": node.current_term,
                "commit_index": node.commit_index,
                "last_index": node.log.last_index,
                "state": node.sm.snapshot(),
                "applied": len(node.applied_requests),
                "online": not self.sim.network.is_offline(nid),
            }
        return out

    # ---- 客户端操作（提交后驱动事件泵，避免阻塞死锁）----
    def _submit_and_wait(self, target: str, submit, is_done,
                         timeout: float = 3.0):
        """在目标节点线程上提交一个异步操作，再在主线程驱动事件泵直到完成。"""
        box: dict = {}

        def job():
            try:
                box["handle"] = submit(self.sim.nodes[target])
            except Exception as e:  # noqa: BLE001 - 跨线程带回
                box["error"] = e

        self.sim.schedule(0.0, job)
        deadline = self.sim.now() + timeout

        def finished():
            return "error" in box or ("handle" in box and is_done(box["handle"]))

        ok = self.sim.run_until(finished, timeout=timeout)
        if "error" in box:
            raise box["error"]
        if not ok or "handle" not in box:
            raise TimeoutError(f"operation on {target} not finished in {timeout}s")
        return box["handle"]

    def write(self, client_id: str, request_id: str, key: str, value: object,
              *, node_id: str | None = None, timeout: float = 3.0) -> dict:
        target = node_id or self.leader()
        if target is None:
            raise NotLeaderError(term=0, leader_id=None)
        prop = self._submit_and_wait(
            target,
            lambda n: n.submit_write(client_id, request_id,
                                     {"op": "set", "key": key, "value": value}),
            lambda p: p.event.is_set(),
            timeout=timeout,
        )
        if prop.error:
            raise prop.error
        return {"index": prop.index, "result": prop.result, "leader": target,
                "term": prop.term}

    def read(self, key: str, *, node_id: str | None = None,
             timeout: float = 3.0) -> object:
        target = node_id or self.leader()
        if target is None:
            raise NotLeaderError(term=0, leader_id=None)
        pr = self._submit_and_wait(
            target,
            lambda n: n.submit_read(key),
            lambda r: r.event.is_set(),
            timeout=timeout,
        )
        if pr.error:
            raise pr.error
        return pr.result

    def wait_leader(self, timeout: float = 3.0) -> str:
        assert self.run_until(lambda: self.leader() is not None, timeout), \
            "no leader elected in time"
        return self.leader()  # type: ignore[return-value]
