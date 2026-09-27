"""本地集群门面：在单进程内组装多个 RaftNode 实例与持久化目录。

执行模型
--------
模拟器事件循环跑在一条专用后台线程上；每个节点的定时器、RPC 与客户端任务都
只在这条线程上执行 -> 节点内部无锁也不会乱序。客户端（可以有很多线程）通过
每节点任务队列提交操作，任务在事件线程上串行执行，用 Event 同步结果。
"""

from __future__ import annotations

import logging
import queue
import shutil
import threading
from pathlib import Path

from raft_consensus.errors import NotLeaderError
from raft_consensus.node import NodeConfig, RaftNode
from raft_consensus.simulation import Simulator

logger = logging.getLogger("raft.cluster")


class _NodeTask:
    __slots__ = ("fn", "event", "value", "error")

    def __init__(self, fn):
        self.fn = fn
        self.event = threading.Event()
        self.value = None
        self.error: Exception | None = None


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

        # 每节点任务队列（由后台事件泵线程轮询执行）
        self._task_queues: dict[str, queue.Queue[_NodeTask | None]] = {
            nid: queue.Queue() for nid in self.ids
        }
        self._pump_stop = threading.Event()
        self._sim_lock = threading.Lock()  # 所有事件泵操作串行化
        self._pump = threading.Thread(target=self._pump_loop, daemon=True,
                                      name="raft-pump")

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
            is_peer_reachable=lambda peer, src=nid:
                self.sim.network.reachable(src, peer),
        )

    # ---- 后台事件泵 ----
    def _step(self) -> bool:
        with self._sim_lock:
            return self.sim.step()

    def _pump_loop(self) -> None:
        while not self._pump_stop.is_set():
            advanced = self._step()
            for nid, q in self._task_queues.items():
                if self.sim.network.is_offline(nid):
                    continue
                drained = 0
                while drained < 64:
                    try:
                        task = q.get_nowait()
                    except queue.Empty:
                        break
                    drained += 1
                    if task is None:
                        continue
                    try:
                        with self._sim_lock:
                            task.value = task.fn(self.sim.nodes[nid])
                    except Exception as e:  # noqa: BLE001 - 跨线程带回
                        task.error = e
                    finally:
                        task.event.set()
            if not advanced:
                self._pump_stop.wait(0.002)  # 空闲让出 CPU

    # ---- 生命周期 ----
    def start(self) -> None:
        for nid in self.ids:
            self.sim.nodes[nid].start()
        self._pump.start()

    def stop(self) -> None:
        self._pump_stop.set()
        self._pump.join(timeout=2)
        for node in list(self.sim.nodes.values()):
            node.shutdown()

    def reset_data(self) -> None:
        if self.data_root.exists():
            shutil.rmtree(self.data_root)

    # ---- 故障注入 ----
    def crash(self, node_id: str) -> None:
        # 排空该节点在途任务，以 NodeOfflineError 失败，避免客户端永久挂起
        q = self._task_queues[node_id]
        pending: list[_NodeTask] = []
        while True:
            try:
                t = q.get_nowait()
            except queue.Empty:
                break
            if t is not None:
                pending.append(t)
        with self._sim_lock:
            self.sim.crash(node_id)
        from raft_consensus.errors import NodeOfflineError
        for t in pending:
            t.error = NodeOfflineError(node=node_id)
            t.event.set()

    def restart(self, node_id: str) -> None:
        with self._sim_lock:
            self.sim.restart(node_id)

    def partition(self, groups: list[set[str]] | None) -> None:
        with self._sim_lock:
            self.sim.network.partition(groups)

    def run_until(self, predicate, timeout: float = 5.0) -> bool:
        deadline = self.sim.now() + timeout
        while True:
            with self._sim_lock:
                if predicate():
                    return True
                if self.sim.now() > deadline:
                    return False
                advanced = self.sim.step()
            if not advanced:
                self._pump_stop.wait(0.002)

    def run_for(self, seconds: float) -> None:
        deadline = self.sim.now() + seconds
        while True:
            with self._sim_lock:
                if self.sim.now() >= deadline:
                    return
                if not self.sim.step():
                    break
            self._pump_stop.wait(0.001)

    # ---- 观测 ----
    def leader(self) -> str | None:
        leaders = [n.node_id for n in self.sim.nodes.values()
                   if n.role == "leader"]
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

    # ---- 客户端操作 ----
    def _submit(self, node_id: str, fn, timeout: float = 5.0):
        task = _NodeTask(fn)
        self._task_queues[node_id].put(task)
        if not task.event.wait(timeout):
            raise TimeoutError(f"operation on {node_id} timed out")
        if task.error:
            raise task.error
        return task.value

    def _wait_handle(self, handle, is_done, timeout: float):
        deadline = self.sim.now() + timeout
        while not is_done(handle):
            if self.sim.now() > deadline:
                raise TimeoutError("consensus result not ready in time")
            with self._sim_lock:
                advanced = self.sim.step()
            if not advanced:
                self._pump_stop.wait(0.002)

    def write(self, client_id: str, request_id: str, key: str, value: object,
              *, node_id: str | None = None, timeout: float = 5.0) -> dict:
        target = node_id or self.leader()
        if target is None:
            raise NotLeaderError(term=0, leader_id=None)
        prop = self._submit(
            target,
            lambda n: n.submit_write(client_id, request_id,
                                     {"op": "set", "key": key, "value": value}),
            timeout=timeout,
        )
        self._wait_handle(prop, lambda p: p.event.is_set(), timeout)
        if prop.error:
            raise prop.error
        return {"index": prop.index, "result": prop.result, "leader": target,
                "term": prop.term}

    def read(self, key: str, *, node_id: str | None = None,
             timeout: float = 5.0) -> object:
        target = node_id or self.leader()
        if target is None:
            raise NotLeaderError(term=0, leader_id=None)
        pr = self._submit(target, lambda n: n.submit_read(key), timeout=timeout)
        self._wait_handle(pr, lambda r: r.event.is_set(), timeout)
        if pr.error:
            raise pr.error
        return pr.result

    def wait_leader(self, timeout: float = 3.0) -> str:
        assert self.run_until(lambda: self.leader() is not None, timeout), \
            "no leader elected in time"
        return self.leader()  # type: ignore[return-value]
