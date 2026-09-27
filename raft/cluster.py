"""进程内测试集群：多个 RaftNode 共享 LoopbackTransport。

提供选主等待、网络分区/隔离/恢复、崩溃重启等故障注入。
崩溃重启的节点可以选择使用 FileStorage（验证磁盘恢复），
默认使用 InMemoryStorage（崩溃即丢内存、保留进程对象以便重启模拟节点恢复边界：
重启后存储为空 -> 从对端追赶日志）。
"""

from __future__ import annotations

import logging
import random
import time

from .node import RaftNode
from .statemachine import StateMachine
from .storage import FileStorage, InMemoryStorage, Storage
from .transport import LoopbackTransport

log = logging.getLogger("raft")


class InProcessCluster:
    def __init__(
        self,
        size: int = 3,
        *,
        ids: list[str] | None = None,
        transport: LoopbackTransport | None = None,
        storage_factory=None,
        **node_kwargs,
    ) -> None:
        node_kwargs.setdefault("election_timeout", (0.12, 0.25))
        node_kwargs.setdefault("heartbeat_interval", 0.04)
        self._node_kwargs = node_kwargs
        self._ids = ids or [f"n{i + 1}" for i in range(size)]
        self.transport = transport or LoopbackTransport()
        self._storage_factory = storage_factory or (lambda nid: InMemoryStorage())
        self.nodes_map: dict[str, RaftNode] = {}
        self._build_all()

    def _new_node(self, node_id: str, storage: Storage) -> RaftNode:
        peers = [p for p in self._ids if p != node_id]
        return RaftNode(
            node_id, peers, storage, StateMachine(), self.transport,
            state_machine_factory=StateMachine,
            rng=random.Random(hash(node_id) & 0xFFFFFFFF),
            **self._node_kwargs,
        )

    def _build_all(self) -> None:
        for nid in self._ids:
            self.nodes_map[nid] = self._new_node(nid, self._storage_factory(nid))

    def start(self) -> None:
        for n in self.nodes_map.values():
            n.start()

    def stop(self) -> None:
        for n in self.nodes_map.values():
            try:
                n.stop()
            except Exception:  # pragma: no cover
                log.exception("stop node failed")

    def nodes(self) -> dict[str, RaftNode]:
        return dict(self.nodes_map)

    def get(self, node_id: str) -> RaftNode:
        return self.nodes_map[node_id]

    def leaders(self) -> list[RaftNode]:
        return [n for n in self.nodes_map.values() if n.is_leader()]

    def wait_for_leader(self, timeout: float = 3.0, *, term: int | None = None) -> RaftNode:
        """等待出现唯一主。

        不同任期短暂出现两个主是合法的（分区愈合瞬间旧主尚未退位），
        这里持续等待视图稳定；同一任期出现两个主则立即判失败。
        """
        deadline = time.time() + timeout
        last_terms: dict[str, int] = {}
        while time.time() < deadline:
            leaders = [
                n for n in self.nodes_map.values()
                if n.is_leader() and (term is None or n.status().term == term)
            ]
            by_term: dict[int, list[RaftNode]] = {}
            for n in leaders:
                by_term.setdefault(n.status().term, []).append(n)
            for t, ns in by_term.items():
                if len(ns) > 1:
                    raise AssertionError(
                        f"two leaders in the SAME term {t}: {[n.node_id for n in ns]}"
                    )
            if leaders:
                terms = {n.node_id: n.status().term for n in leaders}
                # 视图稳定（连续两轮相同）且只有一个主时返回最高任期者
                if len(leaders) == 1 and last_terms == terms:
                    return leaders[0]
                last_terms = terms
            else:
                last_terms = {}
            time.sleep(0.01)
        raise AssertionError(f"no stable leader within {timeout}s")

    def wait_until_committed(self, index: int, *, nodes: list[str] | None = None, timeout: float = 3.0):
        targets = [self.nodes_map[i] for i in nodes] if nodes else list(self.nodes_map.values())
        deadline = time.time() + timeout
        pending = list(targets)
        while time.time() < deadline:
            pending = [n for n in targets if n.status().commit_index < index]
            if not pending:
                return
            time.sleep(0.01)
        raise AssertionError(
            f"index {index} not committed on {[n.node_id for n in pending]} within {timeout}s"
        )

    def wait_until_applied(self, index: int, *, nodes: list[str] | None = None, timeout: float = 3.0):
        """等待指定节点的状态机真正应用到 index（commit 之后的异步 apply 完成）。"""
        targets = [self.nodes_map[i] for i in nodes] if nodes else list(self.nodes_map.values())
        deadline = time.time() + timeout
        while time.time() < deadline:
            pending = [n for n in targets if n.status().last_applied < index]
            if not pending:
                return
            time.sleep(0.01)
        raise AssertionError(
            f"index {index} not applied on {[n.node_id for n in pending]} within {timeout}s"
        )

    # --- 网络故障 ---------------------------------------------------- #
    def partition(self, groups: list[list[str]]) -> None:
        log.warning("PARTITION groups=%s", groups)
        self.transport.partition(groups)

    def isolate(self, node_id: str) -> None:
        log.warning("ISOLATE node=%s", node_id)
        self.transport.isolate(node_id, self._ids)

    def heal(self) -> None:
        log.warning("NETWORK HEAL")
        self.transport.unblock_all()

    # --- 崩溃 / 重启 ------------------------------------------------- #
    def crash_node(self, node_id: str) -> None:
        n = self.nodes_map[node_id]
        self.transport.set_down(node_id, True)
        n.crash()

    def restart_node(self, node_id: str, *, fresh_storage: bool = False) -> RaftNode:
        """重启节点。

        fresh_storage=False（默认）：沿用同一 Storage 实例，模拟进程重启后
        从持久化日志恢复；
        fresh_storage=True：使用全新空存储，模拟数据盘损坏，节点必须从
        多数派重新追赶全部日志。
        """
        old = self.nodes_map[node_id]
        if fresh_storage:
            storage = self._storage_factory(node_id)
            node = self._new_node(node_id, storage)
            self.nodes_map[node_id] = node
        else:
            node = old
        self.transport.set_down(node_id, False)
        node.restart()
        return node
