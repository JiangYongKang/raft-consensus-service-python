"""网络抽象：模拟节点崩溃丢弃、网络分区与不可达。

网络本身不做任期/身份判断（那是节点 handler 的职责），它只负责：
- 离线节点：发给它或由它发出的消息一律丢弃；
- 分区：两端不在同一分区时丢弃（模拟不可达）；
- 可达消息：交给注入的 send_hook（通常排入模拟器事件队列，可附加延迟/乱序/丢包）。
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from raft_consensus.messages import Envelope

logger = logging.getLogger("raft.network")


@dataclass(slots=True)
class Network:
    members: tuple[str, ...]
    _send_hook: Callable[[Envelope], None] | None = None
    _offline: set[str] = field(default_factory=set)
    # 分区表示：每个节点映射到一个分区 id；None 表示全连通
    _groups: dict[str, int] | None = None
    drop_rate: float = 0.0
    _rng_state: object = None  # 随机丢包由模拟器在需要时注入，骨架先保留

    def bind_sender(self, hook: Callable[[Envelope], None]) -> None:
        self._send_hook = hook

    def set_offline(self, node_id: str, offline: bool) -> None:
        if offline:
            self._offline.add(node_id)
        else:
            self._offline.discard(node_id)
        logger.info("network: node %s -> %s", node_id,
                    "OFFLINE" if offline else "ONLINE")

    def is_offline(self, node_id: str) -> bool:
        return node_id in self._offline

    def partition(self, groups: list[set[str]] | None) -> None:
        if groups is None:
            self._groups = None
            logger.info("network: partition HEALED (full connectivity)")
            return
        mapping: dict[str, int] = {}
        for gid, group in enumerate(groups):
            for n in group:
                mapping[n] = gid
        self._groups = mapping
        logger.info("network: partition applied groups=%s",
                    [sorted(g) for g in groups])

    def reachable(self, src: str, dst: str) -> bool:
        if src in self._offline or dst in self._offline:
            return False
        if self._groups is not None:
            gs = self._groups.get(src)
            gd = self._groups.get(dst)
            if gs != gd:
                return False
        return True

    def send(self, env: Envelope) -> None:
        if env.src in self._offline:
            logger.info("drop(from offline src=%s dst=%s id=%s)",
                        env.src, env.dst, env.msg_id)
            return
        if env.dst in self._offline:
            logger.info("drop(to offline dst=%s src=%s id=%s)",
                        env.dst, env.src, env.msg_id)
            return
        if self._groups is not None and \
                self._groups.get(env.src) != self._groups.get(env.dst):
            logger.info("drop(partitioned src=%s dst=%s id=%s)",
                        env.src, env.dst, env.msg_id)
            return
        assert self._send_hook is not None, "network sender not bound"
        self._send_hook(env)
