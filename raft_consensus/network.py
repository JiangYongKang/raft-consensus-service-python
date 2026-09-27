"""网络抽象（骨架）：可模拟崩溃丢弃、网络分区与传输延迟。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from raft_consensus.messages import Envelope


@dataclass(slots=True)
class Network:
    """模拟网络。

    - offline 集合内的节点：发给它 / 由它发出的消息一律丢弃
    - partitions：分区集合列表，消息两端必须属于同一分区才可达
    - send：把消息排入模拟器事件队列（由 Simulator 实现时注入）
    """

    members: tuple[str, ...]
    _send_hook: Callable[[Envelope], None] | None = None  # 实现时填充

    def bind_sender(self, hook: Callable[[Envelope], None]) -> None:
        raise NotImplementedError

    def set_offline(self, node_id: str, offline: bool) -> None:
        raise NotImplementedError

    def is_offline(self, node_id: str) -> bool:
        raise NotImplementedError

    def partition(self, groups: list[set[str]] | None) -> None:
        """设置分区（如 [{'a','b'},{'c'}]）；None 表示恢复全连通。"""
        raise NotImplementedError

    def reachable(self, src: str, dst: str) -> bool:
        raise NotImplementedError

    def send(self, env: Envelope) -> None:
        raise NotImplementedError
