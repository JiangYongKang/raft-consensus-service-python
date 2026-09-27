"""离散事件模拟器（骨架）：在单进程内用多实例驱动整个集群。"""

from __future__ import annotations

from collections.abc import Callable

from raft_consensus.network import Network
from raft_consensus.node import RaftNode


class Simulator:
    """事件循环 + 网络传输 + 时钟。

    time_unit: 1 个调度单位对应的"秒"。测试用确定性 tick（可注入随机种子），
    本地演示可使用实时 sleep 模式。
    """

    def __init__(self, node_ids: tuple[str, ...], *, realtime: bool = False,
                 time_unit: float = 0.01, seed: int = 0) -> None:
        raise NotImplementedError

    @property
    def network(self) -> Network:
        raise NotImplementedError

    @property
    def nodes(self) -> dict[str, RaftNode]:
        raise NotImplementedError

    def add_node(self, node: RaftNode) -> None:
        raise NotImplementedError

    def now(self) -> float:
        raise NotImplementedError

    def schedule(self, delay: float, fn: Callable[[], None]) -> None:
        """delay 以秒计（内部换算为 tick / 或真实 sleep）。"""
        raise NotImplementedError

    def send(self, env) -> None:
        raise NotImplementedError

    def run_until(self, predicate: Callable[[], bool], timeout: float = 5.0,
                  max_ticks: int = 100_000) -> bool:
        """驱动事件直到 predicate 为真；返回是否成功。"""
        raise NotImplementedError

    def run_for(self, seconds: float) -> None:
        raise NotImplementedError

    def crash(self, node_id: str) -> None:
        """模拟节点崩溃：标记离线，丢弃其待处理定时器与在途响应。"""
        raise NotImplementedError

    def restart(self, node_id: str) -> None:
        """按持久化数据重建节点实例（RaftNode 重新构造并 start）。"""
        raise NotImplementedError
