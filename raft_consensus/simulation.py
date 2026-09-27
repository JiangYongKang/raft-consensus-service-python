"""离散事件模拟器：单进程内用多个节点实例驱动整个集群。

设计要点：
- 所有事件在单条驱动线程上串行执行 -> 节点内部无需加锁也不会数据竞争；
  "多实例"体现在多个独立 RaftNode + 独立持久化目录 + 独立定时器。
- 事件带 (owner, generation)：节点崩溃时代际 +1，其未触发定时器与在途
  响应自动失效；重启则按磁盘数据重建节点（测试恢复语义）。
- realtime=False：虚拟时钟瞬时推进（确定性，适合单测）；
  realtime=True：按墙钟 sleep 到事件触发时刻（本地演示，可肉眼观察）。
"""

from __future__ import annotations

import heapq
import itertools
import logging
import time
import uuid
from collections.abc import Callable

from raft_consensus.messages import Envelope
from raft_consensus.network import Network
from raft_consensus.node import RaftNode

logger = logging.getLogger("raft.sim")


class Simulator:
    def __init__(self, node_ids: tuple[str, ...], *, realtime: bool = False,
                 time_unit: float = 0.01, delivery_delay: float = 0.004,
                 seed: int = 0) -> None:
        self.node_ids = tuple(node_ids)
        self.realtime = realtime
        self.time_unit = time_unit
        self.delivery_delay = delivery_delay
        self._network = Network(members=self.node_ids)
        self._network.bind_sender(self._enqueue_envelope)
        self.nodes: dict[str, RaftNode] = {}
        self.builders: dict[str, Callable[[], RaftNode]] = {}
        self.generation: dict[str, int] = {n: 0 for n in node_ids}
        self._heap: list = []
        self._seq = itertools.count()
        self._now = 0.0          # 虚拟秒
        self._wall_start = time.monotonic()
        self._stopped = False

    @property
    def network(self) -> Network:
        return self._network

    def add_node(self, node: RaftNode) -> None:
        self.nodes[node.node_id] = node

    def register_builder(self, node_id: str, builder: Callable[[], RaftNode]) -> None:
        self.builders[node_id] = builder

    def now(self) -> float:
        if self.realtime:
            return time.monotonic() - self._wall_start
        return self._now

    def make_scheduler(self, owner: str) -> Callable[[float, Callable[[], None]], None]:
        def sched(delay: float, fn: Callable[[], None]) -> None:
            self._schedule_owned(owner, delay, fn)
        return sched

    def make_clock(self) -> Callable[[], float]:
        return self.now

    def _schedule_owned(self, owner: str, delay: float, fn: Callable[[], None]) -> None:
        due = self._now + max(delay, 0.0)
        gen = self.generation.get(owner, 0)
        heapq.heappush(self._heap, (due, next(self._seq), owner, gen, fn))

    def schedule(self, delay: float, fn: Callable[[], None]) -> None:
        # 系统级事件（不属于某个节点），不会因崩溃失效
        self._schedule_owned("__system__", delay, fn)

    def _enqueue_envelope(self, env: Envelope) -> None:
        # 消息接收事件归 dst 所有：dst 崩溃/重启后旧代际消息自动丢弃
        self._schedule_owned(env.dst, self.delivery_delay,
                             lambda: self._deliver(env))

    def _deliver(self, env: Envelope) -> None:
        target = self.nodes.get(env.dst)
        if target is None:
            logger.info("drop(no node) id=%s dst=%s", env.msg_id, env.dst)
            return
        if self._network.is_offline(env.dst):
            return
        target.dispatch(env)

    def send(self, env: Envelope) -> None:
        self._network.send(env)

    def _next_valid_event(self):
        while self._heap:
            ev = heapq.heappop(self._heap)
            due, _, owner, gen, fn = ev
            if owner == "__system__":
                return ev
            if self.generation.get(owner, 0) == gen and \
                    not self._network.is_offline(owner):
                return ev
            # 崩溃节点的遗留事件：丢弃
        return None

    def step(self) -> bool:
        """执行一个有效事件，返回是否执行了。"""
        ev = self._next_valid_event()
        if ev is None:
            return False
        due, _, owner, gen, fn = ev
        self._now = due
        if self.realtime:
            wait = (self._wall_start + due) - time.monotonic()
            if wait > 0:
                time.sleep(wait)
        fn()
        return True

    def run_until(self, predicate: Callable[[], bool], timeout: float = 5.0,
                  max_ticks: int = 200_000) -> bool:
        deadline = self._now + timeout
        steps = 0
        while not predicate():
            if self._now > deadline or steps > max_ticks:
                return False
            if not self.step():
                # 没有可执行事件了
                return predicate()
            steps += 1
        return True

    def run_for(self, seconds: float) -> None:
        deadline = self._now + seconds
        while self._heap and self._now < deadline:
            ev = self._next_valid_event()
            if ev is None:
                break
            due, _, _, _, fn = ev
            if due > deadline:
                break
            if self.realtime:
                target_wall = self._wall_start + due
                wait = target_wall - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
            self._now = due
            fn()

    def crash(self, node_id: str) -> None:
        node = self.nodes.pop(node_id, None)
        if node is not None:
            node.shutdown()
        self.generation[node_id] += 1
        self._network.set_offline(node_id, True)
        logger.warning("=== node %s CRASHED (gen=%d) ===",
                       node_id, self.generation[node_id])

    def restart(self, node_id: str) -> RaftNode:
        assert node_id in self.builders, f"no builder for {node_id}"
        self._network.set_offline(node_id, False)
        node = self.builders[node_id]()   # 构造时从磁盘恢复
        self.nodes[node_id] = node
        logger.warning("=== node %s RESTARTED (gen=%d), restored from disk ===",
                       node_id, self.generation[node_id])
        node.start()
        return node

    def new_msg_id(self) -> str:
        return uuid.uuid4().hex[:10]
