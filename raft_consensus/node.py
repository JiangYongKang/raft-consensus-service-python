"""Raft 节点核心（骨架：仅类型与接口，实现待逐函数填充）。

线程模型：每个节点由单线程的事件循环（Simulator）驱动，所有方法都在
该线程内串行执行，因此节点内部无需额外加锁；跨节点通过消息队列通信。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from raft_consensus.log import LogEntry, RaftLog
from raft_consensus.messages import (
    AppendEntries,
    AppendEntriesResult,
    Envelope,
    RequestVote,
    RequestVoteResult,
)
from raft_consensus.statemachine import KVStateMachine
from raft_consensus.storage import PersistentStore

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


@dataclass(slots=True)
class NodeConfig:
    node_id: str
    peers: tuple[str, ...]           # 含自己
    data_dir: str | Path
    election_timeout_min: float = 0.15   # 秒（真实时钟）/ tick（模拟时钟）
    election_timeout_max: float = 0.30
    heartbeat_interval: float = 0.05


@dataclass(slots=True)
class Proposal:
    """一次客户端写请求在领导者上的登记。"""

    client_id: str
    request_id: str
    command: dict
    term: int
    index: int = -1
    done: bool = False
    result: object = None


class RaftNode:
    def __init__(self, config: NodeConfig,
                 send: Callable[[Envelope], None],
                 schedule: Callable[[float, Callable[[], None]], None],
                 now: Callable[[], float],
                 applied_callback: Callable[[str, int, int, dict, object], None],
                 log: Callable[[str], None] | None = None) -> None:
        # 实现待填充
        raise NotImplementedError

    # ---- 生命周期 ----
    def start(self) -> None:
        raise NotImplementedError

    def shutdown(self) -> None:
        raise NotImplementedError

    # ---- 客户端入口 ----
    def client_write(self, client_id: str, request_id: str,
                     command: dict) -> tuple[int, object]:
        """返回 (提交位点, 应用结果)；非领导者/旧任期抛对应异常。"""
        raise NotImplementedError

    def client_read(self, key: str) -> object:
        raise NotImplementedError

    # ---- 内部协调 RPC（必须校验 caller 身份与任期）----
    def handle_request_vote(self, msg: RequestVote, caller: str) -> RequestVoteResult:
        raise NotImplementedError

    def handle_append_entries(self, msg: AppendEntries, caller: str) -> AppendEntriesResult:
        raise NotImplementedError

    def dispatch(self, env: Envelope) -> None:
        """网络层入口：校验 caller 身份后分发到对应 handler。"""
        raise NotImplementedError

    # ---- 事件循环回调 ----
    def on_election_timeout(self) -> None:
        raise NotImplementedError

    def on_heartbeat(self) -> None:
        raise NotImplementedError
