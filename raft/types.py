"""核心数据结构（值对象）。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True)
class LogEntry:
    """一条日志条目。

    index 从 1 开始；term 是该条目所属任期；
    client_id / request_id 用于幂等去重；
    client_seq 是该客户端单调递增的序列号，用于拒绝过期请求。
    """

    index: int
    term: int
    command: dict
    client_id: str
    request_id: str
    client_seq: int
    result: dict | None = None  # 状态机应用后由主节点回填（非共识字段）


@dataclass
class RequestVoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool
    reason: str = ""


@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry] = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int = 0
    reason: str = ""


@dataclass
class Snapshot:
    """一份紧凑的状态快照：覆盖 ``last_included_index`` 及之前的全部已提交历史。

    data 是状态机在压缩点的完整状态；dedup / client_max_seq 是幂等去重与
    客户端序列号水位，随快照一起持久化/传输，保证压缩后旧请求的重试仍然幂等。
    """

    last_included_index: int
    last_included_term: int
    data: dict = field(default_factory=dict)
    dedup: dict = field(default_factory=dict)  # "client_id\u0001request_id" -> [index, client_seq, result]
    client_max_seq: dict = field(default_factory=dict)  # client_id -> max client_seq


@dataclass
class InstallSnapshotRequest:
    """主节点向落后过多的 follower 发送快照（追赶用）。

    snapshot_id 是本次发送的请求标识，便于在日志中追踪“这次追赶是从谁那里、
    用哪个请求带回来的”。
    """

    term: int
    leader_id: str
    last_included_index: int
    last_included_term: int
    snapshot_id: str
    data: dict = field(default_factory=dict)
    dedup: dict = field(default_factory=dict)
    client_max_seq: dict = field(default_factory=dict)


@dataclass
class InstallSnapshotResponse:
    term: int
    success: bool
    last_included_index: int = 0  # 接收方当前快照边界（无论是否采纳）
    reason: str = ""


@dataclass
class ClientCommand:
    client_id: str
    request_id: str
    client_seq: int
    command: dict


@dataclass
class ClientResponse:
    success: bool
    index: int = 0
    term: int = 0
    result: dict | None = None
    code: str = ""
    reason: str = ""
    leader_id: str | None = None


@dataclass
class NodeStatus:
    node_id: str
    role: Role
    term: int
    voted_for: str | None
    leader_id: str | None
    commit_index: int
    last_applied: int
    last_log_index: int
    last_log_term: int
    snapshot_index: int = 0
    snapshot_term: int = 0
