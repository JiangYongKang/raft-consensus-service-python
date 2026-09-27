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
