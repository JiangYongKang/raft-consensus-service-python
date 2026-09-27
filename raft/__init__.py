"""本地 Raft 一致性与协调服务。"""

from .errors import (
    ConsensusError,
    DuplicateRequestError,
    InvalidCommitError,
    LogInconsistentError,
    NotLeaderError,
    StaleTermError,
    UnauthorizedError,
)
from .types import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    ClientResponse,
    LogEntry,
    NodeStatus,
    RequestVoteRequest,
    RequestVoteResponse,
    Role,
)

__all__ = [
    "ConsensusError",
    "DuplicateRequestError",
    "InvalidCommitError",
    "LogInconsistentError",
    "NotLeaderError",
    "StaleTermError",
    "UnauthorizedError",
    "LogEntry",
    "Role",
    "NodeStatus",
    "RequestVoteRequest",
    "RequestVoteResponse",
    "AppendEntriesRequest",
    "AppendEntriesResponse",
    "ClientResponse",
]
