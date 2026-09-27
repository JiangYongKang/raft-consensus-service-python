"""本地可运行的 Raft 分布式一致性与协调服务。

模块概览见 docs/design.md。所有失败均以可区分的异常类型表达原因。
"""

from raft_consensus.errors import (
    ConsensusError,
    NotLeaderError,
    StaleTermError,
    AlreadyVotedError,
    LogCheckMismatchError,
    CommittedRollbackError,
    DuplicateRequestError,
    UnknownCallerError,
    NodeOfflineError,
    PartitionedError,
)

__all__ = [
    "ConsensusError",
    "NotLeaderError",
    "StaleTermError",
    "AlreadyVotedError",
    "LogCheckMismatchError",
    "CommittedRollbackError",
    "DuplicateRequestError",
    "UnknownCallerError",
    "NodeOfflineError",
    "PartitionedError",
]
