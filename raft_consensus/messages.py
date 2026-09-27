"""内部协调 RPC 的消息定义（骨架：仅数据结构）。

所有内部消息都带 caller(来源节点) 与 term(请求任期)，
接收方必须校验身份与任期，过期/越权请求以可区分原因拒绝。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from raft_consensus.log import LogEntry


@dataclass(frozen=True, slots=True)
class RequestVote:
    term: int
    candidate_id: str          # 即 caller
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True, slots=True)
class RequestVoteResult:
    term: int                  # 接收方当前任期（候选人据此回退）
    vote_granted: bool
    voter_id: str
    reason: str                # 拒绝原因说明，保证结论可解释


@dataclass(frozen=True, slots=True)
class AppendEntries:
    term: int
    leader_id: str             # 即 caller
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]
    leader_commit: int
    request_id: str            # 内部 RPC 请求标识，便于日志追踪


@dataclass(frozen=True, slots=True)
class AppendEntriesResult:
    term: int
    success: bool
    follower_id: str
    match_index: int           # 成功时跟随者已复制到的位点
    hint_index: int            # 失败时建议领导者回退到的位点
    reason: str


# 统一的网络信封
@dataclass(frozen=True, slots=True)
class Envelope:
    src: str
    dst: str
    payload: object
    msg_id: str
