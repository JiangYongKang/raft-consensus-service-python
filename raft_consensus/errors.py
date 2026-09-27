"""可区分的失败原因。

每一类失败都对应一个独立异常，便于调用方与测试精确判断"为什么被拒绝"，
而不是靠字符串匹配或静默忽略。
"""

from __future__ import annotations


class ConsensusError(Exception):
    """所有一致性相关错误的基类。"""

    def __init__(self, reason: str, *, term: int | None = None, node: str | None = None):
        super().__init__(reason)
        self.reason = reason
        self.term = term
        self.node = node


class NotLeaderError(ConsensusError):
    """当前节点不是主节点（或在本任期尚未确认主节点身份），拒绝写入。"""

    def __init__(self, *, term: int, leader_id: str | None, node: str | None = None):
        super().__init__(f"not leader in term {term}; hint leader={leader_id!r}",
                         term=term, node=node)
        self.leader_id = leader_id


class StaleTermError(ConsensusError):
    """请求携带的任期比接收方更旧，必须拒绝（不能静默覆盖）。"""

    def __init__(self, *, request_term: int, current_term: int,
                 node: str | None = None, kind: str = "request"):
        super().__init__(
            f"stale term: {kind} term {request_term} < current term {current_term}",
            term=current_term, node=node,
        )
        self.request_term = request_term
        self.current_term = current_term


class AlreadyVotedError(ConsensusError):
    """同一任期已经把票投给了另一个候选人，拒绝重复投票。"""

    def __init__(self, *, term: int, voted_for: str | None, node: str | None = None):
        super().__init__(
            f"already voted in term {term} for {voted_for!r}", term=term, node=node,
        )
        self.voted_for = voted_for


class LogCheckMismatchError(ConsensusError):
    """AppendEntries 的前序位点/任期一致性检查不匹配，拒绝并回退位点。

    携带论文 §5.3 快速回退协商信息：
      conflict_term / conflict_first_index / follower_last_index。
    """

    def __init__(self, *, expected_index: int, expected_term: int,
                 hint_index: int, node: str | None = None,
                 conflict_term: int = 0, conflict_first_index: int = 0,
                 follower_last_index: int = 0):
        super().__init__(
            f"log mismatch at index {expected_index} term {expected_term}; "
            f"hint next index {hint_index}",
            node=node,
        )
        self.expected_index = expected_index
        self.expected_term = expected_term
        self.hint_index = hint_index
        self.conflict_term = conflict_term
        self.conflict_first_index = conflict_first_index
        self.follower_last_index = follower_last_index


class CommittedRollbackError(ConsensusError):
    """复制/截断会覆盖已提交的日志条目，明确拒绝位点回退。"""

    def __init__(self, *, commit_index: int, requested_index: int, node: str | None = None):
        super().__init__(
            f"refuse to roll back committed entries: commit_index={commit_index} "
            f"requested_index={requested_index}",
            node=node,
        )
        self.commit_index = commit_index
        self.requested_index = requested_index


class DuplicateRequestError(ConsensusError):
    """同一 (client_id, request_id) 重复提交；不产生重复结果。

    注意：幂等命中在正常路径返回首次结果，本异常只在显式检查/冲突场景使用。
    """

    def __init__(self, *, client_id: str, request_id: str, node: str | None = None):
        super().__init__(
            f"duplicate request {client_id}/{request_id}", node=node,
        )
        self.client_id = client_id
        self.request_id = request_id


class UnknownCallerError(ConsensusError):
    """内部协调操作的调用方身份未知或不合法。"""

    def __init__(self, *, caller: str, node: str | None = None):
        super().__init__(f"unknown/unauthorized caller {caller!r}", node=node)
        self.caller = caller


class NodeOfflineError(ConsensusError):
    """目标节点处于崩溃/离线状态，消息被丢弃。"""

    def __init__(self, *, node: str):
        super().__init__(f"node {node!r} is offline", node=node)


class PartitionedError(ConsensusError):
    """消息跨越当前网络分区，被丢弃（模拟不可达）。"""

    def __init__(self, *, src: str, dst: str):
        super().__init__(f"network partition between {src!r} and {dst!r}", node=dst)
        self.src = src
        self.dst = dst
