"""可区分的一致性错误类型。

所有错误都带 ``code``（机器可判别）与说明性 ``reason``，
保证失败不会被静默忽略。
"""


class ConsensusError(Exception):
    """一致性错误基类。"""

    code = "consensus_error"

    def __init__(self, reason: str, *, term: int | None = None):
        super().__init__(reason)
        self.reason = reason
        self.term = term

    def __str__(self) -> str:  # pragma: no cover - 调试用
        t = "" if self.term is None else f" [term={self.term}]"
        return f"{self.code}: {self.reason}{t}"


class NotLeaderError(ConsensusError):
    """请求发给了非主节点，或当前没有主节点。"""

    code = "not_leader"


class StaleTermError(ConsensusError):
    """请求任期比节点当前任期旧，必须拒绝。"""

    code = "stale_term"


class InvalidCommitError(ConsensusError):
    """提交位点不合法（回退或超过已知日志末端）。"""

    code = "invalid_commit_position"


class LogInconsistentError(ConsensusError):
    """追加日志时与本地日志冲突（位点/任期不一致）。"""

    code = "log_inconsistent"


class DuplicateRequestError(ConsensusError):
    """重复请求（同一客户端请求标识重复且参数不同）。"""

    code = "duplicate_request"

    def __init__(self, reason: str, *, request_id: str | None = None, term: int | None = None):
        super().__init__(reason, term=term)
        self.request_id = request_id


class StaleClientRequestError(ConsensusError):
    """客户端序列号回退（过期/重放的客户端请求），与旧任期错误相区分。"""

    code = "stale_client_seq"

    def __init__(self, reason: str, *, client_id: str | None = None, got_seq: int | None = None,
                 max_seq: int | None = None, term: int | None = None):
        super().__init__(reason, term=term)
        self.client_id = client_id
        self.got_seq = got_seq
        self.max_seq = max_seq


class UnauthorizedError(ConsensusError):
    """内部协调操作调用方身份或任期校验失败（越权/过期）。"""

    code = "unauthorized"


class TimeoutError_(ConsensusError):
    """在超时时间内未达成多数派确认。"""

    code = "not_replicated_timeout"
