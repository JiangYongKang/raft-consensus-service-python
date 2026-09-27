"""Raft 复制日志的数据结构与接口（骨架：实现待填充）。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class LogEntry:
    """一条日志：位点 index 从 1 开始；0 表示空哨兵。"""

    term: int
    index: int
    client_id: str
    request_id: str
    command: dict
    client_seq: int = 0


@dataclass(slots=True)
class RaftLog:
    """内存中的复制日志。持久化由 storage 负责，这里只做纯逻辑。

    entries[0] 是 index=0、term=0 的哨兵条目。
    """

    entries: list[LogEntry] = field(default_factory=list)

    def __post_init__(self) -> None:
        # 实现待填充：确保哨兵存在。
        raise NotImplementedError

    @property
    def last_index(self) -> int:
        raise NotImplementedError

    @property
    def last_term(self) -> int:
        raise NotImplementedError

    def term_at(self, index: int) -> int:
        raise NotImplementedError

    def entry_at(self, index: int) -> LogEntry:
        raise NotImplementedError

    def append(self, entries: list[LogEntry]) -> int:
        """追加新条目（仅在领导者产生新日志时使用），返回最后位点。"""
        raise NotImplementedError

    def match_and_truncate(self, prev_index: int, prev_term: int,
                           entries: list[LogEntry], commit_index: int) -> None:
        """按 Raft 规则做一致性检查并追加；拒绝回退已提交条目。"""
        raise NotImplementedError

    def entries_since(self, index: int) -> list[LogEntry]:
        raise NotImplementedError

    def up_to_date(self, last_index: int, last_term: int) -> bool:
        """候选人日志是否不比本节点旧（投票用）。"""
        raise NotImplementedError
