"""Raft 复制日志的数据结构与纯逻辑。

约定：
- 位点 index 从 1 开始；entries[0] 是 index=0/term=0 的哨兵。
- 本类只做内存逻辑，落盘/恢复由 storage.PersistentStore 负责。
- 任何会删除/覆盖 index <= commit_index 条目的操作，都会抛出
  CommittedRollbackError 被明确拒绝（不允许"从中途继续"式的静默回退）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from raft_consensus.errors import (
    CommittedRollbackError,
    LogCheckMismatchError,
)

@dataclass(frozen=True, slots=True)
class LogEntry:
    """一条日志：位点 index 从 1 开始；0 表示空哨兵。"""

    term: int
    index: int
    client_id: str
    request_id: str
    command: dict
    client_seq: int = 0


_SENTINEL = object()


def _sentinel_entry() -> LogEntry:
    return LogEntry(term=0, index=0, client_id="", request_id="", command={})


@dataclass(slots=True)
class RaftLog:
    entries: list[LogEntry] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.entries:
            self.entries = [_sentinel_entry()]
        else:
            assert self.entries[0].index == 0 and self.entries[0].term == 0

    @property
    def last_index(self) -> int:
        return self.entries[-1].index

    @property
    def last_term(self) -> int:
        return self.entries[-1].term

    def term_at(self, index: int) -> int:
        if index <= 0:
            return 0
        if index > self.last_index:
            raise IndexError(f"log index {index} beyond last {self.last_index}")
        return self.entries[index].term

    def entry_at(self, index: int) -> LogEntry:
        return self.entries[index]

    def append(self, entries: list[LogEntry]) -> int:
        """领导者产生新日志：按顺序追加。条目的 index 必须连续衔接。"""
        for e in entries:
            if e.index != self.last_index + 1:
                raise ValueError(
                    f"non-contiguous append: got index {e.index}, "
                    f"expected {self.last_index + 1}",
                )
            self.entries.append(e)
        return self.last_index

    def match_and_truncate(self, prev_index: int, prev_term: int,
                           new_entries: list[LogEntry],
                           commit_index: int) -> None:
        """跟随者侧按 Raft 论文 §5.3 处理 AppendEntries 的日志部分。

        1) prev 位点/任期必须匹配，否则抛 LogCheckMismatchError（带回退提示）；
        2) 冲突条目（同位点不同任期）连同后续一起删除；
        3) 已存在的跳过，缺失的追加；
        4) 任何试图删除 index <= commit_index 条目的操作抛
           CommittedRollbackError —— 已提交内容绝不允许回退。
        """
        # --- 边界 0：prev_index 不能落在已提交区间之内再要求覆盖 ---
        if prev_index < commit_index:
            raise CommittedRollbackError(
                commit_index=commit_index, requested_index=prev_index,
            )

        # --- 边界 1：prev 一致性检查 ---
        if prev_index > self.last_index:
            raise LogCheckMismatchError(
                expected_index=prev_index, expected_term=prev_term,
                hint_index=self.last_index,
                follower_last_index=self.last_index)
        actual_term = self.term_at(prev_index)
        if actual_term != prev_term:
            # XTerm/XIndex：冲突任期在本地日志中的第一条位点
            first = prev_index
            while first > 1 and self.entries[first - 1].term == actual_term:
                first -= 1
            raise LogCheckMismatchError(
                expected_index=prev_index, expected_term=prev_term,
                hint_index=first - 1,
                conflict_term=actual_term, conflict_first_index=first,
                follower_last_index=self.last_index)

        # --- 合并 new_entries ---
        for offset, incoming in enumerate(new_entries):
            idx = prev_index + 1 + offset
            if idx <= self.last_index:
                if self.entries[idx].term == incoming.term:
                    # 同任期同位点：视为同一条（幂等重传），跳过
                    continue
                # 冲突：从 idx 起截断。已提交区间受保护。
                if idx <= commit_index:
                    raise CommittedRollbackError(
                        commit_index=commit_index, requested_index=idx,
                    )
                del self.entries[idx:]
            self.entries.append(incoming)

    def entries_since(self, index: int) -> list[LogEntry]:
        """返回 index 之后（不含 index）的所有条目；越界返回空列表。"""
        if index < 0:
            raise IndexError(index)
        if index >= self.last_index:
            return []
        return self.entries[index + 1:]

    def up_to_date(self, last_index: int, last_term: int) -> bool:
        """候选人日志是否"至少和本节点一样新"（投票限制，论文 §5.4.1）。"""
        if last_term != self.last_term:
            return last_term > self.last_term
        return last_index >= self.last_index
