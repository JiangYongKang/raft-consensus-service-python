"""持久化存储：当前任期、投票、日志条目。

内存版用于单测；文件版（JSON）用于多进程本地集群与崩溃重启验证。
文件写入采用“临时文件 + fsync + 原子 rename”，保证崩溃后状态完整。
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict

from .errors import InvalidCommitError
from .types import LogEntry


def _entry_from_dict(d: dict) -> LogEntry:
    return LogEntry(
        index=d["index"],
        term=d["term"],
        command=d["command"],
        client_id=d["client_id"],
        request_id=d["request_id"],
        client_seq=d["client_seq"],
    )


class Storage:
    """持久化存储抽象基类。"""

    @property
    def current_term(self) -> int:
        raise NotImplementedError

    @property
    def voted_for(self) -> str | None:
        raise NotImplementedError

    def save_term(self, term: int, voted_for: str | None) -> None:
        raise NotImplementedError

    def append_entries(self, entries: list[LogEntry]) -> None:
        """在日志末尾追加；调用方保证 entries 的 index 连续且严格递增。"""
        raise NotImplementedError

    def truncate_from(self, index: int) -> None:
        """删除所有 index >= ``index`` 的条目（冲突截断）。"""
        raise NotImplementedError

    def get_entry(self, index: int) -> LogEntry | None:
        raise NotImplementedError

    def last_index(self) -> int:
        raise NotImplementedError

    def last_term(self) -> int:
        raise NotImplementedError

    def term_at(self, index: int) -> int:
        """返回 index 处条目的任期；index==0 返回 0；不存在抛 IndexError。"""
        raise NotImplementedError

    def entries_from(self, start_index: int) -> list[LogEntry]:
        raise NotImplementedError

    def all_entries(self) -> list[LogEntry]:
        raise NotImplementedError

    def save_commit_index(self, commit_index: int) -> None:
        """持久化已提交位点（用于崩溃后恢复提交边界，必须单调不减）。"""
        raise NotImplementedError

    def commit_index(self) -> int:
        raise NotImplementedError

    def reload(self) -> None:
        """从持久介质重新读取（重启恢复用）；内存实现为空操作。"""
        pass

    def close(self) -> None:
        pass


class InMemoryStorage(Storage):
    def __init__(self) -> None:
        self._term = 0
        self._voted_for: str | None = None
        self._log: list[LogEntry] = []
        self._commit_index = 0

    @property
    def current_term(self) -> int:
        return self._term

    @property
    def voted_for(self) -> str | None:
        return self._voted_for

    def save_term(self, term: int, voted_for: str | None) -> None:
        if term < self._term:
            raise ValueError(f"term cannot go backwards: {term} < {self._term}")
        self._term = term
        self._voted_for = voted_for

    def append_entries(self, entries: list[LogEntry]) -> None:
        for e in entries:
            if self._log and e.index != self._log[-1].index + 1:
                raise IndexError(
                    f"non-contiguous append: got {e.index}, expected {self._log[-1].index + 1}"
                )
            if not self._log and e.index != 1:
                raise IndexError(f"first entry must have index 1, got {e.index}")
            self._log.append(e)

    def truncate_from(self, index: int) -> None:
        if index <= 0:
            raise IndexError(f"truncate index must be positive, got {index}")
        if index > len(self._log):
            return
        del self._log[index - 1 :]

    def get_entry(self, index: int) -> LogEntry | None:
        if 1 <= index <= len(self._log):
            return self._log[index - 1]
        return None

    def last_index(self) -> int:
        return len(self._log)

    def last_term(self) -> int:
        return self._log[-1].term if self._log else 0

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        e = self.get_entry(index)
        if e is None:
            raise IndexError(f"no entry at index {index}")
        return e.term

    def entries_from(self, start_index: int) -> list[LogEntry]:
        if start_index <= 0:
            return list(self._log)
        return list(self._log[start_index - 1 :])

    def all_entries(self) -> list[LogEntry]:
        return list(self._log)

    def save_commit_index(self, commit_index: int) -> None:
        if commit_index < self._commit_index:
            raise InvalidCommitError(
                f"commit index cannot go backwards: {commit_index} < {self._commit_index}",
                term=None,
            )
        self._commit_index = commit_index

    def commit_index(self) -> int:
        return self._commit_index


class FileStorage(Storage):
    """JSON 文件持久化。状态包含 term / voted_for / log，整体原子落盘。"""

    def __init__(self, data_dir: str, node_id: str) -> None:
        self._node_id = node_id
        os.makedirs(data_dir, exist_ok=True)
        self._path = os.path.join(data_dir, f"{node_id}.state.json")
        self._term = 0
        self._voted_for: str | None = None
        self._log: list[LogEntry] = []
        self._commit_index = 0
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self._path):
            return
        with open(self._path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self._term = int(data.get("current_term", 0))
        self._voted_for = data.get("voted_for")
        self._log = [_entry_from_dict(d) for d in data.get("log", [])]
        self._commit_index = int(data.get("commit_index", 0))

    def _persist(self) -> None:
        payload = {
            "node_id": self._node_id,
            "current_term": self._term,
            "voted_for": self._voted_for,
            "log": [asdict(e) for e in self._log],
            "commit_index": self._commit_index,
        }
        d = os.path.dirname(self._path)
        fd, tmp = tempfile.mkstemp(prefix=".state-", suffix=".tmp", dir=d)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._path)
            dfd = os.open(d, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    @property
    def current_term(self) -> int:
        return self._term

    @property
    def voted_for(self) -> str | None:
        return self._voted_for

    def save_term(self, term: int, voted_for: str | None) -> None:
        if term < self._term:
            raise ValueError(f"term cannot go backwards: {term} < {self._term}")
        self._term = term
        self._voted_for = voted_for
        self._persist()

    def append_entries(self, entries: list[LogEntry]) -> None:
        for e in entries:
            if self._log and e.index != self._log[-1].index + 1:
                raise IndexError(
                    f"non-contiguous append: got {e.index}, expected {self._log[-1].index + 1}"
                )
            if not self._log and e.index != 1:
                raise IndexError(f"first entry must have index 1, got {e.index}")
            self._log.append(e)
        if entries:
            self._persist()

    def truncate_from(self, index: int) -> None:
        if index <= 0:
            raise IndexError(f"truncate index must be positive, got {index}")
        if index > len(self._log):
            return
        del self._log[index - 1 :]
        self._persist()

    def get_entry(self, index: int) -> LogEntry | None:
        if 1 <= index <= len(self._log):
            return self._log[index - 1]
        return None

    def last_index(self) -> int:
        return len(self._log)

    def last_term(self) -> int:
        return self._log[-1].term if self._log else 0

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        e = self.get_entry(index)
        if e is None:
            raise IndexError(f"no entry at index {index}")
        return e.term

    def entries_from(self, start_index: int) -> list[LogEntry]:
        if start_index <= 0:
            return list(self._log)
        return list(self._log[start_index - 1 :])

    def all_entries(self) -> list[LogEntry]:
        return list(self._log)

    def save_commit_index(self, commit_index: int) -> None:
        if commit_index < self._commit_index:
            raise InvalidCommitError(
                f"commit index cannot go backwards: {commit_index} < {self._commit_index}",
                term=None,
            )
        self._commit_index = commit_index
        self._persist()

    def commit_index(self) -> int:
        return self._commit_index

    def reload(self) -> None:
        """重新从磁盘读取：重启后必须看到崩溃期间其他进程对文件的写入。"""
        self._load()
