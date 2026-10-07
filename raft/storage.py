"""持久化存储：当前任期、投票、日志条目、状态快照。

内存版用于单测；文件版（JSON）用于多进程本地集群与崩溃重启验证。
文件写入采用“临时文件 + fsync + 原子 rename”，保证崩溃后状态完整。

日志压缩：``compact_upto(index)`` 丢弃 index 及之前的条目，
以 (base_index, base_term) 作为日志逻辑起点；快照本体（状态机数据 +
幂等去重元数据）通过 ``save_snapshot`` / ``load_snapshot`` 存取。
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

    # --- 日志压缩与快照 --------------------------------------------- #
    def snapshot_index(self) -> int:
        """快照覆盖到的日志位点（日志逻辑起点）；无快照时为 0。"""
        raise NotImplementedError

    def snapshot_term(self) -> int:
        """快照边界条目的任期；无快照时为 0。"""
        raise NotImplementedError

    def save_snapshot(self, meta: dict, data: dict) -> None:
        """持久化快照：meta 含 snapshot_id/last_included_index/last_included_term。"""
        raise NotImplementedError

    def load_snapshot(self) -> tuple[dict, dict] | None:
        """读取快照 (meta, data)；无快照返回 None。"""
        raise NotImplementedError

    def compact_upto(self, index: int, term: int | None = None) -> None:
        """丢弃 index 及之前的日志条目（index 必须 <= 已提交且已快照）。

        ``term`` 为边界条目任期；缺省时从本地日志查（本地压缩场景），
        快照追赶场景下边界条目不在本地日志中，由调用方显式传入。
        """
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
        self._base_index = 0
        self._base_term = 0
        self._snapshot: tuple[dict, dict] | None = None

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
            expected = self._log[-1].index + 1 if self._log else self._base_index + 1
            if e.index != expected:
                raise IndexError(
                    f"non-contiguous append: got {e.index}, expected {expected}"
                )
            self._log.append(e)

    def truncate_from(self, index: int) -> None:
        if index <= self._base_index:
            raise IndexError(
                f"cannot truncate into snapshot: index {index} <= base {self._base_index}"
            )
        offset = index - self._base_index - 1
        if offset >= len(self._log):
            return
        del self._log[offset:]

    def get_entry(self, index: int) -> LogEntry | None:
        offset = index - self._base_index - 1
        if 0 <= offset < len(self._log):
            return self._log[offset]
        return None

    def last_index(self) -> int:
        return self._log[-1].index if self._log else self._base_index

    def last_term(self) -> int:
        return self._log[-1].term if self._log else self._base_term

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        if index == self._base_index:
            return self._base_term
        e = self.get_entry(index)
        if e is None:
            raise IndexError(f"no entry at index {index}")
        return e.term

    def entries_from(self, start_index: int) -> list[LogEntry]:
        offset = max(0, start_index - self._base_index - 1)
        return list(self._log[offset:])

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

    # --- 日志压缩与快照 --------------------------------------------- #
    def snapshot_index(self) -> int:
        return self._base_index

    def snapshot_term(self) -> int:
        return self._base_term

    def save_snapshot(self, meta: dict, data: dict) -> None:
        self._snapshot = (dict(meta), data)

    def load_snapshot(self) -> tuple[dict, dict] | None:
        return self._snapshot

    def compact_upto(self, index: int, term: int | None = None) -> None:
        if index <= self._base_index:
            return
        if index > self._commit_index:
            raise InvalidCommitError(
                f"cannot compact uncommitted entries: {index} > commit {self._commit_index}",
                term=None,
            )
        if term is None:
            term = self.term_at(index)
        del self._log[: index - self._base_index]
        self._base_index = index
        self._base_term = term


class FileStorage(Storage):
    """JSON 文件持久化。状态包含 term / voted_for / log，整体原子落盘。

    快照单独存放在 ``{node_id}.snapshot.json``（meta + 状态机数据 +
    幂等元数据）；主状态文件只保留快照边界之后的日志条目。
    """

    def __init__(self, data_dir: str, node_id: str) -> None:
        self._node_id = node_id
        os.makedirs(data_dir, exist_ok=True)
        self._path = os.path.join(data_dir, f"{node_id}.state.json")
        self._snapshot_path = os.path.join(data_dir, f"{node_id}.snapshot.json")
        self._term = 0
        self._voted_for: str | None = None
        self._log: list[LogEntry] = []
        self._commit_index = 0
        self._base_index = 0
        self._base_term = 0
        self._load()

    def _load(self) -> None:
        # 先读快照确定日志逻辑起点，再读主状态文件并丢弃被快照覆盖的条目
        self._base_index = 0
        self._base_term = 0
        snap = self.load_snapshot()
        if snap is not None:
            meta, _ = snap
            self._base_index = int(meta.get("last_included_index", 0))
            self._base_term = int(meta.get("last_included_term", 0))
        if not os.path.exists(self._path):
            self._term = 0
            self._voted_for = None
            self._log = []
            self._commit_index = 0
            return
        with open(self._path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self._term = int(data.get("current_term", 0))
        self._voted_for = data.get("voted_for")
        self._log = [
            e for e in (_entry_from_dict(d) for d in data.get("log", []))
            if e.index > self._base_index
        ]
        self._commit_index = int(data.get("commit_index", 0))

    @staticmethod
    def _atomic_write(path: str, payload: dict) -> None:
        d = os.path.dirname(path)
        fd, tmp = tempfile.mkstemp(prefix=".state-", suffix=".tmp", dir=d)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            dfd = os.open(d, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def _persist(self) -> None:
        payload = {
            "node_id": self._node_id,
            "current_term": self._term,
            "voted_for": self._voted_for,
            "log": [asdict(e) for e in self._log],
            "commit_index": self._commit_index,
        }
        self._atomic_write(self._path, payload)

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
            expected = self._log[-1].index + 1 if self._log else self._base_index + 1
            if e.index != expected:
                raise IndexError(
                    f"non-contiguous append: got {e.index}, expected {expected}"
                )
            self._log.append(e)
        if entries:
            self._persist()

    def truncate_from(self, index: int) -> None:
        if index <= self._base_index:
            raise IndexError(
                f"cannot truncate into snapshot: index {index} <= base {self._base_index}"
            )
        offset = index - self._base_index - 1
        if offset >= len(self._log):
            return
        del self._log[offset:]
        self._persist()

    def get_entry(self, index: int) -> LogEntry | None:
        offset = index - self._base_index - 1
        if 0 <= offset < len(self._log):
            return self._log[offset]
        return None

    def last_index(self) -> int:
        return self._log[-1].index if self._log else self._base_index

    def last_term(self) -> int:
        return self._log[-1].term if self._log else self._base_term

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        if index == self._base_index:
            return self._base_term
        e = self.get_entry(index)
        if e is None:
            raise IndexError(f"no entry at index {index}")
        return e.term

    def entries_from(self, start_index: int) -> list[LogEntry]:
        offset = max(0, start_index - self._base_index - 1)
        return list(self._log[offset:])

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

    # --- 日志压缩与快照 --------------------------------------------- #
    def snapshot_index(self) -> int:
        return self._base_index

    def snapshot_term(self) -> int:
        return self._base_term

    def save_snapshot(self, meta: dict, data: dict) -> None:
        payload = {"meta": dict(meta), "data": data}
        self._atomic_write(self._snapshot_path, payload)
        # 注意：不在此处推进 base。base 只由 compact_upto 移动（崩溃重启时
        # _load 会从快照文件恢复 base 并丢弃被覆盖的日志条目）。

    def load_snapshot(self) -> tuple[dict, dict] | None:
        if not os.path.exists(self._snapshot_path):
            return None
        with open(self._snapshot_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload["meta"], payload["data"]

    def compact_upto(self, index: int, term: int | None = None) -> None:
        if index <= self._base_index:
            return
        if index > self._commit_index:
            raise InvalidCommitError(
                f"cannot compact uncommitted entries: {index} > commit {self._commit_index}",
                term=None,
            )
        if term is None:
            term = self.term_at(index)
        del self._log[: index - self._base_index]
        self._base_index = index
        self._base_term = term
        self._persist()

    def reload(self) -> None:
        """重新从磁盘读取：重启后必须看到崩溃期间其他进程对文件的写入。"""
        self._load()
