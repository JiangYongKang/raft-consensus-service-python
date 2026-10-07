"""持久化存储：当前任期、投票、日志条目、提交位点与状态快照。

内存版用于单测；文件版（JSON）用于多进程本地集群与崩溃重启验证。
文件写入采用“临时文件 + fsync + 原子 rename”，保证崩溃后状态完整。

日志压缩：``save_snapshot`` 把 ``last_included_index`` 及之前的已应用历史
压缩为一份 Snapshot（状态机数据 + 幂等去重水位），日志中这些旧条目随即被
丢弃，磁盘上不再保留。此后日志下标带“快照偏移量”：第一条日志下标为
``snapshot_index + 1``；``last_index`` / ``term_at`` 等在快照边界处
返回快照的边界下标与任期，保证复制协议的一致性检查语义不变。
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict

from .errors import InvalidCommitError
from .types import LogEntry, Snapshot


def _entry_from_dict(d: dict) -> LogEntry:
    return LogEntry(
        index=d["index"],
        term=d["term"],
        command=d["command"],
        client_id=d["client_id"],
        request_id=d["request_id"],
        client_seq=d["client_seq"],
    )


def _snapshot_from_dict(d: dict) -> Snapshot:
    return Snapshot(
        last_included_index=int(d["last_included_index"]),
        last_included_term=int(d["last_included_term"]),
        data=dict(d.get("data", {})),
        dedup={k: list(v) for k, v in d.get("dedup", {}).items()},
        client_max_seq={k: int(v) for k, v in d.get("client_max_seq", {}).items()},
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
        """删除所有 index >= ``index`` 的条目（冲突截断）。

        不允许截断进快照覆盖区（那些是已提交历史，受快照保护）。
        """
        raise NotImplementedError

    def get_entry(self, index: int) -> LogEntry | None:
        raise NotImplementedError

    def last_index(self) -> int:
        """日志末端下标；日志为空时返回快照边界下标（无快照则为 0）。"""
        raise NotImplementedError

    def last_term(self) -> int:
        raise NotImplementedError

    def first_index(self) -> int:
        """第一条仍在日志中的条目下标（快照边界 + 1）。"""
        raise NotImplementedError

    def term_at(self, index: int) -> int:
        """返回 index 处条目的任期；index==0 返回 0；快照边界返回快照任期；
        已被压缩丢弃的更早日历抛 IndexError。"""
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

    # --- 快照 -------------------------------------------------------- #
    def snapshot(self) -> Snapshot | None:
        """当前快照；从未压缩过时为 None。"""
        raise NotImplementedError

    def snapshot_index(self) -> int:
        raise NotImplementedError

    def snapshot_term(self) -> int:
        raise NotImplementedError

    def save_snapshot(self, snap: Snapshot) -> int:
        """本地压缩：保存快照并丢弃其覆盖的日志条目。返回丢弃的条数。"""
        raise NotImplementedError

    def install_snapshot(self, snap: Snapshot) -> None:
        """安装来自主节点的快照（追赶）：替换快照并裁剪日志。

        若本地日志在快照边界处的条目任期与快照一致，保留边界之后的后缀；
        否则整段日志都被快照取代（边界之前的已提交历史以快照为准）。
        """
        raise NotImplementedError

    def reload(self) -> None:
        """从持久介质重新读取（重启恢复用）；内存实现为空操作。"""
        pass

    def close(self) -> None:
        pass


class _LogCore(Storage):
    """快照偏移日志的公共实现；子类只需实现 ``_persist`` / ``_load``。"""

    def __init__(self) -> None:
        self._term = 0
        self._voted_for: str | None = None
        self._log: list[LogEntry] = []  # _log[0].index == _snap_index + 1（非空时）
        self._commit_index = 0
        self._snapshot: Snapshot | None = None

    # --- 子类钩子 ---------------------------------------------------- #
    def _persist(self) -> None:
        """把当前状态整体落盘；内存实现为空操作。"""

    def _persist_snapshot(self) -> None:
        """把快照单独落盘；默认跟随整体落盘。"""

    # --- 任期 / 投票 -------------------------------------------------- #
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

    # --- 日志 --------------------------------------------------------- #
    def _snap_index(self) -> int:
        return self._snapshot.last_included_index if self._snapshot else 0

    def append_entries(self, entries: list[LogEntry]) -> None:
        for e in entries:
            expected = self.last_index() + 1
            if e.index != expected:
                raise IndexError(
                    f"non-contiguous append: got {e.index}, expected {expected}"
                )
            self._log.append(e)
        if entries:
            self._persist()

    def truncate_from(self, index: int) -> None:
        if index <= 0:
            raise IndexError(f"truncate index must be positive, got {index}")
        if index <= self._snap_index():
            raise InvalidCommitError(
                f"cannot truncate into snapshot region: index {index} "
                f"<= snapshot boundary {self._snap_index()}",
                term=None,
            )
        offset = index - self._snap_index() - 1
        if offset >= len(self._log):
            return
        del self._log[offset:]
        self._persist()

    def get_entry(self, index: int) -> LogEntry | None:
        offset = index - self._snap_index() - 1
        if 0 <= offset < len(self._log):
            return self._log[offset]
        return None

    def last_index(self) -> int:
        return self._log[-1].index if self._log else self._snap_index()

    def last_term(self) -> int:
        if self._log:
            return self._log[-1].term
        return self._snapshot.last_included_term if self._snapshot else 0

    def first_index(self) -> int:
        return self._snap_index() + 1

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        if self._snapshot and index == self._snapshot.last_included_index:
            return self._snapshot.last_included_term
        e = self.get_entry(index)
        if e is None:
            raise IndexError(f"no entry at index {index} (compacted or beyond tail)")
        return e.term

    def entries_from(self, start_index: int) -> list[LogEntry]:
        start = max(start_index, self.first_index())
        offset = start - self._snap_index() - 1
        return list(self._log[offset:])

    def all_entries(self) -> list[LogEntry]:
        return list(self._log)

    # --- 提交位点 ------------------------------------------------------ #
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

    # --- 快照 ---------------------------------------------------------- #
    def snapshot(self) -> Snapshot | None:
        return self._snapshot

    def snapshot_index(self) -> int:
        return self._snap_index()

    def snapshot_term(self) -> int:
        return self._snapshot.last_included_term if self._snapshot else 0

    def _check_snapshot_monotonic(self, snap: Snapshot) -> None:
        if snap.last_included_index <= self._snap_index():
            raise InvalidCommitError(
                f"snapshot boundary cannot go backwards: {snap.last_included_index} "
                f"<= current snapshot {self._snap_index()}",
                term=None,
            )

    def save_snapshot(self, snap: Snapshot) -> int:
        self._check_snapshot_monotonic(snap)
        if snap.last_included_index > self._commit_index:
            # 本地压缩只允许覆盖已提交历史
            raise InvalidCommitError(
                f"snapshot covers uncommitted entries: {snap.last_included_index} "
                f"> commit_index {self._commit_index}",
                term=None,
            )
        boundary = self.get_entry(snap.last_included_index)
        if boundary is not None and boundary.term != snap.last_included_term:
            raise InvalidCommitError(
                f"snapshot term mismatch at {snap.last_included_index}: "
                f"log term {boundary.term} != snapshot term {snap.last_included_term}",
                term=None,
            )
        drop = snap.last_included_index - self._snap_index()
        del self._log[:drop]
        self._snapshot = snap
        self._persist_snapshot()
        self._persist()
        return drop

    def install_snapshot(self, snap: Snapshot) -> None:
        # 来自主节点的快照是多数派已提交历史的权威载体：不要求本地
        # commit_index 已覆盖（追赶场景恰恰相反），只需边界单调前进。
        self._check_snapshot_monotonic(snap)
        boundary = self.get_entry(snap.last_included_index)
        if boundary is not None and boundary.term == snap.last_included_term:
            # 边界对得上：保留边界之后的本地后缀（与快照内容必然一致）
            keep_from = snap.last_included_index + 1
            self._log = [e for e in self._log if e.index >= keep_from]
        else:
            # 边界缺失或任期不符：整段日志以快照为准
            self._log = []
        self._snapshot = snap
        self._commit_index = max(self._commit_index, snap.last_included_index)
        self._persist_snapshot()
        self._persist()


class InMemoryStorage(_LogCore):
    """进程内存储：不落盘，供单测使用。"""

    def __init__(self) -> None:
        super().__init__()


class FileStorage(_LogCore):
    """JSON 文件持久化。

    状态文件 ``{node_id}.state.json`` 保存 term / voted_for / log / commit_index；
    快照单独存 ``{node_id}.snapshot.json``——压缩后旧条目从状态文件中消失，
    磁盘上不再保留已压缩历史。两处写入均为“临时文件 + fsync + 原子 rename”。
    """

    def __init__(self, data_dir: str, node_id: str) -> None:
        self._node_id = node_id
        os.makedirs(data_dir, exist_ok=True)
        self._path = os.path.join(data_dir, f"{node_id}.state.json")
        self._snapshot_path = os.path.join(data_dir, f"{node_id}.snapshot.json")
        super().__init__()
        self._load()

    @staticmethod
    def _atomic_write(path: str, payload: dict) -> None:
        d = os.path.dirname(path)
        fd, tmp = tempfile.mkstemp(prefix=".tmp-", suffix=".tmp", dir=d)
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

    def _load(self) -> None:
        if os.path.exists(self._path):
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._term = int(data.get("current_term", 0))
            self._voted_for = data.get("voted_for")
            self._log = [_entry_from_dict(d) for d in data.get("log", [])]
            self._commit_index = int(data.get("commit_index", 0))
        if os.path.exists(self._snapshot_path):
            with open(self._snapshot_path, "r", encoding="utf-8") as f:
                self._snapshot = _snapshot_from_dict(json.load(f))

    def _persist(self) -> None:
        payload = {
            "node_id": self._node_id,
            "current_term": self._term,
            "voted_for": self._voted_for,
            "log": [asdict(e) for e in self._log],
            "commit_index": self._commit_index,
        }
        self._atomic_write(self._path, payload)

    def _persist_snapshot(self) -> None:
        if self._snapshot is None:
            return
        self._atomic_write(self._snapshot_path, asdict(self._snapshot))

    def reload(self) -> None:
        """重新从磁盘读取：重启后必须看到崩溃期间其他进程对文件的写入。"""
        self._term = 0
        self._voted_for = None
        self._log = []
        self._commit_index = 0
        self._snapshot = None
        self._load()
