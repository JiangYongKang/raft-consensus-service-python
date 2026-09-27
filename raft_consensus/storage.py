"""持久化存储：任期/投票与日志条目落盘，节点重启后据此恢复。

文件布局（每个节点一个目录）：
  state.json —— {"term": int, "voted_for": str|null}，tmp+rename 原子写
  log.jsonl  —— 每行一条 LogEntry；顺序 append+flush；截断时原子重写

所有写操作都 fsync，保证"多数节点确认持久化"中的持久化是真实落盘。
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from raft_consensus.log import LogEntry


class PersistentStore:
    def __init__(self, base_dir: str | Path, node_id: str) -> None:
        self.node_id = node_id
        self.dir = Path(base_dir) / node_id
        self.state_path = self.dir / "state.json"
        self.log_path = self.dir / "log.jsonl"

    def init(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        if not self.state_path.exists():
            self._write_state(0, None, 0)
        if not self.log_path.exists():
            # 创建空日志文件（不含哨兵；哨兵在内存中重建）
            self.log_path.touch()

    # ---- state ----
    def load_all(self) -> tuple[int, str | None, int]:
        """返回 (current_term, voted_for, commit_index)。"""
        with self.state_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return int(data["term"]), data.get("voted_for"), int(data.get("commit_index", 0))

    def load_state(self) -> tuple[int, str | None]:
        term, voted_for, _ = self.load_all()
        return term, voted_for

    def save_state(self, term: int, voted_for: str | None) -> None:
        self._write_state(term, voted_for, None)

    def save_commit_index(self, commit_index: int) -> None:
        term, voted_for, _ = self.load_all()
        self._write_state(term, voted_for, commit_index)

    def _write_state(self, term: int, voted_for: str | None,
                     commit_index: int | None) -> None:
        if commit_index is None:
            _, _, commit_index = self.load_all() if self.state_path.exists() \
                else (0, None, 0)
        tmp = self.state_path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump({"term": term, "voted_for": voted_for,
                       "commit_index": commit_index}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.state_path)
        self._fsync_dir()

    # ---- log ----
    @staticmethod
    def _encode(e: LogEntry) -> dict:
        return {
            "term": e.term, "index": e.index,
            "client_id": e.client_id, "request_id": e.request_id,
            "command": e.command, "client_seq": e.client_seq,
        }

    @staticmethod
    def _decode(d: dict) -> LogEntry:
        return LogEntry(
            term=int(d["term"]), index=int(d["index"]),
            client_id=d["client_id"], request_id=d["request_id"],
            command=d.get("command", {}), client_seq=int(d.get("client_seq", 0)),
        )

    def load_entries(self) -> list[LogEntry]:
        entries: list[LogEntry] = []
        with self.log_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    entries.append(self._decode(json.loads(line)))
        return entries

    def append_entries(self, entries: list[LogEntry]) -> None:
        with self.log_path.open("a", encoding="utf-8") as f:
            for e in entries:
                f.write(json.dumps(self._encode(e), ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def truncate_and_write(self, prefix_len: int, entries: list[LogEntry]) -> None:
        """保留磁盘上前 prefix_len 条（不含哨兵，因为哨兵不落盘），重写其余。"""
        kept: list[LogEntry] = []
        with self.log_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    kept.append(self._decode(json.loads(line)))
        # 内存 entries 含哨兵：磁盘第 k 条对应内存下标 k（0..prefix_len-1 不含哨兵行）
        kept = kept[:prefix_len]
        kept.extend(entries)
        tmp = self.log_path.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            for e in kept:
                f.write(json.dumps(self._encode(e), ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.log_path)
        self._fsync_dir()

    def _fsync_dir(self) -> None:
        try:
            fd = os.open(str(self.dir), os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def rewrite_all(self, entries: list[LogEntry]) -> None:
        """原子重写整份日志（内存 -> 磁盘），用于确认前的持久化。跳过哨兵。"""
        tmp = self.log_path.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            for e in entries:
                if e.index == 0:
                    continue
                f.write(json.dumps(self._encode(e), ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.log_path)
        self._fsync_dir()

    def reset(self) -> None:
        for p in (self.state_path, self.log_path):
            if p.exists():
                p.unlink()
