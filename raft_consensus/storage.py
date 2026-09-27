"""持久化存储（骨架）：任期/投票、日志条目落盘，重启后恢复。"""

from __future__ import annotations

import json
from pathlib import Path

from raft_consensus.log import LogEntry


class PersistentStore:
    """基于 JSON 文件的持久化。

    每个节点一个目录：
      state.json —— current_term / voted_for（同步写盘并 flush）
      log.jsonl  —— 每行一条 LogEntry（append 后 flush；截断时重写）
    """

    def __init__(self, base_dir: str | Path, node_id: str) -> None:
        self.node_id = node_id
        self.dir = Path(base_dir) / node_id

    def init(self) -> None:
        """确保目录与文件存在；不存在则初始化为 term=0 / 空日志。"""
        raise NotImplementedError

    def load_state(self) -> tuple[int, str | None]:
        raise NotImplementedError

    def save_state(self, term: int, voted_for: str | None) -> None:
        raise NotImplementedError

    def load_entries(self) -> list[LogEntry]:
        raise NotImplementedError

    def append_entries(self, entries: list[LogEntry]) -> None:
        """顺序追加并 flush 到磁盘。"""
        raise NotImplementedError

    def truncate_and_write(self, prefix_len: int, entries: list[LogEntry]) -> None:
        """保留前 prefix_len 条（含哨兵），删除其后条目，再写入 entries。"""
        raise NotImplementedError

    def reset(self) -> None:
        """删除全部持久化数据（测试用）。"""
        raise NotImplementedError
