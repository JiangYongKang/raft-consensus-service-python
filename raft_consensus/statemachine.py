"""应用状态机（骨架）：从已提交日志顺序应用得到 KV 状态。"""

from __future__ import annotations


class KVStateMachine:
    """确定性键值状态机；命令形如 {"op": "set"/"delete", "key": ..., "value": ...}。"""

    def __init__(self) -> None:
        self._data: dict[str, object] = {}

    def apply(self, index: int, term: int, command: dict) -> object:
        """应用一条已提交命令，返回可作为客户端响应的结果。"""
        raise NotImplementedError

    def snapshot(self) -> dict[str, object]:
        raise NotImplementedError
