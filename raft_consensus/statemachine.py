"""应用状态机：从已提交日志顺序应用得到 KV 状态。

确定性是关键：同样的日志序列在每个节点上重放，必然得到相同状态。
重启时节点从持久化日志第 1 条重放，即可恢复出一致的 KV 数据。
"""

from __future__ import annotations

from typing import Any


class KVStateMachine:
    """支持的命令：
      {"op": "set", "key": str, "value": Any} -> {"ok": True, "key":..., "old":...}
      {"op": "delete", "key": str}            -> {"ok": True, "key":..., "old":...}
      {"op": "noop"}                          -> {"ok": True}
    """

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    def apply(self, index: int, term: int, command: dict) -> dict:
        op = command.get("op")
        if op == "set":
            key = command["key"]
            old = self._data.get(key)
            self._data[key] = command["value"]
            return {"ok": True, "op": "set", "key": key, "old": old}
        if op == "delete":
            key = command["key"]
            old = self._data.pop(key, None)
            return {"ok": True, "op": "delete", "key": key, "old": old}
        if op == "noop":
            return {"ok": True, "op": "noop"}
        # 未知命令也确定性地记录结果，保证各节点一致
        return {"ok": False, "error": f"unknown op {op!r}"}

    def get(self, key: str) -> Any:
        return self._data.get(key)

    def snapshot(self) -> dict[str, Any]:
        return dict(self._data)
