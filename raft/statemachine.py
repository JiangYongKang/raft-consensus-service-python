"""确定性状态机：按日志顺序应用命令。

实现一个线程安全的 KV 存储：
  {"op": "set", "key": k, "value": v}
  {"op": "delete", "key": k}
  {"op": "noop"}

应用时校验 index 必须严格连续（位点回退/跳跃都会被拒绝），
保证“不能从中途继续”。
"""

from __future__ import annotations

import threading

from .errors import InvalidCommitError


class StateMachine:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, object] = {}
        self._last_applied = 0

    @property
    def last_applied(self) -> int:
        with self._lock:
            return self._last_applied

    def apply(self, index: int, term: int, command: dict) -> dict:
        with self._lock:
            expected = self._last_applied + 1
            if index != expected:
                # 位点回退或跳跃：明确拒绝，而不是悄悄从中间状态继续
                raise InvalidCommitError(
                    f"apply index must be strictly contiguous: got {index}, "
                    f"expected {expected}",
                    term=term,
                )
            op = command.get("op")
            if op == "set":
                self._data[command["key"]] = command.get("value")
                result = {"op": "set", "key": command["key"], "value": command.get("value")}
            elif op == "delete":
                existed = command["key"] in self._data
                self._data.pop(command["key"], None)
                result = {"op": "delete", "key": command["key"], "existed": existed}
            elif op == "noop":
                result = {"op": "noop"}
            else:
                raise ValueError(f"unknown command op: {op!r}")
            self._last_applied = index
            return result

    def get(self, key: str) -> object:
        with self._lock:
            return self._data.get(key)

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._data)
