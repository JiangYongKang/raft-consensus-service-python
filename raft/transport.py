"""节点间 RPC 传输层：

- ``LoopbackTransport``：进程内直接调用，支持分区、节点宕机模拟、随机延迟，供单测；
- ``HttpTransport``：JSON over HTTP（标准库 urllib），供多进程本地集群。

来源节点从请求体中的 candidate_id / leader_id 推断，分区为对称双向。
"""

from __future__ import annotations

import json
import logging
import random
import time
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import Protocol

from .types import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    LogEntry,
    RequestVoteRequest,
    RequestVoteResponse,
)

log = logging.getLogger("raft")


class RaftHandler(Protocol):
    def handle_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse: ...

    def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse: ...

    def handle_install_snapshot(self, req: InstallSnapshotRequest) -> InstallSnapshotResponse: ...


def append_entries_to_dict(req: AppendEntriesRequest) -> dict:
    d = asdict(req)
    return d


def append_entries_from_dict(d: dict) -> AppendEntriesRequest:
    return AppendEntriesRequest(
        term=d["term"],
        leader_id=d["leader_id"],
        prev_log_index=d["prev_log_index"],
        prev_log_term=d["prev_log_term"],
        entries=[LogEntry(**e) for e in d.get("entries", [])],
        leader_commit=d["leader_commit"],
    )


class Transport(ABC):
    @abstractmethod
    def request_vote(
        self, target: str, req: RequestVoteRequest, *, timeout: float = 1.0
    ) -> RequestVoteResponse:
        raise NotImplementedError

    @abstractmethod
    def append_entries(
        self, target: str, req: AppendEntriesRequest, *, timeout: float = 1.0
    ) -> AppendEntriesResponse:
        raise NotImplementedError

    @abstractmethod
    def install_snapshot(
        self, target: str, req: InstallSnapshotRequest, *, timeout: float = 2.0
    ) -> InstallSnapshotResponse:
        raise NotImplementedError

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


class LoopbackTransport(Transport):
    """进程内传输：直接在调用线程执行远端 handler。"""

    def __init__(self, *, jitter: tuple[float, float] = (0.0, 0.002)) -> None:
        self._handlers: dict[str, RaftHandler] = {}
        self._down: set[str] = set()
        self._blocked: set[frozenset[str]] = set()
        self._jitter = jitter
        self._lock = __import__("threading").Lock()

    def register(self, node_id: str, handler: RaftHandler) -> None:
        with self._lock:
            self._handlers[node_id] = handler
            self._down.discard(node_id)

    def unregister(self, node_id: str) -> None:
        with self._lock:
            self._handlers.pop(node_id, None)

    # --- 故障注入 --------------------------------------------------- #
    def set_down(self, node_id: str, down: bool = True) -> None:
        with self._lock:
            if down:
                self._down.add(node_id)
            else:
                self._down.discard(node_id)

    def block(self, a: str, b: str) -> None:
        with self._lock:
            self._blocked.add(frozenset((a, b)))

    def unblock_all(self) -> None:
        with self._lock:
            self._blocked.clear()

    def partition(self, groups: list[list[str]]) -> None:
        """只允许同组互通；先清空旧规则再按分组重设。"""
        with self._lock:
            self._blocked.clear()
            ids = [n for g in groups for n in g]
            groupset = [set(g) for g in groups]
            for i, a in enumerate(ids):
                for b in ids[i + 1 :]:
                    if not any(a in g and b in g for g in groupset):
                        self._blocked.add(frozenset((a, b)))

    def isolate(self, node_id: str, all_ids: list[str]) -> None:
        with self._lock:
            for other in all_ids:
                if other != node_id:
                    self._blocked.add(frozenset((node_id, other)))

    def _reachable(self, source: str, target: str) -> None:
        if target in self._down:
            raise ConnectionError(f"node {target} is down")
        if source in self._down:
            raise ConnectionError(f"node {source} is down")
        if frozenset((source, target)) in self._blocked:
            raise ConnectionError(f"network partition between {source} and {target}")

    def _delay(self) -> None:
        lo, hi = self._jitter
        if hi > 0:
            time.sleep(random.uniform(lo, hi))

    # --- RPC -------------------------------------------------------- #
    def request_vote(
        self, target: str, req: RequestVoteRequest, *, timeout: float = 1.0
    ) -> RequestVoteResponse:
        with self._lock:
            self._reachable(req.candidate_id, target)
            handler = self._handlers.get(target)
        if handler is None:
            raise ConnectionError(f"unknown node {target}")
        self._delay()
        return handler.handle_request_vote(req)

    def append_entries(
        self, target: str, req: AppendEntriesRequest, *, timeout: float = 1.0
    ) -> AppendEntriesResponse:
        with self._lock:
            self._reachable(req.leader_id, target)
            handler = self._handlers.get(target)
        if handler is None:
            raise ConnectionError(f"unknown node {target}")
        self._delay()
        return handler.handle_append_entries(req)

    def install_snapshot(
        self, target: str, req: InstallSnapshotRequest, *, timeout: float = 2.0
    ) -> InstallSnapshotResponse:
        with self._lock:
            self._reachable(req.leader_id, target)
            handler = self._handlers.get(target)
        if handler is None:
            raise ConnectionError(f"unknown node {target}")
        self._delay()
        return handler.handle_install_snapshot(req)


class HttpTransport(Transport):
    """HTTP JSON 传输（无第三方依赖，urllib）。

    可选 ``network_rule_file``：指向共享的 network.json，用于多进程本地
    模拟网络分区。文件内容形如 {"partition": [["node1"], ["node2","node3"]]}，
    表示仅同组互通；文件不存在视为全连通。
    """

    def __init__(
        self,
        peers: dict[str, str],
        connect_timeout: float = 0.5,
        *,
        self_id: str | None = None,
        network_rule_file: str | None = None,
    ) -> None:
        # peers: node_id -> "http://127.0.0.1:PORT"
        self._peers = dict(peers)
        self._connect_timeout = connect_timeout
        self._self_id = self_id
        self._rule_file = network_rule_file

    def _check_partition(self, target: str) -> None:
        if not self._rule_file or not self._self_id:
            return
        try:
            with open(self._rule_file, "r", encoding="utf-8") as f:
                rule = json.load(f)
        except FileNotFoundError:
            return
        groups = rule.get("partition") or []
        if not groups:
            return
        for g in groups:
            if self._self_id in g and target not in g:
                raise ConnectionError(
                    f"partitioned: {self._self_id} cannot reach {target}"
                )

    def _post(self, target: str, path: str, payload: dict, timeout: float) -> dict:
        self._check_partition(target)
        url = self._peers[target].rstrip("/") + path
        data = json.dumps(payload).encode("utf-8")
        http_req = urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with urllib.request.urlopen(http_req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise ConnectionError(f"HTTP {e.code} from {target}: {body}") from None
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            raise ConnectionError(f"call {target} failed: {e}") from None

    def request_vote(
        self, target: str, req: RequestVoteRequest, *, timeout: float = 1.0
    ) -> RequestVoteResponse:
        d = self._post(target, "/raft/request-vote", asdict(req), timeout)
        return RequestVoteResponse(**d)

    def append_entries(
        self, target: str, req: AppendEntriesRequest, *, timeout: float = 1.0
    ) -> AppendEntriesResponse:
        d = self._post(target, "/raft/append-entries", append_entries_to_dict(req), timeout)
        return AppendEntriesResponse(**d)

    def install_snapshot(
        self, target: str, req: InstallSnapshotRequest, *, timeout: float = 2.0
    ) -> InstallSnapshotResponse:
        d = self._post(target, "/raft/install-snapshot", asdict(req), timeout)
        return InstallSnapshotResponse(**d)
