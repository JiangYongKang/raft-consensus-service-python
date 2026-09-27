"""单节点 FastAPI 服务：把一个 RaftNode 暴露为 HTTP 服务。

端点一览：
  客户端 API（对外）
    POST /client/command       状态变更（body 含 client_id/request_id/client_seq/command）
    GET  /client/state/{key}   线性一致读（仅主节点）
    GET  /status               节点角色/任期/位点

  节点间 Raft RPC（内部）
    POST /raft/request-vote
    POST /raft/append-entries

  管理 API（本地模拟，需 X-RAFT-AUTH 令牌）
    POST /admin/step-down      身份+令牌+任期校验的内部协调操作
    POST /admin/partition      设置进程间分区（通过落盘共享的连通性文件模拟）
    POST /admin/heal
    POST /admin/crash
    POST /admin/restart

节点配置来自环境变量：
  RAFT_NODE_ID / RAFT_PEERS / RAFT_DATA_DIR / RAFT_HTTP_PORT / RAFT_ADMIN_TOKEN
"""

from __future__ import annotations

import json
import logging
import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from .errors import (
    ConsensusError,
    DuplicateRequestError,
    StaleClientRequestError,
    UnauthorizedError,
)
from .node import RaftNode
from .statemachine import StateMachine
from .storage import FileStorage
from .transport import HttpTransport, append_entries_from_dict
from .types import RequestVoteRequest

log = logging.getLogger("raft")


def _error_response(code: str, reason: str, status_code: int, **extra) -> JSONResponse:
    payload = {"success": False, "error": {"code": code, "reason": reason}}
    payload.update(extra)
    return JSONResponse(status_code=status_code, content=payload)


class NodeHolder:
    """持有当前 Raft 节点；支持 crash/restart（用同一 FileStorage 恢复）。"""

    def __init__(self, node_id: str, peers_url: dict[str, str], data_dir: str, admin_token: str):
        self.node_id = node_id
        self.peers_url = peers_url
        self.data_dir = data_dir
        self.admin_token = admin_token
        self._lock = threading.Lock()
        self.node: RaftNode | None = None
        self.crashed = False
        self.build(start=True)

    def _build_node(self, start: bool) -> RaftNode:
        peer_ids = [p for p in self.peers_url if p != self.node_id]
        storage = FileStorage(self.data_dir, self.node_id)
        eto = os.environ.get("RAFT_ELECTION_TIMEOUT_MS", "150,300")
        lo_s, hi_s = eto.split(",")
        hb_ms = int(os.environ.get("RAFT_HEARTBEAT_MS", "50"))
        node = RaftNode(
            self.node_id,
            peer_ids,
            storage,
            StateMachine(),
            HttpTransport(
                self.peers_url,
                self_id=self.node_id,
                network_rule_file=os.path.join(self.data_dir, "network.json"),
            ),
            admin_token=self.admin_token,
            state_machine_factory=StateMachine,
            election_timeout=(int(lo_s) / 1000.0, int(hi_s) / 1000.0),
            heartbeat_interval=hb_ms / 1000.0,
        )
        if start:
            node.start()
        return node

    def build(self, start: bool = True) -> RaftNode:
        with self._lock:
            self.node = self._build_node(start)
            self.crashed = not start
            return self.node

    def crash(self) -> None:
        with self._lock:
            if self.node is not None:
                self.node.crash()
            self.crashed = True

    def restart(self) -> None:
        with self._lock:
            # 原节点的 FileStorage 即磁盘状态；新建节点对象从磁盘恢复
            self.node = self._build_node(start=True)
            self.crashed = False


def create_app(
    node_id: str | None = None,
    *,
    peers_url: dict[str, str] | None = None,
    data_dir: str | None = None,
    admin_token: str | None = None,
) -> FastAPI:
    node_id = node_id or os.environ.get("RAFT_NODE_ID", "node1")
    data_dir = data_dir or os.environ.get("RAFT_DATA_DIR", "./.raft-data")
    admin_token = admin_token or os.environ.get("RAFT_ADMIN_TOKEN", "local-cluster-token")
    if peers_url is None:
        peers_url = _parse_peers(os.environ.get("RAFT_PEERS", ""), node_id)

    holder = NodeHolder(node_id, peers_url, data_dir, admin_token)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        with holder._lock:
            if holder.node is not None:
                holder.node.stop()

    application = FastAPI(title=f"raft-node-{node_id}", lifespan=lifespan)
    application.state.holder = holder

    def require_node() -> RaftNode | None:
        return None if holder.crashed else holder.node

    # ---------------------------------------------------------------- #
    # 客户端 API
    # ---------------------------------------------------------------- #
    @application.post("/client/command")
    async def client_command(body: dict):
        node = require_node()
        if node is None:
            return _error_response("node_down", "node has crashed and is not restarted", 503)
        for field in ("client_id", "request_id", "client_seq", "command"):
            if field not in body:
                return _error_response("bad_request", f"missing field: {field}", 400)
        try:
            resp = node.submit_command(
                body["client_id"],
                body["request_id"],
                int(body["client_seq"]),
                body["command"],
                timeout=float(body.get("timeout", 2.0)),
            )
        except DuplicateRequestError as e:
            return _error_response(
                e.code, e.reason, 409, request_id=e.request_id, term=e.term
            )
        except StaleClientRequestError as e:
            return _error_response(
                e.code, e.reason, 409,
                client_id=e.client_id, got_seq=e.got_seq, max_seq=e.max_seq, term=e.term,
            )
        except ConsensusError as e:
            return _error_response(e.code, e.reason, 422, term=e.term)
        if not resp.success:
            return _error_response(
                resp.code, resp.reason, 503,
                term=resp.term, index=resp.index, leader_id=resp.leader_id,
            )
        return {
            "success": True,
            "index": resp.index,
            "term": resp.term,
            "result": resp.result,
            "leader_id": resp.leader_id,
        }

    @application.get("/client/state/{key}")
    async def client_read(key: str):
        node = require_node()
        if node is None:
            return _error_response("node_down", "node crashed", 503)
        resp = node.read_state(key)
        if not resp.success:
            return _error_response(
                resp.code, resp.reason, 503, term=resp.term, leader_id=resp.leader_id
            )
        return {"success": True, "term": resp.term, "state": resp.result}

    @application.get("/status")
    async def status():
        node = require_node()
        if node is None:
            return {"node_id": holder.node_id, "crashed": True}
        s = node.status()
        return {
            "node_id": s.node_id,
            "role": s.role.value,
            "term": s.term,
            "voted_for": s.voted_for,
            "leader_id": s.leader_id,
            "commit_index": s.commit_index,
            "last_applied": s.last_applied,
            "last_log_index": s.last_log_index,
            "last_log_term": s.last_log_term,
            "crashed": False,
        }

    # ---------------------------------------------------------------- #
    # 内部 Raft RPC
    # ---------------------------------------------------------------- #
    @application.post("/raft/request-vote")
    async def raft_request_vote(body: dict):
        node = require_node()
        if node is None:
            return _error_response("node_down", "node crashed", 503)
        try:
            resp = node.handle_request_vote(RequestVoteRequest(**body))
        except UnauthorizedError as e:
            return _error_response(e.code, e.reason, 403, term=e.term)
        except TypeError as e:
            return _error_response("bad_request", str(e), 400)
        return {"term": resp.term, "vote_granted": resp.vote_granted, "reason": resp.reason}

    @application.post("/raft/append-entries")
    async def raft_append_entries(body: dict):
        node = require_node()
        if node is None:
            return _error_response("node_down", "node crashed", 503)
        try:
            req = append_entries_from_dict(body)
            resp = node.handle_append_entries(req)
        except UnauthorizedError as e:
            return _error_response(e.code, e.reason, 403, term=e.term)
        except (KeyError, TypeError) as e:
            return _error_response("bad_request", str(e), 400)
        return {
            "term": resp.term,
            "success": resp.success,
            "match_index": resp.match_index,
            "reason": resp.reason,
        }

    # ---------------------------------------------------------------- #
    # 管理 API（令牌鉴权）
    # ---------------------------------------------------------------- #
    def check_admin(token: str | None) -> JSONResponse | None:
        if token != admin_token:
            return _error_response(
                "unauthorized", "admin operation requires valid X-RAFT-AUTH token", 401
            )
        return None

    @application.post("/admin/step-down")
    async def admin_step_down(body: dict, x_raft_auth: str | None = Header(default=None)):
        err = check_admin(x_raft_auth)
        if err is not None:
            return err
        node = require_node()
        if node is None:
            return _error_response("node_down", "node crashed", 503)
        try:
            out = node.internal_step_down(
                str(body.get("caller_id", "")),
                int(body.get("term", -1)),
                x_raft_auth or "",
            )
        except UnauthorizedError as e:
            return _error_response(e.code, e.reason, 403, term=e.term)
        except ConsensusError as e:
            return _error_response(e.code, e.reason, 409, term=e.term)
        return out

    @application.post("/admin/partition")
    async def admin_partition(body: dict, x_raft_auth: str | None = Header(default=None)):
        err = check_admin(x_raft_auth)
        if err is not None:
            return err
        # HTTP 多进程：把分区规则写入共享文件，每个节点的 HttpTransport 读取。
        rule_path = os.path.join(data_dir, "network.json")
        with open(rule_path, "w", encoding="utf-8") as f:
            json.dump({"partition": body.get("groups", [])}, f)
        return {"accepted": True, "partition": body.get("groups", [])}

    @application.post("/admin/heal")
    async def admin_heal(x_raft_auth: str | None = Header(default=None)):
        err = check_admin(x_raft_auth)
        if err is not None:
            return err
        rule_path = os.path.join(data_dir, "network.json")
        if os.path.exists(rule_path):
            os.unlink(rule_path)
        return {"accepted": True}

    @application.post("/admin/crash")
    async def admin_crash(x_raft_auth: str | None = Header(default=None)):
        err = check_admin(x_raft_auth)
        if err is not None:
            return err
        holder.crash()
        return {"accepted": True, "crashed": True}

    @application.post("/admin/restart")
    async def admin_restart(x_raft_auth: str | None = Header(default=None)):
        err = check_admin(x_raft_auth)
        if err is not None:
            return err
        holder.restart()
        return {"accepted": True, "crashed": False}

    return application


def _parse_peers(raw: str, self_id: str) -> dict[str, str]:
    """解析形如 ``node1=127.0.0.1:8101,node2=...`` 的对端列表。"""
    peers: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        name, addr = item.split("=", 1)
        peers[name.strip()] = f"http://{addr.strip()}"
    peers.setdefault(self_id, f"http://127.0.0.1:{os.environ.get('RAFT_HTTP_PORT', '8101')}")
    return peers


app = create_app()
