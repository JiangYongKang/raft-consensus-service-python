"""Raft 节点核心：选举、任期、复制、提交、恢复、幂等与内部 RPC 权限校验。

线程模型
--------
每个节点的全部方法都在模拟器的单条驱动线程上串行执行（客户端请求也以
"client_call" 事件排入该节点队列），因此节点内部无锁也不会乱序。

持久化与恢复
------------
- current_term / voted_for / commit_index 原子写入 state.json；
- 日志写入 log.jsonl（跟随者先原子重写并 fsync，再回复成功）；
- 重启时从磁盘重建 RaftLog，并重放 index<=commit_index 的条目恢复状态机与
  幂等结果表，从而恢复到一致状态。

可解释的拒绝
------------
旧任期 -> StaleTermError；非主写入 -> NotLeaderError；重复投票 ->
AlreadyVotedError；日志不一致 -> LogCheckMismatchError（带 hint）；
覆盖已提交 -> CommittedRollbackError；身份不合法 -> UnknownCallerError。
"""

from __future__ import annotations

import logging
import random
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from raft_consensus.errors import (
    AlreadyVotedError,
    CommittedRollbackError,
    ConsensusError,
    DuplicateRequestError,
    LogCheckMismatchError,
    NotLeaderError,
    StaleTermError,
    UnknownCallerError,
)
from raft_consensus.log import LogEntry, RaftLog
from raft_consensus.messages import (
    AppendEntries,
    AppendEntriesResult,
    Envelope,
    RequestVote,
    RequestVoteResult,
)
from raft_consensus.statemachine import KVStateMachine
from raft_consensus.storage import PersistentStore

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


@dataclass(slots=True)
class NodeConfig:
    node_id: str
    peers: tuple[str, ...]
    data_dir: str | Path
    election_timeout_min: float = 0.15
    election_timeout_max: float = 0.30
    heartbeat_interval: float = 0.05
    seed: int = 0


@dataclass(slots=True)
class Proposal:
    """客户端写请求在领导者上的登记（index 是决策位点）。"""

    client_id: str
    request_id: str
    command: dict
    term: int
    index: int = -1
    done: bool = False
    result: object = None
    error: Exception | None = None
    event: threading.Event = field(default_factory=threading.Event)


@dataclass(slots=True)
class PendingRead:
    key: str
    term: int
    heartbeat_id: str
    event: threading.Event = field(default_factory=threading.Event)
    result: object = None
    error: Exception | None = None


class RaftNode:
    def __init__(self, config: NodeConfig,
                 send: Callable[[Envelope], None],
                 schedule: Callable[[float, Callable[[], None]], None],
                 now: Callable[[], float],
                 new_msg_id: Callable[[], str],
                 applied_callback: Callable[[str, int, int, dict, object], None]
                    | None = None,
                 is_peer_reachable: Callable[[str], bool] | None = None) -> None:
        self.config = config
        self.node_id = config.node_id
        self.peers = tuple(config.peers)
        self.other_peers = tuple(p for p in config.peers if p != self.node_id)
        self._send_raw = send
        self._schedule = schedule
        self._now = now
        self._new_msg_id = new_msg_id
        self.applied_callback = applied_callback
        self._is_peer_reachable = is_peer_reachable
        self.logr = logging.getLogger(f"raft.node.{self.node_id}")
        self.rng = random.Random(config.seed)

        # ---- 持久化状态 ----
        self.store = PersistentStore(config.data_dir, self.node_id)
        self.store.init()
        term, voted_for, commit_index = self.store.load_all()
        self.current_term: int = term
        self.voted_for: str | None = voted_for

        disk = self.store.load_entries()
        self.log = RaftLog(entries=[LogEntry(0, 0, "", "", {})] + disk)

        # ---- 易失状态 ----
        self.role = FOLLOWER
        self.commit_index: int = commit_index
        self.last_applied: int = 0
        self.leader_id: str | None = None
        self.sm = KVStateMachine()

        # 领导者专用
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self._heartbeat_token = 0

        # 幂等 / 在途状态
        self.applied_requests: dict[tuple[str, str], tuple[int, object]] = {}
        self.inflight: dict[tuple[str, str], Proposal] = {}
        self.proposals_by_index: dict[int, Proposal] = {}
        self.pending_reads: list[PendingRead] = []
        self._read_heartbeat_id: str | None = None

        self._election_token = 0
        self._running = False
        self._correlations: dict[str, Callable[[Envelope], None]] = {}

        # 重启恢复：重放已提交日志
        self._restore_from_disk()

    # ================= 生命周期 =================
    def _restore_from_disk(self) -> None:
        replayed = 0
        for i in range(1, self.commit_index + 1):
            e = self.log.entry_at(i)
            result = self.sm.apply(e.index, e.term, e.command)
            self.applied_requests[(e.client_id, e.request_id)] = (e.index, result)
            self.last_applied = i
            replayed += 1
        if replayed:
            self.logr.info("[restore] replayed %d committed entries; "
                           "term=%d commit=%d state=%s",
                           replayed, self.current_term, self.commit_index,
                           self.sm.snapshot())

    def start(self) -> None:
        self._running = True
        self._reset_election_timer()
        self.logr.info("[start] node up as %s term=%d commit=%d",
                       self.role, self.current_term, self.commit_index)

    def shutdown(self) -> None:
        self._running = False
        self._election_token += 1  # 让未触发的定时器失效
        self._heartbeat_token += 1
        # 唤醒所有在途等待，失败原因可区分
        for p in list(self.inflight.values()):
            p.error = ConsensusError("leader stepped down / node shutdown",
                                     term=self.current_term)
            p.event.set()
        for r in self.pending_reads:
            r.error = NotLeaderError(term=self.current_term,
                                     leader_id=None, node=self.node_id)
            r.event.set()
        self.pending_reads.clear()
        self.logr.warning("[shutdown] node stopped in term=%d role=%s",
                          self.current_term, self.role)

    # ================= 工具 =================
    def _election_timeout(self) -> float:
        return self.rng.uniform(self.config.election_timeout_min,
                                self.config.election_timeout_max)

    def _reset_election_timer(self) -> None:
        self._election_token += 1
        token = self._election_token
        self._schedule(self._election_timeout(),
                       lambda: self._election_timer_fire(token))

    def _election_timer_fire(self, token: int) -> None:
        if token != self._election_token or not self._running:
            return
        self.on_election_timeout()

    def _become_follower(self, term: int, leader: str | None) -> None:
        if term != self.current_term:
            self.logr.info("[term] %d -> %d (become follower, leader=%s)",
                           self.current_term, term, leader)
            self.current_term = term
            self.voted_for = None
            self.store.save_state(term, None)
        self.role = FOLLOWER
        self.leader_id = leader
        self._resolve_inflight_demotion()
        self._fail_pending_reads()
        self._reset_election_timer()

    def _resolve_inflight_demotion(self) -> None:
        for p in list(self.inflight.values()):
            p.error = NotLeaderError(term=self.current_term,
                                     leader_id=self.leader_id, node=self.node_id)
            p.done = True
            p.event.set()
        self.inflight.clear()
        self.proposals_by_index.clear()

    def _send(self, dst: str, payload: object) -> None:
        env = Envelope(src=self.node_id, dst=dst, payload=payload,
                       msg_id=self._new_msg_id())
        self._send_raw(env)

    def _persist_log(self) -> None:
        """把内存日志原子重写到磁盘（fsync）。"""
        self.store.rewrite_all(self.log.entries)

    # ================= 选举 =================
    def on_election_timeout(self) -> None:
        # 从领导者退位：必须以可区分原因终结全部在途客户端提案与读
        if self.role == LEADER:
            self._resolve_inflight_demotion()
            self._fail_pending_reads()
        self.role = CANDIDATE
        self.current_term += 1
        term = self.current_term
        self.voted_for = self.node_id
        self.leader_id = None
        self.store.save_state(term, self.node_id)
        self._reset_election_timer()

        self.logr.info("[election] start election term=%d (candidate=%s)",
                       term, self.node_id)
        votes = {self.node_id}
        rv = RequestVote(
            term=term, candidate_id=self.node_id,
            last_log_index=self.log.last_index, last_log_term=self.log.last_term,
        )
        needed = len(self.peers) // 2 + 1

        def on_result(result: RequestVoteResult | None, peer: str) -> None:
            # result 为 None 表示 RPC 超时/丢失（本实现里网络静默丢弃）
            if result is None or self.role != CANDIDATE or self.current_term != term:
                return
            if result.term > term:
                self._become_follower(result.term, None)
                return
            if result.vote_granted:
                votes.add(peer)
                self.logr.info("[election] granted by %s votes=%d/%d term=%d",
                               peer, len(votes), needed, term)
                if self.role == CANDIDATE and len(votes) >= needed:
                    self._become_leader(term)

        for peer in self.other_peers:
            self._send_rpc(peer, rv,
                           lambda res, p=peer: on_result(res, p))

        # 单节点集群立即当选
        if len(votes) >= needed and self.role == CANDIDATE:
            self._become_leader(term)

    def handle_request_vote(self, msg: RequestVote,
                            caller: str) -> RequestVoteResult:
        if caller != msg.candidate_id:
            raise UnknownCallerError(caller=caller, node=self.node_id)
        # 旧任期请求必须拒绝并说明原因
        if msg.term < self.current_term:
            raise StaleTermError(request_term=msg.term,
                                 current_term=self.current_term,
                                 node=self.node_id, kind="RequestVote")
        if msg.term > self.current_term:
            self._become_follower(msg.term, None)

        can_vote = self.voted_for in (None, msg.candidate_id)
        log_ok = self.log.up_to_date(msg.last_log_index, msg.last_log_term)
        if not can_vote:
            raise AlreadyVotedError(term=self.current_term,
                                    voted_for=self.voted_for,
                                    node=self.node_id)
        if not log_ok:
            return RequestVoteResult(
                term=self.current_term, vote_granted=False,
                voter_id=self.node_id,
                reason=("candidate log not up-to-date: "
                        f"candidate=(idx {msg.last_log_index}, term "
                        f"{msg.last_log_term}) local=(idx {self.log.last_index}, "
                        f"term {self.log.last_term})"),
            )
        self.voted_for = msg.candidate_id
        self.store.save_state(self.current_term, msg.candidate_id)
        self._reset_election_timer()
        self.logr.info("[election] vote GRANTED to %s for term=%d",
                       msg.candidate_id, self.current_term)
        return RequestVoteResult(term=self.current_term, vote_granted=True,
                                 voter_id=self.node_id, reason="granted")

    def _become_leader(self, term: int) -> None:
        self.role = LEADER
        self.leader_id = self.node_id
        self._heartbeat_token += 1  # 作废旧心跳链
        last = self.log.last_index
        self.next_index = {p: last + 1 for p in self.other_peers}
        self.match_index = {p: 0 for p in self.other_peers}
        self.logr.info("[leader] %s ELECTED leader for term=%d (last_index=%d)",
                       self.node_id, term, last)

        # 上任先提交一条 noop（论文 §8），并立即广播心跳
        noop = LogEntry(term=term, index=last + 1, client_id="",
                        request_id="", command={"op": "noop"})
        self.log.append([noop])
        self._persist_log()
        self.proposals_by_index.pop(noop.index, None)
        self._advance_commit()  # 单节点集群：noop 立即构成多数
        self._broadcast_append_entries()
        self._schedule_heartbeat(term, self._heartbeat_token)

    def _schedule_heartbeat(self, term: int, token: int) -> None:
        def fire() -> None:
            if token != self._heartbeat_token or not self._running:
                return
            if self.role == LEADER and self.current_term == term:
                self.on_heartbeat()
                self._schedule_heartbeat(term, token)
        self._schedule(self.config.heartbeat_interval, fire)

    # ================= 复制：领导者侧 =================
    def on_heartbeat(self) -> None:
        if self.role != LEADER:
            return
        self._broadcast_append_entries()

    def _broadcast_append_entries(self) -> None:
        if self.role != LEADER:
            return
        heartbeat_id = self._new_msg_id()
        for peer in self.other_peers:
            self._replicate_to(peer, heartbeat_id)

    def _replicate_to(self, peer: str, request_id: str | None = None) -> None:
        if self.role != LEADER:
            return
        next_idx = self.next_index[peer]
        prev_index = next_idx - 1
        prev_term = self.log.term_at(prev_index) if prev_index >= 0 else 0
        entries = self.log.entries_since(prev_index)
        req_id = request_id or self._new_msg_id()
        ae = AppendEntries(
            term=self.current_term, leader_id=self.node_id,
            prev_log_index=prev_index, prev_log_term=prev_term,
            entries=tuple(entries), leader_commit=self.commit_index,
            request_id=req_id,
        )
        self._send_rpc(peer, ae,
                       lambda res: self._handle_append_result(peer, res))

    def _handle_append_result(self, peer: str,
                              result: AppendEntriesResult) -> None:
        if self.role != LEADER:
            return
        if result.term > self.current_term:
            self._become_follower(result.term, None)
            self._fail_pending_reads()
            return
        if self.current_term != result.term:
            return
        if result.success:
            new_match = result.match_index
            if new_match > self.match_index.get(peer, 0):
                self.match_index[peer] = new_match
                self.next_index[peer] = new_match + 1
                self.logr.info("[replication] ack peer=%s match=%d term=%d",
                               peer, new_match, self.current_term)
            self._advance_commit()
            self._resolve_pending_reads(result.request_id)
            # 可能有新条目可发送（窗口式补发）
            if self.next_index[peer] <= self.log.last_index:
                self._replicate_to(peer)
        else:
            hint = result.hint_index
            self.next_index[peer] = max(1, min(self.next_index[peer] - 1,
                                               hint + 1))
            self.logr.info("[replication] backoff peer=%s next->%d reason=%s",
                           peer, self.next_index[peer], result.reason)
            self._replicate_to(peer)

    def _advance_commit(self) -> None:
        """论文 §5.4.2：只在当前任期下推进 commit_index，且需多数 match。"""
        if self.role != LEADER:
            return
        majority = len(self.peers) // 2 + 1
        for idx in range(self.log.last_index, self.commit_index, -1):
            if self.log.term_at(idx) != self.current_term:
                break  # 再旧的更不可能是当前任期
            count = 1  # 领导者自己
            for p in self.other_peers:
                # 分区/离线节点的旧 match_index 绝不能计入"当前多数确认"
                if self._is_peer_reachable and not self._is_peer_reachable(p):
                    continue
                if self.match_index.get(p, 0) >= idx:
                    count += 1
            if count >= majority:
                old = self.commit_index
                self.commit_index = idx
                self.store.save_commit_index(idx)
                self.logr.info("[commit] leader advances commit %d -> %d "
                               "(votes=%d/%d) term=%d",
                               old, idx, count, majority, self.current_term)
                self._apply_committed()
                break

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            e = self.log.entry_at(self.last_applied)
            result = self.sm.apply(e.index, e.term, e.command)
            if e.client_id:
                self.applied_requests[(e.client_id, e.request_id)] = \
                    (e.index, result)
            self.logr.info(
                "[apply] node=%s term=%d decision_index=%d req=%s src_client=%s "
                "cmd=%s result=%s",
                self.node_id, e.term, e.index, e.request_id, e.client_id,
                e.command, result,
            )
            if self.applied_callback:
                self.applied_callback(self.node_id, e.index, e.term,
                                      e.command, result)
            prop = self.proposals_by_index.pop(e.index, None)
            if prop is not None:
                key = (prop.client_id, prop.request_id)
                self.inflight.pop(key, None)
                prop.done = True
                prop.result = result
                prop.event.set()

    # ================= 复制：跟随者侧 =================
    def handle_append_entries(self, msg: AppendEntries,
                              caller: str) -> AppendEntriesResult:
        if caller != msg.leader_id:
            raise UnknownCallerError(caller=caller, node=self.node_id)
        if msg.term < self.current_term:
            raise StaleTermError(request_term=msg.term,
                                 current_term=self.current_term,
                                 node=self.node_id, kind="AppendEntries")
        if msg.term > self.current_term:
            self._become_follower(msg.term, msg.leader_id)
        elif self.role == CANDIDATE:
            # 已发现合法领导者：回到跟随者
            self.role = FOLLOWER
            self.leader_id = msg.leader_id
            self._resolve_inflight_demotion()
        else:
            self.leader_id = msg.leader_id
        self._reset_election_timer()

        # prev 一致性检查（不匹配会抛 LogCheckMismatchError）
        if msg.prev_log_index > self.log.last_index:
            raise LogCheckMismatchError(
                expected_index=msg.prev_log_index,
                expected_term=msg.prev_log_term,
                hint_index=self.log.last_index, node=self.node_id)
        if msg.prev_log_index >= 1 and \
                self.log.term_at(msg.prev_log_index) != msg.prev_log_term:
            actual = self.log.term_at(msg.prev_log_index)
            hint = msg.prev_log_index - 1
            i = msg.prev_log_index
            while i > 0 and self.log.entries[i].term == actual:
                hint = i - 1
                i -= 1
            raise LogCheckMismatchError(
                expected_index=msg.prev_log_index,
                expected_term=msg.prev_log_term,
                hint_index=hint, node=self.node_id)

        # 合并 entries（内部会拒绝覆盖已提交区间）
        try:
            self.log.match_and_truncate(
                msg.prev_log_index, msg.prev_log_term,
                list(msg.entries), self.commit_index)
        except (LogCheckMismatchError, CommittedRollbackError):
            raise
        self._persist_log()  # 确认前先持久化（fsync）

        if msg.leader_commit > self.commit_index:
            self.commit_index = min(msg.leader_commit, self.log.last_index)
            self.store.save_commit_index(self.commit_index)
            self._apply_committed()

        match_index = msg.prev_log_index + len(msg.entries)
        self.logr.info(
            "[append] from=%s term=%d req=%s prev=(%d,%d) n_new=%d "
            "match=%d commit=%d",
            msg.leader_id, msg.term, msg.request_id,
            msg.prev_log_index, msg.prev_log_term, len(msg.entries),
            match_index, self.commit_index)
        return AppendEntriesResult(
            term=self.current_term, success=True, follower_id=self.node_id,
            match_index=match_index, hint_index=match_index, reason="ok",
            request_id=msg.request_id)

    # ================= RPC 派发与权限边界 =================
    def _send_rpc(self, peer: str, request: object,
                  on_result: Callable[[object], None] | None = None) -> None:
        rpc_id = self._new_msg_id()
        env = Envelope(src=self.node_id, dst=peer, payload=request, msg_id=rpc_id)
        kind = "append" if isinstance(request, AppendEntries) else "vote"

        def reply_handler(resp_env: Envelope) -> None:
            if on_result is None:
                return
            payload = resp_env.payload
            if isinstance(payload, ConsensusError):
                self.logr.info("[rpc] rejected dst=%s id=%s reason=%s",
                               peer, rpc_id, payload.reason)
                on_result(_error_to_result(payload, kind))
            else:
                on_result(payload)

        self._correlations[rpc_id] = reply_handler
        self._send_raw(env)

    def dispatch(self, env: Envelope) -> None:
        # 身份边界：内部协调操作只接受集群成员，且消息 caller 必须与载荷声明一致
        if env.src not in self.peers:
            err = UnknownCallerError(caller=env.src, node=self.node_id)
            self.logr.warning("[auth] reject from unknown caller=%s id=%s: %s",
                              env.src, env.msg_id, err.reason)
            self._send_raw(Envelope(src=self.node_id, dst=env.src,
                                    payload=err, msg_id=env.msg_id))
            return

        payload = env.payload
        try:
            if isinstance(payload, RequestVote):
                result = self.handle_request_vote(payload, env.src)
            elif isinstance(payload, AppendEntries):
                result = self.handle_append_entries(payload, env.src)
            elif isinstance(payload, (RequestVoteResult, AppendEntriesResult)):
                handler = self._correlations.get(env.msg_id)
                if handler:
                    handler(env)
                return
            elif isinstance(payload, ConsensusError):
                handler = self._correlations.get(env.msg_id)
                if handler:
                    handler(env)
                return
            else:
                err = UnknownCallerError(caller=env.src, node=self.node_id)
                self._send_raw(Envelope(src=self.node_id, dst=env.src,
                                        payload=err, msg_id=env.msg_id))
                return
        except ConsensusError as err:
            self.logr.warning("[rpc] reject caller=%s id=%s: %s",
                              env.src, env.msg_id, err.reason)
            self._send_raw(Envelope(src=self.node_id, dst=env.src,
                                    payload=err, msg_id=env.msg_id))
            return

        self._send_raw(Envelope(src=self.node_id, dst=env.src,
                                payload=result, msg_id=env.msg_id))

    # （实例关联表在 __init__ 中建立）

    # ================= 客户端 API =================
    def submit_write(self, client_id: str, request_id: str,
                     command: dict) -> "Proposal":
        """非阻塞提交（必须在节点事件线程上调用）。

        幂等命中立即返回已完成 Proposal；在途重复提交复用同一 Proposal；
        新请求追加一条日志并广播。调用方驱动事件泵等待 prop.event。
        """
        if not client_id:
            raise UnknownCallerError(caller="<empty>", node=self.node_id)
        if self.role != LEADER:
            raise NotLeaderError(term=self.current_term,
                                 leader_id=self.leader_id, node=self.node_id)
        key = (client_id, request_id)
        if key in self.applied_requests:
            idx, result = self.applied_requests[key]
            self.logr.info("[idempotent] client=%s req=%s -> reuse index=%d",
                           client_id, request_id, idx)
            p = Proposal(client_id=client_id, request_id=request_id,
                         command=command, term=self.current_term,
                         index=idx, done=True, result=result)
            p.event.set()
            return p
        if key in self.inflight:
            self.logr.info("[idempotent] client=%s req=%s -> join inflight",
                           client_id, request_id)
            return self.inflight[key]

        entry = LogEntry(term=self.current_term,
                         index=self.log.last_index + 1,
                         client_id=client_id, request_id=request_id,
                         command=command)
        self.log.append([entry])
        self._persist_log()
        prop = Proposal(client_id=client_id, request_id=request_id,
                        command=command, term=self.current_term,
                        index=entry.index)
        self.inflight[key] = prop
        self.proposals_by_index[entry.index] = prop
        self.logr.info("[client-write] client=%s req=%s -> index=%d term=%d",
                       client_id, request_id, entry.index, self.current_term)
        self._advance_commit()  # 单节点立即提交；多节点无副作用
        self._broadcast_append_entries()
        return prop

    def submit_read(self, key: str) -> "PendingRead":
        """非阻塞线性一致读（ReadIndex），必须在节点事件线程上调用。"""
        if self.role != LEADER:
            raise NotLeaderError(term=self.current_term,
                                 leader_id=self.leader_id, node=self.node_id)
        if not self.other_peers:
            pr = PendingRead(key=key, term=self.current_term, heartbeat_id="")
            pr.result = self.sm.get(key)
            pr.event.set()
            return pr
        hb_id = self._new_msg_id()
        pr = PendingRead(key=key, term=self.current_term, heartbeat_id=hb_id)
        self.pending_reads.append(pr)
        for peer in self.other_peers:
            self._replicate_to(peer, hb_id)
        return pr

    def client_write(self, client_id: str, request_id: str,
                     command: dict, timeout: float = 2.0) -> tuple[int, object]:
        """阻塞式包装（节点独立线程/真实网络模式使用）。"""
        prop = self.submit_write(client_id, request_id, command)
        if not prop.event.wait(timeout):
            raise ConsensusError(
                f"write timeout: not committed within {timeout}s "
                f"(index={prop.index})", term=self.current_term,
                node=self.node_id)
        if prop.error:
            raise prop.error
        return prop.index, prop.result

    def client_read(self, key: str, timeout: float = 2.0) -> object:
        """阻塞式线性一致读，不会读到陈旧/未提交数据。"""
        pr = self.submit_read(key)
        if not pr.event.wait(timeout):
            raise ConsensusError("read timeout: leadership unconfirmed",
                                 term=self.current_term, node=self.node_id)
        if pr.error:
            raise pr.error
        return pr.result

    def _resolve_pending_reads(self, heartbeat_id: str) -> None:
        remaining = []
        for r in self.pending_reads:
            if r.heartbeat_id == heartbeat_id and not r.event.is_set():
                r.result = self.sm.get(r.key)
                r.event.set()
            elif not r.event.is_set():
                remaining.append(r)
        self.pending_reads = remaining

    def _fail_pending_reads(self) -> None:
        for r in self.pending_reads:
            if not r.event.is_set():
                r.error = NotLeaderError(term=self.current_term,
                                         leader_id=self.leader_id,
                                         node=self.node_id)
                r.event.set()
        self.pending_reads.clear()


def _error_to_result(err: ConsensusError, kind: str = "vote") -> object:
    """把拒绝异常映射为对应类型的 RPC 结果（带回当前任期与可解释原因）。"""
    from raft_consensus.messages import RequestVoteResult, AppendEntriesResult
    term = err.term if err.term is not None else 0
    if isinstance(err, StaleTermError):
        term = err.current_term
    node = err.node or "?"
    if kind == "append" or isinstance(err, (LogCheckMismatchError,
                                            CommittedRollbackError)):
        hint = getattr(err, "hint_index",
                       getattr(err, "commit_index", 0))
        return AppendEntriesResult(
            term=term, success=False, follower_id=node,
            match_index=0, hint_index=hint, reason=err.reason)
    return RequestVoteResult(
        term=term, vote_granted=False, voter_id=node, reason=err.reason)
