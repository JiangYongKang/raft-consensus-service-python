"""Raft 节点：选举、日志复制、提交与恢复。

线程模型：``self._lock`` 保护所有易变/持久状态；持锁期间绝不做网络 IO。
后台单线程驱动选举超时与心跳；复制 RPC 在独立工作线程中发出，
每个 follower 最多一个在途 AppendEntries，避免乱序覆盖。

可解释性：所有关键决策（任期变化、投票、拒绝原因、提交位点推进、
幂等命中）都通过 ``raft`` logger 打印，日志带来源节点与请求标识。
"""

from __future__ import annotations

import logging
import random
import threading
import time
import uuid

from .errors import (
    DuplicateRequestError,
    InvalidCommitError,
    NotLeaderError,
    StaleClientRequestError,
    StaleTermError,
    UnauthorizedError,
)
from .statemachine import StateMachine
from .storage import Storage
from .transport import Transport
from .types import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    ClientResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    LogEntry,
    NodeStatus,
    RequestVoteRequest,
    RequestVoteResponse,
    Role,
)

log = logging.getLogger("raft")

_INTERNAL_CLIENT = "__raft__"


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: list[str],
        storage: Storage,
        state_machine: StateMachine,
        transport: Transport,
        *,
        election_timeout: tuple[float, float] = (0.15, 0.30),
        heartbeat_interval: float = 0.05,
        tick_interval: float = 0.01,
        apply_timeout: float = 2.0,
        admin_token: str = "local-cluster-token",
        snapshot_threshold: int = 100,
        rng: random.Random | None = None,
        state_machine_factory=None,
    ) -> None:
        self.node_id = node_id
        self.peers = list(peers)
        self.members = set(self.peers) | {node_id}
        self.storage = storage
        # 工厂用于崩溃重启时重建全新状态机；缺省则永远返回同一实例
        self._sm_factory = state_machine_factory or (lambda: state_machine)
        self.sm = state_machine
        self.transport = transport
        self._election_timeout = election_timeout
        self._heartbeat_interval = heartbeat_interval
        self._tick_interval = tick_interval
        self._apply_timeout = apply_timeout
        self._admin_token = admin_token
        self._snapshot_threshold = snapshot_threshold
        self._rng = rng or random.Random()

        self._lock = threading.RLock()
        self._apply_lock = threading.Lock()

        # 持久状态（缓存在节点上，真实数据在 storage）
        self._role = Role.FOLLOWER
        self._leader_id: str | None = None

        # 易变状态
        self.commit_index = storage.commit_index()
        self.last_applied = 0
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self._sending: set[str] = set()
        self._pending: dict[int, threading.Event] = {}
        self._results: dict[int, dict] = {}
        # (client_id, request_id) -> (index, command, client_seq, term)
        self._dedup: dict[tuple[str, str], tuple[int, dict, int, int]] = {}
        self._client_max_seq: dict[str, int] = {}

        # 计时器/线程
        self._election_deadline = 0.0
        self._heartbeat_deadline = 0.0
        self._running = False
        self._epoch = 0
        self._stop_event: threading.Event = threading.Event()
        self._thread: threading.Thread | None = None

        self._restore_snapshot()
        self._rebuild_indexes()
        self._replay_committed()

    # ------------------------------------------------------------------ #
    # 索引重建与恢复
    # ------------------------------------------------------------------ #
    def _restore_snapshot(self) -> None:
        """若本地存在快照，先把状态机与幂等元数据恢复到快照边界。

        快照是已提交且已应用历史的紧凑形态；恢复后只需重放边界之后的日志。
        """
        snap = self.storage.load_snapshot()
        if snap is None:
            return
        meta, payload = snap
        idx = int(meta["last_included_index"])
        with self._apply_lock:
            self.sm.install(payload.get("state", {}), idx)
        for rec in payload.get("dedup", []):
            self._dedup[(rec["client_id"], rec["request_id"])] = (
                rec["index"], rec["command"], rec["client_seq"], rec.get("term", 0),
            )
        self._client_max_seq.update(payload.get("client_max_seq", {}))
        self._results.update({int(k): v for k, v in payload.get("results", {}).items()})
        self.last_applied = self.sm.last_applied
        log.info(
            "node=%s SNAPSHOT-RESTORE boundary=(%d,%d) snapshot_id=%s applied=%d",
            self.node_id, idx, int(meta["last_included_term"]),
            meta.get("snapshot_id", "-"), self.last_applied,
        )

    def _rebuild_indexes(self) -> None:
        for e in self.storage.all_entries():
            self._dedup[(e.client_id, e.request_id)] = (e.index, e.command, e.client_seq, e.term)
            prev = self._client_max_seq.get(e.client_id, 0)
            if e.client_seq > prev:
                self._client_max_seq[e.client_id] = e.client_seq
        self.last_applied = self.sm.last_applied

    def _replay_committed(self) -> None:
        """重启后把已提交但尚未应用到状态机的日志按序重放。

        状态机的 last_applied 是应用位点的唯一事实来源；重放完成后
        节点的 last_applied 必须与之对齐，避免双轨位点失同步。
        """
        with self._apply_lock:
            while self.sm.last_applied < self.commit_index:
                idx = self.sm.last_applied + 1
                e = self.storage.get_entry(idx)
                if e is None:  # 理论不可能：commit 不超过日志末端
                    raise InvalidCommitError(
                        f"replay gap: committed index {idx} missing from log"
                    )
                result = self.sm.apply(idx, e.term, e.command)
                self._results[idx] = result
                log.info(
                    "node=%s replay index=%d term=%d request_id=%s",
                    self.node_id, idx, e.term, e.request_id,
                )
            # 无论本次是否实际重放，都与状态机对齐（重启前可能已全部应用）
            self.last_applied = self.sm.last_applied

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self.transport.register(self.node_id, self) if hasattr(self.transport, "register") else None
            self._running = True
            stop_event = threading.Event()
            epoch = self._epoch
            self._stop_event = stop_event
            self._reset_election_timer_locked()
            self._thread = threading.Thread(
                target=self._run_loop, args=(epoch, stop_event),
                name=f"raft-{self.node_id}", daemon=True,
            )
            self._thread.start()
        log.info("node=%s started term=%d role=%s", self.node_id, self.storage.current_term, self._role.value)

    def stop(self) -> None:
        self._halt(join=True)
        if hasattr(self.transport, "unregister"):
            self.transport.unregister(self.node_id)
        self.storage.close()

    def crash(self) -> None:
        """模拟崩溃：终止当前代际并从网络注销；持久化数据完整保留。

        递增代际号（epoch），所有上一代在途线程看到 epoch 不匹配后自行
        退出，且不会再触碰新生命周期的状态机。
        """
        log.warning("node=%s CRASH (data retained on storage)", self.node_id)
        self._halt(join=True)
        if hasattr(self.transport, "unregister"):
            self.transport.unregister(self.node_id)

    def restart(self) -> None:
        """从持久化状态恢复并重新上线（角色重置为 follower，等待心跳/超时）。

        崩溃语义：重建一个全新的状态机从已提交日志重放。崩溃时残留的旧
        代际线程只持有旧 epoch/旧状态机对象，无法污染新生命周期。
        """
        self.storage.reload()
        new_sm = self._sm_factory()
        with self._lock:
            self.sm = new_sm
            self._role = Role.FOLLOWER
            self._leader_id = None
            self.next_index = {}
            self.match_index = {}
            self._sending.clear()
            self._results = {}
            self._pending = {}
            self.last_applied = 0
            self.commit_index = self.storage.commit_index()
        self._restore_snapshot()
        self._rebuild_indexes()
        self._replay_committed()
        log.info(
            "node=%s RESTART recovered term=%d commit_index=%d last_log_index=%d applied=%d",
            self.node_id, self.storage.current_term, self.commit_index,
            self.storage.last_index(), self.last_applied,
        )
        self.start()

    def _halt(self, join: bool) -> None:
        # 进入新代际：旧线程据此自我了断
        with self._lock:
            self._running = False
            self._epoch += 1
            stop_event = getattr(self, "_stop_event", None)
            t = self._thread
        if stop_event is not None:
            stop_event.set()
        if join and t is not None and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=1.0)

    def _run_loop(self, epoch: int, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            try:
                self._tick(epoch)
            except Exception:  # pragma: no cover - 后台线程不能因异常退出
                log.exception("node=%s tick error", self.node_id)
            stop_event.wait(self._tick_interval)

    # ------------------------------------------------------------------ #
    # 后台驱动
    # ------------------------------------------------------------------ #
    def _tick(self, epoch: int) -> None:
        now = time.monotonic()
        with self._lock:
            if not self._running or self._epoch != epoch:
                return
            role = self._role
            if role == Role.LEADER:
                due = now >= self._heartbeat_deadline
            else:
                due = now >= self._election_deadline
        if not due:
            return
        if role == Role.LEADER:
            self._broadcast_append_entries()
            with self._lock:
                self._heartbeat_deadline = time.monotonic() + self._heartbeat_interval
        else:
            threading.Thread(
                target=self._start_election, args=(epoch,),
                name=f"elect-{self.node_id}", daemon=True,
            ).start()

    def _reset_election_timer_locked(self) -> None:
        lo, hi = self._election_timeout
        self._election_deadline = time.monotonic() + self._rng.uniform(lo, hi)

    # ------------------------------------------------------------------ #
    # 角色与任期
    # ------------------------------------------------------------------ #
    def _become_follower_locked(self, term: int) -> None:
        old_term = self.storage.current_term
        if term > old_term:
            self.storage.save_term(term, None)
            log.info(
                "node=%s TERM-CHANGE %d->%d role=%s reason=higher-term-seen",
                self.node_id, old_term, term, Role.FOLLOWER.value,
            )
        self._role = Role.FOLLOWER
        self.next_index = {}
        self.match_index = {}
        self._sending.clear()

    def _become_leader_locked(self) -> None:
        term = self.storage.current_term
        self._role = Role.LEADER
        self._leader_id = self.node_id
        last = self.storage.last_index()
        self.next_index = {p: last + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self._sending.clear()
        self._heartbeat_deadline = time.monotonic()
        log.info(
            "node=%s TERM-CHANGE role=%s term=%d reason=won-election decision-index=0",
            self.node_id, Role.LEADER.value, term,
        )
        # 新主立即追加一条当前任期 no-op，用于安全提交前任日志并确立领导权
        noop = LogEntry(
            index=last + 1, term=term, command={"op": "noop"},
            client_id=_INTERNAL_CLIENT, request_id=f"noop-t{term}-{uuid.uuid4().hex[:8]}",
            client_seq=0,
        )
        self.storage.append_entries([noop])
        self._pending.setdefault(noop.index, threading.Event())
        log.info(
            "node=%s append noop index=%d term=%d request_id=%s source=__raft__",
            self.node_id, noop.index, term, noop.request_id,
        )

    # ------------------------------------------------------------------ #
    # 选举
    # ------------------------------------------------------------------ #
    def _start_election(self, epoch: int) -> None:
        with self._lock:
            if not self._running or self._epoch != epoch or self._role == Role.LEADER:
                return
            new_term = self.storage.current_term + 1
            self.storage.save_term(new_term, self.node_id)
            self._role = Role.CANDIDATE
            self._leader_id = None
            self._reset_election_timer_locked()
            req = RequestVoteRequest(
                term=new_term,
                candidate_id=self.node_id,
                last_log_index=self.storage.last_index(),
                last_log_term=self.storage.last_term(),
            )
            election_term = new_term

        log.info(
            "node=%s TERM-CHANGE role=%s term=%d reason=election-timeout source=%s",
            self.node_id, Role.CANDIDATE.value, election_term, self.node_id,
        )

        votes = {self.node_id}
        lock = self._lock

        def ask(peer: str) -> None:
            try:
                resp = self.transport.request_vote(peer, req, timeout=self._election_timeout[1])
            except Exception as e:
                log.debug("node=%s request-vote to=%s failed: %s", self.node_id, peer, e)
                return
            with lock:
                if not self._running or self._epoch != epoch or self.storage.current_term != election_term:
                    return
                if self._role != Role.CANDIDATE:
                    return
                if resp.term > election_term:
                    log.info(
                        "node=%s step-down source=%s reason=stale-term local=%d remote=%d",
                        self.node_id, peer, election_term, resp.term,
                    )
                    self._become_follower_locked(resp.term)
                    return
                if resp.term < election_term:
                    return
                if resp.vote_granted:
                    votes.add(peer)
                    log.info(
                        "node=%s vote-granted by=%s term=%d tally=%d/%d",
                        self.node_id, peer, election_term, len(votes), len(self.members),
                    )
                    needed = len(self.members) // 2 + 1
                    if len(votes) >= needed and self._role == Role.CANDIDATE:
                        self._become_leader_locked()

        threads = [threading.Thread(target=ask, args=(p,), daemon=True) for p in self.peers]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=self._election_timeout[1] + 0.05)

        with self._lock:
            if (self._running and self._epoch == epoch and self._role == Role.CANDIDATE
                    and self.storage.current_term == election_term):
                self._broadcast_append_entries_locked_if_leader()
                self._reset_election_timer_locked()
                log.info(
                    "node=%s election term=%d inconclusive tally=%d/%d -> retry",
                    self.node_id, election_term, len(votes), len(self.members),
                )

    def _broadcast_append_entries_locked_if_leader(self) -> None:
        if self._role == Role.LEADER:
            self._heartbeat_deadline = time.monotonic()
            for p in self.peers:
                self._spawn_send(p)

    # ------------------------------------------------------------------ #
    # RequestVote RPC
    # ------------------------------------------------------------------ #
    def handle_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        entry_epoch = self._epoch
        if req.candidate_id not in self.members:
            raise UnauthorizedError(
                f"request-vote from unknown node {req.candidate_id!r}",
                term=self.storage.current_term,
            )
        with self._lock:
            if not self._running or self._epoch != entry_epoch:
                return RequestVoteResponse(
                    term=self.storage.current_term, vote_granted=False, reason="node_restarting"
                )
            term = self.storage.current_term
            if req.term > term:
                self._become_follower_locked(req.term)
                term = req.term
            if req.term < term:
                log.info(
                    "node=%s vote-denied source=%s reason=stale-term local=%d remote=%d",
                    self.node_id, req.candidate_id, term, req.term,
                )
                return RequestVoteResponse(term=term, vote_granted=False, reason="stale_term")

            up_to_date = (
                req.last_log_term > self.storage.last_term()
                or (
                    req.last_log_term == self.storage.last_term()
                    and req.last_log_index >= self.storage.last_index()
                )
            )
            voted_for = self.storage.voted_for
            can_vote = voted_for is None or voted_for == req.candidate_id
            if not up_to_date:
                reason = "candidate_log_behind"
            elif not can_vote:
                reason = f"already_voted_for={voted_for}"
            else:
                reason = "ok"
            granted = up_to_date and can_vote
            if granted:
                self.storage.save_term(term, req.candidate_id)
                self._reset_election_timer_locked()
            log.info(
                "node=%s vote-decision source=%s granted=%s term=%d reason=%s "
                "candidate=(%d,%d) local=(%d,%d)",
                self.node_id, req.candidate_id, granted, term, reason,
                req.last_log_index, req.last_log_term,
                self.storage.last_index(), self.storage.last_term(),
            )
            return RequestVoteResponse(term=term, vote_granted=granted, reason=reason)

    # ------------------------------------------------------------------ #
    # AppendEntries RPC
    # ------------------------------------------------------------------ #
    def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        entry_epoch = self._epoch
        if req.leader_id not in self.members:
            raise UnauthorizedError(
                f"append-entries from unknown node {req.leader_id!r}",
                term=self.storage.current_term,
            )
        with self._lock:
            if not self._running or self._epoch != entry_epoch:
                return AppendEntriesResponse(
                    term=self.storage.current_term, success=False, reason="node_restarting"
                )
            term = self.storage.current_term

            # 1) 任期校验：旧任期请求必须明确拒绝，不允许静默忽略或覆盖
            if req.term < term:
                log.info(
                    "node=%s append-denied source=%s reason=stale-term local=%d remote=%d "
                    "request_id=%s",
                    self.node_id, req.leader_id, term, req.term,
                    req.entries[0].request_id if req.entries else "-",
                )
                return AppendEntriesResponse(term=term, success=False, reason="stale_term")

            if req.term > term:
                self._become_follower_locked(req.term)
                term = req.term

            if self._role == Role.LEADER and req.term == term:
                # 选举安全约束：同一任期不应出现两个主
                log.error(
                    "node=%s append-denied source=%s reason=duplicate-leader-same-term term=%d",
                    self.node_id, req.leader_id, term,
                )
                return AppendEntriesResponse(
                    term=term, success=False, reason="duplicate_leader_same_term"
                )

            if self._role == Role.CANDIDATE:
                self._role = Role.FOLLOWER

            self._leader_id = req.leader_id
            self._reset_election_timer_locked()

            # 2) 日志一致性检查（先裁掉已被本地快照覆盖的前缀）
            entries = req.entries
            prev_index, prev_term = req.prev_log_index, req.prev_log_term
            base = self.storage.snapshot_index()
            if prev_index < base:
                entries = [e for e in entries if e.index > base]
                prev_index, prev_term = base, self.storage.snapshot_term()
                log.info(
                    "node=%s append-trim source=%s reason=covered-by-snapshot "
                    "boundary=%d kept=%d",
                    self.node_id, req.leader_id, base, len(entries),
                )
            last = self.storage.last_index()
            if prev_index > last:
                log.info(
                    "node=%s append-denied source=%s reason=missing-prev local_last=%d prev=(%d,%d)",
                    self.node_id, req.leader_id, last, prev_index, prev_term,
                )
                return AppendEntriesResponse(
                    term=term, success=False, match_index=last, reason="missing_prev_entries"
                )
            if prev_index > 0 and self.storage.term_at(prev_index) != prev_term:
                conflict = self.storage.term_at(prev_index)
                log.info(
                    "node=%s append-denied source=%s reason=log-inconsistent at=%d "
                    "local_term=%d leader_term=%d -> truncate",
                    self.node_id, req.leader_id, prev_index, conflict, prev_term,
                )
                self.storage.truncate_from(prev_index)
                return AppendEntriesResponse(
                    term=term, success=False,
                    match_index=max(0, prev_index - 1), reason="log_inconsistent",
                )

            # 3) 追加新条目（同 index 不同任期则截断冲突后缀）
            appended: list[LogEntry] = []
            for e in entries:
                existing = self.storage.get_entry(e.index)
                if existing is None:
                    self.storage.append_entries([e])
                    appended.append(e)
                elif existing.term != e.term:
                    log.info(
                        "node=%s truncate-from index=%d source=%s request_id=%s",
                        self.node_id, e.index, req.leader_id, e.request_id,
                    )
                    # 被截断的未提交条目：清掉其幂等记录，避免指向错误结果
                    for old in self.storage.entries_from(e.index):
                        self._dedup.pop((old.client_id, old.request_id), None)
                    self.storage.truncate_from(e.index)
                    self.storage.append_entries([e])
                    appended.append(e)
                # 已存在且任期相同：幂等跳过，绝不重复应用
            # follower 侧同步维护幂等索引：主备切换后新主才能识别
            # 客户端重试（快照/重启恢复路径之外的主要来源）
            for e in appended:
                self._dedup[(e.client_id, e.request_id)] = (
                    e.index, e.command, e.client_seq, e.term,
                )
                if e.client_seq > self._client_max_seq.get(e.client_id, 0):
                    self._client_max_seq[e.client_id] = e.client_seq

            # 4) 推进提交位点（只能前进，不能回退）
            advanced = False
            if req.leader_commit > self.commit_index:
                new_commit = min(req.leader_commit, self.storage.last_index())
                if new_commit > self.commit_index:
                    self.storage.save_commit_index(new_commit)
                    old = self.commit_index
                    self.commit_index = new_commit
                    advanced = True
                    log.info(
                        "node=%s COMMIT decision-index %d->%d source=%s term=%d",
                        self.node_id, old, new_commit, req.leader_id, term,
                    )

            match = prev_index + len(entries)
            resp = AppendEntriesResponse(
                term=term, success=True, match_index=match, reason="ok"
            )
        if appended:
            log.info(
                "node=%s appended %d entries from=%s indices=[%d..%d] request_ids=%s",
                self.node_id, len(appended), req.leader_id,
                appended[0].index, appended[-1].index,
                ",".join(e.request_id for e in appended),
            )
        if advanced and self._epoch == entry_epoch:
            self._apply_committed()
        return resp

    # ------------------------------------------------------------------ #
    # InstallSnapshot RPC（快照追赶）
    # ------------------------------------------------------------------ #
    def handle_install_snapshot(self, req: InstallSnapshotRequest) -> InstallSnapshotResponse:
        """接收主节点快照：整体替换已压缩历史对应的状态。

        安全边界：
        - 旧任期请求拒绝；比本地快照还旧的快照幂等忽略；
        - 快照边界与本地日志冲突时，丢弃边界之后的整段本地日志
          （那些条目必然未被提交，否则主节点不可能压出这份快照）；
        - commit_index / last_applied 只前进不回退。
        """
        entry_epoch = self._epoch
        if req.leader_id not in self.members:
            raise UnauthorizedError(
                f"install-snapshot from unknown node {req.leader_id!r}",
                term=self.storage.current_term,
            )
        with self._lock:
            if not self._running or self._epoch != entry_epoch:
                return InstallSnapshotResponse(
                    term=self.storage.current_term, success=False, reason="node_restarting"
                )
            term = self.storage.current_term
            if req.term < term:
                log.info(
                    "node=%s snapshot-denied source=%s reason=stale-term local=%d remote=%d "
                    "snapshot_id=%s",
                    self.node_id, req.leader_id, term, req.term, req.snapshot_id,
                )
                return InstallSnapshotResponse(term=term, success=False, reason="stale_term")
            if req.term > term:
                self._become_follower_locked(req.term)
                term = req.term
            if self._role == Role.LEADER and req.term == term:
                log.error(
                    "node=%s snapshot-denied source=%s reason=duplicate-leader-same-term term=%d",
                    self.node_id, req.leader_id, term,
                )
                return InstallSnapshotResponse(
                    term=term, success=False, reason="duplicate_leader_same_term"
                )
            if self._role == Role.CANDIDATE:
                self._role = Role.FOLLOWER
            self._leader_id = req.leader_id
            self._reset_election_timer_locked()

            idx = req.last_included_index
            base = self.storage.snapshot_index()
            if idx <= base:
                # 已拥有同位或更新的快照：幂等成功，不重复安装
                log.info(
                    "node=%s SNAPSHOT-SKIP source=%s reason=already-covered "
                    "snapshot_id=%s remote_boundary=%d local_boundary=%d",
                    self.node_id, req.leader_id, req.snapshot_id, idx, base,
                )
                return InstallSnapshotResponse(
                    term=term, success=True, match_index=base, reason="already_covered"
                )

            # 边界条目与本地日志冲突 -> 边界之后的本地日志整段作废
            drop_tail = False
            if idx <= self.storage.last_index():
                local_term = self.storage.term_at(idx)
                if local_term != req.last_included_term:
                    drop_tail = True
                    log.info(
                        "node=%s snapshot-conflict at=%d local_term=%d snapshot_term=%d "
                        "-> drop log tail",
                        self.node_id, idx, local_term, req.last_included_term,
                    )

            payload = req.data or {}
            meta = {
                "snapshot_id": req.snapshot_id,
                "last_included_index": idx,
                "last_included_term": req.last_included_term,
            }
            # 顺序：先推进持久化的提交位点，再落快照，最后压缩日志；
            # 任何一步崩溃，重启后都能从已落盘状态继续而不丢已提交结论。
            if idx > self.commit_index:
                self.storage.save_commit_index(idx)
                old_commit = self.commit_index
                self.commit_index = idx
                log.info(
                    "node=%s COMMIT decision-index %d->%d source=%s term=%d via=snapshot",
                    self.node_id, old_commit, idx, req.leader_id, term,
                )
            self.storage.save_snapshot(meta, payload)
            self.storage.compact_upto(idx, term=req.last_included_term)
            if drop_tail:
                self.storage.truncate_from(idx + 1)

            # 本地已应用位点超过快照边界时跳过状态机安装（状态更新，不可回退）
            if idx > self.sm.last_applied:
                with self._apply_lock:
                    self.sm.install(payload.get("state", {}), idx)
            # 合并幂等元数据：快照覆盖的以快照为准，边界之后的本地记录保留
            for rec in payload.get("dedup", []):
                self._dedup[(rec["client_id"], rec["request_id"])] = (
                    rec["index"], rec["command"], rec["client_seq"], rec.get("term", 0),
                )
            self._client_max_seq.update(payload.get("client_max_seq", {}))
            self._results.update(
                {int(k): v for k, v in payload.get("results", {}).items()}
            )
            if self.sm.last_applied > self.last_applied:
                self.last_applied = self.sm.last_applied
            log.info(
                "node=%s SNAPSHOT-INSTALL source=%s snapshot_id=%s boundary=(%d,%d) "
                "commit_index=%d applied=%d",
                self.node_id, req.leader_id, req.snapshot_id, idx,
                req.last_included_term, self.commit_index, self.last_applied,
            )
            return InstallSnapshotResponse(
                term=term, success=True, match_index=idx, reason="ok"
            )

    # ------------------------------------------------------------------ #
    # 主节点复制
    # ------------------------------------------------------------------ #
    def _broadcast_append_entries(self) -> None:
        with self._lock:
            if self._role != Role.LEADER:
                return
            for p in self.peers:
                self._spawn_send(p)

    def _spawn_send(self, target: str) -> None:
        # 调用方持锁
        if target in self._sending:
            return
        self._sending.add(target)
        epoch = self._epoch
        t = threading.Thread(
            target=self._send_worker, args=(target, epoch),
            name=f"send-{self.node_id}-{target}", daemon=True,
        )
        t.start()

    def _send_worker(self, target: str, epoch: int) -> None:
        try:
            while self._running and self._epoch == epoch:
                with self._lock:
                    if self._role != Role.LEADER or self._epoch != epoch:
                        return
                    leader_term = self.storage.current_term
                    ni = self.next_index[target]
                    if ni <= self.storage.snapshot_index():
                        # 对端需要的旧历史已被压缩：改用整份快照追赶
                        snap_req = self._build_snapshot_request_locked(leader_term)
                        req = None
                    else:
                        snap_req = None
                        prev = ni - 1
                        prev_term = self.storage.term_at(prev) if prev >= 0 else 0
                        entries = self.storage.entries_from(ni)
                        req = AppendEntriesRequest(
                            term=leader_term,
                            leader_id=self.node_id,
                            prev_log_index=prev,
                            prev_log_term=prev_term,
                            entries=entries,
                            leader_commit=self.commit_index,
                        )
                        batch_ids = [e.request_id for e in entries]
                if snap_req is None and req is None:
                    return  # 快照暂不可用，等下一轮心跳再试
                if snap_req is not None:
                    if not self._send_snapshot(target, snap_req, epoch, leader_term):
                        return
                    continue
                try:
                    resp = self.transport.append_entries(target, req, timeout=1.0)
                except Exception as e:
                    log.debug("node=%s append to=%s failed: %s", self.node_id, target, e)
                    return

                with self._lock:
                    if (self._role != Role.LEADER or self._epoch != epoch
                            or self.storage.current_term != leader_term):
                        return
                    if resp.term > leader_term:
                        self._become_follower_locked(resp.term)
                        log.info(
                            "node=%s step-down source=%s reason=stale-term %d->%d",
                            self.node_id, target, leader_term, resp.term,
                        )
                        return
                    if not resp.success:
                        # 日志落后：回退 nextIndex 后重试（位点由对端日志校验把关）
                        new_ni = max(1, self.next_index[target] - 1)
                        old_ni = self.next_index[target]
                        self.next_index[target] = new_ni
                        log.info(
                            "node=%s backoff next-index to=%s %d->%d reason=%s request_ids=%s",
                            self.node_id, target, old_ni, new_ni, resp.reason,
                            ",".join(batch_ids) if batch_ids else "-",
                        )
                        continue
                    new_match = req.prev_log_index + len(req.entries)
                    if new_match > self.match_index.get(target, 0):
                        self.match_index[target] = new_match
                    # 与快照路径同理：回退可能使 next_index 低于 match_index，
                    # 成功后必须把它推进到 new_match + 1，避免原地重发。
                    if self.next_index[target] < new_match + 1:
                        self.next_index[target] = new_match + 1
                    advanced = self._advance_commit_index_locked()
                    caught_up = self.next_index[target] > self.storage.last_index()
                if advanced and self._epoch == epoch:
                    self._apply_committed()
                if caught_up:
                    return
        finally:
            with self._lock:
                self._sending.discard(target)

    def _build_snapshot_request_locked(self, leader_term: int) -> InstallSnapshotRequest | None:
        """从本地持久化快照构造 InstallSnapshot 请求（调用方持锁）。"""
        snap = self.storage.load_snapshot()
        if snap is None:
            return None
        meta, payload = snap
        return InstallSnapshotRequest(
            term=leader_term,
            leader_id=self.node_id,
            snapshot_id=str(meta.get("snapshot_id", "-")),
            last_included_index=int(meta["last_included_index"]),
            last_included_term=int(meta["last_included_term"]),
            data=payload,
        )

    def _send_snapshot(
        self, target: str, req: InstallSnapshotRequest, epoch: int, leader_term: int
    ) -> bool:
        """发送一次 InstallSnapshot；返回 True 表示应继续复制循环。"""
        log.info(
            "node=%s SNAPSHOT-SEND to=%s snapshot_id=%s boundary=(%d,%d) term=%d",
            self.node_id, target, req.snapshot_id,
            req.last_included_index, req.last_included_term, leader_term,
        )
        try:
            resp = self.transport.install_snapshot(target, req, timeout=2.0)
        except Exception as e:
            log.debug("node=%s snapshot to=%s failed: %s", self.node_id, target, e)
            return False
        with self._lock:
            if (self._role != Role.LEADER or self._epoch != epoch
                    or self.storage.current_term != leader_term):
                return False
            if resp.term > leader_term:
                self._become_follower_locked(resp.term)
                log.info(
                    "node=%s step-down source=%s reason=stale-term %d->%d",
                    self.node_id, target, leader_term, resp.term,
                )
                return False
            if not resp.success:
                log.info(
                    "node=%s snapshot-denied by=%s reason=%s snapshot_id=%s",
                    self.node_id, target, resp.reason, req.snapshot_id,
                )
                return False
            idx = req.last_included_index
            if idx > self.match_index.get(target, 0):
                self.match_index[target] = idx
            # next_index 独立于 match_index 推进：回退可能已把它压得比
            # match_index 低（对端磁盘清空重启），快照成功后必须前进，
            # 否则会在快照边界上原地空转。
            if self.next_index.get(target, 0) < idx + 1:
                self.next_index[target] = idx + 1
            log.info(
                "node=%s SNAPSHOT-ACK from=%s snapshot_id=%s match_index=%d",
                self.node_id, target, req.snapshot_id, idx,
            )
            return True

    def _advance_commit_index_locked(self) -> bool:
        """多数派推进提交位点；只直接提交当前任期条目（Raft 安全约束）。"""
        if self._role != Role.LEADER:
            return False
        term = self.storage.current_term
        needed = len(self.members) // 2 + 1
        for n in range(self.storage.last_index(), self.commit_index, -1):
            if self.storage.term_at(n) != term:
                continue
            tally = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if tally >= needed:
                old = self.commit_index
                self.storage.save_commit_index(n)
                self.commit_index = n
                log.info(
                    "node=%s COMMIT decision-index %d->%d term=%d tally=%d/%d source=quorum",
                    self.node_id, old, n, term, tally, len(self.members),
                )
                return True
        return False

    def _apply_committed(self) -> None:
        try:
            self._apply_committed_loop()
        finally:
            self._maybe_snapshot()

    def _apply_committed_loop(self) -> None:
        while True:
            with self._lock:
                if self.last_applied >= self.commit_index:
                    return
                idx = self.last_applied + 1
                entry = self.storage.get_entry(idx)
            if entry is None:
                raise InvalidCommitError(f"commit index {idx} has no log entry")
            with self._apply_lock:
                # 状态机位点是唯一事实来源；已应用则跳过（幂等保护）
                if self.sm.last_applied < idx:
                    applied_result = self.sm.apply(idx, entry.term, entry.command)
                    self._results[idx] = applied_result
                    log.info(
                        "node=%s APPLY index=%d term=%d source=%s request_id=%s",
                        self.node_id, idx, entry.term, entry.client_id, entry.request_id,
                    )
            with self._lock:
                # 与状态机对齐（可能其他线程已顺带应用）
                if self.sm.last_applied > self.last_applied:
                    self.last_applied = self.sm.last_applied
                for done in range(idx, self.last_applied + 1):
                    ev = self._pending.pop(done, None)
                    if ev is not None:
                        ev.set()
                if self.last_applied < idx:
                    return  # 已被其他线程处理，避免 busy loop

    # ------------------------------------------------------------------ #
    # 日志压缩（快照）
    # ------------------------------------------------------------------ #
    def _maybe_snapshot(self) -> None:
        """已应用历史积累超过阈值时，压缩成快照并丢弃对应旧日志。

        只压缩“已提交且已应用”的边界，因此已生效结果不会丢、重或回退；
        压缩在应用循环之外、持锁瞬间完成，不阻塞新写入的提交。
        """
        with self._lock:
            idx = self.last_applied
            base = self.storage.snapshot_index()
            if (
                self._snapshot_threshold <= 0
                or idx == 0
                or idx - base < self._snapshot_threshold
            ):
                return
            term = self.storage.term_at(idx)
            snapshot_id = f"snap-{self.node_id}-{idx}-{uuid.uuid4().hex[:8]}"
            payload = {
                "state": self.sm.snapshot(),
                # 幂等元数据随快照一起保存：重启后已提交结论仍可去重
                "dedup": [
                    {
                        "client_id": c, "request_id": r, "index": i,
                        "command": cmd, "client_seq": seq, "term": t,
                    }
                    for (c, r), (i, cmd, seq, t) in self._dedup.items()
                    if i <= idx
                ],
                "client_max_seq": dict(self._client_max_seq),
                "results": {str(k): v for k, v in self._results.items() if k <= idx},
            }
            meta = {
                "snapshot_id": snapshot_id,
                "last_included_index": idx,
                "last_included_term": term,
            }
            self.storage.save_snapshot(meta, payload)
            self.storage.compact_upto(idx, term=term)
            log.info(
                "node=%s SNAPSHOT-COMPACT boundary=(%d,%d) snapshot_id=%s "
                "threshold=%d log_start=%d",
                self.node_id, idx, term, snapshot_id,
                self._snapshot_threshold, self.storage.snapshot_index() + 1,
            )


    # ------------------------------------------------------------------ #
    # 客户端接口
    # ------------------------------------------------------------------ #
    def submit_command(
        self,
        client_id: str,
        request_id: str,
        client_seq: int,
        command: dict,
        *,
        timeout: float | None = None,
    ) -> ClientResponse:
        timeout = self._apply_timeout if timeout is None else timeout
        key = (client_id, request_id)
        with self._lock:
            if self._role != Role.LEADER:
                return ClientResponse(
                    success=False, term=self.storage.current_term,
                    code="not_leader",
                    reason=f"node {self.node_id} is {self._role.value}, not leader",
                    leader_id=self._leader_id,
                )

            term = self.storage.current_term

            # 幂等：相同 (client_id, request_id) 必须返回首次结果，绝不重复执行
            prior = self._dedup.get(key)
            if prior is not None:
                idx, prior_cmd, prior_seq, prior_term = prior
                if prior_cmd != command:
                    raise DuplicateRequestError(
                        f"request_id={request_id!r} reused by client {client_id!r} "
                        f"with a different command",
                        request_id=request_id, term=term,
                    )
                log.info(
                    "node=%s IDEMPOTENT-HIT index=%d source=%s request_id=%s",
                    self.node_id, idx, client_id, request_id,
                )
                if idx <= self.commit_index:
                    # 条目可能已被压缩进快照：任期取自去重记录而非日志
                    return ClientResponse(
                        success=True, index=idx, term=prior_term,
                        result=self._results.get(idx), leader_id=self.node_id,
                    )
                ev = self._pending.setdefault(idx, threading.Event())
            else:
                # 顺序边界：同一客户端的序列号必须严格递增，过期/乱序请求拒绝
                max_seq = self._client_max_seq.get(client_id, 0)
                if client_seq <= max_seq:
                    raise StaleClientRequestError(
                        f"stale client sequence: client={client_id!r} got seq={client_seq}, "
                        f"max seen={max_seq}",
                        client_id=client_id, got_seq=client_seq, max_seq=max_seq, term=term,
                    )
                idx = self.storage.last_index() + 1
                entry = LogEntry(
                    index=idx, term=term, command=command,
                    client_id=client_id, request_id=request_id, client_seq=client_seq,
                )
                self.storage.append_entries([entry])
                self._dedup[key] = (idx, command, client_seq, term)
                self._client_max_seq[client_id] = client_seq
                ev = threading.Event()
                self._pending[idx] = ev
                log.info(
                    "node=%s PROPOSE index=%d term=%d source=%s request_id=%s seq=%d",
                    self.node_id, idx, term, client_id, request_id, client_seq,
                )

            self._spawn_send_for_new_proposal_locked()

        committed = ev.wait(timeout)
        if not committed:
            return ClientResponse(
                success=False, index=idx, term=term, code="not_replicated_timeout",
                reason=(
                    f"entry {idx} not committed by quorum within {timeout}s; "
                    "it may still commit later — retry with the same request_id"
                ),
                leader_id=self.node_id,
            )
        with self._lock:
            # 等待期间条目可能已被压缩：任期从去重记录取，不读日志
            rec = self._dedup.get(key)
            resp_term = rec[3] if rec is not None else term
            return ClientResponse(
                success=True, index=idx,
                term=resp_term, result=self._results.get(idx),
                leader_id=self.node_id,
            )

    def _spawn_send_for_new_proposal_locked(self) -> None:
        # 主节点自己的日志已更新，立即驱动一轮复制
        self._heartbeat_deadline = time.monotonic() + self._heartbeat_interval
        for p in self.peers:
            self._spawn_send(p)

    def read_state(self, key: str) -> ClientResponse:
        """线性一致读（简化版 ReadIndex）：仅主节点可服务。"""
        with self._lock:
            if self._role != Role.LEADER:
                return ClientResponse(
                    success=False, term=self.storage.current_term, code="not_leader",
                    reason=f"node {self.node_id} is {self._role.value}, not leader",
                    leader_id=self._leader_id,
                )
            # 主节点身份由持续心跳维持；读到的是已应用状态
            return ClientResponse(
                success=True, term=self.storage.current_term,
                result={"key": key, "value": self.sm.get(key)}, leader_id=self.node_id,
            )

    def status(self) -> NodeStatus:
        with self._lock:
            return NodeStatus(
                node_id=self.node_id,
                role=self._role,
                term=self.storage.current_term,
                voted_for=self.storage.voted_for,
                leader_id=self._leader_id,
                commit_index=self.commit_index,
                last_applied=self.last_applied,
                last_log_index=self.storage.last_index(),
                last_log_term=self.storage.last_term(),
                snapshot_index=self.storage.snapshot_index(),
                snapshot_term=self.storage.snapshot_term(),
            )

    def is_leader(self) -> bool:
        with self._lock:
            return self._running and self._role == Role.LEADER

    # ------------------------------------------------------------------ #
    # 内部协调操作：身份 + 令牌 + 任期三重校验
    # ------------------------------------------------------------------ #
    def internal_step_down(self, caller_id: str, term: int, auth_token: str) -> dict:
        if auth_token != self._admin_token:
            raise UnauthorizedError(
                f"internal step-down denied: bad auth token from {caller_id!r}",
                term=self.storage.current_term,
            )
        if caller_id not in self.members:
            raise UnauthorizedError(
                f"internal step-down denied: caller {caller_id!r} is not a cluster member",
                term=self.storage.current_term,
            )
        with self._lock:
            current = self.storage.current_term
            if term < current:
                raise StaleTermError(
                    f"internal step-down denied: stale term {term} < {current} "
                    f"from {caller_id!r}",
                    term=current,
                )
            if term > current:
                self._become_follower_locked(term)
            elif self._role == Role.LEADER:
                self._role = Role.FOLLOWER
                self.next_index = {}
                self.match_index = {}
                log.info(
                    "node=%s TERM-CHANGE role=%s term=%d reason=admin-step-down source=%s",
                    self.node_id, Role.FOLLOWER.value, term, caller_id,
                )
            # 主动退位：清空主节点引用，等待选举超时重新参与
            self._leader_id = None
            self._sending.clear()
            return {
                "node_id": self.node_id,
                "role": self._role.value,
                "term": self.storage.current_term,
                "accepted": True,
            }
