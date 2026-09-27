"""权限边界测试：内部协调操作必须校验调用方身份与任期。"""

from __future__ import annotations

import pytest

from raft_consensus.errors import (
    AlreadyVotedError,
    NotLeaderError,
    StaleTermError,
    UnknownCallerError,
)
from raft_consensus.messages import (
    AppendEntries,
    Envelope,
    RequestVote,
)


def test_unknown_caller_rejected_via_dispatch(cluster3):
    """来源不是集群成员的内部 RPC 必须被拒绝（UnknownCallerError 回传）。"""
    cluster3.wait_leader()
    target = cluster3.ids[0]
    node = cluster3.node(target)
    rv = RequestVote(term=999, candidate_id="rogue-node",
                     last_log_index=0, last_log_term=0)
    env = Envelope(src="rogue-node", dst=target, payload=rv, msg_id="x1")
    # 不经过 network（模拟伪造来源直接投递）
    node.dispatch(env)
    # 响应消息通过 send 发出；这里直接验证：rogue 不会得到选票，
    # 且节点没有把票投出去
    assert node.voted_for != "rogue-node"


def test_caller_payload_mismatch_rejected(cluster3):
    """消息来源节点与载荷声明的 leader/candidate 不一致 -> 拒绝。"""
    cluster3.wait_leader()
    target = cluster3.ids[0]
    node = cluster3.node(target)
    # src=n1 但载荷里 leader_id 写成另一个节点
    ae = AppendEntries(term=node.current_term + 1, leader_id="n2",
                       prev_log_index=0, prev_log_term=0, entries=(),
                       leader_commit=0, request_id="forge1")
    with pytest.raises(UnknownCallerError):
        node.handle_append_entries(ae, caller="n1")


def test_stale_request_vote_rejected(cluster3):
    cluster3.wait_leader()
    target = cluster3.ids[0]
    node = cluster3.node(target)
    term = node.current_term
    rv = RequestVote(term=term - 1, candidate_id="n9",
                     last_log_index=100, last_log_term=100)
    with pytest.raises(StaleTermError) as ei:
        node.handle_request_vote(rv, caller="n9")
    assert ei.value.current_term == term
    assert node.voted_for != "n9"


def test_double_vote_same_term_rejected(cluster3):
    """同一任期已投给一个候选人，第二个候选人必须被可区分原因拒绝。"""
    target = cluster3.ids[0]
    node = cluster3.node(target)
    cluster3.wait_leader()
    term = node.current_term
    # 手工制造"已投票"状态
    node.voted_for = "n1"
    node.store.save_state(term, "n1")
    rv = RequestVote(term=term, candidate_id="n2",
                     last_log_index=node.log.last_index,
                     last_log_term=node.log.last_term)
    with pytest.raises(AlreadyVotedError) as ei:
        node.handle_request_vote(rv, caller="n2")
    assert ei.value.voted_for == "n1"


def test_write_to_follower_rejected_not_leader(cluster3):
    cluster3.wait_leader()
    follower = next(n for n in cluster3.ids
                    if cluster3.node(n).role != "leader")
    with pytest.raises(NotLeaderError) as ei:
        cluster3.write("c", "x", "k", 1, node_id=follower, timeout=2)
    # 错误里要能给出当前主提示，便于客户端重试，而不是静默失败
    assert ei.value.leader_id is not None


def test_empty_client_identity_rejected(cluster3):
    leader = cluster3.wait_leader()
    node = cluster3.node(leader)
    with pytest.raises(UnknownCallerError):
        node.submit_write("", "req", {"op": "noop"})


def test_writes_only_through_current_term(cluster3):
    """任期过期后，旧领导者上的在途写必须以 NotLeaderError 失败，可区分。"""
    leader1 = cluster3.wait_leader()
    node1 = cluster3.node(leader1)
    # 在事件锁内同步提交，确保提案在分区前就登记在旧主上
    with cluster3._sim_lock:
        prop = node1.submit_write(
            "c", "will-stall", {"op": "set", "key": "s", "value": 1})
    assert not prop.done
    followers = [n for n in cluster3.ids if n != leader1]
    cluster3.partition([{leader1}, set(followers)])
    cluster3.run_until(lambda: node1.role != "leader", timeout=3)
    # 在途提案被以可区分原因终结，而不是被静默提交
    assert prop.event.is_set()
    assert isinstance(prop.error, NotLeaderError)
    cluster3.partition(None)
