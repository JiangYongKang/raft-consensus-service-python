"""可解释性测试：日志必须打印任期变化、决策位点、来源节点与请求标识。"""

from __future__ import annotations

import logging

import pytest


def test_logs_contain_term_decision_source_request(caplog, cluster3):
    leader = cluster3.wait_leader()
    caplog.set_level(logging.INFO, logger="raft")
    cluster3.write("client-Z", "req-observable-1", "k", "v")
    cluster3.run_until(
        lambda: all(n.commit_index >= 2 for n in cluster3.sim.nodes.values()),
        timeout=3)

    text = "\n".join(r.getMessage() for r in caplog.records)

    # 决策位点（decision_index=...）
    assert "decision_index=2" in text
    # 请求标识与来源客户端
    assert "req-observable-1" in text
    assert "client-Z" in text
    # 至少出现一次任期切换/选举日志
    assert ("term=1" in text or "become" in text or "ELECTED" in text)


def test_stale_rejection_reason_is_logged(caplog, cluster3):
    cluster3.wait_leader()
    follower = cluster3.ids[0]
    node = cluster3.node(follower)
    caplog.set_level(logging.WARNING, logger=f"raft.node.{follower}")
    from raft_consensus.messages import AppendEntries, Envelope
    stale = AppendEntries(
        term=node.current_term - 1, leader_id="n2",
        prev_log_index=0, prev_log_term=0, entries=(),
        leader_commit=0, request_id="explain-me")
    # 走完整派发路径，拒绝原因应进入日志
    node.dispatch(Envelope(src="n2", dst=follower, payload=stale,
                           msg_id="explain-me"))
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "stale term" in text
    assert "explain-me" in text
