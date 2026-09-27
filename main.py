"""服务入口。

- 默认：启动 FastAPI。提供健康检查与一个本地 3 实例集群的只读信息接口；
  集群为按需懒加载（在进程内模拟，不连接任何外部服务）。
- `python main.py --demo`：运行完整本地一致性演示
  （选举 / 崩溃重启 / 分区愈合 / 幂等重试 / 并发写入）。
"""

from __future__ import annotations

import argparse
import tempfile

import uvicorn
from fastapi import FastAPI

app = FastAPI(title="local-raft-consensus")

_cluster = None


def _get_cluster():
    """懒加载一个 3 节点本地集群（仅用于 HTTP 演示，不对外提供强一致存储）。"""
    global _cluster
    if _cluster is None:
        from raft_consensus.cluster import LocalCluster

        _cluster = LocalCluster(
            3, data_root=tempfile.mkdtemp(prefix="raft-http-"), clean=True)
        _cluster.start()
        _cluster.wait_leader(timeout=3)
    return _cluster


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "local-raft-consensus", "mode": "in-process simulation",
            "docs": "/docs"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/cluster/status")
def cluster_status() -> dict:
    """返回各节点角色/任期/提交位点/状态，用于人工核对一致性。"""
    c = _get_cluster()
    return {"leader": c.leader(), "terms": c.terms(),
            "nodes": c.state_on_each()}


@app.post("/cluster/demo-scenario")
def demo_scenario() -> dict:
    """运行一轮幂等写并返回各节点状态（演示多数派提交与收敛）。"""
    c = _get_cluster()
    import uuid

    rid = uuid.uuid4().hex[:12]
    r1 = c.write("http-client", rid, "greeting", "hello-raft")
    r2 = c.write("http-client", rid, "greeting", "hello-raft")  # 幂等重试
    c.run_until(
        lambda: len({n.commit_index for n in c.sim.nodes.values()}) == 1,
        timeout=3)
    return {"first": r1, "retry_same_index": r2["index"] == r1["index"],
            "nodes": c.state_on_each()}


def _run_demo() -> None:
    from raft_consensus.demo import main as demo_main

    demo_main()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true", help="运行本地一致性演示")
    args = parser.parse_args()
    if args.demo:
        _run_demo()
    else:
        uvicorn.run(app, host="127.0.0.1", port=8000)
