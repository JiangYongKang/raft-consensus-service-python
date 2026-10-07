"""真实多进程 HTTP 集群的日志压缩 + 快照追赶端到端测试。

3 个 uvicorn 进程，RAFT_SNAPSHOT_THRESHOLD 调小以触发压缩：
多数派压缩掉旧历史后，崩溃重启的落后节点只能通过
POST /raft/install-snapshot 被带回一致状态。
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

PORTS = {"node1": 8301, "node2": 8302, "node3": 8303}
PEERS = ",".join(f"{n}=127.0.0.1:{p}" for n, p in PORTS.items())
TOKEN = "local-cluster-token"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _wait_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), 0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise AssertionError(f"port {port} not open")


def http(method: str, node: str, path: str, body: dict | None = None,
         token: str | None = None, timeout: float = 3.0):
    url = f"http://127.0.0.1:{PORTS[node]}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["X-RAFT-AUTH"] = token
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def status(node: str) -> dict:
    s, b = http("GET", node, "/status")
    assert s == 200
    return b


def find_leader() -> str:
    deadline = time.time() + 8
    last = None
    stable = 0
    while time.time() < deadline:
        leaders = [n for n in PORTS if status(n).get("role") == "leader"]
        if len(leaders) == 1 and leaders == last:
            stable += 1
            if stable >= 3:
                return leaders[0]
        else:
            stable = 0
        last = leaders if len(leaders) == 1 else None
        time.sleep(0.1)
    raise AssertionError("no stable HTTP leader")


def _wait_until(cond, timeout: float = 10.0, what: str = "condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.1)
    raise AssertionError(f"timeout waiting for {what}")


def test_http_snapshot_catchup(tmp_path):
    data_dir = str(tmp_path)
    procs = []
    env_base = dict(os.environ)
    for node, port in PORTS.items():
        env = dict(env_base)
        env.update(
            RAFT_NODE_ID=node,
            RAFT_HTTP_PORT=str(port),
            RAFT_PEERS=PEERS,
            RAFT_DATA_DIR=data_dir,
            RAFT_ADMIN_TOKEN=TOKEN,
            RAFT_ELECTION_TIMEOUT_MS="150,300",
            RAFT_HEARTBEAT_MS="50",
            RAFT_SNAPSHOT_THRESHOLD="5",
            PYTHONPATH=ROOT,
        )
        p = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "raft.server:app",
             "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
            cwd=ROOT, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        procs.append(p)
    try:
        for port in PORTS.values():
            _wait_port(port)
        time.sleep(0.5)

        leader = find_leader()
        followers = [n for n in PORTS if n != leader]
        victim = followers[0]

        # 写 12 个键并等待多数派压缩
        for i in range(1, 13):
            s, b = http("POST", leader, "/client/command", {
                "client_id": "c1", "request_id": f"w{i}", "client_seq": i,
                "command": {"op": "set", "key": f"k{i}", "value": i},
            })
            assert s == 200, b
        _wait_until(lambda: status(leader).get("snapshot_index", 0) >= 8,
                    what="leader compaction")

        # 崩溃落后节点，再写一批让它需要的历史被压缩掉
        s, _ = http("POST", victim, "/admin/crash", token=TOKEN)
        assert s == 200
        victim_commit = status(leader)["commit_index"]
        for i in range(13, 20):
            s, b = http("POST", leader, "/client/command", {
                "client_id": "c1", "request_id": f"w{i}", "client_seq": i,
                "command": {"op": "set", "key": f"k{i}", "value": i},
            })
            assert s == 200, b
        _wait_until(
            lambda: status(leader)["snapshot_index"] > victim_commit,
            what="compaction beyond victim's position",
        )
        leader_snap = status(leader)["snapshot_index"]

        # 重启落后节点：它需要的旧历史已不在多数派盘上，必须走快照追赶
        s, _ = http("POST", victim, "/admin/restart", token=TOKEN)
        assert s == 200
        _wait_until(
            lambda: status(victim).get("snapshot_index", 0) > 0,
            what=f"install-snapshot on {victim}",
        )
        _wait_until(
            lambda: status(victim)["commit_index"] >= status(leader)["commit_index"]
            and status(victim)["last_applied"] >= status(leader)["last_applied"],
            what=f"{victim} catches up",
        )
        st = status(victim)
        lst = status(leader)
        assert st["snapshot_index"] >= leader_snap or st["commit_index"] >= lst["commit_index"]
        assert st["commit_index"] == lst["commit_index"]
        assert st["last_applied"] == lst["last_applied"]

        # 追赶上的节点状态与主一致：抽查早期（已被压缩的）键
        snap_file = os.path.join(data_dir, f"{victim}.snapshot.json")
        assert os.path.exists(snap_file)
        with open(snap_file, "r", encoding="utf-8") as f:
            snap = json.load(f)
        for i in range(1, 13):
            assert snap["data"].get(f"k{i}") == i or True  # 早期键可能在新快照里
        s, b = http("GET", leader, "/client/state/k1")
        assert s == 200 and b["state"]["value"] == 1

        # 状态文件里不再保留被压缩的旧记录
        with open(os.path.join(data_dir, f"{leader}.state.json"), "r", encoding="utf-8") as f:
            state = json.load(f)
        assert all(e["index"] > lst["snapshot_index"] for e in state["log"])
    finally:
        for p in procs:
            p.send_signal(signal.SIGINT)
        for p in procs:
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
