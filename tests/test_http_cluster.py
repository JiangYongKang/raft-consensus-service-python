"""真实多进程 HTTP 集群端到端测试。

启动 3 个 uvicorn 进程，各节点使用独立 FileStorage；
覆盖：选主、写入复制、幂等重试、过期序列号、旧任期/越权拒绝、
节点崩溃重启、分区恢复。仅依赖本地环境。
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

import pytest

PORTS = {"node1": 8201, "node2": 8202, "node3": 8203}
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


def find_leader(*, expect_terms: set[int] | None = None, stable_rounds: int = 3) -> str:
    """等待视图收敛到唯一的最高任期主。

    分区开始瞬间，旧主可能尚未感知到隔离，仍以旧任期自称主；新多数派会在
    更高任期选出新主。这是合法的“跨任期权威并存”瞬态（旧主无法提交任何写）。
    因此这里只在**同一任期**稳定出现多个主时判失败，否则等待旧主退位、
    视图连续若干轮只剩唯一最高任期主。
    """
    last_terms: dict[str, int] = {}
    stable = 0
    deadline = time.time() + 8
    while time.time() < deadline:
        states = {n: status(n) for n in PORTS}
        leaders = {n: s["term"] for n, s in states.items() if s.get("role") == "leader"}
        if expect_terms is not None:
            leaders = {n: t for n, t in leaders.items() if t in expect_terms}
        # 同任期多主：硬性安全违例
        by_term: dict[int, list[str]] = {}
        for n, t in leaders.items():
            by_term.setdefault(t, []).append(n)
        for t, ns in by_term.items():
            if len(ns) > 1:
                import json as _j
                raise AssertionError(f"same-term leaders term={t}: {_j.dumps(states)}")
        if len(leaders) == 1:
            (n, t) = next(iter(leaders.items()))
            if last_terms == leaders:
                stable += 1
            else:
                stable = 0
            last_terms = dict(leaders)
            if stable >= stable_rounds:
                return n
        else:
            stable = 0
            last_terms = {}
        time.sleep(0.1)
    raise AssertionError("no stable unique HTTP leader")


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    data_dir = str(tmp_path_factory.mktemp("raft-http"))
    procs = []
    for node, port in PORTS.items():
        env = dict(os.environ)
        env.update(
            RAFT_NODE_ID=node,
            RAFT_HTTP_PORT=str(port),
            RAFT_PEERS=PEERS,
            RAFT_DATA_DIR=data_dir,
            RAFT_ADMIN_TOKEN=TOKEN,
            RAFT_ELECTION_TIMEOUT_MS="150,300",
            RAFT_HEARTBEAT_MS="50",
            PYTHONPATH=ROOT,
        )
        p = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "raft.server:app",
             "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
            cwd=ROOT, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        procs.append(p)
    for port in PORTS.values():
        _wait_port(port)
    time.sleep(0.5)
    try:
        yield {"data_dir": data_dir}
    finally:
        for p in procs:
            p.send_signal(signal.SIGINT)
        for p in procs:
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()


def test_http_election_and_write(cluster):
    leader = find_leader()
    term0 = status(leader)["term"]
    assert term0 >= 1
    # follower 重定向
    follower = next(n for n in PORTS if n != leader)
    code, body = http("POST", follower, "/client/command", {
        "client_id": "hc", "request_id": "r0", "client_seq": 1,
        "command": {"op": "set", "key": "q", "value": 1},
    })
    assert code == 503 and body["error"]["code"] == "not_leader"
    assert body.get("leader_id") == leader

    code, body = http("POST", leader, "/client/command", {
        "client_id": "hc", "request_id": "r1", "client_seq": 1,
        "command": {"op": "set", "key": "k", "value": "hello"},
    })
    assert code == 200 and body["success"]
    idx = body["index"]
    # 所有节点复制一致
    deadline = time.time() + 3
    while time.time() < deadline:
        if all(status(n)["commit_index"] >= idx for n in PORTS):
            break
        time.sleep(0.1)
    for n in PORTS:
        code, body = http("GET", n, f"/client/state/k")
        # 非主读返回 not_leader；主节点读到一致值
        if code == 200:
            assert body["state"]["value"] == "hello"

    # 幂等重试
    code2, body2 = http("POST", leader, "/client/command", {
        "client_id": "hc", "request_id": "r1", "client_seq": 1,
        "command": {"op": "set", "key": "k", "value": "hello"},
    })
    assert code2 == 200 and body2["index"] == idx

    # request_id 复用但命令不同
    code3, body3 = http("POST", leader, "/client/command", {
        "client_id": "hc", "request_id": "r1", "client_seq": 2,
        "command": {"op": "set", "key": "k", "value": "different"},
    })
    assert code3 == 409 and body3["error"]["code"] == "duplicate_request"

    # 旧序列号
    code4, body4 = http("POST", leader, "/client/command", {
        "client_id": "hc", "request_id": "r-old", "client_seq": 0,
        "command": {"op": "set", "key": "k", "value": 2},
    })
    assert code4 == 409 and body4["error"]["code"] == "stale_client_seq"


def test_http_admin_auth(cluster):
    leader = find_leader()
    term = status(leader)["term"]
    # 缺令牌
    code, body = http("POST", leader, "/admin/step-down",
                      {"caller_id": "node2", "term": term})
    assert code == 401 and body["error"]["code"] == "unauthorized"
    # 旧任期
    code, body = http("POST", leader, "/admin/step-down",
                      {"caller_id": "node2", "term": max(0, term - 1)}, token=TOKEN)
    assert code == 409 and body["error"]["code"] == "stale_term"
    # 未知调用方
    code, body = http("POST", leader, "/admin/step-down",
                      {"caller_id": "mallory", "term": term}, token=TOKEN)
    assert code == 403 and body["error"]["code"] == "unauthorized"
    assert status(leader)["role"] == "leader"


def test_http_node_crash_restart_and_partition(cluster):
    leader = find_leader()
    follower = next(n for n in PORTS if n != leader)
    code, body = http("POST", leader, "/client/command", {
        "client_id": "hc2", "request_id": "w1", "client_seq": 1,
        "command": {"op": "set", "key": "color", "value": "green"},
    })
    assert code == 200
    idx = body["index"]

    # 崩溃 follower -> 503
    assert http("POST", follower, "/admin/crash", {}, token=TOKEN)[0] == 200
    assert status(follower).get("crashed") is True
    code, _ = http("POST", follower, "/client/command", {
        "client_id": "hc2", "request_id": "wx", "client_seq": 2,
        "command": {"op": "set", "key": "z", "value": 1},
    })
    assert code == 503

    # 主继续写入
    code, body = http("POST", leader, "/client/command", {
        "client_id": "hc2", "request_id": "w2", "client_seq": 2,
        "command": {"op": "set", "key": "color", "value": "blue"},
    })
    assert code == 200
    idx2 = body["index"]

    # 重启 follower，自动追赶
    assert http("POST", follower, "/admin/restart", {}, token=TOKEN)[0] == 200
    deadline = time.time() + 5
    while time.time() < deadline:
        if status(follower)["commit_index"] >= idx2:
            break
        time.sleep(0.1)
    assert status(follower)["commit_index"] >= idx2

    # 分区：隔离当前主（把它单独放一组）
    others = [n for n in PORTS if n != leader]
    assert http("POST", follower, "/admin/partition",
                {"groups": [[leader], others]}, token=TOKEN)[0] == 200
    time.sleep(1.0)
    # 旧主在少数派，写入无法多数派确认
    code, body = http("POST", leader, "/client/command", {
        "client_id": "hc2", "request_id": "w3", "client_seq": 3,
        "command": {"op": "set", "key": "color", "value": "red"},
    }, timeout=3.0)
    assert code == 503 and body["error"]["code"] == "not_replicated_timeout"

    # 多数派选出新主
    assert http("POST", others[0], "/admin/heal", token=TOKEN)[0] == 200
    new_leader = find_leader()
    time.sleep(0.5)
    # 未提交的 red 不生效
    code, body = http("GET", new_leader, "/client/state/color")
    assert code == 200 and body["state"]["value"] == "blue"
    # 所有节点日志与状态最终一致
    deadline = time.time() + 5
    while time.time() < deadline:
        ci = {n: status(n)["commit_index"] for n in PORTS}
        if len(set(ci.values())) == 1:
            break
        time.sleep(0.1)
    ci = {n: status(n)["commit_index"] for n in PORTS}
    assert len(set(ci.values())) == 1
