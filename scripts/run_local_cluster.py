"""本地多进程集群启动脚本：在同一台机器上启动 3 个 Raft 节点进程。

用法：
    uv run python scripts/run_local_cluster.py            # 前台运行，Ctrl+C 停止
    uv run python scripts/run_local_cluster.py --client   # 启动后自动跑一轮演示客户端

节点状态保存在 ./.raft-data/<node_id>.state.json（删除该目录即重置集群）。
也可以单独启动节点：
    RAFT_NODE_ID=node1 RAFT_HTTP_PORT=8101 \
    RAFT_PEERS=node1=127.0.0.1:8101,node2=127.0.0.1:8102,node3=127.0.0.1:8103 \
    uv run uvicorn raft.server:app --port 8101
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PEERS = "node1=127.0.0.1:8101,node2=127.0.0.1:8102,node3=127.0.0.1:8103"
PORTS = {"node1": 8101, "node2": 8102, "node3": 8103}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", action="store_true", help="启动后执行演示客户端")
    parser.add_argument("--keep-data", action="store_true", help="保留上次数据（默认清空）")
    args = parser.parse_args()

    data_dir = os.path.join(HERE, ".raft-data")
    if not args.keep_data:
        for f in os.listdir(data_dir) if os.path.isdir(data_dir) else []:
            os.unlink(os.path.join(data_dir, f))

    procs: list[subprocess.Popen] = []
    for node_id, port in PORTS.items():
        env = dict(os.environ)
        env.update(
            RAFT_NODE_ID=node_id,
            RAFT_HTTP_PORT=str(port),
            RAFT_PEERS=PEERS,
            RAFT_DATA_DIR=data_dir,
            RAFT_ADMIN_TOKEN="local-cluster-token",
        )
        p = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "raft.server:app",
             "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
            cwd=HERE, env=env,
        )
        procs.append(p)

    try:
        if args.client:
            time.sleep(1.5)
            _demo()
        else:
            print("3-node Raft cluster running:", flush=True)
            for nid, port in PORTS.items():
                print(f"  {nid}: http://127.0.0.1:{port}/status", flush=True)
            print("Ctrl+C to stop.", flush=True)
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs:
            p.send_signal(signal.SIGINT)
        for p in procs:
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()


def _get(path_node: str, path: str):
    with urllib.request.urlopen(f"http://127.0.0.1:{PORTS[path_node]}{path}", timeout=2) as r:
        import json
        return json.loads(r.read())


def _demo() -> None:
    import json
    for _ in range(30):
        leader = None
        for nid in PORTS:
            try:
                s = _get(nid, "/status")
            except Exception:
                continue
            if s.get("role") == "leader":
                leader = nid
                break
        if leader:
            break
        time.sleep(0.2)
    print(f"[demo] leader = {leader}", flush=True)
    body = json.dumps({
        "client_id": "demo-client",
        "request_id": "demo-1",
        "client_seq": 1,
        "command": {"op": "set", "key": "greeting", "value": "hello-raft"},
    }).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORTS[leader]}/client/command",
        data=body, headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=3) as r:
        print("[demo] write:", json.loads(r.read()), flush=True)
    # 线性一致读只由主节点服务
    print(f"[demo] read @{leader}:", _get(leader, "/client/state/greeting"), flush=True)
    # follower 拒绝读并指明主节点
    follower = next(n for n in PORTS if n != leader)
    code, body = _raw("GET", follower, "/client/state/greeting")
    print(f"[demo] read @{follower} rejected: HTTP {code} {body['error']['code']} "
          f"leader_hint={body.get('leader_id')}", flush=True)
    # 各节点持久化位点一致
    for nid in PORTS:
        st = _get(nid, "/status")
        print(f"[demo] status {nid}: role={st['role']} term={st['term']} "
              f"commit={st['commit_index']} applied={st['last_applied']}", flush=True)


def _raw(method: str, node: str, path: str):
    import json
    import urllib.error
    url = f"http://127.0.0.1:{PORTS[node]}{path}"
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


if __name__ == "__main__":
    main()
