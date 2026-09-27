"""服务入口。

- 默认：启动 FastAPI（健康检查 + 集群信息）。
- `python main.py --demo`：运行本地多实例一致性演示（选举/崩溃/分区/幂等/并发）。
"""

from __future__ import annotations

import argparse

import uvicorn
from fastapi import FastAPI

app = FastAPI(title="local-raft-consensus")


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "local-raft-consensus", "mode": "in-process simulation"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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
