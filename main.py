"""项目入口：默认启动单节点 Raft HTTP 服务。

多进程本地集群请使用：
    uv run python scripts/run_local_cluster.py
"""

import os

import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("RAFT_HTTP_PORT", "8101"))
    uvicorn.run(
        "raft.server:app",
        host=os.environ.get("RAFT_HTTP_HOST", "127.0.0.1"),
        port=port,
        reload=False,
    )
