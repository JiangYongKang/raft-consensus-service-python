"""测试公共设施：配置 raft 日志，打印任期变化/决策位点/来源/请求标识。"""

from __future__ import annotations

import logging
import os
import sys

import pytest

# 让测试日志带上要求的全部信息
_FMT = (
    "%(asctime)s %(levelname)s [%(name)s] "
    "term/decision/source/request -> %(message)s"
)


def pytest_configure(config):
    level_name = os.environ.get("RAFT_TEST_LOG", "WARNING")
    logging.basicConfig(level=getattr(logging, level_name.upper(), logging.WARNING),
                        format=_FMT, stream=sys.stdout, force=True)
    logging.getLogger("raft").setLevel(getattr(logging, level_name.upper(), logging.WARNING))


@pytest.fixture
def cluster_factory():
    created = []

    def _make(size=3, **kwargs):
        from raft.cluster import InProcessCluster
        c = InProcessCluster(size, **kwargs)
        created.append(c)
        c.start()
        return c

    yield _make

    for c in created:
        c.stop()
