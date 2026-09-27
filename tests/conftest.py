"""测试公共夹具：临时数据目录 + 可重复的快速时钟集群。"""

from __future__ import annotations

import logging
import pathlib

import pytest

from raft_consensus.cluster import LocalCluster


@pytest.fixture()
def tmp_data(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path / "cluster"


@pytest.fixture()
def cluster3(tmp_data):
    c = LocalCluster(3, data_root=tmp_data, clean=True, seed=7)
    c.start()
    yield c
    c.stop()
