"""本地集群门面（骨架）：组装节点/存储/状态机，提供客户端 API。"""

from __future__ import annotations

from pathlib import Path

from raft_consensus.simulation import Simulator


class LocalCluster:
    """一个进程内的多实例集群，供测试与本地演示使用，不依赖任何外部服务。"""

    def __init__(self, size: int = 5, *, data_root: str | Path = "./.cluster-data",
                 realtime: bool = False, seed: int = 1,
                 base_port: int = 0) -> None:
        raise NotImplementedError

    def start(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError

    def leader(self) -> str | None:
        raise NotImplementedError

    def write(self, client_id: str, request_id: str, key: str, value: object) -> dict:
        raise NotImplementedError

    def read(self, key: str) -> object:
        raise NotImplementedError

    def crash(self, node_id: str) -> None:
        raise NotImplementedError

    def restart(self, node_id: str) -> None:
        raise NotImplementedError

    def partition(self, groups: list[set[str]] | None) -> None:
        raise NotImplementedError

    def state_on_each(self) -> dict[str, dict]:
        """每个节点状态机快照 + 任期/角色/位点，用于断言最终一致。"""
        raise NotImplementedError
