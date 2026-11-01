import json
import os
import sys
from dataclasses import dataclass, field


@dataclass
class NodeConfig:
    id: str
    http: str  # agent base URL


@dataclass
class AgentConfig:
    self_name: str        # pass during launch (A, B and such)
    meta_port: int        # port this agent's HTTP server listens on
    sglang_port: int      # internal SGLang HTTP port
    page_size: int        # must match proxy hasher and SGLang --page-size
    pull_timeout_sec: int
    peers: list[NodeConfig] = field(default_factory=list)  # all other nodes


def load_config(path: str, self_name: str, meta_port: int, sglang_port: int) -> AgentConfig:
    with open(path) as f:
        raw = json.load(f)

    model_name = raw.get("model")
    if not model_name:
        raise ValueError("config: missing 'model' field")

    models = raw.get("models", {})
    if model_name not in models:
        raise ValueError(f"config: model {model_name!r} not defined in models")

    model = models[model_name]
    page_size = model.get("page_size", 0)
    if page_size <= 0:
        raise ValueError(f"config: model {model_name!r}: page_size must be > 0")

    nodes = raw.get("nodes", [])
    if not nodes:
        raise ValueError("config: must list at least one node")

    ids = [n["id"] for n in nodes]
    if self_name not in ids:
        raise ValueError(f"config: self_name {self_name!r} not found in nodes")

    peers = [NodeConfig(id=n["id"], http=n["http"]) for n in nodes if n["id"] != self_name]

    pull_timeout_sec = raw.get("request_timeout_sec", 30)
    if pull_timeout_sec <= 0:
        pull_timeout_sec = 30

    return AgentConfig(
        self_name=self_name,
        meta_port=meta_port,
        sglang_port=sglang_port,
        page_size=page_size,
        pull_timeout_sec=pull_timeout_sec,
        peers=peers,
    )
