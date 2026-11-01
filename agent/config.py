import json
from dataclasses import dataclass, field


@dataclass
class NodeConfig:
    id: str
    http: str  # peer agent base URL, e.g. "1.2.3.4:9100"


@dataclass
class AgentConfig:
    self_name: str           # node id, e.g. "A"
    meta_port: int           # this agent's HTTP server port (from config meta field)
    sglang_port: int         # internal SGLang HTTP port (from config sglang field)
    page_size: int           # must match proxy hasher and SGLang --page-size
    pull_timeout_sec: int    # timeout for a NIXL pull
    request_timeout_sec: int # overall request timeout
    peers: list[NodeConfig] = field(default_factory=list)


def load_config(path: str, self_name: str) -> AgentConfig:
    with open(path) as f:
        raw = json.load(f)

    model_name = raw.get("model")
    if not model_name:
        raise ValueError("config: missing 'model' field")

    models = raw.get("models", {})
    if model_name not in models:
        raise ValueError(f"config: model {model_name!r} not defined in models")

    page_size = models[model_name].get("page_size", 0)
    if page_size <= 0:
        raise ValueError(f"config: model {model_name!r}: page_size must be > 0")

    nodes = raw.get("nodes", [])
    if not nodes:
        raise ValueError("config: must list at least one node")

    self_node = next((n for n in nodes if n["id"] == self_name), None)
    if self_node is None:
        raise ValueError(f"config: self_name {self_name!r} not found in nodes")
    if not self_node.get("ip"):
        raise ValueError(f"config: node {self_name!r}: ip is required")

    meta_port = int(self_node["meta"].lstrip(":"))
    sglang_port = int(self_node["sglang"].lstrip(":"))

    peers = []
    for n in nodes:
        if n["id"] == self_name:
            continue
        if not n.get("ip"):
            raise ValueError(f"config: node {n['id']!r}: ip is required")
        peers.append(NodeConfig(id=n["id"], http=n["ip"] + ":" + n["http"].lstrip(":")))

    pull_timeout_sec = raw.get("pull_timeout_sec", 30)
    if pull_timeout_sec <= 0:
        pull_timeout_sec = 30

    request_timeout_sec = raw.get("request_timeout_sec", 30)
    if request_timeout_sec <= 0:
        request_timeout_sec = 30

    return AgentConfig(
        self_name=self_name,
        meta_port=meta_port,
        sglang_port=sglang_port,
        page_size=page_size,
        pull_timeout_sec=pull_timeout_sec,
        request_timeout_sec=request_timeout_sec,
        peers=peers,
    )
