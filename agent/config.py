import json
from dataclasses import dataclass, field


@dataclass
class NodeConfig:
    id: str
    http: str  # peer agent base URL, e.g. "1.2.3.4:9100"


@dataclass
class AgentConfig:
    self_name: str           # node id, e.g. "A"
    http_port: int           # port this agent's HTTP server listens on (/generate, /agent, /keys)
    sglang_port: int         # SGLang internal port, always on localhost
    page_size: int           # must match proxy hasher and SGLang --page-size
    model_path: str          # HuggingFace model id or local path, passed to SGLang --model
    cpu_offload_gb: int      # SGLang --cpu-offload-gb
    mem_fraction_static: float  # SGLang --mem-fraction-static
    backend_module_path: str # path to the HiCacheStorage backend module
    meta_dir: str            # directory for NIXL metadata exchange files
    kv_events_port: str      # ZMQ KV events port, e.g. "5557"
    load_port: str           # ZMQ load publisher port, e.g. "6000"
    node_ip: str             # this node's IP, used to build ZMQ bind addresses
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

    http_port = int(self_node["http"])
    sglang_port = int(self_node["sglang"])

    peers = []
    for n in nodes:
        if n["id"] == self_name:
            continue
        if not n.get("ip"):
            raise ValueError(f"config: node {n['id']!r}: ip is required")
        peers.append(NodeConfig(id=n["id"], http=n["ip"] + ":" + n["http"]))

    model_path = raw.get("model_path", "")
    if not model_path:
        raise ValueError("config: missing 'model_path' field")

    cpu_offload_gb = int(raw.get("cpu_offload_gb", 0))
    mem_fraction_static = float(raw.get("mem_fraction_static", 0.85))
    backend_module_path = raw.get("backend_module_path", "")
    meta_dir = raw.get("meta_dir", "")

    kv_events_port = self_node.get("kv_events", "")
    load_port = self_node.get("load", "")
    node_ip = self_node["ip"]

    pull_timeout_sec = raw.get("pull_timeout_sec", 30)
    if pull_timeout_sec <= 0:
        pull_timeout_sec = 30

    request_timeout_sec = raw.get("request_timeout_sec", 30)
    if request_timeout_sec <= 0:
        request_timeout_sec = 30

    return AgentConfig(
        self_name=self_name,
        http_port=http_port,
        sglang_port=sglang_port,
        page_size=page_size,
        model_path=model_path,
        cpu_offload_gb=cpu_offload_gb,
        mem_fraction_static=mem_fraction_static,
        backend_module_path=backend_module_path,
        meta_dir=meta_dir,
        kv_events_port=kv_events_port,
        load_port=load_port,
        node_ip=node_ip,
        pull_timeout_sec=pull_timeout_sec,
        request_timeout_sec=request_timeout_sec,
        peers=peers,
    )
