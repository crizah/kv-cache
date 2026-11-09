#!/usr/bin/env python3
"""Print shell export statements for SGLang launch vars.

Usage:
    eval "$(python3 export.py --self-name A --config-path /path/to/config.json)"
    and then call ./launch.sh
    ts just makes the json fields into env vars cos() for some reason we chose to make config a json file
    AND we also have env var for config path in proxy so idek why we didnt just make .env te config. WHATEVER
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agent.config import load_config


def q(s: str) -> str:
    return "'" + str(s).replace("'", "'\\''") + "'"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-name", required=True)
    parser.add_argument("--config-path", required=True)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config_path)
    cfg = load_config(config_path, args.self_name)

    with open(config_path) as f:
        raw = __import__("json").load(f)

    peer_name = next(n["id"] for n in raw["nodes"] if n["id"] != args.self_name)
    repo_root = os.path.dirname(config_path)

    # module_path must be a Python importable name (not a file path);
    # repo_root is added to PYTHONPATH so `agent.backend` resolves correctly
    module_path = cfg.backend_module_path.replace("/", ".").removesuffix(".py")

    extra_cfg = json.dumps({
        "backend_name": "agent_backend",
        "module_path":  module_path,
        "class_name":   "AgentBackend",
        "self_name":    cfg.self_name,
        "peer_name":    peer_name,
        "config_path":  config_path,
        "meta_dir":     cfg.meta_dir,
        "interface_v1": 1,
    })

    kv_events_cfg = json.dumps({
        "publisher": "zmq",
        "endpoint":  f"tcp://*:{cfg.kv_events_port}",
        "topic":     "kv-events",
    })

    exports = {
        "SGLANG_MODEL_PATH":     cfg.model_path,
        "SGLANG_PORT":           str(cfg.sglang_port),
        "SGLANG_PAGE_SIZE":      str(cfg.page_size),
        "SGLANG_KV_EVENTS_CFG":  kv_events_cfg,
        "SGLANG_LOAD_ENDPOINT":  f"tcp://*:{cfg.load_port}",
        "SGLANG_CPU_OFFLOAD_GB": str(cfg.cpu_offload_gb),
        "SGLANG_MEM_FRACTION":   str(cfg.mem_fraction_static),
        "SGLANG_EXTRA_CFG":      extra_cfg,
    }

    print(f"export PYTHONPATH={q(repo_root)}")
    for k, v in exports.items():
        print(f"export {k}={q(v)}")


if __name__ == "__main__":
    main()
