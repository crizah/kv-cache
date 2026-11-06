"""PeerNixlStorage – network version of the HiCache L3 backend.

KV bytes move only by NIXL READ from the peer's registered host memory.
Metadata (agent info and page key records) are exchanged via a small HTTP
server each node runs on meta_port.

Extra-config keys:
  backend_name / module_path / class_name   required by the `dynamic` loader
  interface_v1: 1                           routes to batch_get_v1 / batch_set_v1
  self_name       this node's name, e.g. "A"
  meta_port       port for this node's HTTP meta server
  peer_url        base URL of the peer's meta server, e.g. "http://10.0.0.2:9100"
  sglang_port     this node's SGLang HTTP port (used to scrape /metrics for /health)

HTTP endpoints on meta_port:
  GET /agent          → {"agent": name, "meta": hex, "base": int, "nbytes": int}
  GET /keys?since=N   → {"records": [...], "next": int}
  GET /health         → {"num_running_reqs": N, "num_waiting_reqs": N,
                          "num_tokens": N, "max_total_num_tokens": N}

Runs on: machine A and machine B (symmetric; both publish and pull).
"""

import json
import logging
import os
import re
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, List, Optional
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

import torch

from sglang.srt.mem_cache.hicache_storage import (
    HiCacheStorage,
    HiCacheStorageConfig,
    HiCacheStorageExtraInfo,
)

logger = logging.getLogger(__name__)
TAG = "[peer_nixl]"


class PeerNixlStorage(HiCacheStorage):
    def __init__(self, storage_config: HiCacheStorageConfig, kwargs=None):
        from nixl._api import nixl_agent, nixl_agent_config

        cfg = storage_config.extra_config or {}
        self.self_name: str = cfg["self_name"]
        self.meta_port: int = int(cfg["meta_port"])
        self.peer_url: str = cfg["peer_url"].rstrip("/")
        self.sglang_port: int = int(cfg.get("sglang_port", 30000))

        self.agent_name = f"{self.self_name}-{uuid.uuid4().hex[:8]}"
        self.agent = nixl_agent(self.agent_name, nixl_agent_config(backends=["UCX"]))
        self.mem_pool_host = None

        self._lock = threading.Lock()
        self._agent_info: Optional[dict] = None   # set after register_mem_pool_host
        self._local_keys: List[dict] = []         # {"k": key, "p": [k_ptr, v_ptr], "n": elem_bytes}

        # consumer state
        self._peer_agent = None
        self._peer_keys: dict = {}   # key -> (k_ptr, v_ptr, elem_bytes)
        self._peer_offset: int = 0   # next since= value to poll

        os.makedirs("logs", exist_ok=True)
        self._pull_log = open(f"logs/{self.self_name}_pull.jsonl", "a")

        self._start_http_server()
        logger.info(f"{TAG} agent {self.agent_name} created; meta server on :{self.meta_port}")

    # ── HTTP meta server ────────────────────────────────────────────────────

    def _start_http_server(self):
        backend = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                pass  # suppress access log noise

            def do_GET(self):
                parsed = urlparse(self.path)
                if parsed.path == "/agent":
                    self._serve_agent()
                elif parsed.path == "/keys":
                    qs = parse_qs(parsed.query)
                    since = int(qs.get("since", ["0"])[0])
                    self._serve_keys(since)
                elif parsed.path == "/health":
                    self._serve_health()
                else:
                    self.send_response(404)
                    self.end_headers()

            def _json(self, code, obj):
                body = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _serve_agent(self):
                with backend._lock:
                    info = backend._agent_info
                if info is None:
                    self._json(503, {"error": "not ready"})
                    return
                self._json(200, info)

            def _serve_keys(self, since: int):
                with backend._lock:
                    records = backend._local_keys[since:]
                    nxt = since + len(records)
                self._json(200, {"records": records, "next": nxt})

            def _serve_health(self):
                self._json(200, backend._scrape_health())

        srv = HTTPServer(("0.0.0.0", self.meta_port), Handler)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()

    def _scrape_health(self) -> dict:
        """Pull load figures from SGLang's /metrics Prometheus endpoint."""
        defaults = {"num_running_reqs": 0, "num_waiting_reqs": 0,
                    "num_tokens": 0, "max_total_num_tokens": 0}
        try:
            with urlopen(f"http://127.0.0.1:{self.sglang_port}/metrics", timeout=1) as r:
                text = r.read().decode()
        except Exception:
            return defaults

        def _g(pattern, default=0):
            m = re.search(pattern, text)
            return int(float(m.group(1))) if m else default

        return {
            "num_running_reqs":    _g(r"sglang:num_running_reqs\s+([\d.]+)"),
            "num_waiting_reqs":    _g(r"sglang:num_waiting_reqs\s+([\d.]+)"),
            "num_tokens":          _g(r"sglang:num_used_tokens\s+([\d.]+)"),
            "max_total_num_tokens": _g(r"sglang:max_total_num_tokens\s+([\d.]+)"),
        }

    # ── registration ────────────────────────────────────────────────────────

    def register_mem_pool_host(self, mem_pool_host):
        super().register_mem_pool_host(mem_pool_host)
        buf = mem_pool_host.kv_buffer
        base, nbytes = buf.data_ptr(), buf.numel() * buf.element_size()
        self._reg = self.agent.register_memory([(base, nbytes, 0, "")], "DRAM")
        meta = self.agent.get_agent_metadata()
        info = {"agent": self.agent_name, "meta": meta.hex(), "base": base, "nbytes": nbytes}
        with self._lock:
            self._agent_info = info
        logger.info(
            f"{TAG} registered host KV pool: base=0x{base:x} bytes={nbytes} "
            f"layout={mem_pool_host.layout} page_size={mem_pool_host.page_size}"
        )

    def register_mem_host_pool_v2(self, host_pool, host_pool_name):
        super().register_mem_host_pool_v2(host_pool, host_pool_name)

    # ── producer ─────────────────────────────────────────────────────────────

    def batch_set_v1(self, keys: List[str], host_indices: torch.Tensor, extra_info=None) -> List[bool]:
        ptrs, sizes = self.mem_pool_host.get_page_buffer_meta(host_indices)
        assert len(ptrs) == 2 * len(keys)
        records = []
        with self._lock:
            for i, k in enumerate(keys):
                rec = {"k": k, "p": [ptrs[2 * i], ptrs[2 * i + 1]], "n": sizes[2 * i]}
                self._local_keys.append(rec)
                records.append(rec)
        logger.info(f"{TAG} published {len(keys)} pages; total={len(self._local_keys)}")
        return [True] * len(keys)

    # ── consumer ─────────────────────────────────────────────────────────────

    def _fetch_peer_agent(self) -> bool:
        if self._peer_agent is not None:
            return True
        try:
            with urlopen(f"{self.peer_url}/agent", timeout=2) as r:
                d = json.loads(r.read())
        except Exception:
            return False
        self._peer_agent = self.agent.add_remote_agent(bytes.fromhex(d["meta"]))
        if isinstance(self._peer_agent, bytes):
            self._peer_agent = self._peer_agent.decode()
        self._peer_region = (d["base"], d["nbytes"])
        logger.info(f"{TAG} connected to peer agent {self._peer_agent}")
        return True

    def _poll_peer_keys(self):
        try:
            with urlopen(f"{self.peer_url}/keys?since={self._peer_offset}", timeout=2) as r:
                d = json.loads(r.read())
        except Exception:
            return
        for rec in d["records"]:
            self._peer_keys[rec["k"]] = (rec["p"][0], rec["p"][1], rec["n"])
        self._peer_offset = d["next"]

    def _refresh_peer(self) -> bool:
        if not self._fetch_peer_agent():
            return False
        self._poll_peer_keys()
        return True

    def exists(self, key: str) -> bool:
        if not self._refresh_peer():
            return False
        return key in self._peer_keys

    def batch_exists(self, keys: List[str], extra_info: Optional[HiCacheStorageExtraInfo] = None) -> int:
        if not self._refresh_peer():
            return 0
        n = sum(1 for k in keys if k in self._peer_keys)
        # HiCache requires a contiguous prefix match
        n = next((i for i, k in enumerate(keys) if k not in self._peer_keys), n)
        if n:
            logger.info(f"{TAG} batch_exists: {n}/{len(keys)} peer pages present")
        return n

    def batch_get_v1(self, keys: List[str], host_indices: torch.Tensor, extra_info=None) -> List[bool]:
        t_lookup0 = time.perf_counter()
        self._refresh_peer()
        lptrs, lsizes = self.mem_pool_host.get_page_buffer_meta(host_indices)
        loc, rem = [], []
        for i, k in enumerate(keys):
            rec = self._peer_keys.get(k)
            if rec is None:
                return [True] * i + [False] * (len(keys) - i)
            for j in range(2):
                loc.append((lptrs[2 * i + j], lsizes[2 * i + j], 0))
                rem.append((rec[j], rec[2], 0))
        lookup_ms = (time.perf_counter() - t_lookup0) * 1e3

        t_read0 = time.perf_counter()
        ld = self.agent.get_xfer_descs(loc, "DRAM")
        rd = self.agent.get_xfer_descs(rem, "DRAM")
        h = self.agent.initialize_xfer("READ", ld, rd, self._peer_agent)
        if not h:
            logger.error(f"{TAG} initialize_xfer failed")
            return [False] * len(keys)
        state = self.agent.transfer(h)
        while state not in ("DONE", "ERR"):
            state = self.agent.check_xfer_state(h)
        self.agent.release_xfer_handle(h)
        read_ms = (time.perf_counter() - t_read0) * 1e3

        if state == "ERR":
            logger.error(f"{TAG} NIXL READ failed")
            return [False] * len(keys)

        nbytes = sum(x[1] for x in loc)
        entry = {
            "ts": time.time(),
            "pages": len(keys),
            "bytes": nbytes,
            "lookup_ms": round(lookup_ms, 3),
            "read_ms": round(read_ms, 3),
        }
        self._pull_log.write(json.dumps(entry) + "\n")
        self._pull_log.flush()

        logger.info(
            f"{TAG} NIXL READ: pages={len(keys)} bytes={nbytes} "
            f"lookup={lookup_ms:.1f}ms read={read_ms:.1f}ms"
        )
        return [True] * len(keys)

    # ── legacy abstract API (unused when interface_v1=1) ─────────────────────

    def get(self, key, target_location=None, target_sizes=None):
        raise NotImplementedError("use interface_v1")

    def batch_get(self, keys, target_locations=None, target_sizes=None):
        raise NotImplementedError("use interface_v1")

    def set(self, key, value=None, target_location=None, target_sizes=None) -> bool:
        raise NotImplementedError("use interface_v1")

    def batch_set(self, keys, values=None, target_locations=None, target_sizes=None) -> bool:
        raise NotImplementedError("use interface_v1")
