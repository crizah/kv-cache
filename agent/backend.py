import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

import requests

from sglang.srt.mem_cache.hicache_storage import (
    HiCacheStorage,
    HiCacheStorageConfig,
    HiCacheStorageExtraInfo,
)

from agent.config import load_config
from agent.handlers.generate import handle_generate
from agent.handlers.meta import handle_agent, handle_keys
from agent.services.nixl import NixlService

logger = logging.getLogger(__name__)
TAG = "[backend]"

CONFIG_PATH = os.environ.get("CONFIG_PATH", "../config.json")


class AgentBackend(HiCacheStorage):
    def __init__(self, storage_config: HiCacheStorageConfig, kwargs=None):
        extra = storage_config.extra_config or {}
        self_name = extra["self_name"]
        config_path = extra.get("config_path", CONFIG_PATH)

        self.cfg = load_config(config_path, self_name)
        os.makedirs(self.cfg.meta_dir, exist_ok=True)
        self.nixl_svc = NixlService(self_name)

        self.local_keys: dict[int, tuple] = {}  # hash -> (k_ptr, v_ptr, elem_bytes)
        self.lock = threading.Lock()            # guards local_keys

        self._peer_key_cache: dict[str, dict[int, tuple]] = {}  # peer_id -> {hash: (k_ptr, v_ptr, elem_bytes)}

        self._start_http_server()
        logger.info(f"{TAG} started node={self_name} http_port={self.cfg.http_port}")

    # ---- SGLang HiCache interface -----------------------------------------

    def register_mem_pool_host(self, mem_pool_host):
        super().register_mem_pool_host(mem_pool_host)
        self.mem_pool_host = mem_pool_host
        self.nixl_svc.register_mem_pool(mem_pool_host.kv_buffer)

    def batch_set_v1(self, keys, host_indices, extra_info=None):
        """Source side: SGLang finished a prefill, record the memory locations."""
        ptrs, sizes = self.mem_pool_host.get_page_buffer_meta(host_indices)
        with self.lock:
            for i, k in enumerate(keys):
                self.local_keys[k] = (ptrs[2 * i], ptrs[2 * i + 1], sizes[2 * i])
        logger.info(f"{TAG} batch_set_v1: stored {len(keys)} blocks total={len(self.local_keys)}")
        return [True] * len(keys)

    def batch_exists(self, keys, extra_info=None):
        """
        Destination side: returns count of contiguous prefix blocks that exist on
        the active peer. SGLang uses this to know how many blocks to pull.
        """
        peer_id = self.nixl_svc.active_peer_id
        if peer_id is None:
            return 0

        peer_keys = self._fetch_peer_keys(peer_id, keys)
        count = 0
        for k in keys:
            if k not in peer_keys:
                break
            count += 1
        if count:
            logger.info(f"{TAG} batch_exists: {count}/{len(keys)} blocks on peer {peer_id}")
        return count

    def batch_get_v1(self, keys, host_indices, extra_info=None):
        """
        Destination side: SGLang wants the actual KV bytes for `keys`.
        Pulls from active peer via NIXL READ into our local CPU buffer.
        """
        peer_id = self.nixl_svc.active_peer_id
        if peer_id is None:
            return [False] * len(keys)

        peer_keys = self._fetch_peer_keys(peer_id, keys)
        lptrs, lsizes = self.mem_pool_host.get_page_buffer_meta(host_indices)

        loc, rem = [], []
        for i, k in enumerate(keys):
            entry = peer_keys.get(k)
            if entry is None:
                # block missing on peer — return partial success up to here
                return [True] * i + [False] * (len(keys) - i)
            k_ptr_src, v_ptr_src, elem_bytes = entry
            for j in range(2):
                loc.append((lptrs[2 * i + j], lsizes[2 * i + j], 0))
                rem.append(((k_ptr_src if j == 0 else v_ptr_src), elem_bytes, 0))

        ok = self.nixl_svc.read(peer_id, loc, rem)
        return [ok] * len(keys)

    # ---- non-v1 interface stubs (SGLang routes to _v1 via interface_v1=1) ----

    def exists(self, key, extra_info=None) -> bool:
        raise NotImplementedError("use interface_v1")

    def get(self, key, target_location=None, target_sizes=None):
        raise NotImplementedError("use interface_v1")

    def batch_get(self, keys, target_locations=None, target_sizes=None):
        raise NotImplementedError("use interface_v1")

    def set(self, key, value=None, target_location=None, target_sizes=None) -> bool:
        raise NotImplementedError("use interface_v1")

    def batch_set(self, keys, values=None, target_locations=None, target_sizes=None) -> bool:
        raise NotImplementedError("use interface_v1")

    # ---- internal ---------------------------------------------------------

    def _fetch_peer_keys(self, peer_id: str, keys: list) -> dict:
        """
        Fetches pointer map for `keys` from peer's /keys endpoint.
        Results are cached; only missing keys are re-fetched.
        """
        cached = self._peer_key_cache.get(peer_id, {})
        missing = [k for k in keys if k not in cached]
        if not missing:
            return cached

        peer = next((p for p in self.cfg.peers if p.id == peer_id), None)
        if peer is None:
            logger.error(f"{TAG} unknown peer {peer_id}")
            return cached

        try:
            resp = requests.get(
                f"http://{peer.http}/keys",
                params={"hashes": ",".join(str(h) for h in missing)},
                timeout=self.cfg.pull_timeout_sec,
            )
            resp.raise_for_status()
            fetched = {int(h): tuple(v) for h, v in resp.json().items()}
            if peer_id not in self._peer_key_cache:
                self._peer_key_cache[peer_id] = {}
            self._peer_key_cache[peer_id].update(fetched)
            return {**cached, **fetched}
        except Exception as e:
            logger.error(f"{TAG} /keys fetch from {peer_id} failed: {e}")
            return cached

    def _start_http_server(self):
        backend = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                parsed = urlparse(self.path)
                if parsed.path == "/generate":
                    handle_generate(self, backend.cfg, backend.nixl_svc)
                else:
                    self.send_error(404)

            def do_GET(self):
                parsed = urlparse(self.path)
                if parsed.path == "/agent":
                    handle_agent(self, backend.nixl_svc)
                elif parsed.path == "/keys":
                    handle_keys(self, backend, parsed.query)
                else:
                    self.send_error(404)

            def log_message(self, fmt, *args):
                logger.debug(f"[http] {self.address_string()} {fmt % args}")

        srv = HTTPServer(("0.0.0.0", self.cfg.http_port), Handler)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        logger.info(f"{TAG} HTTP server on 0.0.0.0:{self.cfg.http_port}")
