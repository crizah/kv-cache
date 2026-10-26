"""PeerNixlStorage: HiCache L3 backend that pulls KV pages from a peer SGLang
server's host (CPU) KV pool with NIXL READ. KV bytes move only through NIXL.

Extra-config keys (--hicache-storage-backend-extra-config):
  backend_name/module_path/class_name  required by the `dynamic` backend
  interface_v1: 1     makes the cache controller call batch_get_v1/batch_set_v1
  self_name           this server's name, e.g. "A"
  peer_name           the other server's name, e.g. "B"
  meta_dir            directory for the metadata exchange (small JSON files only)

Metadata files in meta_dir (no KV data):
  <name>.agent.json   NIXL agent name + metadata (hex) + registered region
  <name>.keys.jsonl   append-only: {"k": key, "p": [k_ptr, v_ptr], "n": elem_bytes}
"""

import json
import logging
import os
import threading
import time
import uuid
from typing import Any, List, Optional

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
        self.self_name = cfg["self_name"]
        self.peer_name = cfg["peer_name"]
        self.meta_dir = cfg["meta_dir"]
        os.makedirs(self.meta_dir, exist_ok=True)
        self.agent_name = f"{self.self_name}-{uuid.uuid4().hex[:8]}"
        self.agent = nixl_agent(self.agent_name, nixl_agent_config(backends=["UCX"]))
        self.mem_pool_host = None
        self.local_keys = {}  # key -> (k_ptr, v_ptr, elem_bytes)
        self.peer_keys = {}  # key -> (k_ptr, v_ptr, elem_bytes)
        self._peer_off = 0
        self._peer_agent = None
        self._lock = threading.Lock()
        self._keys_path = os.path.join(self.meta_dir, f"{self.self_name}.keys.jsonl")
        open(self._keys_path, "w").close()
        self.bytes_read = 0
        self.pages_read = 0
        logger.info(f"{TAG} agent {self.agent_name} created, self={self.self_name} peer={self.peer_name}")

    # ---- registration -------------------------------------------------
    def register_mem_pool_host(self, mem_pool_host):
        super().register_mem_pool_host(mem_pool_host)
        buf = mem_pool_host.kv_buffer
        base, nbytes = buf.data_ptr(), buf.numel() * buf.element_size()
        self._reg = self.agent.register_memory([(base, nbytes, 0, "")], "DRAM")
        meta = self.agent.get_agent_metadata()
        tmp = os.path.join(self.meta_dir, f".{self.self_name}.agent.tmp")
        with open(tmp, "w") as f:
            json.dump(
                {"agent": self.agent_name, "meta": meta.hex(), "base": base, "nbytes": nbytes},
                f,
            )
        os.replace(tmp, os.path.join(self.meta_dir, f"{self.self_name}.agent.json"))
        logger.info(
            f"{TAG} registered host KV pool with NIXL: base=0x{base:x} bytes={nbytes} "
            f"layout={mem_pool_host.layout} page_size={mem_pool_host.page_size}; metadata published"
        )

    def register_mem_host_pool_v2(self, host_pool, host_pool_name):
        super().register_mem_host_pool_v2(host_pool, host_pool_name)

    # ---- producer -----------------------------------------------------
    def batch_set_v1(self, keys: List[str], host_indices: torch.Tensor, extra_info=None) -> List[bool]:
        ptrs, sizes = self.mem_pool_host.get_page_buffer_meta(host_indices)
        assert len(ptrs) == 2 * len(keys), (len(ptrs), len(keys))
        lines = []
        with self._lock:
            for i, k in enumerate(keys):
                rec = (ptrs[2 * i], ptrs[2 * i + 1], sizes[2 * i])
                self.local_keys[k] = rec
                lines.append(json.dumps({"k": k, "p": [rec[0], rec[1]], "n": rec[2]}))
            with open(self._keys_path, "a") as f:
                f.write("\n".join(lines) + "\n")
        logger.info(f"{TAG} published {len(keys)} pages (no copy); total published={len(self.local_keys)}")
        return [True] * len(keys)

    # ---- consumer -----------------------------------------------------
    def _refresh_peer(self):
        if self._peer_agent is None:
            p = os.path.join(self.meta_dir, f"{self.peer_name}.agent.json")
            if not os.path.exists(p):
                return False
            d = json.load(open(p))
            self._peer_agent = self.agent.add_remote_agent(bytes.fromhex(d["meta"]))
            if isinstance(self._peer_agent, bytes):
                self._peer_agent = self._peer_agent.decode()
            logger.info(f"{TAG} added remote agent {self._peer_agent}, region 0x{d['base']:x}+{d['nbytes']}")
        path = os.path.join(self.meta_dir, f"{self.peer_name}.keys.jsonl")
        if os.path.exists(path):
            with open(path) as f:
                f.seek(self._peer_off)
                data = f.read()
            end = data.rfind("\n") + 1  # only complete lines
            for line in data[:end].splitlines():
                if line:
                    r = json.loads(line)
                    self.peer_keys[r["k"]] = (r["p"][0], r["p"][1], r["n"])
            self._peer_off += end
        return True

    def exists(self, key: str) -> bool:
        if not self._refresh_peer():
            return False
        return key in self.peer_keys

    def batch_exists(self, keys: List[str], extra_info: Optional[HiCacheStorageExtraInfo] = None) -> int:
        if not self._refresh_peer():
            return 0
        n = 0
        for k in keys:
            if k not in self.peer_keys:
                break
            n += 1
        if n:
            logger.info(f"{TAG} batch_exists: {n}/{len(keys)} pages present on peer {self.peer_name}")
        return n

    def batch_get_v1(self, keys: List[str], host_indices: torch.Tensor, extra_info=None) -> List[bool]:
        self._refresh_peer()
        lptrs, lsizes = self.mem_pool_host.get_page_buffer_meta(host_indices)
        loc, rem = [], []
        for i, k in enumerate(keys):
            rec = self.peer_keys.get(k)
            if rec is None:
                return [True] * i + [False] * (len(keys) - i)
            for j in range(2):
                loc.append((lptrs[2 * i + j], lsizes[2 * i + j], 0))
                rem.append((rec[j], rec[2], 0))
        t0 = time.perf_counter()
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
        if state == "ERR":
            logger.error(f"{TAG} NIXL READ failed")
            return [False] * len(keys)
        nbytes = sum(x[1] for x in loc)
        self.bytes_read += nbytes
        self.pages_read += len(keys)
        logger.info(
            f"{TAG} NIXL READ from agent {self._peer_agent}: pages={len(keys)} descs={len(loc)} "
            f"bytes={nbytes} in {(time.perf_counter()-t0)*1e3:.1f} ms (cumulative pages={self.pages_read} bytes={self.bytes_read})"
        )
        return [True] * len(keys)

    # ---- legacy abstract API (unused: interface_v1 routes to the v1 calls) ----
    def get(self, key, target_location=None, target_sizes=None):
        raise NotImplementedError("use interface_v1")

    def batch_get(self, keys, target_locations=None, target_sizes=None):
        raise NotImplementedError("use interface_v1")

    def set(self, key, value=None, target_location=None, target_sizes=None) -> bool:
        raise NotImplementedError("use interface_v1")

    def batch_set(self, keys, values=None, target_locations=None, target_sizes=None) -> bool:
        raise NotImplementedError("use interface_v1")
