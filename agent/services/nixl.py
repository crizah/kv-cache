import base64
import json
import logging
import threading
import time
import uuid
from pathlib import Path

import requests

logger = logging.getLogger(__name__)
TAG = "[nixl]"

META_DIR = Path(".meta")

# TODO: have all the loggers log timestamps

class NixlService:
    """
    Wraps the NIXL agent for this node.
    Initialized by the backend after SGLang hands us the host KV buffer.
    Peers are connected lazily on first pull request via ensure_peer_connected().
    Metadata is cached to .meta/<peer_id>.json so the handshake only happens once
    per cluster lifetime (or until .meta/ is cleared).
    """

    def __init__(self, node_id: str):
        from nixl._api import nixl_agent, nixl_agent_config

        self.node_id = node_id
        self.mem_size: int = 0
        self._reg = None
        self._lock = threading.Lock()
        self._peer_agents: dict[str, str] = {}  # peer_id -> remote agent name (from add_remote_agent)

        agent_name = f"{node_id}-{uuid.uuid4().hex[:8]}"
        self._agent = nixl_agent(agent_name, nixl_agent_config(backends=["UCX"]))
        logger.info(f"{TAG} NIXL agent created: {agent_name}")

    def register_mem_pool(self, buf) -> None:
        """Called once by the backend when SGLang provides the host KV buffer."""
        base = buf.data_ptr()
        nbytes = buf.numel() * buf.element_size()
        self._reg = self._agent.register_memory([(base, nbytes, 0, "")], "DRAM")
        self.mem_size = nbytes
        logger.info(f"{TAG} registered host KV pool: base=0x{base:x} bytes={nbytes}")

    def get_agent_metadata(self) -> bytes:
        return self._agent.get_agent_metadata()

    def ensure_peer_connected(self, peer_id: str, peer_url: str) -> None:
        """
        One-time NIXL handshake with a peer. Caches result to .meta/<peer_id>.json.
        If the file already exists, the handshake is skipped (metadata exchange is
        a one-time cost — will show in wall-clock on a cold cluster).
        """
        with self._lock:
            if peer_id in self._peer_agents:
                return

            META_DIR.mkdir(exist_ok=True)
            cache_path = META_DIR / f"{peer_id}.json"

            if cache_path.exists():
                data = json.loads(cache_path.read_text())
                logger.info(f"{TAG} loaded cached metadata for peer {peer_id}")
            else:
                logger.info(f"{TAG} fetching metadata from peer {peer_id} at {peer_url}")
                resp = requests.get(f"http://{peer_url}/agent", timeout=30)
                resp.raise_for_status()
                data = resp.json()
                cache_path.write_text(json.dumps(data))
                logger.info(f"{TAG} cached metadata for peer {peer_id} to {cache_path}")

            nixl_meta = base64.b64decode(data["nixl_metadata"])
            remote_name = self._agent.add_remote_agent(nixl_meta)
            if isinstance(remote_name, bytes):
                remote_name = remote_name.decode()
            self._peer_agents[peer_id] = remote_name
            logger.info(f"{TAG} NIXL handshake complete with peer {peer_id} remote_agent={remote_name}")

    def set_active_peer(self, peer_id: str, peer_url: str) -> None:
        """Called by generate handler before forwarding to SGLang."""
        self._pending_peer = (peer_id, peer_url)
        self._active_peer_id = peer_id

    @property
    def active_peer_id(self) -> str | None:
        return getattr(self, "_active_peer_id", None)

    def read(self, peer_id: str, loc: list, rem: list) -> bool:
        """
        NIXL READ: pull remote memory into local buffers.
        loc / rem are lists of (ptr, size, 0) tuples — one per k/v tensor per block.
        Returns True on success.
        """
        pending = getattr(self, "_pending_peer", None)
        if pending and pending[0] == peer_id and peer_id not in self._peer_agents:
            self.ensure_peer_connected(pending[0], pending[1])

        remote_agent = self._peer_agents.get(peer_id)
        if remote_agent is None:
            logger.error(f"{TAG} read: peer {peer_id} not connected")
            return False

        t0 = time.perf_counter()
        ld = self._agent.get_xfer_descs(loc, "DRAM")
        rd = self._agent.get_xfer_descs(rem, "DRAM")
        h = self._agent.initialize_xfer("READ", ld, rd, remote_agent)
        if not h:
            logger.error(f"{TAG} initialize_xfer failed for peer {peer_id}")
            return False

        state = self._agent.transfer(h)
        while state not in ("DONE", "ERR"):
            state = self._agent.check_xfer_state(h)
        self._agent.release_xfer_handle(h)

        elapsed_ms = (time.perf_counter() - t0) * 1e3
        if state == "ERR":
            logger.error(f"{TAG} NIXL READ failed for peer {peer_id} in {elapsed_ms:.1f}ms")
            return False

        nbytes = sum(x[1] for x in loc)
        logger.info(f"{TAG} NIXL READ from {peer_id}: descs={len(loc)} bytes={nbytes} {elapsed_ms:.1f}ms")
        return True
