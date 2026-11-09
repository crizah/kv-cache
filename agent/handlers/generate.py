import logging

import requests

from agent.config import AgentConfig

logger = logging.getLogger(__name__)
TAG = "[generate]"
CHUNK = 4096

# gets the req from proxy, sends it to sglang server with appropriate headers and information
# streams the sglang res back to proxy by creating a tunnel between them
def handle_generate(req_handler, cfg: AgentConfig, nixl_backend) -> None:
    length = int(req_handler.headers.get("Content-Length", 0))
    body = req_handler.rfile.read(length) if length else b""

    nixl_backend.nixl_svc.set_active_peer(None, None)
    peer_id = req_handler.headers.get("X-Kv-Source")
    if peer_id:
        peer = next((p for p in cfg.peers if p.id == peer_id), None)
        if peer is None:
            req_handler.send_error(400, f"unknown peer id: {peer_id}")
            return
        nixl_backend.nixl_svc.set_active_peer(peer_id, peer.http)
        logger.info(f"{TAG} pull requested from peer {peer_id}")

    forward_headers = {
        k: v for k, v in req_handler.headers.items()
        if k.lower() not in ("host", "x-kv-source", "content-length")
    }

    try:
        # sglang always on local host
        resp = requests.post(
            f"http://127.0.0.1:{cfg.sglang_port}/generate",
            data=body,
            headers=forward_headers,
            stream=True,
            timeout=cfg.pull_timeout_sec,
        )
    except requests.RequestException as e:
        # TODO: retry with other nodes or buffer here
        req_handler.send_error(502, f"sglang unreachable: {e}")
        return

    req_handler.send_response(resp.status_code)
    for k, v in resp.headers.items():
        if k.lower() != "transfer-encoding":
            req_handler.send_header(k, v)
    req_handler.end_headers()

    for chunk in resp.iter_content(chunk_size=CHUNK):
        if chunk:
            try:
                req_handler.wfile.write(chunk)
                req_handler.wfile.flush()
            except BrokenPipeError:
                break
