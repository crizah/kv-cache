import base64
import json
import logging
from urllib.parse import parse_qs

logger = logging.getLogger(__name__)
TAG = "[meta]"


def handle_agent(req_handler, nixl_svc) -> None:
    meta_bytes = nixl_svc.get_agent_metadata()
    body = json.dumps({
        "node_id":       nixl_svc.node_id,
        "nixl_metadata": base64.b64encode(meta_bytes).decode(),
        "mem_size":      nixl_svc.mem_size,
    }).encode()
    req_handler.send_response(200)
    req_handler.send_header("Content-Type", "application/json")
    req_handler.send_header("Content-Length", str(len(body)))
    req_handler.end_headers()
    req_handler.wfile.write(body)
    logger.info(f"{TAG} served agent metadata, mem_size={nixl_svc.mem_size}")


def handle_key_count(req_handler, backend) -> None:
    # GET /keys/count — returns {"count": N}
    with backend.lock:
        count = len(backend.local_keys)
    body = json.dumps({"count": count}).encode()
    req_handler.send_response(200)
    req_handler.send_header("Content-Type", "application/json")
    req_handler.send_header("Content-Length", str(len(body)))
    req_handler.end_headers()
    req_handler.wfile.write(body)


def handle_keys(req_handler, backend, query: str) -> None:
    # GET /keys?hashes=h0,h1,h2,...
    # returns {str(hash): [k_ptr, v_ptr, elem_bytes]} for each hash we have
    params = parse_qs(query)
    raw = params.get("hashes", [""])[0]
    if not raw:
        req_handler.send_error(400, "missing hashes param")
        return

    requested = [h for h in raw.split(",") if h]
    result = {}
    with backend.lock:
        for h in requested:
            entry = backend.local_keys.get(h)
            if entry is not None:
                result[str(h)] = list(entry)  # [k_ptr, v_ptr, elem_bytes]

    body = json.dumps(result).encode()
    req_handler.send_response(200)
    req_handler.send_header("Content-Type", "application/json")
    req_handler.send_header("Content-Length", str(len(body)))
    req_handler.end_headers()
    req_handler.wfile.write(body)
    logger.info(f"{TAG} /keys: requested={len(requested)} found={len(result)}")
