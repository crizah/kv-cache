import base64
import json
import logging

logger = logging.getLogger(__name__)
TAG = "[meta]"


def handle_agent(req_handler, nixl_svc) -> None:
    meta_bytes = nixl_svc.get_agent_metadata()  # bytes from nixl_agent.get_agent_metadata()
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
    logger.info(f"{TAG} served agent metadata to peer, mem_size={nixl_svc.mem_size}")
