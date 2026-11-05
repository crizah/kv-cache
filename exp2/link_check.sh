#!/bin/bash
# link_check.sh – measure the link between the two instances.
#
# Modes:
#   ./link_check.sh server [kv_bytes_per_token]             run on machine A
#   ./link_check.sh client <A_HOST> [kv_bytes_per_token]    run on machine B
#
# Server: starts iperf3 (port 19877) and a NIXL process that registers a DRAM
#         buffer sized 16384 tokens and publishes its metadata over HTTP on
#         port 19876 (bound to 0.0.0.0 so B can reach it).
# Client: iperf3 against A, ping A for RTT, then NIXL READs (B reads A's
#         memory) for 512 … 16384 token-sized buffers and prints bandwidth.
#
# kv_bytes_per_token: bytes per token in the KV cache; default 147456
#   (Qwen3-8B: 2 × 36 layers × 8 KV heads × 128 head_dim × 2 bytes)
#
# UCX_TLS=tcp on both sides so this works without RDMA.

set -euo pipefail

META_PORT=19876
IPERF_PORT=19877
MAX_TOKENS=16384
TOKEN_COUNTS="512 1024 2048 4096 8192 16384"

usage() {
    echo "Usage:"
    echo "  ./link_check.sh server [kv_bytes_per_token]"
    echo "  ./link_check.sh client <A_HOST> [kv_bytes_per_token]"
    exit 1
}

run_server() {
    KV_BYTES_PER_TOKEN=${1:-147456}

    SERVER_SCRIPT=$(mktemp /tmp/nixl_server_XXXXX.py)
    trap 'rm -f "$SERVER_SCRIPT"' EXIT

    cat > "$SERVER_SCRIPT" << 'PYEOF'
"""Registers a DRAM buffer with NIXL, publishes metadata over HTTP, then waits."""
import ctypes, json, sys, uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from nixl._api import nixl_agent, nixl_agent_config

kv_bytes_per_token = int(sys.argv[1])
meta_port = int(sys.argv[2])
max_tokens = int(sys.argv[3])

buf_size = kv_bytes_per_token * max_tokens
agent_name = f"srv-{uuid.uuid4().hex[:8]}"
agent = nixl_agent(agent_name, nixl_agent_config(backends=["UCX"]))

buf = (ctypes.c_char * buf_size)()
base = ctypes.addressof(buf)
agent.register_memory([(base, buf_size, 0, "")], "DRAM")
meta = agent.get_agent_metadata()

info = {"agent": agent_name, "meta": meta.hex(), "base": base, "size": buf_size}

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        body = json.dumps(info).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

print(f"[server] NIXL ready, base=0x{base:x} size={buf_size} meta_port={meta_port}", flush=True)
HTTPServer(("0.0.0.0", meta_port), H).serve_forever()
PYEOF

    if command -v iperf3 >/dev/null 2>&1; then
        echo "[link_check] iperf3 server on port $IPERF_PORT"
        iperf3 -s -p "$IPERF_PORT" &
        IPERF_PID=$!
        trap 'kill $IPERF_PID 2>/dev/null; rm -f "$SERVER_SCRIPT"' EXIT
    else
        echo "[link_check] iperf3 not found – skipping TCP throughput test"
    fi

    echo "[link_check] this machine's IP(s):"
    hostname -I 2>/dev/null || true
    echo "[link_check] now run on machine B: ./link_check.sh client <THIS_HOST> $KV_BYTES_PER_TOKEN"
    echo ""

    UCX_TLS=tcp python3 "$SERVER_SCRIPT" "$KV_BYTES_PER_TOKEN" "$META_PORT" "$MAX_TOKENS"
}

run_client() {
    A_HOST=$1
    KV_BYTES_PER_TOKEN=${2:-147456}

    echo "[link_check] TCP throughput → $A_HOST (iperf3)"
    if command -v iperf3 >/dev/null 2>&1; then
        iperf3 -c "$A_HOST" -p "$IPERF_PORT" -t 5 || true
    else
        echo "  iperf3 not found – skipping"
    fi

    echo ""
    echo "[link_check] TCP connect RTT → $A_HOST:$META_PORT (ICMP ping is often denied in containers)"
    python3 - "$A_HOST" "$META_PORT" << 'PYEOF'
import socket, statistics, sys, time

host, port = sys.argv[1], int(sys.argv[2])
rtts = []
for _ in range(5):
    s = socket.socket()
    t0 = time.perf_counter()
    try:
        s.connect((host, port))
        rtts.append((time.perf_counter() - t0) * 1e3)
    finally:
        s.close()

if rtts:
    print(f"  median {statistics.median(rtts):.2f} ms over {len(rtts)} connects")
else:
    print("  no successful connects (is the server running?)")
PYEOF
    echo ""

    CLIENT_SCRIPT=$(mktemp /tmp/nixl_client_XXXXX.py)
    trap 'rm -f "$CLIENT_SCRIPT"' EXIT

    cat > "$CLIENT_SCRIPT" << 'PYEOF'
"""Connects to A's NIXL agent and measures READ bandwidth over the network."""
import ctypes, json, sys, time, uuid
from urllib.request import urlopen
from nixl._api import nixl_agent, nixl_agent_config

a_host = sys.argv[1]
meta_port = int(sys.argv[2])
kv_bytes_per_token = int(sys.argv[3])
token_counts = [int(x) for x in sys.argv[4].split(",")]

# fetch server metadata (retry up to 10 s, the server may still be starting)
for _ in range(100):
    try:
        with urlopen(f"http://{a_host}:{meta_port}/agent", timeout=1) as r:
            srv = json.loads(r.read())
        break
    except Exception:
        time.sleep(0.1)
else:
    print("[client] ERROR: server never became ready", flush=True)
    sys.exit(1)

agent_name = f"cli-{uuid.uuid4().hex[:8]}"
agent = nixl_agent(agent_name, nixl_agent_config(backends=["UCX"]))

remote_agent = agent.add_remote_agent(bytes.fromhex(srv["meta"]))
if isinstance(remote_agent, bytes):
    remote_agent = remote_agent.decode()

max_buf = srv["size"]

# local buffer to receive into
local_buf = (ctypes.c_char * max_buf)()
local_base = ctypes.addressof(local_buf)

print(f"\n{'tokens':>8}  {'bytes':>12}  {'ms':>8}  {'Gbps':>8}")
for n_tokens in token_counts:
    buf_bytes = kv_bytes_per_token * n_tokens
    if buf_bytes > max_buf:
        print(f"{n_tokens:>8}  {'SKIP (too large)':>20}")
        continue

    loc = [(local_base, buf_bytes, 0)]
    rem = [(srv["base"], buf_bytes, 0)]

    ld = agent.get_xfer_descs(loc, "DRAM")
    rd = agent.get_xfer_descs(rem, "DRAM")

    # warm-up
    h = agent.initialize_xfer("READ", ld, rd, remote_agent)
    state = agent.transfer(h)
    while state not in ("DONE", "ERR"):
        state = agent.check_xfer_state(h)
    agent.release_xfer_handle(h)

    # measure (3 trials, take median)
    times = []
    for _ in range(3):
        h = agent.initialize_xfer("READ", ld, rd, remote_agent)
        t0 = time.perf_counter()
        state = agent.transfer(h)
        while state not in ("DONE", "ERR"):
            state = agent.check_xfer_state(h)
        agent.release_xfer_handle(h)
        if state == "DONE":
            times.append((time.perf_counter() - t0) * 1e3)

    if not times:
        print(f"{n_tokens:>8}  {buf_bytes:>12}  {'ERR':>8}  {'ERR':>8}")
        continue
    times.sort()
    med_ms = times[len(times) // 2]
    gbps = (buf_bytes / (med_ms / 1e3)) / 1e9
    print(f"{n_tokens:>8}  {buf_bytes:>12}  {med_ms:>8.2f}  {gbps:>8.2f}", flush=True)

print("[client] done", flush=True)
PYEOF

    echo "[link_check] NIXL READ (B reads A), kv_bytes_per_token=$KV_BYTES_PER_TOKEN"
    echo "  token_counts=$TOKEN_COUNTS"
    echo ""

    COUNTS_CSV=$(echo $TOKEN_COUNTS | tr ' ' ',')
    UCX_TLS=tcp python3 "$CLIENT_SCRIPT" "$A_HOST" "$META_PORT" "$KV_BYTES_PER_TOKEN" "$COUNTS_CSV"
}

MODE=${1:-}
case "$MODE" in
    server) shift; run_server "$@" ;;
    client) shift; [[ $# -ge 1 ]] || usage; run_client "$@" ;;
    *) usage ;;
esac