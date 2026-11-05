#!/bin/bash
# Runs on: machine A or machine B (one instance each, same region)
# Usage: launch_node.sh <name> <gpu> <port> <meta_port> <peer_url> [model]
#   name      node identifier, e.g. A or B
#   gpu       CUDA device index (0 on a single-GPU instance)
#   port      SGLang HTTP port
#   meta_port port for this node's HTTP meta server (peer_nixl_backend)
#   peer_url  base URL of the PEER machine's meta server, e.g. http://<B_HOST>:8766
#   model     HuggingFace model path (default: Qwen/Qwen3-8B)
#
# Examples (two machines, same region):
#   On A: ./launch_node.sh A 0 30000 8765 http://<B_HOST>:8766
#   On B: ./launch_node.sh B 0 30001 8766 http://<A_HOST>:8765

set -euo pipefail

N=$1; G=$2; PORT=$3; META_PORT=$4; PEER_URL=$5
MODEL=${6:-Qwen/Qwen3-8B}
# The image pre-downloads weights here via SGLANG_MODEL at boot. Pointing at the
# same directory means the server starts from the cache instead of re-downloading.
DOWNLOAD_DIR=${DOWNLOAD_DIR:-/workspace/models}

# Qwen3-8B (~16 GB bf16 weights) on a 24 GB card:
#   0.85 × 24 = 20.4 GB total, ~4.3 GB left for KV cache.
#   A 16.5K-token prompt needs ~2.5 GB (16,896 × 147,456 B/token), so it fits.
MEM_FRAC=0.85
CTX=16896   # 264 × page_size_64; covers a 16.5K-token prompt

KV_EVENTS_PORT=$((PORT + 200))   # e.g. 30000 → 30200

mkdir -p /workspace/exp2/logs /workspace/exp2/results

export PYTHONPATH=/workspace/exp2
export UCX_TLS=tcp
# Force UCX onto the Vast overlay interface. Instances in the same physical
# cluster share a private LAN on eth0 with all ports open between them, so UCX
# can advertise a routable address instead of the NAT'd one. Without this UCX
# may pick the internet bridge (eth1) and the peer cannot connect.
export UCX_NET_DEVICES=${UCX_NET_DEVICES:-eth0}
export CUDA_VISIBLE_DEVICES=$G

EXTRA=$(python3 -c "
import json, sys
print(json.dumps({
    'backend_name':  'peer_nixl',
    'module_path':   'peer_nixl_backend',
    'class_name':    'PeerNixlStorage',
    'interface_v1':  1,
    'self_name':     sys.argv[1],
    'meta_port':     int(sys.argv[2]),
    'peer_url':      sys.argv[3],
    'sglang_port':   int(sys.argv[4]),
}))
" "$N" "$META_PORT" "$PEER_URL" "$PORT")

# NOTE: verify --kv-events-config key names against the installed SGLang version:
#   python -c "import sglang; help(sglang.launch_server)" or check server_args.py
KV_CFG="{\"zmq_port\": $KV_EVENTS_PORT}"

PYTHON=/opt/sglang/bin/python

CMD="$PYTHON -m sglang.launch_server \
  --model-path $MODEL \
  --download-dir $DOWNLOAD_DIR \
  --tp-size 1 \
  --port $PORT \
  --page-size 64 \
  --mem-fraction-static $MEM_FRAC \
  --context-length $CTX \
  --enable-hierarchical-cache \
  --hicache-ratio 2 \
  --hicache-mem-layout page_first \
  --hicache-write-policy write_through \
  --hicache-storage-prefetch-policy wait_complete \
  --hicache-storage-backend dynamic \
  --hicache-storage-backend-extra-config '$EXTRA' \
  --stream-interval 1 \
  --enable-metrics \
  --kv-events-config '$KV_CFG'"

echo "$CMD" > /workspace/exp2/logs/${N}_args.txt
echo "[launch_node] starting $N on GPU $G port $PORT meta_port $META_PORT model=$MODEL peer=$PEER_URL"
eval "$CMD" 2>&1 | tee /workspace/exp2/logs/${N}_server.log