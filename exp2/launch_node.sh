#!/bin/bash
# Usage: launch_node.sh <name> <gpu> <port> <meta_port> <peer_url> [model]
set -euo pipefail

N=$1; G=$2; PORT=$3; META_PORT=$4; PEER_URL=$5
MODEL=${6:-/models/Qwen3-32B}
MEM_FRAC=${MEM_FRAC:-0.85}
CTX=${CTX:-33024}
KV_EVENTS_PORT=$((PORT + 200))

mkdir -p /exp2/logs /exp2/results

export PYTHONPATH=/exp2
export UCX_TLS=${UCX_TLS:-tcp}
export UCX_NET_DEVICES=${UCX_NET_DEVICES:-eth0}
export CUDA_VISIBLE_DEVICES=$G

EXTRA=$(python3 -c "
import json, sys
print(json.dumps({
    'backend_name': 'peer_nixl',
    'module_path': 'peer_nixl_backend',
    'class_name': 'PeerNixlStorage',
    'interface_v1': 1,
    'self_name': sys.argv[1],
    'meta_port': int(sys.argv[2]),
    'peer_url': sys.argv[3],
    'sglang_port': int(sys.argv[4]),
}))
" "$N" "$META_PORT" "$PEER_URL" "$PORT")

ARGS=(
  --model-path "$MODEL"
  --host 0.0.0.0
  --tp-size 1
  --port "$PORT"
  --page-size 64
  --mem-fraction-static "$MEM_FRAC"
  --context-length "$CTX"
  --enable-hierarchical-cache
  --hicache-ratio 2
  --hicache-mem-layout page_first
  --hicache-write-policy write_through
  --hicache-storage-prefetch-policy wait_complete
  --hicache-storage-backend dynamic
  --hicache-storage-backend-extra-config "$EXTRA"
  --stream-interval 1
  --enable-metrics
)

if [ "${KV_EVENTS:-0}" = "1" ]; then
  ARGS+=(--kv-events-config "{\"publisher\":\"zmq\",\"endpoint\":\"tcp://*:$KV_EVENTS_PORT\",\"topic\":\"kv-events\"}")
fi

echo "python3 -m sglang.launch_server ${ARGS[*]}" > /exp2/logs/${N}_args.txt
python3 -m sglang.launch_server "${ARGS[@]}" 2>&1 | tee /exp2/logs/${N}_server.log
