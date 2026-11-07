#!/bin/bash
set -euo pipefail

export SGLANG_L3_DIR="${SGLANG_L3_DIR:-/root/kv-cache/l3}"
export SGLANG_HICACHE_FILE_BACKEND_STORAGE_DIR="$SGLANG_L3_DIR"
mkdir -p "$SGLANG_L3_DIR"

exec python3 -m sglang.launch_server \
    --model-path                           "$SGLANG_MODEL_PATH"    \
    --host                                 0.0.0.0                 \
    --port                                 "$SGLANG_PORT"          \
    --tp-size                              1                       \
    --context-length                       33024                   \
    --page-size                            "$SGLANG_PAGE_SIZE"     \
    --mem-fraction-static                  "$SGLANG_MEM_FRACTION"  \
    --cpu-offload-gb                       "$SGLANG_CPU_OFFLOAD_GB"\
    --stream-interval                      1                       \
    --enable-metrics                                               \
    --enable-hierarchical-cache                                    \
    --hicache-ratio                        4                       \
    --hicache-mem-layout                   page_first              \
    --hicache-write-policy                 write_through           \
    --hicache-storage-prefetch-policy      wait_complete           \
    --hicache-storage-backend              file                    \
    --kv-events-config                     "$SGLANG_KV_EVENTS_CFG" \
    --load-publish-endpoint                "$SGLANG_LOAD_ENDPOINT"
