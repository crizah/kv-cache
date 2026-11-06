#!/usr/bin/env bash
set -euo pipefail

exec python3 -m sglang.launch_server \
    --model                                "$SGLANG_MODEL_PATH"    \
    --port                                 "$SGLANG_PORT"          \
    --page-size                            "$SGLANG_PAGE_SIZE"     \
    --kv-events-config                     "$SGLANG_KV_EVENTS_CFG" \
    --load-publish-endpoint                "$SGLANG_LOAD_ENDPOINT" \
    --cpu-offload-gb                       "$SGLANG_CPU_OFFLOAD_GB"\
    --mem-fraction-static                  "$SGLANG_MEM_FRACTION"  \
    --stream-interval                       1\
    --enable-metrics \
    --hicache-storage-backend              dynamic                  \
    --hicache-storage-backend-extra-config "$SGLANG_EXTRA_CFG"
