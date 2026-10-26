#!/bin/bash
# usage: launch.sh NAME GPU PORT PEER
N=$1; G=$2; PORT=$3; PEER=$4
cd /workspace/exp
export PYTHONPATH=/workspace/exp UCX_TLS=tcp CUDA_VISIBLE_DEVICES=$G
exec /opt/sglang/bin/python -m sglang.launch_server --model-path Qwen/Qwen3-0.6B --tp-size 1 --port $PORT \
  --mem-fraction-static 0.5 --enable-hierarchical-cache --hicache-ratio 2 --page-size 64 \
  --hicache-mem-layout page_first --hicache-write-policy write_through \
  --hicache-storage-prefetch-policy wait_complete --hicache-storage-backend dynamic \
  --hicache-storage-backend-extra-config "{\"backend_name\":\"peer_nixl\",\"module_path\":\"peer_nixl_backend\",\"class_name\":\"PeerNixlStorage\",\"interface_v1\":1,\"self_name\":\"$N\",\"peer_name\":\"$PEER\",\"meta_dir\":\"/workspace/exp/meta\"}"
