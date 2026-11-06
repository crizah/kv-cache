#!/bin/bash

mkdir -p /tmp/mps /tmp/mps-log
pip install nixl

export UCX_NET_DEVICES=eth0 UCX_TLS=tcp
export CUDA_MPS_PIPE_DIRECTORY=/tmp/mps CUDA_MPS_LOG_DIRECTORY=/tmp/mps-log
nvidia-cuda-mps-control -d
export CUDA_MPS_ACTIVE_THREAD_PERCENTAGE=25
