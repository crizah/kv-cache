#!/usr/bin/env python3
import sys
import zmq

port = sys.argv[1] if len(sys.argv) > 1 else "5557"
ctx = zmq.Context()
sock = ctx.socket(zmq.SUB)
sock.setsockopt(zmq.SUBSCRIBE, b"")
sock.connect(f"tcp://localhost:{port}")
print(f"listening on tcp://localhost:{port} ...", flush=True)
while True:
    frames = sock.recv_multipart()
    print(f"frames={len(frames)}  sizes={[len(f) for f in frames]}", flush=True)
    for i, f in enumerate(frames):
        print(f"  [{i}]: {f[:300]}", flush=True)
