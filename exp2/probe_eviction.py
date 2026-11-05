#!/usr/bin/env python3
"""probe_eviction.py – overflow GPU and host pools to trigger BlockRemoved events.

Runs on: machine A (against localhost)

Sends ~40 unique 2K-token prompts so both the GPU KV pool and the HiCache host
pool overflow. Records frames for 60 s total.
Expected output: BlockRemoved events with medium "GPU" and "CPU_PINNED".

Usage:
  python probe_eviction.py \\
    --port 30000 \\
    --kv-events-port 30200 \\
    --out-dir results
"""

import argparse
import base64
import json
import os
import random
import struct
import time

import msgpack
import requests
import zmq


TOKEN_ID_MIN = 100
TOKEN_ID_MAX = 50_000
PROMPT_TOKENS = 2048
NUM_PROMPTS = 40


def rand_ids(rng: random.Random, n: int) -> list:
    return [rng.randint(TOKEN_ID_MIN, TOKEN_ID_MAX) for _ in range(n)]


def collect_frames(sock: zmq.Socket, duration: float) -> list:
    frames = []
    deadline = time.time() + duration
    while time.time() < deadline:
        try:
            sock.poll(timeout=int(max(1, (deadline - time.time()) * 1000)))
            parts = sock.recv_multipart(zmq.NOBLOCK)
            if len(parts) == 3:
                frames.append(parts)
        except zmq.Again:
            pass
    return frames


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=30000)
    ap.add_argument("--kv-events-port", type=int, default=30200, dest="kv_events_port")
    ap.add_argument("--out-dir", default="results", dest="out_dir")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    sock.setsockopt(zmq.SUBSCRIBE, b"")
    sock.setsockopt(zmq.RCVTIMEO, 200)
    sock.connect(f"tcp://localhost:{args.kv_events_port}")
    time.sleep(0.5)

    rng = random.Random(77)
    base_url = f"http://localhost:{args.port}"

    print(f"Sending {NUM_PROMPTS} × {PROMPT_TOKENS}-token prompts to overflow KV pools…")
    for i in range(NUM_PROMPTS):
        ids = rand_ids(rng, PROMPT_TOKENS)
        requests.post(f"{base_url}/generate", json={
            "input_ids": ids,
            "sampling_params": {"temperature": 0, "max_new_tokens": 1},
        }, timeout=120)
        print(f"  prompt {i+1}/{NUM_PROMPTS} sent", end="\r")
    print()

    print("Collecting events for 60s…")
    frames = collect_frames(sock, 60.0)
    sock.close()
    ctx.term()

    raw_path = os.path.join(args.out_dir, "eviction_raw.jsonl")
    dec_path = os.path.join(args.out_dir, "eviction_decoded.jsonl")

    gpu_removed = 0
    cpu_removed = 0

    with open(raw_path, "w") as rf, open(dec_path, "w") as df:
        for topic, seq_bytes, payload in frames:
            seq = struct.unpack(">Q", seq_bytes)[0]
            rf.write(json.dumps({
                "seq": seq,
                "topic_b64": base64.b64encode(topic).decode(),
                "payload_b64": base64.b64encode(payload).decode(),
            }) + "\n")
            try:
                raw = msgpack.unpackb(payload, raw=False)
                ts = raw[0] if isinstance(raw, list) and raw else 0
                evts = raw[1] if isinstance(raw, list) and len(raw) > 1 else []
                dec = {"seq": seq, "ts": ts, "events": evts if isinstance(evts, list) else []}
                df.write(json.dumps(dec) + "\n")
                for ev in dec["events"]:
                    if isinstance(ev, dict) and ev.get("type") == "BlockRemoved":
                        m = ev.get("medium", "")
                        if m == "GPU":
                            gpu_removed += 1
                        elif m == "CPU_PINNED":
                            cpu_removed += 1
            except Exception as e:
                df.write(json.dumps({"seq": seq, "error": str(e)}) + "\n")

    print(f"\nRaw frames → {raw_path}")
    print(f"Decoded    → {dec_path}")
    print(f"\nBlockRemoved GPU={gpu_removed}  CPU_PINNED={cpu_removed}")
    if gpu_removed == 0:
        print("  WARNING: no GPU evictions – GPU pool may not be full yet")
    if cpu_removed == 0:
        print("  WARNING: no CPU_PINNED evictions – host pool may not be full yet; try more prompts")


if __name__ == "__main__":
    main()
