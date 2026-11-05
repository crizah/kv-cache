#!/usr/bin/env python3
"""probe_events.py – record raw KV events from A's ZMQ socket.

Runs on: machine A (against localhost)

Sends two prompts to A:
  1. input_ids prompt (1024 tokens, for exact token control)
  2. text prompt       (for tokenizer parity check)

Subscribes to A's kv-events ZMQ PUB socket and collects frames for 15 s each.
Saves:
  results/events_raw.jsonl     raw frames: {seq, topic_b64, payload_b64}
  results/events_decoded.jsonl decoded events: {seq, ts, events: [...]}

Prints a summary:
  - event types seen
  - medium values seen
  - block_size
  - hash count per event
  - whether CPU_PINNED BlockStored appears (expected under write_through)
  - whether parent_block_hash chains correctly

Usage:
  python probe_events.py \\
    --port 30000 \\
    --kv-events-port 30200 \\
    --out-dir results
"""

import argparse
import base64
import json
import os
import sys
import time

import msgpack
import requests
import zmq


PAGE_SIZE = 64
TOKEN_ID_MIN = 100
TOKEN_ID_MAX = 50_000


def rand_ids(n: int) -> list:
    import random
    rng = random.Random(99)
    return [rng.randint(TOKEN_ID_MIN, TOKEN_ID_MAX) for _ in range(n)]


def decode_batch(seq: int, payload: bytes) -> dict:
    raw = msgpack.unpackb(payload, raw=False)
    if not isinstance(raw, list) or len(raw) < 2:
        return {"seq": seq, "error": f"bad batch: {type(raw)}"}
    ts = raw[0]
    raw_events = raw[1] if isinstance(raw[1], list) else []
    dp_rank = raw[2] if len(raw) > 2 else None
    events = []
    for ev in raw_events:
        if isinstance(ev, dict):
            events.append(ev)
    return {"seq": seq, "ts": ts, "dp_rank": dp_rank, "events": events}


def collect_frames(sock: zmq.Socket, duration: float) -> list:
    frames = []
    deadline = time.time() + duration
    while time.time() < deadline:
        try:
            sock.poll(timeout=int((deadline - time.time()) * 1000))
            parts = sock.recv_multipart(zmq.NOBLOCK)
            if len(parts) == 3:
                frames.append(parts)
        except zmq.Again:
            pass
    return frames


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=30000, help="SGLang HTTP port")
    ap.add_argument("--kv-events-port", type=int, default=30200, dest="kv_events_port")
    ap.add_argument("--out-dir", default="results", dest="out_dir")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    raw_path = os.path.join(args.out_dir, "events_raw.jsonl")
    dec_path = os.path.join(args.out_dir, "events_decoded.jsonl")

    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    sock.setsockopt(zmq.SUBSCRIBE, b"")
    sock.setsockopt(zmq.RCVTIMEO, 200)
    sock.connect(f"tcp://localhost:{args.kv_events_port}")
    time.sleep(0.5)  # let subscription propagate

    base_url = f"http://localhost:{args.port}"

    # --- prompt 1: input_ids (1024 tokens) ---
    ids_1024 = rand_ids(1024)
    print(f"Sending input_ids prompt (1024 tokens)…")
    requests.post(f"{base_url}/generate", json={
        "input_ids": ids_1024,
        "sampling_params": {"temperature": 0, "max_new_tokens": 1},
    }, timeout=60)
    print("Collecting events for 15s…")
    frames_ids = collect_frames(sock, 15.0)
    print(f"  got {len(frames_ids)} frames")

    # --- prompt 2: text ---
    text_prompt = "The quick brown fox jumps over the lazy dog. " * 30
    print(f"Sending text prompt…")
    requests.post(f"{base_url}/generate", json={
        "text": text_prompt,
        "sampling_params": {"temperature": 0, "max_new_tokens": 1},
    }, timeout=60)
    print("Collecting events for 15s…")
    frames_text = collect_frames(sock, 15.0)
    print(f"  got {len(frames_text)} frames")

    sock.close()
    ctx.term()

    all_frames = frames_ids + frames_text

    with open(raw_path, "w") as rf, open(dec_path, "w") as df:
        for topic, seq_bytes, payload in all_frames:
            import struct
            seq = struct.unpack(">Q", seq_bytes)[0]

            raw_rec = {
                "seq": seq,
                "topic_b64": base64.b64encode(topic).decode(),
                "payload_b64": base64.b64encode(payload).decode(),
            }
            rf.write(json.dumps(raw_rec) + "\n")

            dec = decode_batch(seq, payload)
            df.write(json.dumps(dec) + "\n")

    print(f"\nRaw frames  → {raw_path}")
    print(f"Decoded     → {dec_path}")

    # --- summary ---
    all_decoded = []
    with open(dec_path) as f:
        for line in f:
            line = line.strip()
            if line:
                all_decoded.append(json.loads(line))

    type_counts: dict = {}
    mediums_seen: set = set()
    block_sizes: set = set()
    total_hashes = 0
    cpu_pinned_stored = False
    chain_ok = True
    last_block_hash_by_prefix: dict = {}  # track parent chain

    for rec in all_decoded:
        for ev in rec.get("events", []):
            t = ev.get("type", "?")
            type_counts[t] = type_counts.get(t, 0) + 1
            medium = ev.get("medium")
            if medium:
                mediums_seen.add(medium)
            if ev.get("block_size"):
                block_sizes.add(ev["block_size"])
            hashes = ev.get("block_hashes", [])
            total_hashes += len(hashes)
            if t == "BlockStored":
                if medium == "CPU_PINNED":
                    cpu_pinned_stored = True
                parent = ev.get("parent_block_hash")
                if hashes:
                    key = tuple(hashes[:-1])
                    if key in last_block_hash_by_prefix:
                        if last_block_hash_by_prefix[key] != parent:
                            chain_ok = False
                    last_block_hash_by_prefix[tuple(hashes)] = hashes[-1]

    print("\n=== Summary ===")
    print(f"Total frames decoded: {len(all_decoded)}")
    print(f"Event type counts:   {type_counts}")
    print(f"Mediums seen:        {sorted(mediums_seen)}")
    print(f"Block sizes seen:    {sorted(block_sizes)}")
    print(f"Total hash values:   {total_hashes}")
    print(f"CPU_PINNED stored:   {'YES' if cpu_pinned_stored else 'NO'} (expected YES under write_through)")
    print(f"Parent hash chain:   {'OK' if chain_ok else 'BROKEN'}")

    # save summary alongside for the Go offline test
    summary = {
        "ids_1024_token_ids": ids_1024,
        "text_prompt": text_prompt,
        "type_counts": type_counts,
        "mediums": sorted(mediums_seen),
        "block_sizes": sorted(block_sizes),
        "cpu_pinned_stored": cpu_pinned_stored,
        "chain_ok": chain_ok,
    }
    with open(os.path.join(args.out_dir, "events_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Summary JSON → {args.out_dir}/events_summary.json")


if __name__ == "__main__":
    main()
