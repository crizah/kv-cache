#!/usr/bin/env python3
"""probe_health.py – record health responses from both nodes.

Runs on: machine B (hits both nodes over the network)

Steps:
  1. GET /health on both nodes while idle – save baseline.
  2. Start 8 streaming decode jobs on A.
  3. Poll /health on both every second for 20 s – save under load.

Hits the peer_nixl_backend meta server's /health endpoint (not SGLang's
native /health, which returns only "ok").

Usage:
  python probe_health.py \\
    --a http://<A_HOST>:30000 \\
    --b http://<B_HOST>:30001 \\
    --a-meta http://<A_HOST>:8765 \\
    --b-meta http://<B_HOST>:8766 \\
    --out-dir results
"""

import argparse
import json
import os
import random
import threading
import time

import requests


TOKEN_ID_MIN = 100
TOKEN_ID_MAX = 50_000


def rand_ids(rng: random.Random, n: int) -> list:
    return [rng.randint(TOKEN_ID_MIN, TOKEN_ID_MAX) for _ in range(n)]


def get_health(meta_url: str) -> dict:
    try:
        r = requests.get(f"{meta_url}/health", timeout=3)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def stream_decode_loop(url: str, rng: random.Random, stop_evt: threading.Event):
    """Keep 8 streaming decode jobs running on A."""
    while not stop_evt.is_set():
        ids = rand_ids(rng, 128)
        payload = {
            "input_ids": ids,
            "sampling_params": {"temperature": 0, "max_new_tokens": 512, "ignore_eos": True},
            "stream": True,
        }
        try:
            with requests.post(f"{url}/generate", json=payload,
                               stream=True, timeout=120) as resp:
                for _ in resp.iter_lines():
                    if stop_evt.is_set():
                        break
        except Exception:
            time.sleep(0.1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True, help="A's SGLang HTTP URL")
    ap.add_argument("--b", required=True, help="B's SGLang HTTP URL")
    ap.add_argument("--a-meta", required=True, dest="a_meta")
    ap.add_argument("--b-meta", required=True, dest="b_meta")
    ap.add_argument("--out-dir", default="results", dest="out_dir")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    out_path = os.path.join(args.out_dir, "health.jsonl")

    records = []

    def snap(phase: str):
        ts = time.time()
        ha = get_health(args.a_meta)
        hb = get_health(args.b_meta)
        entry = {"ts": ts, "phase": phase, "A": ha, "B": hb}
        records.append(entry)
        return entry

    # --- baseline (idle) ---
    print("Baseline (idle)…")
    for _ in range(3):
        e = snap("idle")
        print(f"  A={e['A']}  B={e['B']}")
        time.sleep(0.5)

    # --- under load ---
    print("Starting 8 decode streams on A…")
    stop_evt = threading.Event()
    rng = random.Random(55)
    threads = [threading.Thread(target=stream_decode_loop,
                                args=(args.a, random.Random(rng.randint(0, 2**32)), stop_evt),
                                daemon=True)
               for _ in range(8)]
    for t in threads:
        t.start()

    time.sleep(2)  # let streams get going

    print("Polling health every 1s for 20s…")
    for i in range(20):
        e = snap("loaded")
        print(f"  t={i+1:2d}s  A={e['A']}  B={e['B']}")
        time.sleep(1)

    stop_evt.set()

    with open(out_path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    print(f"\nHealth records → {out_path}")

    # verify required fields are present
    required = {"num_running_reqs", "num_waiting_reqs", "num_tokens", "max_total_num_tokens"}
    missing = set()
    for rec in records:
        for node_data in (rec["A"], rec["B"]):
            for field in required:
                if field not in node_data:
                    missing.add(field)

    if missing:
        print(f"WARNING: missing fields in health responses: {missing}")
        print("  Check that peer_nixl_backend._scrape_health() regex patterns match SGLang's /metrics output")
    else:
        print("All required health fields present.")


if __name__ == "__main__":
    main()
