#!/usr/bin/env python3
"""sweep.py – TTFT break-even measurement across prefix lengths.

Runs on: machine B

Measures TTFT on B for three conditions per trial:
  recompute  fresh prefix + suffix sent only to B (cold cache)
  pull       prefix goes to A first; B pulls it, then receives prefix+new suffix
  local      immediately after pull: same prefix + another new suffix → B (hot cache)

Usage:
  python sweep.py \\
    --a http://<A_HOST>:30000 \\
    --b http://<B_HOST>:30001 \\
    --a-meta http://<A_HOST>:8765 \\
    --b-meta http://<B_HOST>:8766 \\
    --lengths 128,256,512,1024,2048,4096,8192,16384 \\
    --warmup 3 --trials 20 --suffix 128 --seed 42 \\
    --out results/sweep.csv
"""

import argparse
import csv
import json
import random
import sys
import time
from urllib.request import urlopen
from urllib.error import URLError
import requests

PAGE_SIZE = 64
# Token IDs in this range are safely away from special tokens (Qwen3-8B vocab ~152k).
TOKEN_ID_MIN = 100
TOKEN_ID_MAX = 50_000


def rand_ids(rng: random.Random, n: int) -> list:
    return [rng.randint(TOKEN_ID_MIN, TOKEN_ID_MAX) for _ in range(n)]


def flush_cache(url: str):
    requests.post(f"{url}/flush_cache", timeout=10)


def peer_key_count(meta_url: str) -> int:
    with urlopen(f"{meta_url}/keys?since=0", timeout=5) as r:
        return json.loads(r.read())["next"]


def peer_keys_since(meta_url: str, since: int) -> int:
    """Returns total key count (= new next value)."""
    with urlopen(f"{meta_url}/keys?since={since}", timeout=5) as r:
        return json.loads(r.read())["next"]


def wait_for_keys(meta_url: str, since: int, need: int, timeout: float = 30.0):
    """Block until A has published at least `need` new keys after `since`."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        nxt = peer_keys_since(meta_url, since)
        if nxt - since >= need:
            return
        time.sleep(0.05)
    raise TimeoutError(f"A did not publish {need} keys within {timeout}s (got {peer_keys_since(meta_url, since) - since})")


def stream_ttft(url: str, payload: dict):
    """POST to /generate with stream=True; return (ttft_ms, meta_info) from first chunk."""
    t0 = time.perf_counter()
    with requests.post(f"{url}/generate", json=payload, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        for raw in resp.iter_lines():
            if not raw:
                continue
            t1 = time.perf_counter()
            line = raw.decode() if isinstance(raw, bytes) else raw
            if line.startswith("data: "):
                chunk = json.loads(line[6:])
            else:
                try:
                    chunk = json.loads(line)
                except json.JSONDecodeError:
                    continue
            ttft_ms = (t1 - t0) * 1e3
            meta = chunk.get("meta_info", {})
            return ttft_ms, meta
    return None, {}


def generate_no_stream(url: str, payload: dict) -> dict:
    payload = dict(payload)
    payload.pop("stream", None)
    r = requests.post(f"{url}/generate", json=payload, timeout=120)
    r.raise_for_status()
    return r.json().get("meta_info", {})


def cached_tokens(meta: dict) -> int:
    for key in ("cached_tokens", "num_cached_tokens", "prefix_cache_len"):
        if key in meta:
            return int(meta[key])
    return -1


def match_pull_log(pull_log_path: str, ts: float, window: float = 5.0) -> dict:
    """Find the pull log entry closest in time to ts, within window seconds."""
    best = {}
    best_dt = window + 1
    try:
        with open(pull_log_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                dt = abs(rec.get("ts", 0) - ts)
                if dt < best_dt:
                    best_dt = dt
                    best = rec
    except FileNotFoundError:
        pass
    return best if best_dt <= window else {}


def make_payload(ids: list, suffix_ids: list, stream: bool = True) -> dict:
    return {
        "input_ids": ids + suffix_ids,
        "sampling_params": {"temperature": 0, "max_new_tokens": 1},
        "stream": stream,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True, help="A's SGLang HTTP URL")
    ap.add_argument("--b", required=True, help="B's SGLang HTTP URL")
    ap.add_argument("--a-meta", required=True, dest="a_meta")
    ap.add_argument("--b-meta", required=True, dest="b_meta")
    ap.add_argument("--lengths", default="128,256,512,1024,2048,4096,8192,16384")
    ap.add_argument("--warmup", type=int, default=3)
    ap.add_argument("--trials", type=int, default=20)
    ap.add_argument("--suffix", type=int, default=128)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="results/sweep.csv")
    args = ap.parse_args()

    lengths = [int(x) for x in args.lengths.split(",")]
    rng = random.Random(args.seed)

    import os; os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    pull_log_path = f"logs/B_pull.jsonl"

    fieldnames = ["ts", "L", "trial", "condition", "ttft_ms", "cached_tokens",
                  "prompt_tokens", "valid", "lookup_ms", "read_ms", "bytes_read", "pages"]

    with open(args.out, "w", newline="") as csvf:
        writer = csv.DictWriter(csvf, fieldnames=fieldnames)
        writer.writeheader()

        # one throwaway pull for connection setup
        print("Throwaway pull...")
        throw_ids = rand_ids(rng, 128)
        suf_ids = rand_ids(rng, args.suffix)
        before = peer_key_count(args.a_meta)
        generate_no_stream(args.a, {"input_ids": throw_ids + suf_ids,
                                    "sampling_params": {"temperature": 0, "max_new_tokens": 1}})
        try:
            wait_for_keys(args.a_meta, before, len(throw_ids) // PAGE_SIZE, timeout=20)
        except TimeoutError:
            pass
        generate_no_stream(args.b, {"input_ids": throw_ids + rand_ids(rng, args.suffix),
                                    "sampling_params": {"temperature": 0, "max_new_tokens": 1}})

        # randomize length order
        length_order = lengths[:]
        rng.shuffle(length_order)

        for L in length_order:
            n_pages = L // PAGE_SIZE
            print(f"\n=== L={L} tokens ({n_pages} pages) ===")
            flush_cache(args.a)
            flush_cache(args.b)

            all_trials = list(range(args.warmup + args.trials))
            conditions = ["recompute", "pull", "local"]

            for trial_idx in all_trials:
                warmup = trial_idx < args.warmup
                t_num = trial_idx - args.warmup

                for cond in conditions:
                    # Fresh token IDs for every trial/condition to avoid accidental cache hits.
                    prefix_ids = rand_ids(rng, L)
                    suf1 = rand_ids(rng, args.suffix)  # for A or recompute
                    suf2 = rand_ids(rng, args.suffix)  # for B pull
                    suf3 = rand_ids(rng, args.suffix)  # for B local

                    row = {"L": L, "trial": t_num, "condition": cond,
                           "ttft_ms": None, "cached_tokens": None, "prompt_tokens": None,
                           "valid": False, "lookup_ms": None, "read_ms": None,
                           "bytes_read": None, "pages": None, "ts": None}

                    if cond == "recompute":
                        t_send = time.time()
                        ttft, meta = stream_ttft(args.b, make_payload(prefix_ids, suf1))
                        ct = cached_tokens(meta)
                        pt = meta.get("prompt_tokens", L + args.suffix)
                        valid = (ct == 0)
                        row.update(ts=t_send, ttft_ms=round(ttft, 3) if ttft else None,
                                   cached_tokens=ct, prompt_tokens=pt,
                                   valid=valid and ttft is not None)

                    elif cond == "pull":
                        # send prefix to A first
                        before_keys = peer_key_count(args.a_meta)
                        generate_no_stream(args.a, {
                            "input_ids": prefix_ids + suf1,
                            "sampling_params": {"temperature": 0, "max_new_tokens": 1},
                        })
                        wait_for_keys(args.a_meta, before_keys, n_pages)
                        t_send = time.time()
                        ttft, meta = stream_ttft(args.b, make_payload(prefix_ids, suf2))
                        ct = cached_tokens(meta)
                        pt = meta.get("prompt_tokens", L + args.suffix)
                        valid = (ct == L)
                        pull_entry = match_pull_log(pull_log_path, t_send)
                        row.update(ts=t_send, ttft_ms=round(ttft, 3) if ttft else None,
                                   cached_tokens=ct, prompt_tokens=pt,
                                   valid=valid and ttft is not None,
                                   lookup_ms=pull_entry.get("lookup_ms"),
                                   read_ms=pull_entry.get("read_ms"),
                                   bytes_read=pull_entry.get("bytes"),
                                   pages=pull_entry.get("pages"))

                    elif cond == "local":
                        # same prefix; reuse what pull cached
                        t_send = time.time()
                        ttft, meta = stream_ttft(args.b, make_payload(prefix_ids, suf3))
                        ct = cached_tokens(meta)
                        pt = meta.get("prompt_tokens", L + args.suffix)
                        valid = (ct >= L)
                        row.update(ts=t_send, ttft_ms=round(ttft, 3) if ttft else None,
                                   cached_tokens=ct, prompt_tokens=pt,
                                   valid=valid and ttft is not None)

                    if not warmup:
                        writer.writerow(row)
                        csvf.flush()
                        status = "VALID" if row["valid"] else "INVALID"
                        print(f"  trial={t_num} {cond:12s} ttft={row['ttft_ms']:.1f}ms ct={row['cached_tokens']} [{status}]")

            # flush both caches before next length
            flush_cache(args.a)
            flush_cache(args.b)

    print(f"\nDone. Results written to {args.out}")
    _print_summary(args.out)


def _print_summary(path: str):
    import statistics
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            if r["valid"] == "True" and r["ttft_ms"]:
                rows.append(r)

    from collections import defaultdict
    groups = defaultdict(list)
    for r in rows:
        groups[(int(r["L"]), r["condition"])].append(float(r["ttft_ms"]))

    print(f"\n{'L':>7}  {'condition':12}  {'median_ms':>10}  {'p95_ms':>10}  {'n':>4}")
    for (L, cond), vals in sorted(groups.items()):
        vals.sort()
        p95 = vals[int(0.95 * len(vals))]
        print(f"{L:>7}  {cond:12}  {statistics.median(vals):>10.1f}  {p95:>10.1f}  {len(vals):>4}")


if __name__ == "__main__":
    main()
