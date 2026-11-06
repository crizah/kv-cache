#!/usr/bin/env python3
"""find_msp.py – Saturation-point (MSP) finder for a single SGLang node.

Sends Poisson-arrival cache-hit workload directly to nodeA at increasing
rates and finds the highest rate that keeps p95 TTFT under the SLO.
Node addresses and ports are read from config.json.

Usage:
  # generate and store the L documents only (run once per prefix length)
  python find_msp.py --length 8192 --gen-only

  # full run for one prefix length (auto-generates docs if missing)
  python find_msp.py --length 8192

  # explicit config path or non-default node
  python find_msp.py --config /path/to/config.json --node-id B --length 8192

Flags:
  --config       path to config.json (default: ../config.json)
  --node-id      which node entry to target (default: A)
  --length       prefix length to test; one of 2048, 8192, 16384, 24576
  --gen-only     generate and save L documents, then exit
  --l-docs-dir   where L documents live (default: phase-1/data)
  --out-dir      where to write results (default: phase-1/results)
  --seed         rng seed (default: 42)

  - L documents write to phase-1/data/L_docs_{length}.json   // we need to mount this to the host
  - Results table writes to phase-1/results/msp_table_{length}.json // and this
  - MSP number writes to phase-1/results/msp_{length}.json // and this
"""

import argparse
import json
import math
import os
import re
import random
import sys
import threading
import time
from pathlib import Path

import requests

# ── constants ─────────────────────────────────────────────────────────────────
L_DOCS          = 25       # number of cached documents to keep warm on nodeA
SUFFIX_TOKENS   = 128      # fresh suffix appended to every generated request
OUTPUT_TOKENS   = 256      # max_new_tokens for every request
PAGE_SIZE       = 64       # must match --page-size on the SGLang server
TOKEN_MIN       = 100
TOKEN_MAX       = 50_000

STEP_DURATION_S = 120      # seconds to send at each rate level
REST_S          = 30       # rest between steps to let the queue drain
START_RATE      = 0.25     # initial offered rate (req/s)
RATE_MULTIPLIER = 1.25     # multiply rate by this each passing step
TTFT_SLO_S      = 10.0     # p95 TTFT must stay below this (seconds)
E2E_SLO_S       = 30.0     # p95 end-to-end latency must stay below this (seconds)
MIN_FINISH_RATIO = 0.95    # fraction of sent requests that must complete
# ─────────────────────────────────────────────────────────────────────────────


def rand_ids(rng: random.Random, n: int) -> list[int]:
    return [rng.randint(TOKEN_MIN, TOKEN_MAX) for _ in range(n)]


def load_tokenizer_path(config_path: str) -> str:
    with open(config_path) as f:
        cfg = json.load(f)
    model = cfg["model"]
    rel = cfg["models"][model]["tokenizer_path"]
    # path in config.json is relative to the repo root (parent of config.json)
    return str(Path(config_path).parent / rel)


def flush_cache(sglang_url: str):
    requests.post(f"{sglang_url}/flush_cache", timeout=10).raise_for_status()


def waiting_queue(sglang_url: str) -> int:
    """Read num_waiting_reqs from SGLang's Prometheus /metrics endpoint."""
    r = requests.get(f"{sglang_url}/metrics", timeout=5)
    r.raise_for_status()
    m = re.search(r"sglang:num_waiting_reqs\s+([\d.]+)", r.text)
    return int(float(m.group(1))) if m else 0


# ── document generation ───────────────────────────────────────────────────────

def docs_path(l_docs_dir: str, length: int) -> Path:
    return Path(l_docs_dir) / f"L_docs_{length}.json"


def generate_docs(length: int, l_docs_dir: str, tokenizer_path: str, seed: int) -> list[dict]:
    from tokenizers import Tokenizer
    tk = Tokenizer.from_file(tokenizer_path)
    rng = random.Random(seed)
    docs = []
    for _ in range(L_DOCS):
        ids = rand_ids(rng, length)
        text = tk.decode(ids)
        docs.append({"ids": ids, "text": text})
    p = docs_path(l_docs_dir, length)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(docs, f)
    print(f"Saved {L_DOCS} documents ({length} tokens each) → {p}")
    return docs


def load_docs(length: int, l_docs_dir: str, tokenizer_path: str, seed: int) -> list[dict]:
    p = docs_path(l_docs_dir, length)
    if p.exists():
        with open(p) as f:
            docs = json.load(f)
        print(f"Loaded {len(docs)} documents from {p}")
        return docs
    print(f"Documents not found at {p}, generating…")
    return generate_docs(length, l_docs_dir, tokenizer_path, seed)


# ── warm-up ───────────────────────────────────────────────────────────────────

def warm_node(agent_url: str, docs: list[dict], rng: random.Random):
    """Send each of the L documents once so they're in nodeA's KV cache."""
    print(f"Warming nodeA with {len(docs)} documents…")
    for i, doc in enumerate(docs):
        suffix = rand_ids(rng, SUFFIX_TOKENS)
        payload = {
            "input_ids": doc["ids"] + suffix,
            "sampling_params": {"temperature": 0, "max_new_tokens": OUTPUT_TOKENS},
            "stream": False,
        }
        requests.post(f"{agent_url}/generate", json=payload, timeout=180).raise_for_status()
        print(f"  warmed {i + 1}/{len(docs)}", end="\r", flush=True)
    print()


# ── single-request worker ─────────────────────────────────────────────────────

def _send_one(agent_url: str, doc: dict, seed: int, out: list, slot: int):
    """Send one streaming request; store (ttft, e2e) in out[slot], or None on failure."""
    rng = random.Random(seed)
    suffix = rand_ids(rng, SUFFIX_TOKENS)
    payload = {
        "input_ids": doc["ids"] + suffix,
        "sampling_params": {"temperature": 0, "max_new_tokens": OUTPUT_TOKENS},
        "stream": True,
    }
    t0 = time.perf_counter()
    ttft = None
    t_last = None
    try:
        with requests.post(f"{agent_url}/generate", json=payload, stream=True, timeout=180) as resp:
            resp.raise_for_status()
            for raw in resp.iter_lines():
                if not raw:
                    continue
                now = time.perf_counter()
                if ttft is None:
                    ttft = now - t0
                t_last = now
    except Exception:
        pass
    e2e = (t_last - t0) if t_last is not None else None
    out[slot] = (ttft, e2e)


# ── one rate step ─────────────────────────────────────────────────────────────

def run_step(agent_url: str, sglang_url: str, docs: list[dict],
             rate: float, rng: random.Random) -> dict:
    """Run for STEP_DURATION_S at `rate` req/s with Poisson arrivals.

    Returns a dict with: rate, sent, finished, finish_ratio,
    p95_ttft_s, q_start, q_end, passed.
    """
    deadline = time.time() + STEP_DURATION_S
    results: list = []
    threads: list[threading.Thread] = []
    idx = 0

    q_start = waiting_queue(sglang_url)

    while time.time() < deadline:
        doc = docs[rng.randrange(len(docs))]
        results.append(None)
        t = threading.Thread(
            target=_send_one,
            args=(agent_url, doc, rng.randint(0, 2**31), results, idx),
            daemon=True,
        )
        t.start()
        threads.append(t)
        idx += 1

        # exponential inter-arrival time for Poisson process
        gap = -math.log(max(rng.random(), 1e-9)) / rate
        wake = time.time() + gap
        now = time.time()
        if wake > now:
            time.sleep(wake - now)

    # wait up to STEP_DURATION_S for in-flight requests to finish
    for t in threads:
        t.join(timeout=STEP_DURATION_S)

    q_end = waiting_queue(sglang_url)

    finished = [v for v in results if v is not None]
    sent = len(results)
    finish_ratio = len(finished) / sent if sent else 0.0

    def p95_of(vals):
        s = sorted(v for v in vals if v is not None)
        return s[int(0.95 * len(s))] if s else None

    p95_ttft = p95_of(v[0] for v in finished)
    p95_e2e  = p95_of(v[1] for v in finished)

    # queue growth is the primary saturation signal
    passed = (
        finish_ratio >= MIN_FINISH_RATIO
        and q_end <= q_start
    )

    return {
        "rate": round(rate, 4),
        "sent": sent,
        "finished": len(finished),
        "finish_ratio": round(finish_ratio, 3),
        "p95_ttft_s": round(p95_ttft, 3) if p95_ttft is not None else None,
        "p95_e2e_s":  round(p95_e2e,  3) if p95_e2e  is not None else None,
        "q_start": q_start,
        "q_end": q_end,
        "passed": passed,
    }


# ── main ──────────────────────────────────────────────────────────────────────

def load_node_urls(config_path: str, node_id: str, host_override: str | None = None) -> tuple[str, str]:
    """Return (agent_url, sglang_url) for the given node id from config.json.
    Pass host_override='localhost' when running on the same container as the node.
    """
    with open(config_path) as f:
        cfg = json.load(f)
    nodes = {n["id"]: n for n in cfg["nodes"]}
    if node_id not in nodes:
        raise SystemExit(f"Node id {node_id!r} not found in config. Available: {list(nodes)}")
    n = nodes[node_id]
    host = host_override if host_override else n["ip"]
    agent_url  = f"http://{host}:{n['http']}"
    sglang_url = f"http://{host}:{n['sglang']}"
    return agent_url, sglang_url


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    here = Path(__file__).parent.resolve()

    ap.add_argument("--config", default=str(here / "../config.json"),
                    help="Path to config.json (default: ../config.json relative to this script)")
    ap.add_argument("--node-id", default="A", dest="node_id",
                    help="Node id to target from config (default: A)")
    ap.add_argument("--host", default=None,
                    help="Override the node IP from config (e.g. localhost when running on the same container)")
    ap.add_argument("--length", type=int, required=True, choices=[2048, 8192, 16384, 24576],
                    help="Prefix length to test (tokens)")
    ap.add_argument("--gen-only", action="store_true",
                    help="Only generate and save L documents; do not touch the node")
    ap.add_argument("--l-docs-dir", default=str(here / "data"),
                    help="Directory to store/load L documents (default: phase-1/data/)")
    ap.add_argument("--out-dir", default=str(here / "results"),
                    help="Directory to write results (default: phase-1/results/)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    tokenizer_path = load_tokenizer_path(args.config)

    if args.gen_only:
        generate_docs(args.length, args.l_docs_dir, tokenizer_path, args.seed)
        return

    docs = load_docs(args.length, args.l_docs_dir, tokenizer_path, args.seed)
    rng = random.Random(args.seed)

    agent_url, sglang_url = load_node_urls(args.config, args.node_id, args.host)
    print(f"Targeting node {args.node_id!r}: agent={agent_url}  sglang={sglang_url}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    table_path = out_dir / f"msp_table_{args.length}.json"
    msp_path   = out_dir / f"msp_{args.length}.json"

    flush_cache(sglang_url)
    warm_node(agent_url, docs, rng)

    rows: list[dict] = []
    rate = START_RATE
    last_passing_rate: float | None = None

    header = f"{'rate':>8}  {'sent':>5}  {'fin':>5}  {'ratio':>6}  {'p95_ttft':>10}  {'p95_e2e':>9}  {'q_start':>7}  {'q_end':>6}  {'':>5}"
    print(f"\n{header}")
    print("-" * len(header))

    while True:
        row = run_step(agent_url, sglang_url, docs, rate, rng)
        rows.append(row)

        ttft_str = f"{row['p95_ttft_s']:.2f}s" if row["p95_ttft_s"] is not None else "   N/A"
        e2e_str  = f"{row['p95_e2e_s']:.2f}s"  if row["p95_e2e_s"]  is not None else "   N/A"
        result_str = "PASS" if row["passed"] else "FAIL"
        print(
            f"{row['rate']:>8.3f}  {row['sent']:>5}  {row['finished']:>5}  "
            f"{row['finish_ratio']:>6.3f}  {ttft_str:>10}  {e2e_str:>9}  "
            f"{row['q_start']:>7}  {row['q_end']:>6}  {result_str:>5}"
        )

        with open(table_path, "w") as f:
            json.dump(rows, f, indent=2)

        if row["passed"]:
            last_passing_rate = rate
            print(f"  → resting {REST_S}s before next step…")
            time.sleep(REST_S)
            rate = round(rate * RATE_MULTIPLIER, 4)
        else:
            if last_passing_rate is None:
                print("Failed at the starting rate. Node may already be overloaded.")
                sys.exit(1)

            print(f"\nFirst failure at {rate:.3f} req/s.")
            print(f"Confirming MSP = {last_passing_rate:.3f} req/s…")
            time.sleep(REST_S)

            confirm = run_step(agent_url, sglang_url, docs, last_passing_rate, rng)
            confirm["note"] = "confirmation"
            rows.append(confirm)

            with open(table_path, "w") as f:
                json.dump(rows, f, indent=2)

            if not confirm["passed"]:
                prev_rate = round(last_passing_rate / RATE_MULTIPLIER, 4)
                print(f"Confirmation failed. Dropping one step to {prev_rate:.3f} req/s.")
                last_passing_rate = prev_rate

            msp = {"length": args.length, "msp_req_per_s": last_passing_rate}
            with open(msp_path, "w") as f:
                json.dump(msp, f, indent=2)

            print(f"\nMSP (length={args.length}): {last_passing_rate:.3f} req/s")
            print(f"Results table → {table_path}")
            print(f"MSP           → {msp_path}")
            break


if __name__ == "__main__":
    main()
