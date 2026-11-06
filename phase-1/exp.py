#!/usr/bin/env python3
"""exp.py – Measure proxy routing performance for a single prefix length.

Sends 75 requests through the proxy (50 cached-doc + 25 new), measures
TTFT, end-to-end latency, TPOT, and cache hit rate per request, and
reports a summary at the end.

The proxy must be running and add_load.py must be holding nodeA at the
desired saturation level before this script is started.

Usage:
  python exp.py --length 8192

Flags:
  --config      path to config.json (default: ../config.json relative to script)
  --proxy-host  host where the proxy is reachable (default: localhost)
  --length      prefix length being tested; one of 2048, 8192, 16384, 24576
  --l-docs-dir  directory where L documents are stored (default: phase-1/data/)
  --out-dir     directory to write results (default: phase-1/results/)
  --seed        rng seed for suffix generation (default: 42)
"""

import argparse
import json
import random
import statistics
import time
import uuid
from pathlib import Path

import requests

SUFFIX_TOKENS  = 128
OUTPUT_TOKENS  = 256
TOKEN_MIN      = 100
TOKEN_MAX      = 50_000

TOTAL_REQUESTS = 75
CACHED_REQUESTS = 50   # use L docs + fresh suffix; sent through proxy
NEW_REQUESTS    = 25   # random tokens of length prefix_length + suffix_length


def rand_ids(rng: random.Random, n: int) -> list[int]:
    return [rng.randint(TOKEN_MIN, TOKEN_MAX) for _ in range(n)]


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return json.load(f)


def proxy_url(cfg: dict, proxy_host: str) -> str:
    port = cfg["listen_addr"].lstrip(":")
    return f"http://{proxy_host}:{port}"


def load_docs(length: int, l_docs_dir: str) -> list[dict]:
    p = Path(l_docs_dir) / f"L_docs_{length}.json"
    if not p.exists():
        raise SystemExit(
            f"L documents not found at {p}. Run find_msp.py --length {length} --gen-only first."
        )
    with open(p) as f:
        docs = json.load(f)
    print(f"Loaded {len(docs)} L documents from {p}")
    return docs


def stream_request(url: str, payload: dict, req_id: str) -> dict:
    """POST one streaming request; return timing, cache, and routing info."""
    headers = {"X-Request-Id": req_id, "Content-Type": "application/json"}
    t_start = time.perf_counter()
    ttft    = None
    t_end   = None
    chunks  = 0
    last_meta: dict = {}
    served_by   = ""
    pulled_from = ""
    error = None

    try:
        with requests.post(url, json=payload, headers=headers, stream=True, timeout=180) as resp:
            resp.raise_for_status()
            served_by   = resp.headers.get("X-Served-By", "")
            pulled_from = resp.headers.get("X-Pulled-From", "")

            for raw in resp.iter_lines():
                if not raw:
                    continue
                now = time.perf_counter()
                if ttft is None:
                    ttft = now - t_start
                t_end = now
                chunks += 1

                line = raw.decode() if isinstance(raw, bytes) else raw
                try:
                    chunk = json.loads(line[6:] if line.startswith("data: ") else line)
                    if "meta_info" in chunk:
                        last_meta = chunk["meta_info"]
                except (json.JSONDecodeError, ValueError):
                    pass
    except Exception as e:
        error = str(e)

    e2e  = (t_end - t_start) if t_end is not None else None
    tpot = ((e2e - ttft) / (chunks - 1)) if (e2e is not None and ttft is not None and chunks > 1) else None

    cached_tokens = last_meta.get("cached_tokens", last_meta.get("num_cached_tokens", None))
    prompt_tokens = last_meta.get("prompt_tokens",  last_meta.get("num_prompt_tokens",  None))
    hit_rate = (cached_tokens / prompt_tokens) if (cached_tokens is not None and prompt_tokens and prompt_tokens > 0) else None

    return {
        "req_id":       req_id,
        "ttft_s":       round(ttft,  4) if ttft  is not None else None,
        "e2e_s":        round(e2e,   4) if e2e   is not None else None,
        "tpot_s":       round(tpot,  4) if tpot  is not None else None,
        "output_tokens": chunks,
        "cached_tokens": cached_tokens,
        "prompt_tokens": prompt_tokens,
        "hit_rate":      round(hit_rate, 4) if hit_rate is not None else None,
        "served_by":    served_by,
        "pulled_from":  pulled_from,
        "error":        error,
    }


def print_summary(rows: list[dict]):
    def p(label: str, vals: list[float], unit: str = "s"):
        if not vals:
            print(f"  {label}: n/a")
            return
        s = sorted(vals)
        p50 = statistics.median(s)
        p95 = s[int(0.95 * len(s))]
        print(f"  {label}: p50={p50:.3f}{unit}  p95={p95:.3f}{unit}  n={len(s)}")

    all_rows     = [r for r in rows if r["error"] is None]
    cached_rows  = [r for r in all_rows if r.get("kind") == "cached"]
    new_rows     = [r for r in all_rows if r.get("kind") == "new"]

    print(f"\n── Summary ({len(all_rows)}/{len(rows)} requests succeeded) ──")
    print("TTFT:")
    p("all    ", [r["ttft_s"] for r in all_rows if r["ttft_s"] is not None])
    p("cached ", [r["ttft_s"] for r in cached_rows if r["ttft_s"] is not None])
    p("new    ", [r["ttft_s"] for r in new_rows if r["ttft_s"] is not None])
    print("E2E:")
    p("all    ", [r["e2e_s"] for r in all_rows if r["e2e_s"] is not None])
    print("TPOT:")
    p("all    ", [r["tpot_s"] for r in all_rows if r["tpot_s"] is not None], unit="s/tok")

    hit_all    = [r["hit_rate"] for r in all_rows    if r["hit_rate"] is not None]
    hit_cached = [r["hit_rate"] for r in cached_rows if r["hit_rate"] is not None]
    if hit_all:
        print(f"Cache hit rate (all):    mean={statistics.mean(hit_all):.3f}")
    if hit_cached:
        print(f"Cache hit rate (cached): mean={statistics.mean(hit_cached):.3f}")

    nodes = {}
    for r in all_rows:
        n = r["served_by"] or "unknown"
        nodes[n] = nodes.get(n, 0) + 1
    if nodes:
        print("Served by: " + "  ".join(f"{k}={v}" for k, v in sorted(nodes.items())))

    pulls = sum(1 for r in all_rows if r["pulled_from"])
    print(f"Pulls: {pulls}/{len(all_rows)}")


def main():
    here = Path(__file__).parent.resolve()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(here / "../config.json"),
                    help="Path to config.json (default: ../config.json relative to script)")
    ap.add_argument("--proxy-host", default="localhost", dest="proxy_host",
                    help="Host where the proxy is reachable (default: localhost)")
    ap.add_argument("--length", type=int, required=True, choices=[2048, 8192, 16384, 24576],
                    help="Prefix length being tested (tokens)")
    ap.add_argument("--l-docs-dir", default=str(here / "data"),
                    help="Directory where L documents are stored (default: phase-1/data/)")
    ap.add_argument("--out-dir", default=str(here / "results"),
                    help="Directory to write results (default: phase-1/results/)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    cfg   = load_config(args.config)
    purl  = proxy_url(cfg, args.proxy_host)
    docs  = load_docs(args.length, args.l_docs_dir)
    rng   = random.Random(args.seed)

    print(f"Proxy: {purl}")
    print(f"Length={args.length}  cached={CACHED_REQUESTS}  new={NEW_REQUESTS}  total={TOTAL_REQUESTS}")

    from tokenizers import Tokenizer
    tk_path = cfg["models"][cfg["model"]]["tokenizer_path"]
    tk = Tokenizer.from_file(tk_path)

    # build request list: 50 cached + 25 new, shuffled
    reqs = []
    for doc in rng.sample(docs, CACHED_REQUESTS):
        reqs.append({
            "kind": "cached",
            "payload": {
                "text": doc["text"],
                "sampling_params": {"temperature": 0, "max_new_tokens": OUTPUT_TOKENS},
                "stream": True,
                "stream_interval": 1,
            },
        })
    for _ in range(NEW_REQUESTS):
        ids = rand_ids(rng, args.length + SUFFIX_TOKENS)
        reqs.append({
            "kind": "new",
            "payload": {
                "text": tk.decode(ids),
                "sampling_params": {"temperature": 0, "max_new_tokens": OUTPUT_TOKENS},
                "stream": True,
                "stream_interval": 1,
            },
        })
    rng.shuffle(reqs)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    out_path = out_dir / f"exp_{args.length}_{ts}.jsonl"

    rows = []
    generate_url = f"{purl}/generate"

    with open(out_path, "w") as f:
        for i, req in enumerate(reqs):
            req_id = str(uuid.uuid4())
            print(f"[{i+1:>3}/{TOTAL_REQUESTS}] kind={req['kind']}", end="  ", flush=True)
            result = stream_request(generate_url, req["payload"], req_id)
            result["kind"] = req["kind"]
            result["length"] = args.length
            rows.append(result)
            f.write(json.dumps(result) + "\n")
            f.flush()

            status = f"ttft={result['ttft_s']}s  e2e={result['e2e_s']}s  hit={result['hit_rate']}  by={result['served_by']}"
            if result["pulled_from"]:
                status += f"  pull={result['pulled_from']}"
            if result["error"]:
                status = f"ERROR: {result['error']}"
            print(status)

    print_summary(rows)
    print(f"\nResults → {out_path}")


if __name__ == "__main__":
    main()
