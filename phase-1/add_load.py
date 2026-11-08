#!/usr/bin/env python3
"""add_load.py – Maintain a steady Poisson-arrival load on a single SGLang node.

Sends requests directly to SGLang's port. Streams every response, records
per-token gaps and writes them to a JSONL log file for TPOT analysis.

Run find_msp.py first to get the MSP values (stored in phase-1/results/).

Usage:
  python add_load.py --length 8192 --saturation 80
  python add_load.py --length 8192 --rate 0.60   # explicit rate, no MSP lookup

Flags:
  --config       path to config.json (default: ../config.json relative to script)
  --node-id      node entry to target (default: A)
  --host         override the node IP (e.g. localhost when on the same container)
  --length       prefix length; one of 2048, 8192, 16384, 24576
  --saturation   load as % of MSP, e.g. 80 for 80% (default: 80)
  --rate         explicit req/s — overrides --saturation and skips MSP lookup
  --msp-dir      directory containing msp_{length}.json files (default: phase-1/results/)
  --l-docs-dir   directory where L documents are stored (default: phase-1/data/)
  --doc-range    which docs to use as churn, e.g. 15:25 (default: 15:25)
  --gaps-log     path to write per-request JSONL gaps log (default: stdout suppressed)
  --seed         rng seed (default: 42)
"""

import argparse
import asyncio
import json
import math
import random
import signal
import sys
import time
from pathlib import Path

import aiohttp

# ── constants ─────────────────────────────────────────────────────────────────
SUFFIX_TOKENS = 128
OUTPUT_TOKENS = 256
TOKEN_MIN     = 100
TOKEN_MAX     = 50_000
# ─────────────────────────────────────────────────────────────────────────────

_stop: asyncio.Event | None = None


def rand_ids(rng: random.Random, n: int) -> list[int]:
    return [rng.randint(TOKEN_MIN, TOKEN_MAX) for _ in range(n)]


def load_node_urls(config_path: str, node_id: str, host_override: str | None = None) -> tuple[str, str]:
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


def load_msp(msp_dir: str, length: int) -> float:
    p = Path(msp_dir) / f"msp_{length}.json"
    if not p.exists():
        raise SystemExit(
            f"MSP file not found at {p}. Run find_msp.py --length {length} first."
        )
    with open(p) as f:
        return float(json.load(f)["msp_req_per_s"])


def load_docs(length: int, l_docs_dir: str) -> list[dict]:
    p = Path(l_docs_dir) / f"L_docs_{length}.json"
    if not p.exists():
        raise SystemExit(
            f"L documents not found at {p}. Run find_msp.py --length {length} --gen-only first."
        )
    with open(p) as f:
        docs = json.load(f)
    return docs


# ── log writer ────────────────────────────────────────────────────────────────

async def _log_writer(queue: asyncio.Queue, log_path: str):
    with open(log_path, "a") as f:
        while True:
            item = await queue.get()
            if item is None:
                break
            f.write(json.dumps(item) + "\n")
            f.flush()


# ── single streaming request ──────────────────────────────────────────────────

async def _fire(session: aiohttp.ClientSession, sglang_url: str, doc: dict,
               seed: int, counters: dict, log_queue: asyncio.Queue | None):
    rng = random.Random(seed)
    suffix = rand_ids(rng, SUFFIX_TOKENS)
    payload = {
        "input_ids": doc["ids"] + suffix,
        "sampling_params": {
            "temperature": 0,
            "max_new_tokens": OUTPUT_TOKENS,
            "ignore_eos": True,
        },
        "stream": True,
    }
    t_wall   = time.time()
    t0       = time.perf_counter()
    ttft     = None
    t_last   = None
    tokens   = 0
    gaps     = []
    prev_t   = None

    try:
        async with session.post(f"{sglang_url}/generate", json=payload) as resp:
            resp.raise_for_status()
            async for line in resp.content:
                line = line.strip()
                if not line:
                    continue
                now = time.perf_counter()
                if ttft is None:
                    ttft   = now - t0
                    prev_t = now
                else:
                    gaps.append(round(now - prev_t, 6))
                    prev_t = now
                tokens += 1
                t_last = now
        counters["ok"] += 1
    except Exception:
        counters["err"] += 1
        return

    if log_queue is not None and ttft is not None:
        await log_queue.put({
            "t_start":  round(t_wall,  3),
            "ttft":     round(ttft,    6),
            "t_end":    round(t_wall + (t_last - t0), 3),
            "tokens":   tokens,
            "gaps":     gaps,
        })


# ── main loop ─────────────────────────────────────────────────────────────────

async def _maintain_load_async(sglang_url: str, docs: list[dict], rate: float,
                                rng: random.Random, log_path: str | None):
    global _stop
    _stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop.set)

    log_queue: asyncio.Queue | None = None
    log_task = None
    if log_path:
        log_queue = asyncio.Queue()
        log_task = asyncio.create_task(_log_writer(log_queue, log_path))

    counters = {"ok": 0, "err": 0}
    t_start  = time.perf_counter()
    print(f"Sending at {rate:.3f} req/s — Ctrl+C to stop", flush=True)

    connector = aiohttp.TCPConnector(limit=0)
    timeout   = aiohttp.ClientTimeout(total=300)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        while not _stop.is_set():
            doc  = docs[rng.randrange(len(docs))]
            seed = rng.randint(0, 2**31)
            asyncio.create_task(
                _fire(session, sglang_url, doc, seed, counters, log_queue)
            )

            gap       = -math.log(max(rng.random(), 1e-9)) / rate
            remaining = gap
            if remaining > 0:
                try:
                    await asyncio.wait_for(_stop.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass

    if log_queue is not None:
        await log_queue.put(None)
        await log_task

    elapsed = time.perf_counter() - t_start
    total   = counters["ok"] + counters["err"]
    print(
        f"\nStopped. {total} sent ({counters['ok']} ok, {counters['err']} err) "
        f"over {elapsed:.1f}s — actual rate {total / elapsed:.3f} req/s"
    )


def maintain_load(sglang_url: str, docs: list[dict], rate: float,
                  rng: random.Random, log_path: str | None = None):
    asyncio.run(_maintain_load_async(sglang_url, docs, rate, rng, log_path))


# ── entry point ───────────────────────────────────────────────────────────────

def main():
    here = Path(__file__).parent.resolve()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config",    default=str(here / "../config.json"))
    ap.add_argument("--node-id",   default="A", dest="node_id")
    ap.add_argument("--host",      default=None,
                    help="Override node IP from config")
    ap.add_argument("--length",    type=int, required=True, choices=[2048, 8192, 16384, 24576])
    ap.add_argument("--saturation", type=int, default=80,
                    help="Load as %% of MSP (default: 80); ignored if --rate is set")
    ap.add_argument("--rate",      type=float, default=None,
                    help="Explicit req/s — overrides --saturation")
    ap.add_argument("--msp-dir",   default=str(here / "results"), dest="msp_dir")
    ap.add_argument("--l-docs-dir", default=str(here / "data"), dest="l_docs_dir")
    ap.add_argument("--doc-range", default="15:25", dest="doc_range",
                    help="Slice of docs to use as churn, e.g. 15:25 (default: 15:25)")
    ap.add_argument("--gaps-log",  default=None, dest="gaps_log",
                    help="Path to write per-request JSONL gaps log")
    ap.add_argument("--seed",      type=int, default=42)
    args = ap.parse_args()

    if args.rate is not None:
        rate = args.rate
        print(f"length={args.length}  rate={rate:.3f} req/s (explicit)")
    else:
        msp  = load_msp(args.msp_dir, args.length)
        rate = (args.saturation / 100.0) * msp
        print(f"length={args.length}  MSP={msp:.3f} req/s  "
              f"saturation={args.saturation}%  target rate={rate:.3f} req/s")

    _, sglang_url = load_node_urls(args.config, args.node_id, args.host)
    print(f"Node {args.node_id!r}: sglang={sglang_url}")

    all_docs = load_docs(args.length, args.l_docs_dir)
    start_s, end_s = args.doc_range.split(":")
    docs = all_docs[int(start_s):int(end_s)]
    if not docs:
        raise SystemExit(f"doc-range {args.doc_range!r} produced an empty doc list")
    print(f"Loaded {len(docs)} churn documents (indices {args.doc_range})")

    rng = random.Random(args.seed)
    maintain_load(sglang_url, docs, rate, rng, args.gaps_log)


if __name__ == "__main__":
    main()
