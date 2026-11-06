#!/usr/bin/env python3
"""add_load.py – Maintain a steady load on a node at a fraction of its MSP.

Warms the node with the L cached documents, then holds a continuous
Poisson-arrival load at (saturation/100) * MSP req/s until interrupted.
Every request reuses one of the L cached prefixes plus a fresh 128-token
suffix, so the prefixes stay hot and don't get evicted.

Run find_msp.py first to establish the MSP values, then fill in MSP_TABLE.

Usage:
  python add_load.py --length 8192 --saturation 75

Flags:
  --config      path to config.json (default: ../config.json relative to script)
  --node-id     node entry to target (default: A)
  --length      prefix length; one of 2048, 8192, 16384, 24576
  --saturation  load as a percentage of MSP, e.g. 75 for 75% (default: 75)
  --l-docs-dir  directory where L documents are stored (default: phase-1/data/)
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
import requests

# ── MSP table ─────────────────────────────────────────────────────────────────
# TODO: fill these in after running find_msp.py for each prefix length
MSP_TABLE: dict[int, float | None] = {
    2048:  None,
    8192:  None,
    16384: None,
    24576: None,
}
# ─────────────────────────────────────────────────────────────────────────────

SUFFIX_TOKENS  = 128
OUTPUT_TOKENS  = 256
TOKEN_MIN      = 100
TOKEN_MAX      = 50_000

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


def load_docs(length: int, l_docs_dir: str) -> list[dict]:
    p = Path(l_docs_dir) / f"L_docs_{length}.json"
    if not p.exists():
        raise SystemExit(
            f"L documents not found at {p}. Run find_msp.py --length {length} --gen-only first."
        )
    with open(p) as f:
        docs = json.load(f)
    print(f"Loaded {len(docs)} documents from {p}")
    return docs


def warm_node(agent_url: str, docs: list[dict], rng: random.Random):
    print(f"Warming node with {len(docs)} documents…")
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


async def _fire(session: aiohttp.ClientSession, agent_url: str, doc: dict,
               seed: int, counters: dict):
    rng = random.Random(seed)
    suffix = rand_ids(rng, SUFFIX_TOKENS)
    payload = {
        "input_ids": doc["ids"] + suffix,
        "sampling_params": {"temperature": 0, "max_new_tokens": OUTPUT_TOKENS},
        "stream": False,
    }
    try:
        async with session.post(f"{agent_url}/generate", json=payload) as resp:
            resp.raise_for_status()
            await resp.read()
        counters["ok"] += 1
    except Exception:
        counters["err"] += 1


async def _maintain_load_async(agent_url: str, docs: list[dict], rate: float, rng: random.Random):
    global _stop
    _stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop.set)

    counters = {"ok": 0, "err": 0}
    t_start = time.perf_counter()
    print(f"Sending at {rate:.3f} req/s — Ctrl+C to stop\n")

    connector = aiohttp.TCPConnector(limit=0)
    timeout   = aiohttp.ClientTimeout(total=300)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        while not _stop.is_set():
            doc  = docs[rng.randrange(len(docs))]
            seed = rng.randint(0, 2**31)
            asyncio.create_task(_fire(session, agent_url, doc, seed, counters))

            gap       = -math.log(max(rng.random(), 1e-9)) / rate
            remaining = gap
            if remaining > 0:
                try:
                    await asyncio.wait_for(_stop.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass

    elapsed = time.perf_counter() - t_start
    total = counters["ok"] + counters["err"]
    print(f"\nStopped. {total} sent ({counters['ok']} ok, {counters['err']} err) "
          f"over {elapsed:.1f}s — actual rate {total / elapsed:.3f} req/s")


def maintain_load(agent_url: str, docs: list[dict], rate: float, rng: random.Random):
    asyncio.run(_maintain_load_async(agent_url, docs, rate, rng))


def main():
    here = Path(__file__).parent.resolve()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(here / "../config.json"),
                    help="Path to config.json (default: ../config.json relative to script)")
    ap.add_argument("--node-id", default="A", dest="node_id",
                    help="Node id to target from config (default: A)")
    ap.add_argument("--host", default=None,
                    help="Override the node IP from config (e.g. localhost when running on the same container)")
    ap.add_argument("--length", type=int, required=True, choices=[2048, 8192, 16384, 24576],
                    help="Prefix length being tested (tokens)")
    ap.add_argument("--saturation", type=int, default=75,
                    help="Load as a percentage of MSP, e.g. 75 for 75%% (default: 75)")
    ap.add_argument("--l-docs-dir", default=str(here / "data"),
                    help="Directory where L documents are stored (default: phase-1/data/)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    msp = MSP_TABLE.get(args.length)
    if msp is None:
        raise SystemExit(
            f"MSP for length={args.length} is not set in MSP_TABLE. "
            f"Run find_msp.py --length {args.length} and fill it in."
        )

    rate = (args.saturation / 100.0) * msp
    print(f"length={args.length}  MSP={msp:.3f} req/s  "
          f"saturation={args.saturation}%  target rate={rate:.3f} req/s")

    agent_url, _ = load_node_urls(args.config, args.node_id, args.host)
    print(f"Node {args.node_id!r}: agent={agent_url}")

    docs = load_docs(args.length, args.l_docs_dir)
    rng  = random.Random(args.seed)

    warm_node(agent_url, docs, rng)

    maintain_load(agent_url, docs, rate, rng)


if __name__ == "__main__":
    main()
