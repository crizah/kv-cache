#!/usr/bin/env python3
"""interference.py – measure how pulling from A slows A's decode throughput.

Runs on: machine B (drives both A and B over the network)

Keeps 8 streaming decode jobs running on A continuously.
Records inter-token gaps (client-side proxy for TPOT).
At each phase, B sends pulls at a controlled rate by flushing its cache and
re-issuing the same prefix so B must re-pull from A.

Usage:
  python interference.py \\
    --a http://<A_HOST>:30000 \\
    --b http://<B_HOST>:30001 \\
    --a-meta http://<A_HOST>:8765 \\
    --prefix-len 4096 \\
    --streams 8 --phase-secs 60 \\
    --rates 0,0.5,1,2 \\
    --out results/interference.csv
"""

import argparse
import csv
import json
import math
import random
import sys
import threading
import time
from urllib.request import urlopen
import requests

PAGE_SIZE = 64
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
    with urlopen(f"{meta_url}/keys?since={since}", timeout=5) as r:
        return json.loads(r.read())["next"]


def wait_for_keys(meta_url: str, since: int, need: int, timeout: float = 30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if peer_keys_since(meta_url, since) - since >= need:
            return
        time.sleep(0.05)
    raise TimeoutError(f"A did not publish {need} keys within {timeout}s")


class StreamDecoder(threading.Thread):
    """Continuously streams decode jobs from a node, recording inter-token gaps."""

    def __init__(self, url: str, stream_id: int, rng: random.Random, stop_evt: threading.Event):
        super().__init__(daemon=True)
        self.url = url
        self.stream_id = stream_id
        self.rng = rng
        self.stop_evt = stop_evt
        self.gaps: list = []   # (phase_tag, rate_tag, idx, gap_ms)
        self._phase = "baseline"
        self._rate = 0.0

    def set_phase(self, phase: str, rate: float):
        self._phase = phase
        self._rate = rate

    def run(self):
        token_idx = 0
        while not self.stop_evt.is_set():
            ids = rand_ids(self.rng, 128)
            payload = {
                "input_ids": ids,
                "sampling_params": {"temperature": 0, "max_new_tokens": 1024, "ignore_eos": True},
                "stream": True,
            }
            try:
                with requests.post(f"{self.url}/generate", json=payload,
                                   stream=True, timeout=300) as resp:
                    resp.raise_for_status()
                    last_t = None
                    for raw in resp.iter_lines():
                        if self.stop_evt.is_set():
                            break
                        if not raw:
                            continue
                        now = time.perf_counter()
                        line = raw.decode() if isinstance(raw, bytes) else raw
                        if line.startswith("data: ") or line.startswith("{"):
                            if last_t is not None:
                                gap_ms = (now - last_t) * 1e3
                                self.gaps.append((self._phase, self._rate,
                                                   self.stream_id, token_idx, gap_ms))
                                token_idx += 1
                            last_t = now
            except Exception:
                time.sleep(0.1)


class PullDriver(threading.Thread):
    """Issues pulls at rate R by flushing B's cache and resending the prefix."""

    def __init__(self, a_url: str, b_url: str, a_meta: str,
                 prefix_ids: list, suffix_len: int, rng: random.Random,
                 stop_evt: threading.Event):
        super().__init__(daemon=True)
        self.a_url = a_url
        self.b_url = b_url
        self.a_meta = a_meta
        self.prefix_ids = prefix_ids
        self.suffix_len = suffix_len
        self.rng = rng
        self.stop_evt = stop_evt
        self.rate: float = 0.0
        self.latencies: list = []   # (ts, latency_ms, bytes_read)
        self._rate_lock = threading.Lock()

    def set_rate(self, r: float):
        with self._rate_lock:
            self.rate = r

    def run(self):
        while not self.stop_evt.is_set():
            with self._rate_lock:
                r = self.rate
            if r <= 0:
                time.sleep(0.1)
                continue
            interval = 1.0 / r
            flush_cache(self.b_url)
            suf = rand_ids(self.rng, self.suffix_len)
            t0 = time.perf_counter()
            ts = time.time()
            payload = {
                "input_ids": self.prefix_ids + suf,
                "sampling_params": {"temperature": 0, "max_new_tokens": 1},
                "stream": False,
            }
            try:
                resp = requests.post(f"{self.b_url}/generate", json=payload, timeout=120)
                resp.raise_for_status()
            except Exception:
                pass
            lat_ms = (time.perf_counter() - t0) * 1e3
            self.latencies.append((ts, round(lat_ms, 3)))
            sleep = interval - (time.perf_counter() - t0)
            if sleep > 0:
                time.sleep(sleep)


def percentile(vals: list, p: float) -> float:
    if not vals:
        return float("nan")
    s = sorted(vals)
    idx = max(0, min(len(s) - 1, int(math.ceil(p / 100 * len(s))) - 1))
    return s[idx]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True)
    ap.add_argument("--b", required=True)
    ap.add_argument("--a-meta", required=True, dest="a_meta")
    ap.add_argument("--prefix-len", type=int, default=4096, dest="prefix_len")
    ap.add_argument("--streams", type=int, default=8)
    ap.add_argument("--phase-secs", type=int, default=60, dest="phase_secs")
    ap.add_argument("--rates", default="0,0.5,1,2")
    ap.add_argument("--out", default="results/interference.csv")
    args = ap.parse_args()

    rates = [float(x) for x in args.rates.split(",")]
    rng = random.Random(7)
    suffix_len = 128
    warm_in = 10

    import os; os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    # Warm one prefix P on A
    print(f"Warming prefix (L={args.prefix_len}) on A…")
    prefix_ids = rand_ids(rng, args.prefix_len)
    suf = rand_ids(rng, suffix_len)
    before = peer_key_count(args.a_meta)
    requests.post(f"{args.a}/generate", json={
        "input_ids": prefix_ids + suf,
        "sampling_params": {"temperature": 0, "max_new_tokens": 1},
    }, timeout=60)
    wait_for_keys(args.a_meta, before, args.prefix_len // PAGE_SIZE)
    print("Prefix published by A.")

    stop_evt = threading.Event()
    decoders = [StreamDecoder(args.a, i, random.Random(rng.randint(0, 2**32)),
                               stop_evt)
                for i in range(args.streams)]
    pull_driver = PullDriver(args.a, args.b, args.a_meta, prefix_ids, suffix_len,
                              random.Random(rng.randint(0, 2**32)), stop_evt)

    for d in decoders:
        d.start()
    pull_driver.start()

    all_phases = [("baseline_pre", 0.0)] + \
                 [(f"rate_{r}", r) for r in rates if r > 0] + \
                 [("baseline_post", 0.0)]

    phase_gaps: dict = {}
    phase_pulls: dict = {}

    for phase_name, rate in all_phases:
        print(f"\n--- Phase: {phase_name} (rate={rate}/s) warm-in={warm_in}s measured={args.phase_secs}s")
        pull_driver.set_rate(rate)
        for d in decoders:
            d.set_phase(phase_name, rate)

        snap_before = len(decoders[0].gaps) if decoders[0].gaps else 0
        pull_snap_before = len(pull_driver.latencies)

        # warm-in
        time.sleep(warm_in)
        measure_start = time.time()
        snap_measure = sum(len(d.gaps) for d in decoders)
        pull_snap_measure = len(pull_driver.latencies)

        # measure
        time.sleep(args.phase_secs)
        measure_end = time.time()

        pull_driver.set_rate(0.0)

        # collect gaps from measure window
        gaps_this_phase = []
        for d in decoders:
            for entry in d.gaps:
                p, r, sid, idx, gap = entry
                if p == phase_name:
                    gaps_this_phase.append(gap)
        phase_gaps[phase_name] = gaps_this_phase

        pulls_this_phase = pull_driver.latencies[pull_snap_measure:]
        phase_pulls[phase_name] = [x[1] for x in pulls_this_phase]

        if gaps_this_phase:
            print(f"  gaps collected: {len(gaps_this_phase)}")
            print(f"  TPOT p50={percentile(gaps_this_phase, 50):.1f}ms "
                  f"p95={percentile(gaps_this_phase, 95):.1f}ms "
                  f"p99={percentile(gaps_this_phase, 99):.1f}ms")
        if pulls_this_phase:
            print(f"  pulls: {len(pulls_this_phase)} achieved_rate={len(pulls_this_phase)/args.phase_secs:.2f}/s")

    stop_evt.set()

    # Write per-token CSV
    gap_fieldnames = ["phase", "rate", "stream", "idx", "gap_ms"]
    pull_fieldnames = ["phase", "rate", "latency_ms"]

    gap_path = args.out.replace(".csv", "_gaps.csv")
    pull_path = args.out.replace(".csv", "_pulls.csv")

    with open(gap_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=gap_fieldnames)
        w.writeheader()
        for d in decoders:
            for phase, rate, sid, idx, gap in d.gaps:
                w.writerow({"phase": phase, "rate": rate, "stream": sid, "idx": idx, "gap_ms": round(gap, 3)})

    with open(pull_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=pull_fieldnames)
        w.writeheader()
        for phase_name, rate in all_phases:
            for lat in phase_pulls.get(phase_name, []):
                w.writerow({"phase": phase_name, "rate": rate, "latency_ms": lat})

    # Summary JSON
    summary = {}
    baseline_gaps = phase_gaps.get("baseline_pre", [])
    for phase_name, rate in all_phases:
        g = phase_gaps.get(phase_name, [])
        pl = phase_pulls.get(phase_name, [])
        p50 = percentile(g, 50)
        p95 = percentile(g, 95)
        p99 = percentile(g, 99)
        ratio50 = p50 / percentile(baseline_gaps, 50) if baseline_gaps else float("nan")
        summary[phase_name] = {
            "rate": rate,
            "tpot_p50_ms": round(p50, 3),
            "tpot_p95_ms": round(p95, 3),
            "tpot_p99_ms": round(p99, 3),
            "ratio_to_baseline_p50": round(ratio50, 3),
            "pulls": len(pl),
            "achieved_rate": round(len(pl) / args.phase_secs, 3) if pl else 0,
        }

    summary_path = args.out.replace(".csv", "_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\nGaps → {gap_path}")
    print(f"Pulls → {pull_path}")
    print(f"Summary → {summary_path}")
    print("\nNOTE: measured with both servers sharing the same CPU and memory.")
    print("Summary:")
    for k, v in summary.items():
        print(f"  {k}: p50={v['tpot_p50_ms']}ms p95={v['tpot_p95_ms']}ms "
              f"ratio={v['ratio_to_baseline_p50']} pulls_achieved={v['achieved_rate']}/s")


if __name__ == "__main__":
    main()
