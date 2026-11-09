#!/usr/bin/env python3
"""exp.py – Measure proxy routing performance across prefix lengths and saturation levels.

Loops over every (length, sat) cell. For sat > 0, starts add_load.py at the
appropriate rate, warms in, then sends 75 requests (50 cached + 25 new) through
the proxy. For sat == 0, runs with no background load.

Usage:
  python exp.py --lengths 2048,8192 --sat 0,80,100

Flags:
  --config      path to config.json (default: ../config.json relative to script)
  --proxy-host  host where the proxy is reachable (default: localhost)
  --lengths     comma-separated prefix lengths (default: 2048,8192,16384,24576)
  --sat         comma-separated saturation %% values (default: 0,40,80,100)
  --l-docs-dir  directory where L documents are stored (default: phase-1/data/)
  --out-dir     directory to write results (default: phase-1/results/)
  --seed        rng seed (default: 42)
"""

import argparse
import json
import random
import statistics
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import requests

SUFFIX_TOKENS   = 128
OUTPUT_TOKENS   = 256
TOKEN_MIN       = 100
TOKEN_MAX       = 50_000
WARM_IN_S       = 30

TOTAL_REQUESTS  = 20
CACHED_REQUESTS = 10
NEW_REQUESTS    = 10

MSP_TABLE = {
    2048:  2.3283,
    8192:  0.75,
    16384: 0.20,
    24576: 0.1875,
}


def rand_ids(rng: random.Random, n: int) -> list[int]:
    return [rng.randint(TOKEN_MIN, TOKEN_MAX) for _ in range(n)]


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return json.load(f)


def proxy_url(cfg: dict, proxy_host: str) -> str:
    port = cfg["listen_addr"].lstrip(":")
    return f"http://{proxy_host}:{port}"


def node_sglang_url(cfg: dict, host_override: str | None = None) -> str:
    nodes = {n["id"]: n for n in cfg["nodes"]}
    n = nodes["A"]
    host = host_override if host_override else n["ip"]
    return f"http://{host}:{n['sglang']}"


def all_sglang_urls(cfg: dict) -> list[str]:
    return [f"http://{n['ip']}:{n['sglang']}" for n in cfg["nodes"]]


def _wait_drained(url: str, timeout: int = 60):
    """Poll /get_server_info until running==0 or timeout."""
    import urllib.request, json as _json
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{url}/get_server_info", timeout=5) as r:
                info = _json.loads(r.read())
            if info.get("num_running_reqs", 0) == 0:
                return
        except Exception:
            pass
        time.sleep(1)
    print(f"  [warn] {url} still busy after {timeout}s drain, flushing anyway")


def flush_nodes(cfg: dict):
    """Wait for each node to drain, flush its KV cache, then let events propagate."""
    import urllib.request
    for url in all_sglang_urls(cfg):
        _wait_drained(url)
        req = urllib.request.Request(
            f"{url}/flush_cache", data=b"{}",
            headers={"Content-Type": "application/json"}, method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                r.read()
            print(f"  Flushed {url}")
        except Exception as e:
            print(f"  [warn] flush {url}: {e}")
    time.sleep(3)  # let BlockRemoved events reach the proxy


def warm_node_a(sglang_url: str, docs: list[dict]):
    """Send each cached doc directly to A's SGLang to populate its KV cache."""
    import urllib.request
    print(f"  Warming nodeA with {len(docs)} docs…", end="", flush=True)
    for doc in docs:
        payload = json.dumps({
            "text": doc["text"],
            "sampling_params": {"temperature": 0, "max_new_tokens": 1},
            "stream": False,
        }).encode()
        req = urllib.request.Request(
            f"{sglang_url}/generate", data=payload,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                r.read()
            print(".", end="", flush=True)
        except Exception as e:
            print(f"[warn: {e}]", end="", flush=True)
    print(" done")


def load_docs(length: int, l_docs_dir: str) -> list[dict]:
    p = Path(l_docs_dir) / f"L_docs_{length}.json"
    if not p.exists():
        raise SystemExit(
            f"L documents not found at {p}. Run find_msp.py --length {length} --gen-only first."
        )
    with open(p) as f:
        docs = json.load(f)
    return docs


def stream_request(url: str, payload: dict, req_id: str) -> dict:
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
        "req_id":        req_id,
        "ttft_s":        round(ttft,  4) if ttft  is not None else None,
        "e2e_s":         round(e2e,   4) if e2e   is not None else None,
        "tpot_s":        round(tpot,  4) if tpot  is not None else None,
        "output_tokens": chunks,
        "cached_tokens": cached_tokens,
        "prompt_tokens": prompt_tokens,
        "hit_rate":      round(hit_rate, 4) if hit_rate is not None else None,
        "served_by":     served_by,
        "pulled_from":   pulled_from,
        "error":         error,
    }


def print_summary(rows: list[dict], length: int, sat: int):
    def p(label: str, vals: list[float], unit: str = "s"):
        if not vals:
            print(f"  {label}: n/a")
            return
        s = sorted(vals)
        p50 = statistics.median(s)
        p95 = s[int(0.95 * len(s))]
        print(f"  {label}: p50={p50:.3f}{unit}  p95={p95:.3f}{unit}  n={len(s)}")

    all_rows    = [r for r in rows if r["error"] is None]
    cached_rows = [r for r in all_rows if r.get("kind") == "cached"]
    new_rows    = [r for r in all_rows if r.get("kind") == "new"]

    print(f"\n── Summary  length={length}  sat={sat}  ({len(all_rows)}/{len(rows)} ok) ──")
    print("TTFT:")
    p("all    ", [r["ttft_s"] for r in all_rows    if r["ttft_s"] is not None])
    p("cached ", [r["ttft_s"] for r in cached_rows if r["ttft_s"] is not None])
    p("new    ", [r["ttft_s"] for r in new_rows    if r["ttft_s"] is not None])
    print("E2E:")
    p("all    ", [r["e2e_s"]  for r in all_rows    if r["e2e_s"]  is not None])
    print("TPOT:")
    p("all    ", [r["tpot_s"] for r in all_rows    if r["tpot_s"] is not None], unit="s/tok")

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


class LoadProcess:
    """Runs add_load.py as a subprocess at a fixed req/s rate."""

    def __init__(self, sglang_url: str, length: int, rate: float,
                 l_docs_dir: str, gaps_log: str, seed: int, config: str):
        here = Path(__file__).parent.resolve()
        host = sglang_url.split("//")[1].split(":")[0]
        self._cmd = [
            sys.executable, str(here / "add_load.py"),
            "--config",     config,
            "--length",     str(length),
            "--rate",       f"{rate:.6f}",
            "--l-docs-dir", l_docs_dir,
            "--doc-range",  f"0:{CACHED_REQUESTS}",
            "--gaps-log",   gaps_log,
            "--seed",       str(seed),
            "--host",       host,
        ]
        self._proc: subprocess.Popen | None = None
        self._stop_flag = threading.Event()
        self._rate = rate

    def start(self):
        self._stop_flag.clear()
        self._thread = threading.Thread(target=self._watch, daemon=True)
        self._thread.start()

    def _watch(self):
        while not self._stop_flag.is_set():
            self._proc = subprocess.Popen(self._cmd, stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT, text=True)
            for line in self._proc.stdout:
                print(line, end="", flush=True)
            self._proc.wait()
            if self._stop_flag.is_set():
                break
            if self._proc.returncode != 0:
                print(f"  [load] died (rc={self._proc.returncode}), restarting…")
                time.sleep(1)

    def stop(self):
        self._stop_flag.set()
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        self._thread.join(timeout=15)


def main():
    here = Path(__file__).parent.resolve()

    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config",     default=str(here / "../config.json"))
    ap.add_argument("--proxy-host", default="localhost", dest="proxy_host")
    ap.add_argument("--host-a",     default=None, dest="host_a",
                    help="Override nodeA IP for warm-up (default: from config)")
    ap.add_argument("--lengths",    default="2048,8192,16384,24576")
    ap.add_argument("--sat",        default="0,40,80,100")
    ap.add_argument("--l-docs-dir", default=str(here / "data"), dest="l_docs_dir")
    ap.add_argument("--out-dir",    default=str(here / "results"), dest="out_dir")
    ap.add_argument("--seed",       type=int, default=42)
    args = ap.parse_args()

    lengths = [int(x) for x in args.lengths.split(",")]
    sats    = [int(x) for x in args.sat.split(",")]

    cfg         = load_config(args.config)
    purl        = proxy_url(cfg, args.proxy_host)
    sglang_url  = node_sglang_url(cfg, args.host_a)
    rng         = random.Random(args.seed)
    out_dir     = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    from tokenizers import Tokenizer
    tk_path = str(Path(args.config).parent / cfg["models"][cfg["model"]]["tokenizer_path"])
    tk = Tokenizer.from_file(tk_path)

    generate_url = f"{purl}/generate"
    run_ts = int(time.time())

    for length in lengths:
        docs = load_docs(length, args.l_docs_dir)

        for sat in sats:
            print(f"\n{'='*60}")
            print(f"Cell: length={length}  sat={sat}")

            rate = (sat / 100.0) * MSP_TABLE[length]

            # fixed deterministic cached docs (same across all sat levels for this length)
            cell_docs = docs[:CACHED_REQUESTS]

            # flush both nodes then warm A fresh for this cell
            flush_nodes(cfg)
            warm_node_a(sglang_url, cell_docs)
            print("  Waiting 5s for KV events to propagate…")
            time.sleep(5)

            # build request list
            reqs = []
            for doc in cell_docs:
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
                ids = rand_ids(rng, length + SUFFIX_TOKENS)
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

            gaps_log = str(out_dir / f"gaps_{length}_{sat}_{run_ts}.jsonl")
            load: LoadProcess | None = None

            if sat > 0:
                load = LoadProcess(sglang_url, length, rate,
                                   args.l_docs_dir, gaps_log, args.seed, args.config)
                load.start()
                print(f"  Load started at {rate:.4f} req/s, warming in {WARM_IN_S}s…")
                time.sleep(WARM_IN_S)

            rows = []
            out_path = out_dir / f"exp_{length}_{sat}_{run_ts}.jsonl"

            with open(out_path, "w") as f:
                for i, req in enumerate(reqs):
                    req_id = str(uuid.uuid4())
                    print(f"  [{i+1:>3}/{TOTAL_REQUESTS}] kind={req['kind']}", end="  ", flush=True)
                    result = stream_request(generate_url, req["payload"], req_id)
                    result["kind"]   = req["kind"]
                    result["length"] = length
                    result["sat"]    = sat
                    rows.append(result)
                    f.write(json.dumps(result) + "\n")
                    f.flush()

                    status = (f"ttft={result['ttft_s']}s  e2e={result['e2e_s']}s  "
                              f"hit={result['hit_rate']}  by={result['served_by']}")
                    if result["pulled_from"]:
                        status += f"  pull={result['pulled_from']}"
                    if result["error"]:
                        status = f"ERROR: {result['error']}"
                    print(status)

            if load:
                load.stop()

            print_summary(rows, length, sat)
            print(f"  Results → {out_path}")

    print(f"\n{'='*60}")
    print("All cells done.")


if __name__ == "__main__":
    main()
