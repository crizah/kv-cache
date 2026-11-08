#!/usr/bin/env python3
"""run_phase2.py – Phase 2 experiment runner (stdlib only).

Runs one arm of Phase 2 per invocation. Uses docker exec for node-local
commands and direct HTTP to SGLang ports (read from config.json).

Usage:
  python run_phase2.py --system nixl   --lengths 2048,8192,24576 --sat 0,80,100
  python run_phase2.py --system l3     --lengths 2048,8192,24576 --sat 0,80,100 --l3-dir /mnt/l3
  python run_phase2.py --system baseline --lengths 2048,8192,24576 --sat 0,80,100 --host-a nodeA --host-b nodeB
  python3 run_phase2.py --system nixl --lengths 2048,8192,16384 --host-a 172.18.0.2 --host-b 172.18.0.3 --sat 0,80,100 --gpu-a 3 --gpu-b 5 --out-dir phase-2/results/nixl

Flags:
  --system        {nixl,l3,baseline}
  --lengths       comma-separated prefix lengths (default: 2048,8192,24576)
  --sat           comma-separated saturation %% values (default: 0,80,100)
  --trials        runs per cell (default: 5)
  --throttle-gbps optional network throttle applied to nodeA eth0, 6
  --gpu-a         GPU index on nodeA for nvidia-smi (default: 0)
  --gpu-b         GPU index on nodeB for nvidia-smi (default: 0)
  --mps-label     label string to embed in run_meta.json
  --evict         {fadvise,dropcaches,none} L3 eviction method (default: fadvise)
  --l3-dir        path to L3 storage dir (required for --system l3)
  --cont-a        docker container name/id for nodeA (default: nodeA)
  --cont-b        docker container name/id for nodeB (default: nodeB)
  --config        path to config.json (default: ../config.json relative to script)
  --seed          rng seed (default: 42)
  --phase1-data   path to phase-1/data/ (default: ../phase-1/data relative to script)
  --phase1-results path to phase-1/results/ (default: ../phase-1/results relative to script)
  --out-dir       output directory (default: phase-2/results/<system>_<ts>/)
"""

import argparse
import csv
import datetime
import json
import math
import os
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).parent.resolve()

# ── constants ─────────────────────────────────────────────────────────────────
SUFFIX_TOKENS     = 128
OUTPUT_TOKENS     = 1       # write window: 1 output token (we care about TTFT/write time)
CHURN_OUTPUT_TOKENS = 256   # churn: full decode for TPOT signal
TOKEN_MIN         = 100
TOKEN_MAX         = 50_000
CHURN_DOCS_SLICE  = slice(15, 25)   # docs 15–24 are churn (kept hot on A)
# Target docs: 0–14 and 25–39 (skip 15–24 churn). 30 total = 6 sat levels × 5 trials.
TARGET_DOCS_INDICES = list(range(0, 15)) + list(range(25, 40))
TARGETS_PER_SAT   = 5
WARM_IN_S         = 30      # churn warm-in before baseline window
BASELINE_WINDOW_S = 60      # churn-only baseline recording window
TFW_TIMEOUT_S     = 600     # max wait for keys to appear / files to settle
FLOOR_REQUESTS    = 10      # requests for floor measurement
SAMPLE_INTERVAL_S = 1.0
# ─────────────────────────────────────────────────────────────────────────────


# ── helpers ───────────────────────────────────────────────────────────────────

def ts() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


def run(cmd: list[str], *, timeout: int = 30, check: bool = True,
        capture: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, timeout=timeout, check=check,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        text=True,
    )


def docker_exec(container: str, cmd: list[str], *, timeout: int = 60,
                capture: bool = True) -> subprocess.CompletedProcess:
    return run(["docker", "exec", container] + cmd, timeout=timeout, capture=capture)


def http_get(url: str, timeout: int = 10) -> dict:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def http_post(url: str, payload: dict, timeout: int = 300) -> dict:
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def http_post_stream(url: str, payload: dict, timeout: int = 300):
    """POST with stream=True; yields raw non-empty lines."""
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        for raw in r:
            line = raw.strip()
            if line:
                yield line


def flush_node(sglang_url: str):
    req = urllib.request.Request(f"{sglang_url}/flush_cache", data=b"")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            r.read()
    except urllib.error.HTTPError as e:
        if e.code not in (200, 400):  # 400 = already flushed / no-op on some builds
            raise


def waiting_queue(sglang_url: str) -> tuple[int, int]:
    """Return (running, waiting) from SGLang /metrics."""
    import re
    req = urllib.request.Request(f"{sglang_url}/metrics")
    with urllib.request.urlopen(req, timeout=5) as r:
        text = r.read().decode()
    def _extract(pattern):
        m = re.search(pattern, text)
        return int(float(m.group(1))) if m else 0
    running = _extract(r"sglang:num_running_reqs(?:\{[^}]*\})?\s+([\d.]+)")
    waiting = _extract(r"sglang:num_queue_reqs(?:\{[^}]*\})?\s+([\d.]+)")
    return running, waiting


def wait_idle(sglang_url: str, poll_s: float = 2.0, timeout_s: float = 300.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        running, waiting = waiting_queue(sglang_url)
        if running == 0 and waiting == 0:
            return
        time.sleep(poll_s)
    raise TimeoutError(f"Node {sglang_url} did not go idle within {timeout_s}s")


def rand_ids(rng: random.Random, n: int) -> list[int]:
    return [rng.randint(TOKEN_MIN, TOKEN_MAX) for _ in range(n)]


# ── config loading ────────────────────────────────────────────────────────────

def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return json.load(f)


def node_urls(cfg: dict, node_id: str, host_override: str | None = None) -> tuple[str, str]:
    """Return (agent_url, sglang_url) for node."""
    nodes = {n["id"]: n for n in cfg["nodes"]}
    n    = nodes[node_id]
    host = host_override if host_override else n["ip"]
    return (
        f"http://{host}:{n['http']}",
        f"http://{host}:{n['sglang']}",
    )


MSP_TABLE = {
    2048:  2.3283,
    8192:  0.75,
    16384: 0.20,
    24576: 0.1875,
}

def load_msp(phase1_results: str, length: int) -> float:
    if length in MSP_TABLE:
        return MSP_TABLE[length]
    raise SystemExit(f"No MSP value for length={length}. Add it to MSP_TABLE in run_phase2.py.")


def load_docs(phase1_data: str, length: int) -> list[dict]:
    p = Path(phase1_data) / f"L_docs_{length}.json"
    if not p.exists():
        raise SystemExit(f"L docs not found: {p}  — run find_msp.py --length {length} --gen-only first")
    with open(p) as f:
        return json.load(f)


# ── kv_bytes_per_token ────────────────────────────────────────────────────────

def kv_bytes_per_token(cfg: dict) -> float:
    """
    Compute KV bytes per token from model config.json.
    KV = 2 (K+V) × num_heads × head_dim × num_layers × dtype_bytes
    Falls back to a Qwen3-32B estimate (128 × 128 × 64 × 2 × 2 = 4194304) if unreadable.
    """
    model_path = cfg.get("model_path", "")
    model_cfg_path = Path(model_path) / "config.json"
    try:
        with open(model_cfg_path) as f:
            mc = json.load(f)
        num_layers  = mc.get("num_hidden_layers", mc.get("n_layer", 64))
        num_heads   = mc.get("num_key_value_heads", mc.get("num_attention_heads", 8))
        head_dim    = mc.get("head_dim", mc.get("hidden_size", 128) // mc.get("num_attention_heads", 8))
        dtype_bytes = 2  # bfloat16
        return 2 * num_heads * head_dim * num_layers * dtype_bytes
    except Exception:
        # Qwen3-32B: 64 layers, 8 KV heads, 128 head_dim, bf16
        return 2 * 8 * 128 * 64 * 2


# ── floor measurement ─────────────────────────────────────────────────────────

def measure_floor(sglang_b_url: str, rng: random.Random) -> float:
    """Median TTFT of a 128-token prompt on idle B over FLOOR_REQUESTS requests."""
    ttfts = []
    for _ in range(FLOOR_REQUESTS):
        ids     = rand_ids(rng, 128)
        payload = {
            "input_ids": ids,
            "sampling_params": {"temperature": 0, "max_new_tokens": 1},
            "stream": True,
        }
        t0   = time.perf_counter()
        ttft = None
        for _ in http_post_stream(f"{sglang_b_url}/generate", payload):
            if ttft is None:
                ttft = time.perf_counter() - t0
            break
        if ttft is not None:
            ttfts.append(ttft)
    if not ttfts:
        raise RuntimeError("Floor measurement failed — no responses from nodeB")
    ttfts.sort()
    return ttfts[len(ttfts) // 2]


# ── throttle ──────────────────────────────────────────────────────────────────

def apply_throttle(cont_a: str, gbps: float):
    rate_str = f"{gbps}gbit"
    docker_exec(cont_a, [
        "tc", "qdisc", "replace", "dev", "eth0", "root",
        "tbf", "rate", rate_str, "burst", "64mbit", "latency", "50ms",
    ])


def remove_throttle(cont_a: str):
    try:
        docker_exec(cont_a, ["tc", "qdisc", "del", "dev", "eth0", "root"])
    except Exception:
        pass


# ── calibration ───────────────────────────────────────────────────────────────

def calibrate(system: str, cont_a: str, cont_b: str, l3_dir: str | None,
              out_dir: Path) -> dict:
    cal: dict = {"system": system}

    # A→B iperf3 (best effort)
    if system == "nixl":
        try:
            # start iperf3 server on B
            try:
                docker_exec(cont_b, ["pkill", "-f", "iperf3"])
            except Exception:
                pass
            time.sleep(0.5)
            srv = subprocess.Popen(
                ["docker", "exec", cont_b, "iperf3", "-s", "-1"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            time.sleep(1)
            # get B's container IP
            b_ip = docker_exec(cont_b, ["hostname", "-i"]).stdout.strip().split()[0]
            res = docker_exec(cont_a, ["iperf3", "-c", b_ip, "-t", "5", "-J"], timeout=30)
            srv.wait(timeout=10)
            d = json.loads(res.stdout)
            cal["iperf3_a_to_b_gbps"] = d["end"]["sum_sent"]["bits_per_second"] / 1e9
        except Exception as e:
            cal["iperf3_error"] = str(e)

    # L3 direct-I/O dd
    if system == "l3" and l3_dir:
        try:
            test_file = f"{l3_dir}/__cal_test__"
            wr = docker_exec(cont_a, [
                "bash", "-c",
                f"dd if=/dev/zero of={test_file} bs=64M count=32 oflag=direct 2>&1 && "
                f"dd if={test_file} of=/dev/null bs=64M count=32 iflag=direct 2>&1 && "
                f"rm -f {test_file}",
            ], timeout=120)
            cal["l3_dd_output"] = wr.stdout.strip()
        except Exception as e:
            cal["l3_dd_error"] = str(e)

    path = out_dir / "calibration.json"
    path.write_text(json.dumps(cal, indent=2))
    return cal


# ── server info check ─────────────────────────────────────────────────────────

def check_server_info(system: str, sglang_a_url: str, sglang_b_url: str,
                      out_dir: Path) -> tuple[dict, dict]:
    info_a = http_get(f"{sglang_a_url}/get_server_info", timeout=10)
    info_b = http_get(f"{sglang_b_url}/get_server_info", timeout=10)
    (out_dir / "server_info_a.json").write_text(json.dumps(info_a, indent=2))
    (out_dir / "server_info_b.json").write_text(json.dumps(info_b, indent=2))

    return info_a, info_b


# ── per-second sampler ────────────────────────────────────────────────────────

class Sampler:
    """Samples SGLang metrics and system stats once per second in a background thread."""

    def __init__(self, cont_a: str, cont_b: str, sglang_a_url: str,
                 gpu_a: int, gpu_b: int, l3_dir: str | None, out_dir: Path, label: str):
        self.cont_a      = cont_a
        self.cont_b      = cont_b
        self.sglang_a    = sglang_a_url
        self.gpu_a       = gpu_a
        self.gpu_b       = gpu_b
        self.l3_dir      = l3_dir
        self.out_path    = out_dir / f"samples_{label}.csv"
        self._stop       = threading.Event()
        self._rows: list[dict] = []

    def _sample(self) -> dict:
        row: dict = {"t": round(time.time(), 2)}
        try:
            running, waiting = waiting_queue(self.sglang_a)
            row["a_running"] = running
            row["a_waiting"] = waiting
        except Exception:
            pass
        try:
            r = subprocess.run(
                ["docker", "exec", self.cont_a, "nvidia-smi",
                 f"--id={self.gpu_a}", "--query-gpu=utilization.gpu,memory.used",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5,
            )
            parts = r.stdout.strip().split(",")
            row["a_gpu_util"] = int(parts[0].strip())
            row["a_gpu_mem_mb"] = int(parts[1].strip())
        except Exception:
            pass
        try:
            r = subprocess.run(
                ["docker", "exec", self.cont_b, "nvidia-smi",
                 f"--id={self.gpu_b}", "--query-gpu=utilization.gpu,memory.used",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5,
            )
            parts = r.stdout.strip().split(",")
            row["b_gpu_util"] = int(parts[0].strip())
        except Exception:
            pass
        return row

    def start(self):
        def _loop():
            while not self._stop.is_set():
                try:
                    self._rows.append(self._sample())
                except Exception:
                    pass
                self._stop.wait(timeout=SAMPLE_INTERVAL_S)
        self._t = threading.Thread(target=_loop, daemon=True)
        self._t.start()

    def stop(self) -> list[dict]:
        self._stop.set()
        self._t.join(timeout=5)
        if self._rows:
            keys = list(self._rows[0].keys())
            with open(self.out_path, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
                w.writeheader()
                w.writerows(self._rows)
        return self._rows

    def peak_waiting(self) -> int:
        return max((r.get("a_waiting", 0) for r in self._rows), default=0)


# ── churn manager ─────────────────────────────────────────────────────────────

class ChurnProcess:
    """Runs add_load.py as a subprocess; restarts it if it dies."""

    def __init__(self, sglang_a_url: str, length: int, rate: float,
                 phase1_data: str, gaps_log: str, seed: int, config: str):
        here_phase1 = Path(__file__).parent.parent / "phase-1"
        self._cmd = [
            sys.executable, str(here_phase1 / "add_load.py"),
            "--config",    config,
            "--length",    str(length),
            "--rate",      f"{rate:.6f}",
            "--l-docs-dir", phase1_data,
            "--doc-range", "15:25",
            "--gaps-log",  gaps_log,
            "--seed",      str(seed),
        ]
        # override host to use the URL directly — pass node-id A but set host
        # We inject a temporary config override via env or just rely on the url.
        # add_load reads config for the host; for phase-2 we pass --host explicitly.
        host = sglang_a_url.split("//")[1].split(":")[0]
        self._cmd += ["--host", host]
        self._proc: subprocess.Popen | None = None
        self._stop_flag = threading.Event()
        self._stats: dict = {}   # filled from last "Stopped." line of add_load output
        self._rate = rate        # offered rate

    def start(self):
        self._stop_flag.clear()
        self._thread = threading.Thread(target=self._watch, daemon=True)
        self._thread.start()

    def _watch(self):
        import re
        while not self._stop_flag.is_set():
            self._proc = subprocess.Popen(self._cmd, stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT, text=True)
            last_line = ""
            for line in self._proc.stdout:
                print(line, end="", flush=True)
                last_line = line.strip()
            self._proc.wait()
            # parse "Stopped. N sent (N ok, N err) over Xs — actual rate X req/s"
            m = re.search(r"(\d+) sent \((\d+) ok.*?\) over ([\d.]+)s", last_line)
            if m:
                elapsed = float(m.group(3))
                self._stats = {
                    "sent": int(m.group(1)),
                    "ok":   int(m.group(2)),
                    "elapsed_s": elapsed,
                    "completed_rate": round(int(m.group(2)) / elapsed, 4) if elapsed > 0 else 0,
                }
            if self._stop_flag.is_set():
                break
            if self._proc.returncode != 0:
                print(f"  [churn] died (rc={self._proc.returncode}), restarting…")
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

    def stats(self) -> dict:
        return {"offered_rate": self._rate, **self._stats}


# ── key computation ───────────────────────────────────────────────────────────

# ── page key computation (pure Python, matches SGLang's hash_binding.cpp) ────

def compute_page_keys(token_ids: list[int], page_size: int) -> list[str]:
    """
    Replicates SGLang's C++ hash for normal (non-bigram, uint32-fit) token IDs.
    Each page: SHA256(prior_digest_bytes? + tokens_as_uint32_le).
    Returns list of hex-string keys, one per page.
    """
    import hashlib
    import struct
    keys = []
    prior: bytes | None = None
    for start in range(0, len(token_ids), page_size):
        page = token_ids[start:start + page_size]
        h = hashlib.sha256()
        if prior is not None:
            h.update(prior)
        h.update(struct.pack(f"<{len(page)}I", *page))
        prior = h.digest()
        keys.append(prior.hex())
    return keys


# ── tfw polling ───────────────────────────────────────────────────────────────

def poll_nixl_keys(agent_a_url: str, keys: list[str]) -> tuple[bool, float]:
    """Poll A's /keys endpoint until all page keys appear. Returns (ok, elapsed)."""
    t0       = time.perf_counter()
    deadline = time.time() + TFW_TIMEOUT_S
    while time.time() < deadline:
        try:
            resp = urllib.request.urlopen(
                f"{agent_a_url}/keys?hashes={','.join(keys)}",
                timeout=5,
            )
            data = json.loads(resp.read())
            if len(data) == len(keys):
                return True, time.perf_counter() - t0
        except Exception:
            pass
        time.sleep(0.2)
    return False, time.perf_counter() - t0


def poll_l3_files(l3_dir: str, keys: list[int]) -> tuple[bool, float]:
    """Poll until every key's file exists in l3_dir and sizes stabilise."""
    t0       = time.perf_counter()
    deadline = time.time() + TFW_TIMEOUT_S
    prev_sizes: dict[int, int] = {}
    while time.time() < deadline:
        try:
            sizes = {}
            all_exist = True
            for k in keys:
                p = Path(l3_dir) / str(k)
                if not p.exists():
                    all_exist = False
                    break
                sizes[k] = p.stat().st_size
            if all_exist and sizes == prev_sizes and all(s > 0 for s in sizes.values()):
                return True, time.perf_counter() - t0
            prev_sizes = sizes
        except Exception:
            pass
        time.sleep(0.2)
    return False, time.perf_counter() - t0


# ── L3 file deletion ─────────────────────────────────────────────────────────

def delete_l3_files(l3_dir: str, keys: list[int]):
    for k in keys:
        try:
            Path(l3_dir, str(k)).unlink(missing_ok=True)
        except Exception:
            pass


# ── eviction ──────────────────────────────────────────────────────────────────

def evict_l3(cont_a: str, l3_dir: str, keys: list[int], method: str):
    if method == "dropcaches":
        docker_exec(cont_a, ["bash", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"])
    elif method == "fadvise":
        script = (
            "import os, ctypes; "
            "POSIX_FADV_DONTNEED = 4; "
            "libc = ctypes.CDLL('libc.so.6'); "
            "files = " + json.dumps([f"{l3_dir}/{k}" for k in keys]) + "; "
            "[libc.posix_fadvise(open(f,'rb').fileno(), 0, 0, POSIX_FADV_DONTNEED) "
            "for f in files if os.path.exists(f)]"
        )
        docker_exec(cont_a, ["python3", "-c", script], timeout=10)


# ── single generate (streaming, returns timing) ───────────────────────────────

def stream_generate(url: str, payload: dict, timeout: int = 300) -> dict:
    """POST streaming generate; returns {ttft, e2e, tokens, cached_tokens, prompt_tokens, error}."""
    t0     = time.perf_counter()
    ttft   = None
    t_last = None
    tokens = 0
    last_meta: dict = {}
    error  = None
    try:
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            for raw in r:
                line = raw.strip()
                if not line:
                    continue
                now = time.perf_counter()
                if ttft is None:
                    ttft = now - t0
                t_last = now
                tokens += 1
                try:
                    chunk = json.loads(line.decode().removeprefix("data: "))
                    if "meta_info" in chunk:
                        last_meta = chunk["meta_info"]
                except Exception:
                    pass
    except Exception as e:
        error = str(e)

    cached_tokens = last_meta.get("cached_tokens", last_meta.get("num_cached_tokens"))
    prompt_tokens = last_meta.get("prompt_tokens",  last_meta.get("num_prompt_tokens"))
    return {
        "ttft":          round(ttft,            4) if ttft   is not None else None,
        "e2e":           round(t_last - t0,     4) if t_last is not None else None,
        "tokens":        tokens,
        "cached_tokens": cached_tokens,
        "prompt_tokens": prompt_tokens,
        "error":         error,
    }


# ── TPOT from gaps log ────────────────────────────────────────────────────────

def tpot_from_log(gaps_log: str, t_start: float, t_end: float) -> dict:
    """Read gaps JSONL and return p50/p95 TPOT for requests that started in [t_start, t_end]."""
    all_gaps: list[float] = []
    try:
        with open(gaps_log) as f:
            for line in f:
                row = json.loads(line)
                if t_start <= row.get("t_start", 0) <= t_end:
                    all_gaps.extend(row.get("gaps", []))
    except Exception:
        return {}
    if not all_gaps:
        return {}
    all_gaps.sort()
    n = len(all_gaps)
    return {
        "tpot_p50_s": round(all_gaps[n // 2],          4),
        "tpot_p95_s": round(all_gaps[int(0.95 * n)],   4),
        "tpot_n":     n,
    }


# ── main ──────────────────────────────────────────────────────────────────────

def parse_args():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--system",   required=True, choices=["nixl", "l3", "baseline"])
    ap.add_argument("--lengths",  default="2048,8192,24576")
    ap.add_argument("--sat",      default="0,40,50,60,80,100")
    ap.add_argument("--trials",   type=int, default=5)
    ap.add_argument("--throttle-gbps", type=float, default=6.0, dest="throttle_gbps")
    ap.add_argument("--gpu-a",    type=int, default=0, dest="gpu_a")
    ap.add_argument("--gpu-b",    type=int, default=0, dest="gpu_b")
    ap.add_argument("--mps-label", default="", dest="mps_label")
    ap.add_argument("--evict",    default="fadvise", choices=["fadvise", "dropcaches", "none"])
    ap.add_argument("--l3-dir",   default=None, dest="l3_dir")
    ap.add_argument("--cont-a",   default="nodeA", dest="cont_a")
    ap.add_argument("--cont-b",   default="nodeB", dest="cont_b")
    ap.add_argument("--host-a",   default=None, dest="host_a",
                    help="Override nodeA IP/hostname (e.g. when running from host outside docker network)")
    ap.add_argument("--host-b",   default=None, dest="host_b",
                    help="Override nodeB IP/hostname")
    ap.add_argument("--config",   default=str(HERE / "../config.json"))
    ap.add_argument("--seed",     type=int, default=42)
    ap.add_argument("--phase1-data",    default=str(HERE / "../phase-1/data"),    dest="phase1_data")
    ap.add_argument("--phase1-results", default=str(HERE / "../phase-1/results"), dest="phase1_results")
    ap.add_argument("--out-dir",  default=None, dest="out_dir")
    return ap.parse_args()


def main():
    args   = parse_args()
    cfg    = load_config(args.config)
    rng    = random.Random(args.seed)
    system = args.system
    lengths = [int(x) for x in args.lengths.split(",")]
    sats    = [int(x) for x in args.sat.split(",")]

    if system == "l3" and not args.l3_dir:
        raise SystemExit("--l3-dir is required for --system l3")

    # output directory
    run_ts  = ts()
    out_dir = Path(args.out_dir) if args.out_dir else (HERE / "results" / f"{system}_{run_ts}")
    out_dir.mkdir(parents=True, exist_ok=True)
    keys_cache_dir = str(HERE / "data")
    Path(keys_cache_dir).mkdir(parents=True, exist_ok=True)

    agent_a_url, sglang_a_url = node_urls(cfg, "A", args.host_a)
    agent_b_url, sglang_b_url = node_urls(cfg, "B", args.host_b)

    # page size from config
    model_name = cfg["model"]
    page_size  = cfg["models"][model_name]["page_size"]
    kv_bpt     = kv_bytes_per_token(cfg)

    # startup checks
    print("Checking server info…")
    info_a, info_b = check_server_info(system, sglang_a_url, sglang_b_url, out_dir)

    # git commit
    try:
        git_sha = subprocess.check_output(["git", "rev-parse", "HEAD"],
                                          text=True).strip()
    except Exception:
        git_sha = "unknown"

    run_meta = {
        "system":    system,
        "git_sha":   git_sha,
        "flags":     vars(args),
        "timestamp": run_ts,
        "mps_label": args.mps_label,
    }
    (out_dir / "run_meta.json").write_text(json.dumps(run_meta, indent=2))

    # floor measurement
    print("Measuring floor TTFT on nodeB…")
    floor_ttft = measure_floor(sglang_b_url, rng)
    print(f"  floor TTFT = {floor_ttft:.3f}s")

    # calibration
    print("Running calibration…")
    cal = calibrate(system, args.cont_a, args.cont_b, args.l3_dir, out_dir)
    print(f"  calibration: {cal}")

    # progress tracking (skip finished cells on re-run)
    progress_path = out_dir / "progress.json"
    progress: dict = json.loads(progress_path.read_text()) if progress_path.exists() else {}

    # CSV writer
    csv_path  = out_dir / "results.csv"
    csv_fields = [
        "system", "length", "sat", "trial", "doc_idx",
        "tfw_s", "a_ttft_s", "a_e2e_s",
        "b_ttft_s", "b_e2e_s", "b_e2e_total_s",
        "cached_tokens", "prompt_tokens", "valid",
        "effective_gbps", "throttle_gbps", "note",
    ]
    csv_file   = open(csv_path, "a", newline="")
    csv_writer = csv.DictWriter(csv_file, fieldnames=csv_fields, extrasaction="ignore")
    if csv_path.stat().st_size == 0:
        csv_writer.writeheader()

    throttle_applied = False
    l3_keys_written: list[int] = []  # accumulates all keys written across all trials
    try:
        if args.throttle_gbps:
            print(f"Applying throttle {args.throttle_gbps} Gbps on nodeA…")
            apply_throttle(args.cont_a, args.throttle_gbps)
            throttle_applied = True

        for length in lengths:
            docs_all = load_docs(args.phase1_data, length)

            # build ordered target list from indices 0–14 and 25–39 (skip 15–24 churn)
            required = len(sats) * TARGETS_PER_SAT
            if len(TARGET_DOCS_INDICES) < required:
                raise SystemExit(
                    f"Need {required} target doc indices for {len(sats)} sat levels "
                    f"but TARGET_DOCS_INDICES only has {len(TARGET_DOCS_INDICES)}"
                )
            target_indices = TARGET_DOCS_INDICES[:required]
            target_docs = []
            for raw_idx in target_indices:
                if raw_idx >= len(docs_all):
                    raise SystemExit(
                        f"length={length}: need doc index {raw_idx} "
                        f"but file only has {len(docs_all)} docs. "
                        f"Generate docs up to index {max(target_indices)} first."
                    )
                doc = dict(docs_all[raw_idx])
                doc["_idx"] = raw_idx
                target_docs.append(doc)

            for sat in sats:
                cell_key = f"{length}_{sat}"
                if progress.get(cell_key) == "done":
                    print(f"Skipping cell length={length} sat={sat} (already done)")
                    continue

                print(f"\n{'='*60}")
                print(f"Cell: length={length}  sat={sat}  system={system}")

                msp  = load_msp(args.phase1_results, length) if sat > 0 else 0.0
                rate = (sat / 100.0) * msp

                # select 5 target docs for this saturation level (deterministic across arms)
                sat_idx   = sats.index(sat)
                cell_docs = target_docs[sat_idx * TARGETS_PER_SAT:(sat_idx + 1) * TARGETS_PER_SAT]

                # flush both nodes
                print("  Flushing both nodes…")
                flush_node(sglang_a_url)
                flush_node(sglang_b_url)

                gaps_log = str(out_dir / f"gaps_{length}_{sat}.jsonl")
                sampler  = Sampler(args.cont_a, args.cont_b, sglang_a_url,
                                   args.gpu_a, args.gpu_b, args.l3_dir,
                                   out_dir, f"{length}_{sat}")
                sampler.start()

                churn: ChurnProcess | None = None
                if sat > 0:
                    churn = ChurnProcess(sglang_a_url, length, rate,
                                        args.phase1_data, gaps_log,
                                        args.seed, args.config)
                    churn.start()
                    print(f"  Churn started at {rate:.3f} req/s, warming in {WARM_IN_S}s…")
                    time.sleep(WARM_IN_S)

                # baseline window
                baseline_start = time.time()
                print(f"  Baseline window {BASELINE_WINDOW_S}s…")
                time.sleep(BASELINE_WINDOW_S)
                baseline_end = time.time()
                baseline_tpot = tpot_from_log(gaps_log, baseline_start, baseline_end)
                print(f"  Baseline TPOT: {baseline_tpot}")
                (out_dir / f"baseline_tpot_{length}_{sat}.json").write_text(
                    json.dumps({**baseline_tpot, "t_start": baseline_start,
                                "t_end": baseline_end}, indent=2)
                )

                # trials
                cell_rows: list[dict] = []
                for trial_idx in range(args.trials):
                    doc      = cell_docs[trial_idx]
                    doc_idx  = doc["_idx"]
                    row: dict = {
                        "system": system, "length": length, "sat": sat,
                        "trial": trial_idx, "doc_idx": doc_idx,
                        "throttle_gbps": args.throttle_gbps or "",
                    }

                    def do_trial(row=row, doc=doc) -> dict:
                        suffix_a = rand_ids(rng, SUFFIX_TOKENS)
                        suffix_b = rand_ids(rng, SUFFIX_TOKENS)

                        page_keys = compute_page_keys(doc["ids"], page_size) if system != "baseline" else []

                        # ── step 1: write window (send to A, max_new_tokens=1) ──
                        a_res = stream_generate(
                            f"{sglang_a_url}/generate",
                            {
                                "input_ids":       doc["ids"] + suffix_a,
                                "sampling_params": {"temperature": 0,
                                                    "max_new_tokens": OUTPUT_TOKENS,
                                                    "ignore_eos": True},
                                "stream": True,
                            },
                        )
                        row["a_ttft_s"] = a_res["ttft"]
                        row["a_e2e_s"]  = a_res["e2e"]
                        if a_res["error"]:
                            row["note"] = f"A error: {a_res['error']}"
                            row["valid"] = False
                            return row

                        # ── step 2: tfw polling ──
                        tfw_ok  = False
                        tfw_s   = None
                        if system != "baseline":
                            if system == "nixl":
                                tfw_ok, tfw_s = poll_nixl_keys(agent_a_url, page_keys)
                            elif system == "l3":
                                tfw_ok, tfw_s = poll_l3_files(args.l3_dir, page_keys)

                            if not tfw_ok:
                                row["tfw_s"]  = tfw_s
                                row["note"]   = "tfw timeout"
                                row["valid"]  = False
                                return row
                            row["tfw_s"] = round(tfw_s, 4)

                            # ── step 3: evict (l3 only) ──
                            if system == "l3" and args.evict != "none":
                                evict_l3(args.cont_a, args.l3_dir, page_keys, args.evict)

                        # ── step 4: pull window (flush B, send to B) ──
                        if system != "baseline":
                            flush_node(sglang_b_url)
                            b_res = stream_generate(
                                f"{sglang_b_url}/generate",
                                {
                                    "input_ids":       doc["ids"] + suffix_b,
                                    "sampling_params": {"temperature": 0,
                                                        "max_new_tokens": OUTPUT_TOKENS,
                                                        "ignore_eos": True},
                                    "stream": True,
                                },
                            )
                            row["b_ttft_s"]     = b_res["ttft"]
                            row["b_e2e_s"]      = b_res["e2e"]
                            row["cached_tokens"] = b_res["cached_tokens"]
                            row["prompt_tokens"] = b_res["prompt_tokens"]

                            # end-to-end from A request start to B first token
                            row["b_e2e_total_s"] = round(
                                (b_res["ttft"] or 0) + (tfw_s or 0) + (a_res["e2e"] or 0), 4
                            )

                            # validate: cached_tokens must equal length
                            ct = b_res["cached_tokens"]
                            pt = b_res["prompt_tokens"]
                            valid = (ct is not None and ct >= length)
                            row["valid"] = valid
                            if not valid:
                                row["note"] = f"cache miss: cached={ct} prompt={pt}"

                            # effective speed
                            if valid and b_res["ttft"] is not None and b_res["ttft"] > floor_ttft:
                                net_ttft = b_res["ttft"] - floor_ttft
                                row["effective_gbps"] = round(
                                    length * kv_bpt * 8 / net_ttft / 1e9, 4
                                )
                        else:
                            row["valid"] = True

                        # ── cleanup: delete L3 files for this doc ──
                        if system == "l3" and keys:
                            delete_l3_files(args.l3_dir, keys)
                            l3_keys_written.extend(page_keys)

                        return row

                    # run with one retry
                    for attempt in range(2):
                        try:
                            row = do_trial()
                            break
                        except Exception as e:
                            if attempt == 0:
                                print(f"  trial {trial_idx} failed ({e}), retrying…")
                                row["note"] = f"retry after: {e}"
                            else:
                                row["note"] = f"failed: {e}"
                                row["valid"] = False

                    cell_rows.append(row)
                    csv_writer.writerow(row)
                    csv_file.flush()
                    status = "OK" if row.get("valid") else f"INVALID ({row.get('note','')})"
                    print(f"  trial {trial_idx}: doc={doc_idx}  "
                          f"a_ttft={row.get('a_ttft_s')}  "
                          f"b_ttft={row.get('b_ttft_s')}  "
                          f"tfw={row.get('tfw_s')}  "
                          f"eff_gbps={row.get('effective_gbps')}  {status}")

                # stop churn and wait idle
                churn_stats: dict = {}
                if churn:
                    churn.stop()
                    churn_stats = churn.stats()
                    print("  Waiting for A to go idle…")
                    wait_idle(sglang_a_url)

                samples = sampler.stop()

                # cell summary
                summary = {
                    "length": length, "sat": sat,
                    "valid_trials": sum(1 for r in cell_rows if r.get("valid")),
                    "total_trials": args.trials,
                    "baseline_tpot": baseline_tpot,
                    "offered_rate":    churn_stats.get("offered_rate"),
                    "completed_rate":  churn_stats.get("completed_rate"),
                    "peak_a_waiting":  sampler.peak_waiting(),
                }
                (out_dir / f"summary_{length}_{sat}.json").write_text(
                    json.dumps(summary, indent=2)
                )

                progress[cell_key] = "done"
                progress_path.write_text(json.dumps(progress, indent=2))

    finally:
        csv_file.close()
        if throttle_applied:
            print("Removing throttle…")
            remove_throttle(args.cont_a)
        if system == "l3" and args.l3_dir and l3_keys_written:
            print("Cleaning up remaining L3 files…")
            delete_l3_files(args.l3_dir, l3_keys_written)

    # print final table
    print(f"\n{'='*60}")
    print(f"Results written to {out_dir}")
    print(f"CSV: {csv_path}")


if __name__ == "__main__":
    main()
