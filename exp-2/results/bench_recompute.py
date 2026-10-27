import json, random, time, csv, statistics, sys, requests
URL = "http://127.0.0.1:30000/generate"
LENS = [128, 512, 1024, 2048, 4096, 8192, 16384]
rng = random.Random(0)
def ttft(L):
    ids = [rng.randint(1000, 100000) for _ in range(L)]  # well below Qwen special tokens (>=151643)
    body = {"input_ids": ids, "sampling_params": {"max_new_tokens": 1, "temperature": 0}, "stream": True}
    t0 = time.perf_counter()
    with requests.post(URL, json=body, stream=True, timeout=600) as r:
        r.raise_for_status()
        for line in r.iter_lines():
            if line.startswith(b"data:") and b"[DONE]" not in line:
                return (time.perf_counter() - t0) * 1000
    raise RuntimeError("no token")
rows = []
for L in LENS:
    try:
        for _ in range(3): ttft(L)
        for t in range(10):
            rows.append((L, t, ttft(L)))
        print(L, "median", statistics.median(x[2] for x in rows if x[0] == L), flush=True)
    except Exception as e:
        print(f"L={L} FAILED: {e!r}", flush=True)
        break
with open("results/recompute.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["L", "trial", "ttft_ms"]); w.writerows(rows)
