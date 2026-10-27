import torch, csv, statistics
torch.cuda.set_device(0)
PAGE = 64 * 147456
LENS = [1024, 4096, 8192, 16384]
rows = []
for L in LENS:
    n = (L // 64) * PAGE
    h = torch.empty(n, dtype=torch.uint8).pin_memory(); h.fill_(1)
    d = torch.empty(n, dtype=torch.uint8, device="cuda")
    d.copy_(h, non_blocking=True); torch.cuda.synchronize()  # warmup
    ts = []
    for i in range(10):
        s, e = torch.cuda.Event(enable_timing=True), torch.cuda.Event(enable_timing=True)
        s.record(); d.copy_(h, non_blocking=True); e.record(); torch.cuda.synchronize()
        ts.append(s.elapsed_time(e)); rows.append((L, n, i, ts[-1]))
    print(f"L={L} bytes={n} median_ms={statistics.median(ts):.2f} GB/s={n/1e9/(statistics.median(ts)/1000):.2f}", flush=True)
    del h, d
with open("results/h2d.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["L", "bytes", "trial", "h2d_ms"]); w.writerows(rows)
