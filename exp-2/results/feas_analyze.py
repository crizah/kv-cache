import pandas as pd
OVERHEAD_MS = 5.0  # ASSUMPTION: fixed per-pull overhead, not measured
RATES = [1, 5, 10, 25, 100]
rc = pd.read_csv("results/recompute.csv"); nx = pd.read_csv("results/nixl.csv"); h2 = pd.read_csv("results/h2d.csv")
med = rc.groupby("L").ttft_ms.median(); base = med[128]
nxm = nx.groupby("L").agg(bytes=("bytes", "first"), read_ms=("read_ms", "median"))
h2m = h2.groupby("L").h2d_ms.median()
rows = []
for L in nxm.index:
    b = nxm.bytes[L]; saved = med[L] - base; h = h2m[L]; r = nxm.read_ms[L]
    pull = r + h + OVERHEAD_MS; denom = saved - h - OVERHEAD_MS
    be = b * 8 / (denom / 1000) / 1e9 if denom > 0 else None
    rows.append(dict(L=L, bytes=b, ttft_ms=med[L], saved_ms=saved, read_ms=r, h2d_ms=h, pull_loopback_ms=pull,
                     beats=pull < saved, breakeven_gbps=be, loop_gbps=b * 8 / (r / 1000) / 1e9))
d = pd.DataFrame(rows).set_index("L")
f = lambda x: "never" if x is None else f"{x:.2f}"
ver = "NO-GO"
b8 = d.breakeven_gbps[8192]
if b8 is not None and b8 <= 10: ver = "GO"
elif b8 is not None and b8 <= 25: ver = "MAYBE"
out = [f"# Feasibility: Qwen3-4B pull-vs-recompute (RTX 5060 Ti, GPU 0)\n",
       f"## Verdict table\n", f"**Verdict: {ver}** (break-even at L=8192 is {f(b8)} Gbps; GO <=10, MAYBE 10-25, NO-GO >25 or never)\n",
       "| L | KV bytes (MB) | saved_ms | break-even Gbps | pull_loopback_ms | loopback beats recompute? |", "|---|---|---|---|---|---|"]
for L, x in d.iterrows():
    out.append(f"| {L} | {x.bytes/1e6:.0f} | {x.saved_ms:.1f} | {f(x.breakeven_gbps)} | {x.pull_loopback_ms:.1f} | {'yes' if x.beats else 'no'} |")
out += ["", f"`saved_ms(L) = median TTFT(L) - median TTFT(128)` (TTFT(128) = {base:.1f} ms). `pull_loopback_ms = read_ms + h2d_ms + {OVERHEAD_MS:g}`; the {OVERHEAD_MS:g} ms is an **assumed** fixed overhead, not measured.", "",
        "## Measured components (medians)\n", "| L | TTFT ms | read_ms (NIXL tcp loopback) | loopback Gbps | h2d_ms |", "|---|---|---|---|---|"]
for L, x in d.iterrows(): out.append(f"| {L} | {x.ttft_ms:.1f} | {x.read_ms:.1f} | {x.loop_gbps:.1f} | {x.h2d_ms:.1f} |")
out += ["", "Recompute TTFT, all lengths (median ms): " + ", ".join(f"{L}: {v:.1f}" for L, v in med.items()),
        f"Recompute throughput L/TTFT: 4096 -> {4096/med[4096]*1000:.0f} tok/s, 8192 -> {8192/med[8192]*1000:.0f} tok/s", "",
        "## Modeled links\n`pull_ms = bytes*8/(rate*1e9)*1000 + h2d_ms + 5`\n", "| L | Gbps | pull_ms | saved_ms | winner | speedup (recompute/pull or pull/recompute) |", "|---|---|---|---|---|---|"]
for L, x in d.iterrows():
    for rt in RATES:
        p = x.bytes * 8 / (rt * 1e9) * 1000 + x.h2d_ms + OVERHEAD_MS
        w, s = ("pull", x.saved_ms / p) if p < x.saved_ms else ("recompute", p / x.saved_ms)
        out.append(f"| {L} | {rt} | {p:.1f} | {x.saved_ms:.1f} | {w} | {s:.2f}x |")
open("results/feasibility_body.md", "w").write("\n".join(out) + "\n")
print("\n".join(out))
