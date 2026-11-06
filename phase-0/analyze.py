#!/usr/bin/env python3
"""analyze.py – offline analysis of sweep and interference results.

Runs on: local machine (no GPU needed)

Usage:
  python analyze.py \\
    --sweep results/sweep.csv \\
    --interference results/interference_summary.json \\
    --out-dir results/plots
"""

import argparse
import csv
import json
import math
import os
import statistics
from collections import defaultdict


PAGE_SIZE = 64
# Nominal line rates to model (Gbps)
MODEL_RATES_GBPS = [1.0, 5.0, 10.0, 25.0]


def load_sweep(path: str):
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            if r.get("valid", "").lower() != "true":
                continue
            if not r.get("ttft_ms"):
                continue
            rows.append({
                "L": int(r["L"]),
                "condition": r["condition"],
                "ttft_ms": float(r["ttft_ms"]),
                "bytes_read": float(r["bytes_read"]) if r.get("bytes_read") else None,
                "read_ms": float(r["read_ms"]) if r.get("read_ms") else None,
                "pages": int(r["pages"]) if r.get("pages") else None,
            })
    return rows


def percentile(vals: list, p: float) -> float:
    if not vals:
        return float("nan")
    s = sorted(vals)
    idx = max(0, min(len(s) - 1, int(math.ceil(p / 100 * len(s))) - 1))
    return s[idx]


def fit_linear(xs: list, ys: list):
    n = len(xs)
    if n < 2:
        return 0.0, 0.0
    mx, my = statistics.mean(xs), statistics.mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    slope = num / den if den else 0.0
    return slope, my - slope * mx


def fit_quadratic(xs: list, ys: list):
    n = len(xs)
    if n < 3:
        return 0.0, 0.0, statistics.mean(ys) if ys else 0.0
    s0, s1 = n, sum(xs)
    s2 = sum(x**2 for x in xs)
    s3 = sum(x**3 for x in xs)
    s4 = sum(x**4 for x in xs)
    t1, t2 = sum(ys), sum(x * y for x, y in zip(xs, ys))
    t3 = sum(x**2 * y for x, y in zip(xs, ys))
    A = [[s0, s1, s2], [s1, s2, s3], [s2, s3, s4]]
    B = [t1, t2, t3]
    try:
        c, b, a = _solve3(A, B)
        return a, b, c
    except ZeroDivisionError:
        return 0.0, 0.0, statistics.mean(ys) if ys else 0.0


def _solve3(A, B):
    M = [a[:] + [b] for a, b in zip(A, B)]
    for col in range(3):
        pivot = max(range(col, 3), key=lambda r: abs(M[r][col]))
        M[col], M[pivot] = M[pivot], M[col]
        if M[col][col] == 0:
            raise ZeroDivisionError
        for row in range(col + 1, 3):
            f = M[row][col] / M[col][col]
            M[row] = [M[row][j] - f * M[col][j] for j in range(4)]
    x = [0.0] * 3
    for i in range(2, -1, -1):
        x[i] = (M[i][3] - sum(M[i][j] * x[j] for j in range(i + 1, 3))) / M[i][i]
    return x


def find_crossing_lin_quad(xs_range: list, pull_lin, recomp_quad):
    """First L where linear pull < quadratic recompute."""
    for L in sorted(xs_range):
        p = pull_lin[0] * L + pull_lin[1]
        r = recomp_quad[0] * L**2 + recomp_quad[1] * L + recomp_quad[2]
        if p < r:
            return L
    return None


def first_data_crossing(lengths: list, medians_pull: dict, medians_recomp: dict):
    for L in lengths:
        if L in medians_pull and L in medians_recomp:
            if medians_pull[L] < medians_recomp[L]:
                return L
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep", default="results/sweep.csv")
    ap.add_argument("--interference", default="results/interference_summary.json")
    ap.add_argument("--out-dir", default="results/plots", dest="out_dir")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    # ── Sweep analysis ────────────────────────────────────────────────────────
    print("=== Sweep analysis (measured) ===")
    rows = load_sweep(args.sweep)

    groups: dict = defaultdict(list)
    for r in rows:
        groups[(r["L"], r["condition"])].append(r["ttft_ms"])

    lengths = sorted(set(r["L"] for r in rows))
    conditions = ["recompute", "pull", "local"]

    print(f"\n{'L':>7}  {'cond':12}  {'median_ms':>10}  {'p95_ms':>10}  {'n':>4}")
    medians: dict = defaultdict(dict)
    p95s: dict = defaultdict(dict)
    for cond in conditions:
        for L in lengths:
            vals = groups.get((L, cond), [])
            if not vals:
                continue
            med = statistics.median(vals)
            p95 = percentile(vals, 95)
            medians[cond][L] = med
            p95s[cond][L] = p95
            print(f"{L:>7}  {cond:12}  {med:>10.1f}  {p95:>10.1f}  {len(vals):>4}")

    pull_Ls    = sorted(medians["pull"].keys())
    recomp_Ls  = sorted(medians["recompute"].keys())
    pull_xs    = [float(L) for L in pull_Ls]
    pull_ys    = [medians["pull"][L] for L in pull_Ls]
    recomp_xs  = [float(L) for L in recomp_Ls]
    recomp_ys  = [medians["recompute"][L] for L in recomp_Ls]

    pull_lin   = fit_linear(pull_xs, pull_ys)
    recomp_quad = fit_quadratic(recomp_xs, recomp_ys)
    recomp_lin  = fit_linear(recomp_xs, recomp_ys)

    print(f"\nPull fit (linear):         ttft = {pull_lin[0]:.4f}·L + {pull_lin[1]:.1f} ms")
    print(f"Recompute fit (quadratic): ttft = {recomp_quad[0]:.2e}·L² + {recomp_quad[1]:.4f}·L + {recomp_quad[2]:.1f} ms")
    print(f"Recompute fit (linear):    ttft = {recomp_lin[0]:.4f}·L + {recomp_lin[1]:.1f} ms")

    fine_range = list(range(64, 16385, 64))
    L_star_fit  = find_crossing_lin_quad(fine_range, pull_lin, recomp_quad)
    L_star_data = first_data_crossing(lengths, medians["pull"], medians["recompute"])

    def _pages(L):
        return math.ceil(L / PAGE_SIZE) if L else None

    print(f"\nBreak-even from fit:    L* = {L_star_fit} tokens = {_pages(L_star_fit)} pages")
    print(f"Break-even from data:   L* = {L_star_data} tokens = {_pages(L_star_data)} pages")
    if L_star_fit is None and L_star_data is None and pull_xs and recomp_xs:
        avg_pull   = statistics.mean(pull_ys)
        avg_recomp = statistics.mean(recomp_ys)
        winner = "Pull" if avg_pull < avg_recomp else "Recompute"
        print(f"  {winner} wins everywhere in the measured range.")

    # Effective measured NIXL bandwidth
    pull_rows = [r for r in rows
                 if r["condition"] == "pull" and r.get("bytes_read") and r.get("read_ms")]
    if pull_rows:
        gbps_vals = [r["bytes_read"] / (r["read_ms"] / 1e3) / 1e9 for r in pull_rows]
        print(f"\nEffective NIXL pull bandwidth (measured):")
        print(f"  median {statistics.median(gbps_vals):.2f} Gbps  "
              f"p95 {percentile(gbps_vals, 95):.2f} Gbps")

    # ── Modeled-network section ───────────────────────────────────────────────
    # For each nominal line rate, substitute the transfer time:
    #   modeled_read_ms = bytes_read / (rate_gbps * 1e9 / 8) * 1e3
    # and recompute pull TTFT:
    #   modeled_ttft_ms = measured_ttft_ms - measured_read_ms + modeled_read_ms
    # Re-fit the pull curve linearly and find L* against the same recompute fit.
    #
    # NOTE: nominal line rate is an upper bound; real links run slower due to
    # protocol overhead, flow control and congestion.

    if pull_rows:
        print(f"\n=== Modeled-network break-even (nominal line rate) ===")
        print("NOTE: nominal rates are upper bounds – real links run slower.")
        print(f"\n{'rate_Gbps':>10}  {'pull slope ms/tok':>18}  {'pull intercept ms':>18}  {'L*_fit':>8}  {'L*_fit pages':>12}")

        # group pull rows by L for computing per-length medians under each model
        pull_by_L: dict = defaultdict(list)
        for r in pull_rows:
            pull_by_L[r["L"]].append(r)

        modeled_results = {}
        for rate_gbps in MODEL_RATES_GBPS:
            rate_bytes_per_sec = rate_gbps * 1e9 / 8.0

            mod_medians: dict = {}
            for L, recs in pull_by_L.items():
                mod_ttfts = []
                for r in recs:
                    mod_read_ms = r["bytes_read"] / rate_bytes_per_sec * 1e3
                    mod_ttft = r["ttft_ms"] - r["read_ms"] + mod_read_ms
                    mod_ttfts.append(mod_ttft)
                mod_medians[L] = statistics.median(mod_ttfts)

            mod_xs = [float(L) for L in sorted(mod_medians)]
            mod_ys = [mod_medians[L] for L in sorted(mod_medians)]
            mod_lin = fit_linear(mod_xs, mod_ys)
            mod_L_star = find_crossing_lin_quad(fine_range, mod_lin, recomp_quad)

            modeled_results[rate_gbps] = {
                "lin": mod_lin,
                "medians": mod_medians,
                "L_star": mod_L_star,
            }

            pages_str = str(_pages(mod_L_star)) if mod_L_star else "N/A"
            Lstar_str = str(mod_L_star) if mod_L_star else "N/A"
            print(f"{rate_gbps:>10.0f}  {mod_lin[0]:>18.5f}  {mod_lin[1]:>18.1f}  {Lstar_str:>8}  {pages_str:>12}")

        # Per-rate per-length table
        print(f"\nModeled pull median TTFT (ms) by length and nominal rate:")
        header = f"{'L':>7}" + "".join(f"  {'measured':>10}") + "".join(f"  {r:>5}Gbps" for r in MODEL_RATES_GBPS)
        print(header)
        for L in sorted(pull_by_L.keys()):
            meas = statistics.median([r["ttft_ms"] for r in pull_by_L[L]])
            line = f"{L:>7}  {meas:>10.1f}"
            for rate_gbps in MODEL_RATES_GBPS:
                v = modeled_results[rate_gbps]["medians"].get(L)
                line += f"  {v:>8.1f}" if v is not None else f"  {'N/A':>8}"
            print(line)

    # ── Interference analysis ─────────────────────────────────────────────────
    if os.path.exists(args.interference):
        print("\n=== Interference analysis ===")
        print("NOTE: measured with the two servers on separate machines; pulls cross the network.")
        with open(args.interference) as f:
            summary = json.load(f)

        print(f"\n{'phase':25}  {'rate':>6}  {'p50_ms':>8}  {'p95_ms':>8}  {'p99_ms':>8}  {'ratio':>6}")
        for phase, d in summary.items():
            ratio = d.get("ratio_to_baseline_p50", float("nan"))
            print(f"{phase:25}  {d['rate']:>6.1f}  "
                  f"{d['tpot_p50_ms']:>8.1f}  {d['tpot_p95_ms']:>8.1f}  "
                  f"{d['tpot_p99_ms']:>8.1f}  {ratio:>6.3f}")
    else:
        print(f"\n(No interference summary at {args.interference})")

    # ── Plot (optional) ───────────────────────────────────────────────────────
    try:
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(10, 6))
        colors = {"recompute": "#e74c3c", "pull": "#3498db", "local": "#2ecc71"}

        for cond in conditions:
            Ls = sorted(medians[cond].keys())
            if not Ls:
                continue
            ys = [medians[cond][L] for L in Ls]
            ax.plot(Ls, ys, marker="o", label=cond, color=colors.get(cond), linewidth=2)

        # modeled pull curves
        if pull_rows:
            rate_colors = {1.0: "#aed6f1", 5.0: "#5dade2", 10.0: "#2e86c1", 25.0: "#1a5276"}
            for rate_gbps in MODEL_RATES_GBPS:
                res = modeled_results[rate_gbps]
                Ls = sorted(res["medians"].keys())
                ys = [res["medians"][L] for L in Ls]
                ax.plot(Ls, ys, marker=".", linestyle="--",
                        color=rate_colors.get(rate_gbps, "gray"),
                        label=f"pull modeled {rate_gbps:.0f} Gbps", alpha=0.8)

        if L_star_data:
            ax.axvline(L_star_data, linestyle=":", color="gray", alpha=0.7,
                       label=f"L*={L_star_data} (measured data)")

        ax.set_xlabel("Prefix length (tokens)")
        ax.set_ylabel("Median TTFT (ms)")
        ax.set_title("TTFT vs prefix length  –  measured + modeled pull at nominal rates\n"
                     "(nominal rates are upper bounds; real links run slower)")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
        plot_path = os.path.join(args.out_dir, "ttft_vs_length.png")
        fig.savefig(plot_path, dpi=150, bbox_inches="tight")
        print(f"\nPlot → {plot_path}")
        plt.close(fig)
    except ImportError:
        print("\n(matplotlib not available – skipping plot)")


if __name__ == "__main__":
    main()
