#!/usr/bin/env python3
"""Plot e2e latency by prefix length for each router, split by sat level.

One figure per saturation point. Each figure shows median e2e for:
  - cached requests (doc WAS warmed on node A)
  - new requests
across all router types found in subdirectories of the results root.

Usage:
    python3 plot.py [--results-dir phase-1] [--out-dir phase-1/plots]
"""

import argparse
import json
import statistics
from pathlib import Path
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

COLORS = [
    "#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4",
    "#42d4f4", "#f032e6", "#bfef45", "#fabed4", "#469990",
]

LINESTYLES = {
    "cached": "-",
    "new":    "--",
}

MARKERS = {
    "cached": "o",
    "new":    "s",
}


def load_results(results_root: Path) -> dict:
    """
    Returns nested dict:
      data[router][sat][length][kind] = [e2e_s, ...]
    """
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))

    for router_dir in sorted(results_root.iterdir()):
        if not router_dir.is_dir():
            continue
        router = router_dir.name
        for f in router_dir.glob("exp_*.jsonl"):
            with open(f) as fh:
                for line in fh:
                    row = json.loads(line)
                    if row.get("error") or row.get("e2e_s") is None:
                        continue
                    sat    = row["sat"]
                    length = row["length"]
                    kind   = row["kind"]
                    data[router][sat][length][kind].append(row["e2e_s"])

    return data


def median_or_none(vals):
    return statistics.median(vals) if vals else None


def plot_sat(data: dict, sat: int, lengths: list[int], out_path: Path):
    fig, ax = plt.subplots(figsize=(11, 6))

    routers = sorted(data.keys())
    color_map = {r: COLORS[i % len(COLORS)] for i, r in enumerate(routers)}

    for router in routers:
        sat_data = data[router].get(sat, {})
        for kind in ("cached", "new"):
            ys = []
            xs = []
            for length in lengths:
                vals = sat_data.get(length, {}).get(kind, [])
                m = median_or_none(vals)
                if m is not None:
                    xs.append(length)
                    ys.append(m)
            if not xs:
                continue
            label = f"{router}_{kind}"
            ax.plot(
                xs, ys,
                color=color_map[router],
                linestyle=LINESTYLES[kind],
                marker=MARKERS[kind],
                linewidth=1.8,
                markersize=6,
                label=label,
            )

    ax.set_title(f"E2E Latency — sat={sat}%", fontsize=14, pad=12)
    ax.set_xlabel("Prefix length (tokens)", fontsize=11)
    ax.set_ylabel("Median E2E (s)", fontsize=11)
    ax.set_xticks(lengths)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))

    # expand y-axis a touch so lines near each other don't overlap the edges
    ymin, ymax = ax.get_ylim()
    margin = (ymax - ymin) * 0.08
    ax.set_ylim(ymin - margin, ymax + margin)
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    ax.grid(True, which="major", linestyle="--", alpha=0.4)
    ax.grid(True, which="minor", linestyle=":", alpha=0.2)

    ax.legend(
        loc="upper left",
        fontsize=8,
        ncol=2,
        framealpha=0.85,
        borderpad=0.6,
    )

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"  Saved {out_path}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    here = Path(__file__).parent.resolve()
    ap.add_argument("--results-dir", default=str(here), dest="results_dir")
    ap.add_argument("--out-dir",     default=str(here / "plots"), dest="out_dir")
    args = ap.parse_args()

    results_root = Path(args.results_dir)
    out_dir      = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading results from {results_root} …")
    data = load_results(results_root)

    # collect all lengths and sats seen across all routers
    all_lengths = sorted({
        length
        for rd in data.values()
        for sd in rd.values()
        for length in sd
    })
    all_sats = sorted({
        sat
        for rd in data.values()
        for sat in rd
    })

    print(f"Routers : {sorted(data.keys())}")
    print(f"Lengths : {all_lengths}")
    print(f"Sats    : {all_sats}")

    for sat in all_sats:
        out_path = out_dir / f"e2e_sat{sat}.png"
        plot_sat(data, sat, all_lengths, out_path)

    # combined side-by-side figure
    combined_path = out_dir / "e2e_combined.png"
    n = len(all_sats)
    fig, axes = plt.subplots(1, n, figsize=(11 * n, 6), sharey=False)
    if n == 1:
        axes = [axes]

    routers = sorted(data.keys())
    color_map = {r: COLORS[i % len(COLORS)] for i, r in enumerate(routers)}

    for ax, sat in zip(axes, all_sats):
        sat_data_all = data
        for router in routers:
            sat_data = sat_data_all[router].get(sat, {})
            for kind in ("cached", "new"):
                ys, xs = [], []
                for length in all_lengths:
                    vals = sat_data.get(length, {}).get(kind, [])
                    m = median_or_none(vals)
                    if m is not None:
                        xs.append(length)
                        ys.append(m)
                if not xs:
                    continue
                ax.plot(
                    xs, ys,
                    color=color_map[router],
                    linestyle=LINESTYLES[kind],
                    marker=MARKERS[kind],
                    linewidth=1.8,
                    markersize=6,
                    label=f"{router}_{kind}",
                )
        ax.set_title(f"sat={sat}%", fontsize=13, pad=10)
        ax.set_xlabel("Prefix length (tokens)", fontsize=10)
        ax.set_ylabel("Median E2E (s)", fontsize=10)
        ax.set_xticks(all_lengths)
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ymin, ymax = ax.get_ylim()
        margin = (ymax - ymin) * 0.08
        ax.set_ylim(ymin - margin, ymax + margin)
        ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
        ax.grid(True, which="major", linestyle="--", alpha=0.4)
        ax.grid(True, which="minor", linestyle=":", alpha=0.2)
        ax.legend(fontsize=7, ncol=2, framealpha=0.85, loc="upper left")

    fig.suptitle("E2E Latency by Router — all saturation levels", fontsize=14, y=1.01)
    fig.tight_layout()
    fig.savefig(combined_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {combined_path}")

    print("Done.")


if __name__ == "__main__":
    main()
