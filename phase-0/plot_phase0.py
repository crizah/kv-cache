#!/usr/bin/env python3
"""Phase-0 plots: median TTFT vs prefix length for recompute and pull.

Three subplots side by side:
  1. 25% GPU, 10 Gbps  (phase-0/results_part_0_25/results.csv)
  2. 25% GPU,  6 Gbps  (phase-0/25_6gbps/sweep.csv)
  3. 50% GPU, 10 Gbps  (phase-0/results_part_0_50/results.csv)

Usage:
    python3 phase-0/plot_phase0.py [--out phase-0/plots/phase0.png]
"""

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

HERE = Path(__file__).parent.resolve()

PANELS = [
    {
        "label":  "25% GPU · 10 Gbps",
        "path":   HERE / "results_part_0_25" / "results.csv",
        "conditions": {"recompute": "Recompute", "pull": "Pull"},
    },
    {
        "label":  "25% GPU · 6 Gbps",
        "path":   HERE / "25_6gbps" / "sweep.csv",
        "conditions": {"recompute": "Recompute", "pull": "Pull₆"},
    },
    {
        "label":  "50% GPU · 10 Gbps",
        "path":   HERE / "again_50" / "sweep.csv",
        "conditions": {"recompute": "Recompute", "pull": "Pull"},
    },
]

COLORS = {
    "recompute": "#e6194b",
    "pull":      "#4363d8",
}
MARKERS = {
    "recompute": "o",
    "pull":      "s",
}


def load(path: Path) -> dict:
    """Returns data[condition][L] = [ttft_s, ...]"""
    data = defaultdict(lambda: defaultdict(list))
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            cond = row["condition"]
            if cond not in ("recompute", "pull"):
                continue
            L = int(row["L"])
            if L == 24567:
                L = 24576
            ttft_s = float(row["ttft_ms"]) / 1000.0
            data[cond][L].append(ttft_s)
    return data


def p50(vals):
    return statistics.median(vals) if vals else None


def plot_panel(ax, data: dict, title: str):
    lengths = sorted({L for cond_data in data.values() for L in cond_data})

    for cond, label in [("recompute", "Recompute"), ("pull", "Pull")]:
        cond_data = data.get(cond, {})
        xs, ys = [], []
        for L in lengths:
            m = p50(cond_data.get(L, []))
            if m is not None:
                xs.append(L)
                ys.append(m)
        if not xs:
            continue
        ax.plot(xs, ys,
                color=COLORS[cond],
                marker=MARKERS[cond],
                linewidth=1.8,
                markersize=5,
                label=label)

    ax.set_title(title, fontsize=11, pad=8)
    ax.set_xlabel("Prefix length (tokens)", fontsize=9)
    ax.set_ylabel("Median TTFT (s)", fontsize=9)
    ax.set_xticks(lengths)
    ax.set_xticklabels([str(L) for L in lengths], rotation=45, ha="right", fontsize=7)
    ymin, ymax = ax.get_ylim()
    margin = (ymax - ymin) * 0.08
    ax.set_ylim(max(0, ymin - margin), ymax + margin)
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    ax.grid(True, which="major", linestyle="--", alpha=0.4)
    ax.grid(True, which="minor", linestyle=":", alpha=0.2)
    ax.legend(fontsize=8, framealpha=0.85)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=str(HERE / "plots" / "phase0.png"))
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    # individual panels
    for panel in PANELS:
        data = load(panel["path"])
        fig_i, ax_i = plt.subplots(figsize=(7, 5))
        plot_panel(ax_i, data, panel["label"])
        fig_i.tight_layout()
        slug = panel["label"].lower().replace(" ", "_").replace("·", "").replace("/", "").replace("%", "pct").replace("__", "_")
        ind_path = out.parent / f"phase0_{slug}.png"
        fig_i.savefig(ind_path, dpi=150, bbox_inches="tight")
        plt.close(fig_i)
        print(f"  Saved {ind_path}")

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    for ax, panel in zip(axes, PANELS):
        data = load(panel["path"])
        plot_panel(ax, data, panel["label"])

    fig.suptitle("Phase 0: Recompute vs Pull — Median TTFT", fontsize=13, y=1.02)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
