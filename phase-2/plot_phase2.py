#!/usr/bin/env python3
"""Phase-2 plots: p95 E2E latency vs prefix length for KVIP and L3.

Three subplots side by side, one per saturation level (0%, 80%, 100%).
X axis: prefix length. Y axis: p95 b_e2e_total_s.

Usage:
    python3 phase-2/plot_phase2.py [--out phase-2/plots/phase2.png]
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

SYSTEMS = {
    "nixl": ("KVIP", "#4363d8"),
    "l3":   ("L3",   "#e6194b"),
}
SATS    = [0, 80, 100]
MARKERS = {"nixl": "o", "l3": "s"}


def load(path: Path) -> list:
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            if r.get("valid", "True") == "True":
                rows.append(r)
    return rows


def p95(vals):
    if not vals:
        return None
    s = sorted(vals)
    return s[int(0.95 * len(s))]


def plot_panel(ax, data: dict, sat: int):
    lengths = sorted({L for sys_data in data.values() for L in sys_data})

    for sys_key, (label, color) in SYSTEMS.items():
        sys_data = data.get(sys_key, {})
        xs, ys = [], []
        for L in lengths:
            v = p95(sys_data.get(L, {}).get(sat, []))
            if v is not None:
                xs.append(L)
                ys.append(v)
        if not xs:
            continue
        ax.plot(xs, ys,
                color=color,
                marker=MARKERS[sys_key],
                linewidth=1.8,
                markersize=6,
                label=label)

    ax.set_title(f"sat={sat}%", fontsize=11, pad=8)
    ax.set_xlabel("Prefix length (tokens)", fontsize=9)
    ax.set_ylabel("p95 E2E (s)", fontsize=9)
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
    ap.add_argument("--out", default=str(HERE / "plots" / "phase2.png"))
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    # data[sys_key][L][sat] = [e2e_s, ...]
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for sys_key in SYSTEMS:
        rows = load(HERE / sys_key / "results.csv")
        for r in rows:
            L   = int(r["length"])
            sat = int(r["sat"])
            v   = r.get("b_e2e_total_s", "")
            if v not in ("", None):
                data[sys_key][L][sat].append(float(v))

    # individual panels
    for sat in SATS:
        fig_i, ax_i = plt.subplots(figsize=(7, 5))
        plot_panel(ax_i, data, sat)
        fig_i.tight_layout()
        ind_path = out.parent / f"phase2_sat{sat}.png"
        fig_i.savefig(ind_path, dpi=150, bbox_inches="tight")
        plt.close(fig_i)
        print(f"  Saved {ind_path}")

    # combined
    fig, axes = plt.subplots(1, len(SATS), figsize=(7 * len(SATS), 5))
    for ax, sat in zip(axes, SATS):
        plot_panel(ax, data, sat)

    fig.suptitle("Phase 2: p95 E2E Latency — KVIP vs L3", fontsize=13, y=1.02)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {out}")


if __name__ == "__main__":
    main()
