#!/usr/bin/env python3
"""
Plot pull vs recompute ttft_ms (median) per prefix length, one graph per results folder.
Also writes a summary CSV of medians.
"""

import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

BASE = os.path.dirname(os.path.abspath(__file__))
OUT_CSV = os.path.join(BASE, "summary_medians.csv")
OUT_PLOTS = BASE  # plots written alongside script


def find_csv(folder):
    for name in ("results.csv", "result.csv"):
        p = os.path.join(folder, name)
        if os.path.exists(p):
            return p
    raise FileNotFoundError(f"No results CSV in {folder}")


def load_folder(folder):
    csv_path = find_csv(folder)
    df = pd.read_csv(csv_path, usecols=["L", "condition", "ttft_ms"])
    df = df[df["condition"].isin(["recompute", "pull"])]
    label = os.path.basename(folder)  # e.g. results_part_0_100
    df["folder"] = label
    return df


def build_summary(folders):
    frames = [load_folder(f) for f in folders]
    raw = pd.concat(frames, ignore_index=True)
    summary = (
        raw.groupby(["folder", "L", "condition"])["ttft_ms"]
        .median()
        .reset_index()
        .rename(columns={"ttft_ms": "median_ttft_ms"})
    )
    summary = summary.sort_values(["folder", "L", "condition"])
    return summary


def plot_folder(folder_label, df_folder, out_dir):
    pivot = df_folder.pivot(index="L", columns="condition", values="median_ttft_ms").sort_index()

    fig, ax = plt.subplots(figsize=(10, 5))

    for cond, color, marker in [("recompute", "#1f77b4", "o"), ("pull", "#ff7f0e", "s")]:
        if cond in pivot.columns:
            ax.plot(
                range(len(pivot)),
                pivot[cond],
                label=cond,
                color=color,
                marker=marker,
                linewidth=2,
                markersize=6,
            )

    ax.set_xticks(range(len(pivot)))
    ax.set_xticklabels([str(x) for x in pivot.index], rotation=45, ha="right")
    ax.xaxis.set_minor_locator(ticker.NullLocator())

    ax.set_xlabel("Prefix length (tokens)")
    ax.set_ylabel("Median TTFT (ms)")
    ax.set_title(f"Pull vs Recompute — {folder_label}")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    fig.tight_layout()
    out_path = os.path.join(out_dir, f"plot_{folder_label}.png")
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {out_path}")


def main():
    folders = sorted(glob.glob(os.path.join(BASE, "results_part_0_*")))
    folders = [f for f in folders if os.path.isdir(f)]
    if not folders:
        raise SystemExit("No results_part_0_* directories found.")

    print(f"Found {len(folders)} folder(s):")
    for f in folders:
        print(f"  {f}")

    summary = build_summary(folders)
    summary.to_csv(OUT_CSV, index=False)
    print(f"\nSummary CSV written: {OUT_CSV}")
    print(summary.to_string(index=False))

    print("\nGenerating plots...")
    for folder_label, grp in summary.groupby("folder"):
        plot_folder(folder_label, grp, OUT_PLOTS)

    print("\nDone.")


if __name__ == "__main__":
    main()
