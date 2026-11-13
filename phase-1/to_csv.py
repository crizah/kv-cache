#!/usr/bin/env python3
"""Aggregate experiment results into a CSV table.

Rows: one per (router, length, sat, kind) where kind is "cached" or "Unc"
      (Unc = new requests from KVIP run, used as the unconstrained baseline).

Columns: router, length, sat, kind, n, median_ttft_s, median_tpot_s, median_e2e_s

Usage:
    python3 to_csv.py [--results-dir phase-1] [--out phase-1/results.csv]
"""

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

ROUTER_LABELS = {
    "rr":                 "RR",
    "ll":                 "LL",
    "nixl":               "KVIP",
    "cache-only":         "CA",
    "cache-load-no-pull": "CLA",
}


def load(results_root: Path) -> dict:
    # data[router][sat][length][kind] = {ttft: [], tpot: [], e2e: []}
    data = defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(lambda: {"ttft": [], "tpot": [], "e2e": []})
            )
        )
    )
    for router_dir in sorted(results_root.iterdir()):
        if not router_dir.is_dir():
            continue
        router = ROUTER_LABELS.get(router_dir.name, router_dir.name)
        for f in router_dir.glob("exp_*.jsonl"):
            with open(f) as fh:
                for line in fh:
                    row = json.loads(line)
                    if row.get("error") or row.get("e2e_s") is None:
                        continue
                    kind = row["kind"]
                    # only keep "new" rows from KVIP (those become Unc)
                    if kind == "new" and router != "KVIP":
                        continue
                    label = "Unc" if kind == "new" else "cached"
                    bucket = data[router][row["sat"]][row["length"]][label]
                    if row.get("ttft_s") is not None:
                        bucket["ttft"].append(row["ttft_s"])
                    if row.get("tpot_s") is not None:
                        bucket["tpot"].append(row["tpot_s"])
                    bucket["e2e"].append(row["e2e_s"])
    return data


def p95(vals):
    if not vals:
        return ""
    s = sorted(vals)
    return round(s[int(0.95 * len(s))], 4)


def main():
    here = Path(__file__).parent.resolve()
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results-dir", default=str(here), dest="results_dir")
    ap.add_argument("--out",         default=str(here / "results.csv"), dest="out")
    args = ap.parse_args()

    data = load(Path(args.results_dir))

    rows = []
    for router in sorted(data):
        for sat in sorted(data[router]):
            for length in sorted(data[router][sat]):
                for kind in ("cached", "Unc"):
                    b = data[router][sat][length].get(kind)
                    if not b or not b["e2e"]:
                        continue
                    rows.append({
                        "router":      router,
                        "length":      length,
                        "sat":         sat,
                        "kind":        kind,
                        "n":           len(b["e2e"]),
                        "p95_ttft_s":  p95(b["ttft"]),
                        "p95_tpot_s":  p95(b["tpot"]),
                        "p95_e2e_s":   p95(b["e2e"]),
                    })

    out = Path(args.out)
    fieldnames = ["router", "length", "sat", "kind", "n",
                  "p95_ttft_s", "p95_tpot_s", "p95_e2e_s"]
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows → {out}")


if __name__ == "__main__":
    main()
