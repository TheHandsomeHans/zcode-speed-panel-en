#!/usr/bin/env python3
"""Live t/s vs per-call ground-truth reconciliation analysis.

Data source: ~/.zcode/speed-panel-debug.jsonl (panel debug log)
  - kind=cal  : calibration/reconciliation event after each call completes
    (v2 onward includes true_tps / pred_tps / clean_kb)
  - kind=tick : the panel's actual displayed values (tps / pipe / src)
  - kind=call : call ground truth (eff / gen_ms / true_tps)

Usage:
  python scripts/live_vs_true.py                 # analyze the default log
  python scripts/live_vs_true.py <log path>      # analyze the given log
  python scripts/live_vs_true.py --ticks         # extra: tick-level display value distribution

Measurement methodology:
  pred_tps = cleaned-stream bytes integrated over [first_token, completed]
  ÷ generation seconds ÷ current bpt — i.e. the "average t/s predicted by the
  display methodology during that call". The closer pred/true is to 1, the
  more accurate.
Old-format logs (cal without a pred_tps field) automatically fall back to the
tick replay method: compare the mean of io-source tick display values within
the call interval against ground truth.
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

DEFAULT_LOG = Path.home() / ".zcode" / "speed-panel-debug.jsonl"


def load(path: Path):
    calls, ticks, cals = [], [], []
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                d = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            kind = d.get("kind")
            if kind == "call":
                calls.append(d)
            elif kind == "tick":
                ticks.append(d)
            elif kind == "cal":
                cals.append(d)
    ticks.sort(key=lambda x: x["t"])
    return calls, ticks, cals


def recon_new(cals, calls):
    """v2 measurement methodology: cal events carry their own integration reconciliation fields"""
    by_id = {c["id"]: c for c in calls}
    rows = []
    for cal in cals:
        if cal.get("pred_tps") is None:
            continue
        c = by_id.get(cal["id"], {})
        rows.append({
            "done": cal.get("t", 0),
            "gen_s": cal.get("gen_ms", c.get("gen_ms", 0)) / 1000,
            "eff": cal.get("eff", c.get("eff", 0)),
            "true": cal.get("true_tps", c.get("true_tps", 0.0)),
            "pred": cal["pred_tps"],
            "bpt_sample": cal.get("bpt_sample", 0.0),
            "bpt_now": cal.get("bpt_now", 0.0),
            "clean_kb": cal.get("clean_kb", 0.0),
            "skipped": cal.get("skipped", True),
        })
    return rows


def recon_legacy(calls, ticks):
    """Legacy methodology: mean of io-source tick display values within the call interval vs ground truth"""
    rows = []
    for c in calls:
        t0 = c["done"] - c["gen_ms"]
        t1 = c["done"]
        win = [tk for tk in ticks
               if t0 + 1000 <= tk["t"] <= t1 - 500
               and tk.get("tps", 0) > 0 and tk.get("src") == "io"]
        if len(win) < 3:
            continue
        avg = statistics.mean(tk["tps"] for tk in win)
        rows.append({
            "done": t1,
            "gen_s": c["gen_ms"] / 1000,
            "eff": c["eff"],
            "true": c["true_tps"],
            "pred": avg,
            "bpt_sample": 0.0,
            "bpt_now": win[-1].get("bpt", 0.0),
            "clean_kb": 0.0,
            "skipped": False,
        })
    return rows


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else DEFAULT_LOG
    show_ticks = "--ticks" in sys.argv
    if not path.exists():
        print(f"Log not found: {path}")
        sys.exit(1)

    calls, ticks, cals = load(path)
    rows = recon_new(cals, calls)
    mode = "v2 integration reconciliation"
    if not rows:
        rows = recon_legacy(calls, ticks)
        mode = "legacy tick replay (recommend upgrading the panel and resampling)"

    qualified = [r for r in rows if r["eff"] >= 300 and not r["skipped"] and r["true"] > 0]
    print(f"Log: {path}")
    print(f"Methodology: {mode} | {len(calls)} calls, {len(rows)} reconciliation samples, qualified (≥300 tok and entered calibration) {len(qualified)}\n")
    if not qualified:
        print("No qualified reconciliation samples yet (generated once calls with ≥300 tokens complete).")
        return

    print(f"{'done at':>9} {'gen_s':>6} {'eff':>6} {'true':>7} {'pred':>7} {'ratio':>6} {'bpt_samp':>8} {'bpt_eff':>8}")
    ratios = []
    for r in qualified[-40:]:
        ratio = r["pred"] / r["true"] if r["true"] else 0.0
        ratios.append(ratio)
        t = r["done"] / 1000 % 86400
        hh, rem = divmod(int(t), 3600)
        mm, ss = divmod(rem, 60)
        print(f"{hh:02d}:{mm:02d}:{ss:02d}   {r['gen_s']:6.1f} {r['eff']:6d} "
              f"{r['true']:7.1f} {r['pred']:7.1f} {ratio:6.2f} {r['bpt_sample']:8.0f} {r['bpt_now']:8.0f}")

    med = statistics.median(ratios)
    within = sum(1 for x in ratios if 0.8 <= x <= 1.25) / len(ratios)
    print(f"\npred/true median = {med:.2f} (1.00 is exact) | share within ±20% = {within:.0%}")
    if med < 0.8:
        print("→ live readings still systematically low: report this table along with the tick log")
    elif med > 1.25:
        print("→ live readings systematically high: coefficient samples may be polluted by anomalous calls")
    else:
        print("→ live readings match ground truth ✓")

    if show_ticks:
        io_ticks = [t for t in ticks if t.get("src") == "io" and t.get("stream")]
        if io_ticks:
            src_count = {}
            for t in ticks:
                s = t.get("src")
                src_count[s] = src_count.get(s, 0) + 1
            print(f"\ntick source distribution: {src_count}")
            tps = [t["tps"] for t in io_ticks if t.get("tps")]
            print(f"io streaming ticks: n={len(tps)} median {statistics.median(tps):.1f} t/s "
                  f"p90 {sorted(tps)[int(len(tps)*0.9)]:.1f} max {max(tps):.1f}")
            pipes = [t.get("pipe", 0) for t in io_ticks]
            if any(pipes):
                print(f"clean-pipe byte rate: median {statistics.median(pipes):.0f} B/s "
                      f"max {max(pipes):.0f} B/s")


if __name__ == "__main__":
    main()
