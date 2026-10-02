# -*- coding: utf-8 -*-
"""Three-source comparison: gauge live display (live.tps) vs chart measurement
methodology (persisted bucket speed) vs per-call ground truth after completion.
Usage: python scripts/compare3.py [verify-log6.jsonl]
Data source: the three log kinds from the verify sample, kind=tick/call/cal
"""
import json, sys, datetime, zoneinfo, statistics as st
from pathlib import Path

TZ = zoneinfo.ZoneInfo("Asia/Shanghai")
LOG = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent.parent / "src-tauri/target/verify-log6.jsonl"
recs = [json.loads(l) for l in LOG.read_text(encoding="utf-8").splitlines() if l.strip()]
ticks = [r for r in recs if r.get("kind") == "tick"]
t_win0, t_win1 = ticks[0]["t"], ticks[-1]["t"]
# The first tick ingests all of today's historical calls; keep only those completed within the window
calls = [r for r in recs if r.get("kind") == "call" and t_win0 - 5000 <= r["done"] <= t_win1]
cals = {r["id"]: r for r in recs if r.get("kind") == "cal"}
f = lambda t: datetime.datetime.fromtimestamp(t/1000, TZ).strftime('%H:%M:%S')
t0, t1 = t_win0, t_win1
print(f"Sample window {f(t0)} → {f(t1)} ({(t1-t0)/1000:.0f}s)  {len(calls)} calls\n")

print("[Per-call three-source comparison]")
print("done at   eff  gen_s | true tps | gauge mean (coverage) | chart bucket mean | cal sample bpt (active bpt) | gauge/true  chart/true")
gauge_ratios, chart_ratios = [], []
for c in calls:
    done, gen, eff, true_tps = c["done"], c["gen_ms"], c["eff"], c["true_tps"]
    w0 = done - max(gen, 0)
    # gauge: mean of displayed values over stream ticks within the call interval
    on = [tk for tk in ticks if w0 <= tk["t"] <= done and tk["live"]["stream"]]
    gauge = st.mean([tk["live"]["tps"] for tk in on]) if on else 0.0
    cover = len(on) / max(1, sum(1 for tk in ticks if w0 <= tk["t"] <= done))
    # chart: spark_tail is only the last 3 buckets; rebuild from persisted buckets —
    # aggregate call ground truth into 10s buckets, take the interval mean (here:
    # same-window calls' true tps weighted by generation duration = chart methodology)
    same = [x for x in calls if not (x["done"] < w0 or x["done"] - x["gen_ms"] > done)]
    chart = (sum(x["eff"] for x in same) / (sum(x["gen_ms"] for x in same) / 1000.0)) if same else 0.0
    cal = cals.get(c["id"])
    cal_desc = f"{cal['bpt_sample']:.0f}({cal['bpt_now']:.0f})" if cal else "-"
    gr = gauge / true_tps if true_tps > 3 and on else None
    chr_ = chart / true_tps if true_tps > 3 else None
    if gr: gauge_ratios.append(gr)
    if chr_: chart_ratios.append(chr_)
    print(f"  {f(done)}  {eff:>5}  {gen/1000:>4.1f} | {true_tps:>6.1f} | {gauge:>6.1f} ({cover:>3.0%}) | {chart:>6.1f} | {cal_desc:>14} | "
          f"{f'{gr:.2f}x' if gr else '-':>7}  {f'{chr_:.2f}x' if chr_ else '-':>7}")

if gauge_ratios:
    print(f"\n[Summary] gauge/true: median {st.median(gauge_ratios):.2f}x  mean {st.mean(gauge_ratios):.2f}x  range {min(gauge_ratios):.2f}~{max(gauge_ratios):.2f}x")
if chart_ratios:
    print(f"       chart/true:   median {st.median(chart_ratios):.2f}x  mean {st.mean(chart_ratios):.2f}x")
# whole window
integ = sum(tk["live"]["tps"] * 0.5 for tk in ticks[1:] if tk["live"]["stream"])
true_sum = sum(c["eff"] for c in calls if t0 <= c["done"] <= t1)
if true_sum:
    print(f"       integration: gauge {integ:.0f} vs true {true_sum} tok ({(integ-true_sum)/true_sum:+.1%})")
