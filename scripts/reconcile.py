# -*- coding: utf-8 -*-
"""Post-hoc reconciliation: verify-log.jsonl (live display from the panel's own
engine + raw per-process written bytes) × usage DB (true tokens persisted after
each conversation ends), evaluating:
  1) accuracy of the live speed display (per call + whole-window integration)
  2) how much the byte→token self-calibration coefficient is polluted by
     multi-process noise
  3) totals chain: panel final value vs recomputation from persisted data
"""
import json, sqlite3, sys, datetime, zoneinfo
from pathlib import Path

TZ = zoneinfo.ZoneInfo("Asia/Shanghai")
LOG = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("src-tauri/target/verify-log.jsonl")
DB = Path.home() / ".zcode/cli/db/db.sqlite"
MY_SESS = sys.argv[2] if len(sys.argv) > 2 else None

ticks = [json.loads(l) for l in LOG.read_text(encoding="utf-8").splitlines() if l.strip()]
t0, t1 = ticks[0]["t"], ticks[-1]["t"]
dur_s = (t1 - t0) / 1000
print(f"Sample window: {datetime.datetime.fromtimestamp(t0/1000, TZ):%H:%M:%S} → "
      f"{datetime.datetime.fromtimestamp(t1/1000, TZ):%H:%M:%S}  ({dur_s:.0f}s, {len(ticks)} ticks)")

# ---- Raw series ----
raw_pids = {}   # pid -> [(t, bytes)]
files = []      # [(t, total)]
for tk in ticks:
    files.append((tk["t"], tk["files"]))
    for pid, b in tk["raw"].items():
        raw_pids.setdefault(int(pid), []).append((tk["t"], b))
print(f"CLI processes observed: {len(raw_pids)}")

def series_delta(series, w0, w1):
    """Same window-overlap rule as the panel: sum deltas where ct>=w0 and pt<=w1"""
    acc = 0.0
    for (pt, pv), (ct, cv) in zip(series, series[1:]):
        if ct >= w0 and pt <= w1:
            acc += max(0, cv - pv)
    return acc

# ---- True calls persisted to disk (completed within the window) ----
con = sqlite3.connect(f"file:{DB.as_posix()}?mode=ro", uri=True)
cur = con.cursor()
cur.execute("""SELECT id, session_id, completed_at,
               CASE WHEN first_token_at IS NOT NULL AND completed_at > first_token_at
                    THEN completed_at - first_token_at ELSE duration_ms END,
               output_tokens, reasoning_tokens, input_tokens
               FROM model_usage WHERE status='completed' AND completed_at > ? AND completed_at <= ?
               ORDER BY completed_at""", (t0 - 1000, t1 + 1000))
calls = cur.fetchall()
print(f"Calls persisted in window: {len(calls)}, true generated tokens (out+rea) total {sum(c[4]+c[5] for c in calls)}")

# ---- Per-call reconciliation ----
print("\n[Per call] done at   session(last4)  out+rea   gen_s  true tps | panel bpt  clean bpt  pollution | disp mean tps (coverage)")
rows = []
for cid, sess, comp, gen, out, rea, inp in calls:
    eff = out + rea
    gen_s = max(gen, 50) / 1000
    true_tps = eff / gen_s
    w0 = comp - min(gen, 300_000)
    w1 = comp
    per_pid = {p: series_delta(s, w0, w1) for p, s in raw_pids.items()}
    total_b = sum(per_pid.values())
    file_b = series_delta(files, w0, w1)
    top_pid, top_b = max(per_pid.items(), key=lambda kv: kv[1]) if per_pid else (None, 0.0)
    # panel-methodology bpt (sum over all processes − persisted)
    panel_bpt = (total_b - file_b) / eff if eff and total_b > file_b else None
    if panel_bpt is not None:
        panel_bpt = min(max(panel_bpt, 400), 8000)
    # clean-methodology bpt (only the largest-byte process − persisted)
    clean_bpt = (top_b - file_b) / eff if eff and top_b > file_b else None
    others_b = total_b - top_b
    poll = others_b / total_b if total_b else 0
    # mean and coverage of the panel's displayed speed during the call's streaming interval
    disp = [(tk["t"], tk["live"]["tps"]) for tk in ticks
            if w0 <= tk["t"] <= w1 and tk["live"]["stream"]]
    cov = len(disp) / max(1, sum(1 for tk in ticks if w0 <= tk["t"] <= w1))
    disp_mean = sum(v for _, v in disp) / len(disp) if disp else 0.0
    rows.append((cid, sess[-4:], eff, gen_s, true_tps, panel_bpt, clean_bpt, poll, disp_mean, cov))
    print(f"  {datetime.datetime.fromtimestamp(comp/1000, TZ):%H:%M:%S}  {sess[-4:]}"
          f"  {eff:>7}  {gen_s:>6.1f}  {true_tps:>7.1f} | "
          f"{panel_bpt if panel_bpt is None else round(panel_bpt)}  "
          f"{clean_bpt if clean_bpt is None else round(clean_bpt)}  {poll:>5.0%} | "
          f"{disp_mean:>7.1f} ({cov:.0%})")

import statistics as st
def med(xs):
    xs = [x for x in xs if x is not None]
    return st.median(xs) if xs else float("nan")

print(f"\n[Calibration coefficient] panel bpt median={med([r[5] for r in rows]):.0f}  "
      f"clean bpt median={med([r[6] for r in rows]):.0f}  (default 1600, clamped to [400,8000])")
print(f"[Crosstalk] median share of bytes from non-owning processes={med([r[7] for r in rows]):.0%}")

# ---- Whole-window integration: displayed speed × time vs persisted truth ----
integ = sum(tk["live"]["tps"] * 0.5 for tk in ticks[1:] if tk["live"]["stream"])
true_sum = sum(r[2] for r in rows)
print(f"\n[Integration reconciliation] displayed-speed integral≈{integ:.0f} tok  vs true persisted in window {true_sum} tok  "
      f"deviation {(integ-true_sum)/true_sum if true_sum else 0:+.1%}")
mine = [r for r in rows if MY_SESS and r[1] == MY_SESS[-4:]]
if mine:
    print(f"  This session only ({MY_SESS[-4:]}): {len(mine)} calls, {sum(r[2] for r in mine)} tok")

# ---- Totals chain: panel value at the last tick vs recomputation from persisted data ----
last = ticks[-1]["snap"]
cur.execute("""SELECT COALESCE(SUM(output_tokens),0), COALESCE(SUM(reasoning_tokens),0),
               COALESCE(SUM(input_tokens),0), COALESCE(SUM(cache_creation_input_tokens),0),
               COALESCE(SUM(cache_read_input_tokens),0), COUNT(*) FROM model_usage
               WHERE status='completed' AND completed_at >= (
                 SELECT MIN(completed_at) FROM model_usage WHERE completed_at >= ?
                 )""", (t0 - 86_400_000,))
# uses the panel's own "today at midnight" measurement methodology
import time
local_mid = int(datetime.datetime.combine(datetime.datetime.fromtimestamp(t1/1000, TZ).date(),
                                          datetime.time.min, tzinfo=TZ).timestamp() * 1000)
cur.execute("""SELECT COALESCE(SUM(output_tokens),0), COALESCE(SUM(reasoning_tokens),0),
               COALESCE(SUM(input_tokens),0), COALESCE(SUM(cache_creation_input_tokens),0),
               COALESCE(SUM(cache_read_input_tokens),0), COUNT(*) FROM model_usage
               WHERE status='completed' AND completed_at >= ? AND completed_at <= ?""", (local_mid, t1))
o, r, i, cc, cr, n = cur.fetchone()
panel_total = last["out"] + last["rea"] + last["inp"] + last["cc"]
disk_total = o + r + i + cc
print(f"\n[Totals chain] last sampled tick (engine aggregate) = {panel_total}  vs recomputed from disk (same-methodology SQL) = {disk_total}  "
      f"diff {panel_total - disk_total}  ({(panel_total-disk_total)/disk_total:+.4%})")
print(f"  detail: engine out={last['out']} rea={last['rea']} inp={last['inp']} calls={last['calls']}"
      f"  | disk out={o} rea={r} inp={i} calls={n}")
cur.execute("""SELECT SUM(computed_total_tokens) FROM model_usage
               WHERE status='completed' AND completed_at >= ? AND completed_at <= ?""", (local_mid, t1))
print(f"  official computed_total (excl. reasoning) = {cur.fetchone()[0]}  panel's extra reasoning tokens = {last['rea']}")
con.close()
