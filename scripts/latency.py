# -*- coding: utf-8 -*-
"""Start/stop responsiveness and accuracy verification: verify-log2.jsonl (new
logic) × DB true call times. Checks: (1) start latency (lag before start is
detected after the first byte) (2) stop latency (completed → sustained zero)
(3) no 0 readings during the ramp-up segment (4) per-call/integration accuracy
(5) no false positives in quiet periods (streaming should be false)
"""
import json, sqlite3, datetime, zoneinfo, statistics as st
from pathlib import Path

TZ = zoneinfo.ZoneInfo("Asia/Shanghai")
LOG = Path(__file__).parent.parent / "src-tauri/target/verify-log4.jsonl"
ticks = [json.loads(l) for l in LOG.read_text(encoding="utf-8").splitlines() if l.strip()]
t0, t1 = ticks[0]["t"], ticks[-1]["t"]
TICK = 0.7
print(f"Sample window: {datetime.datetime.fromtimestamp(t0/1000, TZ):%H:%M:%S} → "
      f"{datetime.datetime.fromtimestamp(t1/1000, TZ):%H:%M:%S}  ({(t1-t0)/1000:.0f}s, {len(ticks)} ticks)")

con = sqlite3.connect(f"file:{(Path.home()/'.zcode/cli/db/db.sqlite').as_posix()}?mode=ro", uri=True)
cur = con.cursor()
cur.execute("""SELECT session_id, completed_at, first_token_at,
               CASE WHEN completed_at > first_token_at THEN completed_at-first_token_at ELSE duration_ms END,
               output_tokens+reasoning_tokens
               FROM model_usage WHERE status='completed' AND first_token_at IS NOT NULL
                 AND completed_at > ? AND completed_at <= ? ORDER BY completed_at""", (t0 - 1000, t1))
calls = [c for c in cur.fetchall() if c[2] >= t0 - 5000]
print(f"Calls in window: {len(calls)}")

states = [(tk["t"], tk["live"]["stream"], tk["live"]["tps"], tk["live"]["ramp"]) for tk in ticks]

def turn_on_after(ts):
    """First stream=true moment after ts (must have been false just before)"""
    for i, (t, s, _, _) in enumerate(states):
        if t < ts: continue
        if s and (i == 0 or not states[i-1][1]):
            return t
        if s:
            return t  # already on
    return None

def stop_lag_after(comp):
    """Start of the first ≥2-tick false run after comp − the last true tick before it"""
    for i, (t, s, _, _) in enumerate(states):
        if t < comp: continue
        if not s and i > 0 and not states[i-1][1]:
            # false run starts at i-1; find the last true tick before it
            j = i - 1
            while j > 0 and states[j-1][1]:
                j -= 1
            return states[j][0], t
    return None, None

print("\n[Per call] session  done at   start lag  stop tail | ramp zeros | disp mean tps  true tps  ratio")
start_lats, stop_lags, zeros, ratios = [], [], [], []
for k, (sess, comp, ftok, gen, eff) in enumerate(calls):
    on_t = turn_on_after(ftok - 300)
    lat_s = (on_t - ftok) / 1000 if on_t else None
    last_true, false_start = stop_lag_after(comp)
    lat_e = (last_true - comp) / 1000 if last_true else 0.0
    ramp_ticks = [tk for tk in ticks if ftok <= tk["t"] <= min(ftok + 30_000, t1) and tk["live"]["stream"]]
    z = sum(1 for tk in ramp_ticks if tk["live"]["tps"] <= 0.01)
    zeros.append(z)
    win = [tk for tk in ticks if ftok <= tk["t"] <= comp and tk["live"]["stream"]]
    disp = st.mean([tk["live"]["tps"] for tk in win]) if win else 0.0
    true_tps = eff / (gen / 1000)
    r = None
    if eff > 200 and win:
        r = disp / true_tps
        ratios.append(r)
    if lat_s is not None:
        start_lats.append(lat_s)
    if last_true:
        stop_lags.append(lat_e)
    print(f"  {sess[-4:]}  {datetime.datetime.fromtimestamp(comp/1000, TZ):%H:%M:%S}  "
          f"{lat_s if lat_s is None else f'{lat_s:5.1f}s'}   "
          f"{lat_e:5.1f}s | {z}/{len(ramp_ticks)} ticks | {disp:7.1f}  {true_tps:7.1f}  "
          f"{'' if r is None else f'{r:.2f}x'}")

print(f"\n[Summary] start latency: {['%.1f' % x for x in start_lats]}")
print(f"       stop tail (completed → last true tick): {['%.1f' % x for x in stop_lags]}")
print(f"       ramp-up 0 readings: {sum(zeros)} ticks (across {len(zeros)} calls)")
if ratios:
    print(f"       per-call display/truth: median {st.median(ratios):.2f}x  range {min(ratios):.2f}~{max(ratios):.2f}x  mean abs deviation {st.mean([abs(r-1) for r in ratios]):.0%}")
integ = sum(tk["live"]["tps"] * TICK for tk in ticks[1:] if tk["live"]["stream"])
true_sum = sum(c[4] for c in calls)
print(f"       integration: {integ:.0f} tok vs true {true_sum} tok ({(integ-true_sum)/true_sum:+.1%})" if true_sum else "")
stream_n = sum(1 for tk in ticks if tk["live"]["stream"])
ramp_n = sum(1 for tk in ticks if tk["live"]["stream"] and tk["live"]["ramp"])
print(f"       time structure: streaming {stream_n} ticks ({stream_n/len(ticks):.0%}), of which counted-in-stats {ramp_n} ticks")
con.close()
