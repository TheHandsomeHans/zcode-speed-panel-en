#!/usr/bin/env python3
"""Byte→token calibration coefficient estimator benchmark replay (real data).

Data source: ~/.zcode/speed-panel-debug.jsonl (including rotated .1/.2… files,
merged in ascending time order); extracts the accepted calibration sample
sequence of kind=cal records (bpt_sample = cleaned-stream integrated bytes ÷
true output+reasoning tokens), replays each estimator in completion order and
measures the relative error of "predicting the next call's sample" —
error = |active coefficient − that call's true sample| ÷ true sample,
equivalent to the coefficient error of the live reading during that call.

Estimators:
  prior   always uses the platform prior (lower-bound baseline)
  median5 old implementation: plain median of the last 5 samples (incl. prior
          placeholders)
  wmed10  previous implementation: recency-weighted median of the last 10
          samples, two-pass trimming to [0.4, 2.5], half-life 3 samples,
          linear shrink toward the prior when the window has fewer than 3
  wmed16  current implementation (liveio::cal_estimate): wmed10 with the window
          10→16, plus a one-step log shrink toward the newest sample
          (AR1, ρ=0.3, ratio clamped to [0.5, 2] — a single sample moves the
          estimate by ≤ ×1.23, outliers only pull for one beat, and the first
          sample of a step change gets first-mover weight)

Usage:
  python scripts/cal_bench.py                  # analyze the default log (current + rotated)
  python scripts/cal_bench.py <log.jsonl>      # analyze the given log
  python scripts/cal_bench.py --models         # extra: join the DB, group by model (informational)
  python scripts/cal_bench.py --corr           # extra: sample autocorrelation (basis for recency weighting)

Baseline conclusions (2026-09-24 second-round calibration, 262 real samples on this machine):
  prior   med=25.9%
  median5 med=24.3% mean=37.8%
  wmed10  med=21.3% p75=47.8% mean=37.1%
  wmed16  med=20.7% p75=41.5% mean=34.1% (blockwise validation shows the ρ
          extrapolation is stable: test blocks p75 51.9→44.3, mean 37.5→33.7,
          med flat)
Samples' B/token naturally fluctuates call to call (p25~p75=466~740, log-space
lag-1 autocorrelation ≈0.50, lag-2 ≈ 0.27 ≈ ρ², pure AR(1)), which sets an
error floor near a ~20% median — live readings off by twenty to thirty percent
are normal fluctuation; do not tune parameters rashly based on this. Any
estimator change must pass the benchmark: a real replay of this script (compare
before vs after, no regression allowed) + the liveio tests `benchmark_*`
(steady-state AR(1) / step convergence / bounded outliers).
Approaches already replayed and rejected (do not retry lightly):
  per-model calibration (validated in two rounds: GLM-5.3 and Flash distributions
    match; deepseek-v4-flash has only 3 samples, med≈221 — the magnitude gap is
    driven by speed, not the model, and the sample is too thin to support its
    own queue);
  two-parameter model bytes ≈ a×token + c×duration (bpt correlates negatively
    with true speed, r≈-0.3~0.5, caused by chunked rendering; exploiting it
    would require the unknown per-call speed; per-call prediction error med
    48%~90%);
  EWMA / global shrink / larger mean window (half-life 6) / eff- or
    duration-weighted — no gain or worse;
  AR1 "skip the shrink for out-of-range samples" — uniformly worse than the
    "clamped pull" (clamp bounds [0.5,2]).
"""
from __future__ import annotations

import glob
import json
import math
import statistics
import sys
from pathlib import Path

DEFAULT_LOG = Path.home() / ".zcode" / "speed-panel-debug.jsonl"

# Keep in sync with the constants in src-tauri/src/liveio.rs
CAL_QUEUE_CAP = 16
CAL_HALF_LIFE = 3.0
CAL_TRIM_LO, CAL_TRIM_HI = 0.4, 2.5
CAL_SHRINK_N = 3
CAL_AR1_RHO = 0.3
CAL_AR1_CLAMP = 2.0
PRIOR = 600.0


def load_events(path: Path) -> list[dict]:
    """Log + rotated siblings (.1/.2…) merged in file order (oldest first); returns the accepted cal event sequence."""
    files = sorted(
        glob.glob(str(path) + ".*"),
        key=lambda p: int(p.rsplit(".", 1)[-1]) if p.rsplit(".", 1)[-1].isdigit() else 0,
    )
    files.append(str(path))
    evs: list[dict] = []
    for fp in files:
        try:
            with open(fp, encoding="utf-8", errors="ignore") as f:
                for line in f:
                    try:
                        d = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if d.get("kind") == "cal" and not d.get("skipped") and d.get("bpt_sample", 0) > 0:
                        evs.append(d)
        except OSError:
            continue
    return evs


def _wmed(win: list[float], keep) -> float:
    """Recency-weighted median (half-life 3): same measurement methodology as liveio::weighted_median."""
    pairs = [
        (v, 0.5 ** ((len(win) - 1 - i) / CAL_HALF_LIFE))
        for i, v in enumerate(win)
        if keep(v)
    ]
    if not pairs:
        return sorted(win)[len(win) // 2]
    pairs.sort()
    total = sum(w for _, w in pairs)
    acc = 0.0
    for v, w in pairs:
        acc += w
        if acc >= total / 2:
            return v
    return pairs[-1][0]


def cal_estimate(hist: list[float], prior: float = PRIOR) -> float:
    """Same measurement methodology as src-tauri/src/liveio.rs::cal_estimate
    (two-pass trim + AR1 recency shrink + prior shrink)."""
    win = hist[-CAL_QUEUE_CAP:]
    if not win:
        return prior
    center = _wmed(win, lambda _v: True)
    if center <= 0:
        return prior
    est = _wmed(win, lambda v: center * CAL_TRIM_LO <= v <= center * CAL_TRIM_HI)
    ratio = min(max(win[-1] / est, 1.0 / CAL_AR1_CLAMP), CAL_AR1_CLAMP)
    est *= ratio**CAL_AR1_RHO
    shrink = min(len(win) / CAL_SHRINK_N, 1.0)
    return est * shrink + prior * (1 - shrink)


def wmed10(hist: list[float], prior: float = PRIOR) -> float:
    """Previous implementation: 10-sample recency-weighted median (no AR1 shrink), kept for comparison."""
    win = hist[-10:]
    if not win:
        return prior
    center = _wmed(win, lambda _v: True)
    if center <= 0:
        return prior
    est = _wmed(win, lambda v: center * CAL_TRIM_LO <= v <= center * CAL_TRIM_HI)
    shrink = min(len(win) / CAL_SHRINK_N, 1.0)
    return est * shrink + prior * (1 - shrink)


def median5(hist: list[float], prior: float = PRIOR) -> float:
    """Old implementation: plain median of the last 5 (queue shape including prior placeholders)."""
    win = hist[-5:]
    return sorted(win)[len(win) // 2]


def replay(samples: list[float], est) -> list[float]:
    """When predicting sample i, only history [0, i) may be used; error is relative to the true sample."""
    errs, hist = [], []
    for s in samples:
        if hist:
            errs.append(abs(est(hist) - s) / s)
        hist.append(s)
    return errs


def table(name: str, errs: list[float]) -> str:
    errs = sorted(errs)
    med = errs[len(errs) // 2]
    p75 = errs[int(len(errs) * 0.75)]
    p90 = errs[int(len(errs) * 0.90)]
    mean = sum(errs) / len(errs)
    return f"  {name:<10} med={med*100:5.1f}%  p75={p75*100:5.1f}%  p90={p90*100:5.1f}%  mean={mean*100:5.1f}%"


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}
    path = Path(args[0]) if args else DEFAULT_LOG
    evs = load_events(path)
    if len(evs) < 20:
        print(f"Not enough valid calibration samples ({len(evs)}) at: {path}")
        return 1
    samples = [e["bpt_sample"] for e in evs]
    qs = statistics.quantiles(samples, n=4)
    print(f"samples n={len(samples)}  min={min(samples):.0f}  p25={qs[0]:.0f}  "
          f"med={statistics.median(samples):.0f}  p75={qs[2]:.0f}  max={max(samples):.0f}")
    print("Relative error predicting the next call's sample (= the live reading's coefficient error):")
    print(table("prior", replay(samples, lambda _h: PRIOR)))
    print(table("median5", replay(samples, median5)))
    print(table("wmed10", replay(samples, wmed10)))
    print(table("wmed16", replay(samples, cal_estimate)))

    if "--corr" in flags:
        logs = [math.log(s) for s in samples]
        m = sum(logs) / len(logs)
        den = sum((x - m) ** 2 for x in logs)
        for k in (1, 2, 3):
            num = sum((logs[i] - m) * (logs[i + k] - m) for i in range(len(logs) - k))
            print(f"  lag{k} autocorrelation = {num / den:.3f}")

    if "--models" in flags:
        import sqlite3
        dbp = Path.home() / ".zcode" / "cli" / "db" / "db.sqlite"
        if dbp.exists():
            db = sqlite3.connect(f"file:{dbp}?mode=ro", uri=True)
            ids = [e["id"] for e in evs]
            model: dict[str, str] = {}
            for i in range(0, len(ids), 400):
                chunk = ids[i:i + 400]
                q = f"SELECT id, model_id FROM model_usage WHERE id IN ({','.join('?' * len(chunk))})"
                model.update(db.execute(q, chunk))
            groups: dict[str, list[float]] = {}
            for e in evs:
                groups.setdefault(model.get(e["id"], "?"), []).append(e["bpt_sample"])
            print("  Grouped by model (informational: per-model calibration does not improve error, see the module docstring):")
            for name, xs in sorted(groups.items(), key=lambda kv: -len(kv[1])):
                print(f"    {name:<28} n={len(xs):3d}  med={statistics.median(xs):.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
