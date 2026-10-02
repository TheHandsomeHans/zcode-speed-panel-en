//! Live speed measurement: polls the IO write-byte counters of ZCode CLI processes (the streaming render data
//! sent to the desktop UI pipe); while the model streams output this counter grows steadily at tens of KB/s,
//! making it a genuine real-time signal.
//!
//! Platform primitives live in [`platform`]: Windows uses Toolhelp enumeration + GetProcessIoCounters to read
//! cumulative write bytes; macOS uses libproc enumeration + proc_pid_rusage's ri_diskio_byteswritten.
//!
//! - Process discovery: Windows filters `zcode.exe` whose command line contains `zcode.cjs`; macOS matches
//!   `zcode-cli` exactly via KERN_PROCARGS2 command-line args (multiple processes may coexist on both)
//! - Disk-flush deduction (Windows only): growth of rollout/log/WAL is subtracted from byte deltas (the spike
//!   source at the completion/flush instant), deducted only once overall on the **aggregated stream** (deducting
//!   per process when summing processes would subtract the global file growth N times). The deduction allows
//!   negative ticks (misalignment between flush and writes converges via interval sums) and is clamped
//!   non-negative only at window aggregation — clamping each tick to 0 would permanently swallow misaligned
//!   flush growth (measured readings collapsing to 1/5 of ground truth were caused by exactly this). macOS
//!   skips this deduction (the rollout dir's net change can be negative, backfiring on the cleaning stream;
//!   see platform::mac's tracked_files_total)
//! - Noise floor: BASE_NOISE + a per-process adaptive heartbeat floor (capped, so the quantile is not
//!   "poisoned" by streaming deltas during sustained streaming, which would deduct our own output as noise);
//!   parameters differ per platform (mac idle measures strictly 0 bytes, so the static floor is 0)
//! - Burst removal: a tick whose raw delta exceeds the threshold is dropped whole and excluded from
//!   integration (Windows only: request-body upload ~190KB/tick is separable from real streaming ~52KB/tick;
//!   mac streaming is itself burst-per-tick, so the threshold is disabled in CleanParams)
//! - Byte→token conversion [consistency calibration]: after a call completes, take the exact cleaning stream used by the display path,
//!   integrate it over [first_token, completed] and divide by real output_tokens to get a sample;
//!   after admission it feeds the sliding estimator ([`cal_estimate`]) which produces the effective coefficient.
//!   The calibration numerator shares its source with the display numerator, so any systematic deduction
//!   (noise floor/disk flush/misalignment) is cancelled by the coefficient and the display converges to true t/s.
//!   Historical lesson: calibrating on the uncleaned total byte stream while displaying the cleaned stream —
//!   two pipelines with inconsistent measurement methodology once inflated the coefficient 2~3x and biased
//!   readings systematically low. Sample admission also carries a cross-process guard: reject samples when
//!   other processes' byte share in the window is too high (concurrent streaming in another window), so foreign
//!   bytes cannot pollute the attributed process's integral and the coefficient.
//!   The estimator is "16-sample recency-weighted median + mild trimming + AR1 recency shrinkage"
//!   ([`cal_estimate`]): B/token fluctuates naturally per call (262 real local samples:
//!   p25~p75 = 466~740, range 137~1591; log-space lag-1 autocorrelation ≈0.50,
//!   lag-2 ≈ 0.27 ≈ ρ² — pure AR(1); consecutive calls share content style; model-independent:
//!   per-model calibration showed no benefit in two replays, GLM-5.3 and Flash distributions match), so the
//!   estimator can only suppress noise, not eliminate it. Real-replay relative error predicting the next call:
//!   median went from 24.3% for the old 5-sample median → 21.3% for a 10-sample window → 20.7% now,
//!   p75 47.8%→41.5%
//!   (scripts/cal_bench.py, 262 samples; i.e. the coefficient error bound of live readings; synthetic end-to-end
//!   benchmarks in liveio tests `benchmark_*`). Rejected by replay: per-model calibration, bytes
//!   ≈ a×token + c×duration two-parameter model (B/token's negative correlation with real speed comes from
//!   render batching; exploiting it requires the unknown current-call speed, per-call prediction error 48%~90%).
//!   mac's disk write bytes are a page-cache async flush count (lagging write() by seconds~tens of seconds); the
//!   calibration window extends to completed + cal_grace_ms (delayed-flush grace; Windows=0, processed same tick),
//!   with outlier rejection for samples deviating beyond a multiple of the effective coefficient (disabled on Windows).

use crate::metrics::Call;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Current speed statistics window (display magnitude)
const WINDOW_MS: i64 = 30_000;
/// Process/file sampling ring capacity (~3min @700ms, covers the calibration lookback window)
const RING_CAP: usize = 260;
/// Process list refresh interval
const REFRESH_EVERY: Duration = Duration::from_secs(30);
/// Streaming detection threshold (cleaned rate)
const STREAMING_BPS: f64 = 4_000.0;
/// Start/stop is decided by the call gate; this short window only locates the magnitude anchor (first-byte tick)
const DETECT_MS: i64 = 2_500;

/// Stop-detection fallback grace: the gate is still on (the message row's completed has not been backfilled
/// to disk; the CLI side may lag by seconds~minutes) but the cleaned flow rate has stayed below the streaming
/// threshold for this long after the streaming anchor appeared → generation is deemed
/// actually stopped; the reading zeroes out and no longer shows "generating/estimated". Silent-pipe calls
/// without an anchor are unaffected (having no bytes at all is normal for them; the window fallback serves
/// them); normal streaming byte gaps are far shorter than this value, and a misjudgment self-heals the tick
/// bytes resume
const SILENT_STOP_MS: i64 = 15_000;
/// Startup hint window: upper bound on how long the gate can be on without observed streaming bytes (TTFT).
/// Within the window, show a "measuring…" hint (not a misleading estimate); if still no bytes past the window,
/// treat it as a silent-pipe call and fall back to a recent ground-truth estimate (≈)
const TTFT_HINT_MS: i64 = 20_000;
/// Rough upper bound of idle heartbeat noise (B/s), stacked with the per-process adaptive floor (after capping) to filter heartbeats
const BASE_NOISE_BPS: f64 = 3_000.0;
/// Per-tick cap on the per-process adaptive heartbeat floor (bytes/tick). Streaming can reach ~75KB/s (52KB/tick) in practice;
/// without capping, the quantile floor gets lifted by streaming deltas during sustained streaming, deducting our own output as noise
const FLOOR_CAP_BYTES: f64 = 2_000.0;
/// Raw-delta-per-tick rejection threshold: request-body upload measures ~190KB/tick (>90KB even when split),
/// real streaming peaks at ~52KB/tick; take the middle and drop the whole tick (neither bytes nor duration enter integration)
const BURST_TICK_BYTES: f64 = 100_000.0;
/// Initial bytes/token coefficient (cleaned stream measures ~350~900; take a conservative value; converges quickly after calibration)
const DEFAULT_BPT: f64 = 600.0;
/// Plausible range for calibration samples (guards the median against anomalous samples). Under consistency
/// calibration the coefficient absorbs systematic deductions (disk-flush mirror/noise floor/burst-tick removal),
/// so the range must be wide enough not to break convergence;
/// garbage samples are mostly blocked by the call-size gate CAL_MIN_TOKENS
const CAL_MIN: f64 = 100.0;
const CAL_MAX: f64 = 6_000.0;
/// Minimum call size for calibration samples: small calls have a large fixed UI frame overhead share, so they must not become samples
const CAL_MIN_TOKENS: u64 = 300;
/// Round average-speed drift auto-recalibration: a round = one continuous stretch of the gate's "in progress" signal;
/// the displayed speed within the round (io-measured ticks) is averaged arithmetically; if the last round's mean differs
/// from the mean of the previous DRIFT_ROUNDS consecutive rounds by ≥ DRIFT_RATIO (either direction), a magnitude shift is declared
/// (model/tokenizer change; the old coefficient is likely stale)
const DRIFT_ROUNDS: usize = 5;
const DRIFT_RATIO: f64 = 3.0;
/// Session→process attribution switch hysteresis: for a session that already has attribution, switch only if the candidate
/// process's raw bytes in the call window are ≥ this multiple of the current attributed process's. When two CLI windows stream concurrently,
/// the top-writer flips call by call (2026-09-18 incident: attribution for two adjacent calls of the same session oscillated between two pids,
/// calibration samples got polluted by the other window's bytes, the coefficient swung between 262~764 and readings were off 2~3x)
const ATTR_SWITCH_RATIO: f64 = 2.0;
/// Concurrent attribution dedup threshold: when the top process is already owned by another in-progress session, re-attribute to the
/// runner-up only if its window bytes reach this fraction of the top's. Two concurrent streams have similar rates (ratio ~1); an
/// idle process's floor-noise leak is ~0.1; 0.5 sits in the middle — corrects tie mis-attribution without pushing a
/// shared process's session onto an idle process (2026-09-18 incident: a new task in the same ZCode window reused the same app-server
/// process, both sessions' bytes went through the same pid, and the runner-up had only ~0.17 noise ratio)
const ATTR_DEDUP_RATIO: f64 = 0.5;
/// Cross-process guard for calibration samples: if the raw bytes of **other** processes in the call window exceed this fraction of the
/// attributed process's, reject the sample — the attributed process's cleaned integral must then mix in the other window's streaming semantics, distorting the sample
const CROSS_PID_RATIO: f64 = 0.2;

/// Cleaning/calibration parameters (parameterized per platform). The Windows column holds long-term measured tuning values (the original constants above,
/// do not modify); the macOS column was corrected from a 120s probe + 2026-09-17 ground-truth reconciliation (6 cal events;
/// with correct attribution, pred_tps and true_tps matched exactly): during streaming
/// ri_diskio_byteswritten ≈195KB/s, idle strictly 0 bytes, per-tick deltas bursty
/// 0,0,0,+225KB~1.5MB, ground-truth B/token ≈650 (the probe's ≈3900 was a misread),
/// disk counters are page-cache async flushes (lagging write() by seconds~tens of seconds).
#[derive(Clone, Copy)]
pub struct CleanParams {
    /// Per-tick raw-delta rejection threshold (Windows: request-body upload ~190KB/tick vs real streaming
    /// ~52KB/tick; drop the whole tick between them). mac: streaming is itself burst-per-tick
    /// (225KB~1.5MB is normal signal), so u64::MAX disables it — a 100KB threshold would drop all signal
    pub burst_tick_bytes: f64,
    /// Rough upper bound of idle heartbeat noise (B/s). mac idle measures strictly 0 bytes; no static floor needed
    pub base_noise_bps: f64,
    /// Per-tick cap on the per-process adaptive heartbeat floor (bytes/tick). Same on both platforms: the cap only
    /// prevents quantile poisoning; when mac idle is constantly 0 the adaptive floor decays to 0 on its own
    pub floor_cap_bytes: f64,
    /// Lower/upper bounds of the plausible B/token range for calibration samples. mac measures ≈3900, so the upper bound is relaxed for headroom
    pub cal_min: f64,
    pub cal_max: f64,
    /// Minimum call size for calibration samples (platform-independent)
    pub cal_min_tokens: u64,
    /// Initial bytes/token coefficient prior. Windows has long used 600; mac ground-truth reconciliation (2026-09-17,
    /// 6 cal events) measured accepted samples at 614/724, so take 700 — the old value 2000 came from the 120s probe's
    /// ≈3900 misread and underestimated cold-start readings 3x
    pub default_bpt: f64,
    /// Short detection window for the magnitude anchor (first-byte tick). Identical on both platforms in v1; retune mac only if states flap
    pub detect_ms: i64,
    /// Calibration delayed-flush grace (ms): after a call completes, pending waits this long before integration;
    /// the calibration integral and the raw stats window upper bound both extend to completed + grace. mac's
    /// ri_diskio_byteswritten is a page-cache async flush count, lagging write() by seconds~tens of seconds
    /// (a 117s long call measured 96% of bytes landing after completed, the user staring at 0.7 t/s for two minutes
    /// while ground truth was 65.3); Windows' WriteTransferCount is synchronous, so 0 = process same tick
    pub cal_grace_ms: i64,
    /// Calibration outlier rejection multiple: reject a sample whose B/token deviates from the current effective coefficient by more than this multiple
    /// (half-samples from delayed flush / mis-attributed samples stay out of the median). 0 = disabled (Windows)
    pub cal_outlier_ratio: f64,
}

impl CleanParams {
    /// Windows long-term measured values (original constants, unchanged; do not modify)
    #[allow(dead_code)] // referenced only by tests on mac builds; unused in bin builds
    pub fn windows() -> Self {
        Self {
            burst_tick_bytes: BURST_TICK_BYTES,
            base_noise_bps: BASE_NOISE_BPS,
            floor_cap_bytes: FLOOR_CAP_BYTES,
            cal_min: CAL_MIN,
            cal_max: CAL_MAX,
            cal_min_tokens: CAL_MIN_TOKENS,
            default_bpt: DEFAULT_BPT,
            detect_ms: DETECT_MS,
            // Delayed-flush grace and outlier rejection disabled on Windows: WriteTransferCount is synchronous,
            // processed same tick, no outlier filtering — behavior byte-for-byte identical to historical versions
            cal_grace_ms: 0,
            cal_outlier_ratio: 0.0,
        }
    }

    /// macOS measured values: burst is signal so rejection must be disabled, no static floor, coefficient prior set to 700 per
    /// ground-truth reconciliation, 15s delayed-flush grace (page-cache async flush lag), 3x sample outlier rejection
    #[cfg(target_os = "macos")]
    pub fn macos() -> Self {
        Self {
            burst_tick_bytes: u64::MAX as f64,
            base_noise_bps: 0.0,
            floor_cap_bytes: FLOOR_CAP_BYTES,
            cal_min: CAL_MIN,
            cal_max: 12_000.0,
            cal_min_tokens: CAL_MIN_TOKENS,
            default_bpt: 700.0,
            detect_ms: DETECT_MS,
            cal_grace_ms: 15_000,
            cal_outlier_ratio: 3.0,
        }
    }

    pub fn platform() -> Self {
        #[cfg(target_os = "macos")]
        {
            Self::macos()
        }
        #[cfg(not(target_os = "macos"))]
        {
            Self::windows()
        }
    }
}

#[derive(Default, Clone)]
pub struct LiveNow {
    /// Whether CLI processes were successfully discovered (when false the frontend falls back to window/estimate display)
    pub available: bool,
    pub streaming: bool,
    /// Streaming has started but the 30s sliding window is not yet full (reading comes from the active span; the frontend shows "measuring")
    pub ramping: bool,
    /// Call started but no first byte observed yet (TTFT, bounded by the hint window):
    /// the frontend shows a "measuring…" hint instead of an estimate
    pub awaiting: bool,
    pub tps: f64,
    /// Cleaned pipe byte rate (B/s), for debug logs/reconciliation
    pub pipe_bps: f64,
    /// Number of processes whose contribution in the current aggregation window reaches streaming magnitude (1 = single task; >1 = multi-task aggregation;
    /// idle processes' floor-noise leaks are not counted). Round drift detection only samples single-process rounds — the natural
    /// throughput difference from a changing task count is not coefficient drift
    pub n_pids: usize,
    /// Per-task breakdown: live speed of each process in the display set (sum of breakdown = aggregate reading)
    pub tasks: Vec<TaskLive>,
    /// Diagnostics: detection-window cleaned rate of each tracked process (KB/s). For multi-task troubleshooting reconciliation
    /// (during the 2026-09-18 investigation tick had only the npids field and could not answer "which process is writing")
    pub proc_bps: Vec<(u32, f64)>,
}

/// Per-task live breakdown (`LiveNow::tasks` element): one CLI process = one task row.
/// Multiple subagents running in parallel within one process are inseparable at the byte layer, shown honestly as that process's total
#[derive(Clone, Debug, Default)]
pub struct TaskLive {
    pub pid: u32,
    /// The in-progress session attributed to this process (None for a streaming process with no attribution record yet)
    pub session: Option<String>,
    /// Number of in-progress sessions this process hosts (≥2 = multi-task on one process; speed is the combined total; label via n_sessions)
    pub n_sessions: usize,
    pub tps: f64,
    pub streaming: bool,
}

/// Calibration and reconciliation event emitted after a call completes (for debug logs)
#[derive(Clone, Debug, serde::Serialize)]
pub struct CalEvent {
    pub id: String,
    pub session: String,
    pub completed_ms: i64,
    /// Call ground truth (flushed output+reasoning ÷ generation duration)
    pub true_tps: f64,
    pub gen_ms: i64,
    pub eff: u64,
    /// Raw write bytes over the streaming span (uncleaned, summed across all processes; attribution basis and reconciliation baseline)
    pub raw_bytes: f64,
    /// Bytes integrated from the cleaned stream (same measurement methodology as display) over [first_token, completed] (this call's coefficient numerator)
    pub clean_bytes: f64,
    /// This call's cleaned-stream sample B/token (0 = not admitted to calibration)
    pub bpt_sample: f64,
    /// Coefficient in effect after the event
    pub bpt_now: f64,
    /// Not admitted to calibration (call too short / no valid bytes / outlier rejection)
    pub cal_skipped: bool,
    /// Process actually used for the clean integral (attributed-process integral branch; None in the all-process sum branch)
    pub attr_pid: Option<u32>,
    /// Process with the most raw bytes in raw_by_pid (attribution anomaly diagnosis: attr != top
    /// and clean far below raw means the wrong process was attributed)
    pub top_pid: Option<u32>,
    /// Raw bytes of other processes within the call window (cross-process guard diagnostics: the sample is rejected
    /// when this exceeds CROSS_PID_RATIO of the attributed process's bytes)
    pub others_bytes: f64,
}

// ============ Pure computation (cross-platform, unit-testable): tick cleaning / interval integration / median ============

/// Cleaned per-tick pipe bytes. bytes may be negative: when a disk flush is misaligned with the IO counters,
/// the interval integral converges by summation (Σ bytes = Σ raw deltas − Σ flushes − Σ noise floors); never clamp per tick to 0
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct TickRow {
    /// Tick duration
    pub dt_ms: i64,
    /// Cleaned bytes (raw writes − flushed file growth − noise floor, may be negative)
    pub bytes: f64,
    /// Tick end time (wall-clock ms)
    pub end_ms: i64,
}

/// Builds the cleaned tick sequence from a cumulative write-byte series (only per-process noise floors are deducted; tracked file
/// growth is deducted uniformly on the aggregated stream by [`merge_streams`] — deducting per process when
/// summing processes would subtract the global file growth N times). bytes may be negative (flush misalignment cancels via interval integration)
pub(crate) fn build_rows(samples: &[(i64, u64)], min_delta: f64, p: &CleanParams) -> Vec<TickRow> {
    let floor_static = min_delta.min(p.floor_cap_bytes);
    let mut rows = Vec::with_capacity(samples.len());
    for w in samples.windows(2) {
        let (t0, w0) = w[0];
        let (t1, w1) = w[1];
        let dt_ms = t1 - t0;
        if dt_ms <= 0 {
            continue;
        }
        let raw = w1.saturating_sub(w0) as f64;
        // Single-tick bursts such as request-body upload: drop the whole tick (integrate neither bytes nor duration);
        // on mac burst is signal, so the threshold is disabled in CleanParams
        if raw > p.burst_tick_bytes {
            continue;
        }
        let dt_s = dt_ms as f64 / 1000.0;
        rows.push(TickRow {
            dt_ms,
            bytes: raw - floor_static - p.base_noise_bps * dt_s,
            end_ms: t1,
        });
    }
    rows
}

/// Merges multiple per-process cleaned streams into one aggregated stream: same-tick bytes are summed, wall-clock duration counted once
/// (integrating per process would also sum durations, diluting the rate into a cross-process mean), and tracked file
/// growth deducted once overall. Rows align by end_ms (paired sampling in the same polling loop, identical timestamps;
/// a process missing a tick contributes 0 bytes there). For a single input stream this is arithmetically identical to the
/// historical per-row file deduction
pub(crate) fn merge_streams(streams: &[&[TickRow]], files: &[(i64, u64)]) -> Vec<TickRow> {
    use std::collections::BTreeMap;
    let mut merged: BTreeMap<i64, (i64, f64)> = BTreeMap::new();
    for rows in streams {
        for r in rows.iter() {
            let e = merged.entry(r.end_ms).or_insert((r.dt_ms, 0.0));
            // Same-tick dt must match (paired sampling in one polling loop) — if sampling times ever diverge,
            // merging by end_ms would silently split into two rows and double-count duration; fail loudly here instead
            debug_assert_eq!(e.0, r.dt_ms);
            e.1 += r.bytes;
        }
    }
    let mut out = Vec::with_capacity(merged.len());
    for (end_ms, (dt_ms, bytes)) in merged {
        // Tracked file flush growth within [t0, end) (half-open bounds so adjacent intervals don't double count)
        let t0 = end_ms - dt_ms;
        let mut fg = 0f64;
        for f in files.windows(2) {
            let (ft0, fv0) = f[0];
            let (ft1, fv1) = f[1];
            if ft1 > t0 && ft0 < end_ms {
                fg += fv1.saturating_sub(fv0) as f64;
            }
        }
        out.push(TickRow {
            dt_ms,
            bytes: bytes - fg,
            end_ms,
        });
    }
    out
}

/// tracked file total series → growth tick series (for the per-task breakdown to apportion deductions over the window)
pub(crate) fn file_growth_rows(files: &[(i64, u64)]) -> Vec<TickRow> {
    files
        .windows(2)
        .filter_map(|w| {
            let (t0, v0) = w[0];
            let (t1, v1) = w[1];
            let dt_ms = t1 - t0;
            if dt_ms <= 0 {
                return None;
            }
            Some(TickRow {
                dt_ms,
                bytes: v1.saturating_sub(v0) as f64,
                end_ms: t1,
            })
        })
        .collect()
}

/// Integrates [from_ms, to_ms] proportionally by time: boundary-crossing ticks are prorated by overlap duration.
/// Returns (bytes, seconds). Rejected burst ticks contribute no duration and do not dilute the rate
pub(crate) fn integrate(rows: &[TickRow], from_ms: i64, to_ms: i64) -> (f64, f64) {
    let (mut bytes, mut secs) = (0f64, 0f64);
    for r in rows {
        let lo = (r.end_ms - r.dt_ms).max(from_ms);
        let hi = r.end_ms.min(to_ms);
        if hi <= lo {
            continue;
        }
        let ov = (hi - lo) as f64;
        let dt = r.dt_ms as f64;
        bytes += r.bytes * (ov / dt);
        secs += ov / 1000.0;
    }
    (bytes, secs)
}

/// Calibration coefficient estimator (pure function for easy testing/benchmark replay): **recency-weighted median over a sliding window,
/// then one step of logarithmic shrinkage toward the newest sample (AR1)**.
///
/// Background (measured conclusions from 262 real calibration samples on this machine, see scripts/cal_bench.py): B/token
/// samples fluctuate substantially per call (p25~p75 = 466~740), but the fluctuation is not white noise — log-space
/// lag-1 autocorrelation ≈0.50, lag-2 ≈ 0.27 ≈ ρ² (pure AR(1): consecutive calls share content style).
/// The old "plain median of 5 samples" had a median error of 24.3% predicting the next call, nearly identical to the
/// "always use prior 600" baseline (25.9%) — the window was too small to suppress noise. Four improvements in this estimator:
/// 1. Window 5 → 16 ([`CAL_QUEUE_CAP`]): a larger median window, lower variance;
/// 2. Recency weighting: weights decay exponentially with half-life [`CAL_HALF_LIFE`] samples — after a magnitude shift
///    2~3 new samples suffice to catch up (no worse than the old 5-window adaptivity), while steady state keeps 16-sample noise immunity;
/// 3. Mild trimming: centered on the **untrimmed weighted median** (two-pass — if centered on the window's plain median,
///    a magnitude shift would cause new samples to be mistakenly trimmed by the old-magnitude median, locking the coefficient into the old regime),
///    dropping outlier samples outside [`CAL_TRIM_LO`]~[`CAL_TRIM_HI`] multiples and re-taking the weighted median,
///    so a single polluted sample can no longer move the coefficient much;
/// 4. AR1 recency shrinkage ([`CAL_AR1_RHO`]): after trimming, the estimate shrinks logarithmically toward the newest sample with the ratio raised to ρ —
///    the theoretically optimal one-step prediction under AR(1) structure (variance factor √(1-ρ²)≈0.87),
///    giving a head start on the first post-shift sample and narrower steady-state tails. The ratio is clamped to
///    [`CAL_AR1_CLAMP`] multiples: a single sample's influence on the coefficient is bounded (≤ CLAMP^ρ ≈ ×1.23),
///    an outlier's one-tick pull is acceptable and cannot accumulate across ticks (the median anchor does not move).
///
/// `prior` is the platform prior (Windows 600 / mac 700). When the window has fewer than [`CAL_SHRINK_N`]
/// samples, shrink proportionally toward the prior (a single anomalous sample cannot own the coefficient at cold start — preserving the
/// historical property; fully data-driven once 3 samples are in). Real replay (scripts/cal_bench.py, 262
/// local samples, chunked validation confirmed ρ extrapolates stably): median error 24.3% (old 5-window) → 21.3%
/// (10-window without shrinkage) → 20.7%, p75 47.8%→41.5%, mean 37.1%→34.1%;
/// synthetic benchmarks in tests `benchmark_*`. Rejected by replay: per-model queues (GLM-5.3 and
/// Flash distributions match; splitting the window only loses samples), eff/duration weighting (ineffective), bytes
/// ≈ a×token + c×duration two-parameter model (per-call error 48%+), skipping shrinkage for out-of-bounds samples
/// (the clamped pull is better across the board). Samples must be stored ascending ( oldest→newest ), same order as the `cal`
/// queue and the persisted file
pub(crate) fn cal_estimate(samples: &[f64], prior: f64) -> f64 {
    let start = samples.len().saturating_sub(CAL_QUEUE_CAP);
    let win = &samples[start..];
    if win.is_empty() {
        return prior;
    }
    // First pass: untrimmed recency-weighted median as the trimming center (on a magnitude shift it catches up first,
    // so trimming won't mistake new-magnitude samples for outliers)
    let center = weighted_median(win, |_| true);
    if center <= 0.0 {
        return prior;
    }
    // Second pass: retake the weighted median after trimming outliers
    let mut est = weighted_median(win, |v| *v >= center * CAL_TRIM_LO && *v <= center * CAL_TRIM_HI);
    // AR1 recency shrinkage: logarithmic shrinkage toward the newest sample, bounded ratio clamp (see point 4 of the function doc)
    if let Some(last) = win.last() {
        let ratio = (last / est).clamp(1.0 / CAL_AR1_CLAMP, CAL_AR1_CLAMP);
        est *= ratio.powf(CAL_AR1_RHO);
    }
    // Cold-start shrinkage toward the prior: linear transition while window samples < CAL_SHRINK_N, purely data-driven once full
    let shrink = (win.len() as f64 / CAL_SHRINK_N as f64).min(1.0);
    est * shrink + prior * (1.0 - shrink)
}

/// Recency-weighted median within the window: `keep` is a sample filter (for trimming); the newest sample has weight 1,
/// halving every [`CAL_HALF_LIFE`] samples going back; the value is taken where ascending cumulative weight passes half.
/// Window/retained samples are always positive (guaranteed by `cal_sample` admission and the prior); if the filter
/// is empty or all-negative, fall back to the window's plain median
fn weighted_median(win: &[f64], keep: impl Fn(&f64) -> bool) -> f64 {
    let mut pairs: Vec<(f64, f64)> = Vec::with_capacity(win.len());
    for (i, v) in win.iter().enumerate() {
        if keep(v) {
            pairs.push((*v, 0.5f64.powf((win.len() - 1 - i) as f64 / CAL_HALF_LIFE)));
        }
    }
    if pairs.is_empty() {
        let mut sorted: Vec<f64> = win.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        return sorted[sorted.len() / 2];
    }
    pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let total: f64 = pairs.iter().map(|p| p.1).sum();
    let mut acc = 0.0;
    for (v, w) in &pairs {
        acc += w;
        if acc >= total / 2.0 {
            return *v;
        }
    }
    pairs.last().map(|p| p.0).unwrap_or_else(|| pairs[0].0)
}

/// Picks the process set for live display (pure function for easy testing):
/// - All in-progress sessions have live attribution → union of attributed pids: aggregates into true total throughput under
///   concurrent multi-tasking, and also yields the per-task breakdown;
/// - Any in-progress session has no attribution (new session/subagent whose first call has not completed) → None =
///   all-process sum fallback: that session's own process has no attribution record yet, so a union alone would miss it and
///   display another window's speed; idle processes contribute ≈ 0 after floor-noise cleaning, so the cost is negligible;
/// - No in-progress sessions → None (gate off, reading zeroes; the branch taken does not matter)
pub(crate) fn pick_pid_set(
    inflight: &[(String, i64)],
    session_pid: &HashMap<String, u32>,
    active_pids: &HashSet<u32>,
) -> Option<Vec<u32>> {
    if inflight.is_empty() {
        return None;
    }
    let mut set = Vec::with_capacity(inflight.len());
    for (s, _) in inflight {
        match session_pid.get(s) {
            Some(pid) if active_pids.contains(pid) => set.push(*pid),
            // Unattributed or attributed process dead → sum fallback (session_pid is pruned by live pids every tick,
            // so a "dead" hit here can only be a within-tick race; the fallback is equally safe)
            _ => return None,
        }
    }
    set.sort_unstable();
    set.dedup();
    Some(set)
}

/// Session→process attribution switch decision (pure function for easy testing): returns the attributed pid to write this time.
/// With existing attribution, hysteresis applies — switch only if the candidate top process's window raw bytes are ≥
/// ATTR_SWITCH_RATIO times the current attribution's, preventing per-call flips between concurrent windows; with no current attribution or
/// zero window bytes for the current attribution (self-healing path for stale attribution), trust top directly
pub(crate) fn should_reattribute(
    cur: Option<u32>,
    top: Option<u32>,
    raw_by_pid: &HashMap<u32, u64>,
) -> Option<u32> {
    let top = top?;
    match cur {
        Some(c) if c != top => {
            let cur_raw = *raw_by_pid.get(&c).unwrap_or(&0) as f64;
            let top_raw = *raw_by_pid.get(&top).unwrap_or(&0) as f64;
            if top_raw >= cur_raw * ATTR_SWITCH_RATIO && top_raw > cur_raw {
                Some(top)
            } else {
                Some(c)
            }
        }
        _ => Some(top),
    }
}

/// Session→process attribution decision (pure function for easy testing): returns the attributed pid to write this time.
/// `owned` = set of pids already attributed to other **in-progress** sessions. Three branches:
/// - First attribution: if top is already owned and the runner-up's bytes reach ATTR_DEDUP_RATIO of top → attribute to the runner-up
///   (under a concurrent tie, max_by picking top is random; both sessions would squeeze onto the same process and
///   the display set would collapse to a single process); if the runner-up has only noise ratio, keep top (genuine sharing)
/// - Current attribution is owned by another in-progress session: switch if top is unowned and its bytes reach half the current's —
///   in the owned scenario the 2x hysteresis would lock in a historical mis-attribution
/// - Current attribution unowned: keep the original 2x hysteresis semantics (should_reattribute)
pub(crate) fn pick_attribution(
    cur: Option<u32>,
    raw_by_pid: &HashMap<u32, u64>,
    owned: &HashSet<u32>,
) -> Option<u32> {
    // Candidates sorted by window bytes descending, ties by pid ascending — deterministic traversal order, no longer dependent on HashMap order
    let mut cands: Vec<(u32, u64)> = raw_by_pid.iter().map(|(p, b)| (*p, *b)).collect();
    cands.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let top = cands.first().copied()?;
    match cur {
        None => {
            if owned.contains(&top.0) {
                if let Some((pid, bytes)) = cands.get(1) {
                    if !owned.contains(pid)
                        && *bytes as f64 >= (top.1 as f64) * ATTR_DEDUP_RATIO
                        && *bytes >= 20_000
                    {
                        return Some(*pid);
                    }
                }
            }
            Some(top.0)
        }
        Some(c) if c != top.0 && owned.contains(&c) => {
            // Current attribution owned: switch if top is unowned and its bytes reach half the current's
            // (the owned scenario's 2x hysteresis would lock in historical mis-attribution; if top is also owned there is nowhere to go)
            if !owned.contains(&top.0)
                && top.1 as f64 >= (*raw_by_pid.get(&c).unwrap_or(&0) as f64) * ATTR_DEDUP_RATIO
            {
                Some(top.0)
            } else {
                Some(c)
            }
        }
        Some(c) => should_reattribute(Some(c), Some(top.0), raw_by_pid),
    }
}

/// Cross-process guard for calibration samples (pure function for easy testing): a sample is admitted only if other processes' raw bytes
/// within the call window stay within CROSS_PID_RATIO of the attributed process's. While the other window streams concurrently,
/// the attributed process's cleaned integral inevitably mixes in foreign bytes, distorting the B/token sample
pub(crate) fn cross_pid_ok(others_raw: f64, attr_raw: f64) -> bool {
    attr_raw > 0.0 && others_raw <= attr_raw * CROSS_PID_RATIO
}

/// Calibration sample admission: the pipe integral must contribute substantively (≥20% of raw bytes) and the B/token must land in the
/// plausible range without clamping. Silent-pipe calls (all bytes flush at the completion instant; measured samples as low as
/// ~7 B/token) and anomalous-ratio samples are rejected whole, protecting the median coefficient from pollution.
/// On top of that, a sample deviating from the current effective coefficient bpt_now by more than cal_outlier_ratio is
/// rejected (mac: half-samples that integrate only half the bytes due to delayed flush / mis-attributed samples stay out of the median;
/// Windows ratio=0 disables this explicitly, matching historical behavior).
/// Returns (admitted, sample value)
pub(crate) fn cal_sample(
    eff: u64,
    clean_bytes: f64,
    raw_bytes: f64,
    bpt_now: f64,
    p: &CleanParams,
) -> (bool, f64) {
    if eff < p.cal_min_tokens || clean_bytes <= 0.0 || raw_bytes <= 0.0 {
        return (false, 0.0);
    }
    let ratio = clean_bytes / eff as f64;
    let usable = clean_bytes / raw_bytes >= 0.2;
    let in_range = usable && ratio >= p.cal_min && ratio <= p.cal_max;
    // Outlier rejection: outlier_ratio=0 (Windows) disables it explicitly, avoiding 0-division/0-multiply misjudgment;
    // a rejected sample records 0 (consistent with the bpt_sample=0 methodology on the caller side when cal_skipped)
    if in_range
        && p.cal_outlier_ratio > 0.0
        && bpt_now > 0.0
        && (ratio < bpt_now / p.cal_outlier_ratio || ratio > bpt_now * p.cal_outlier_ratio)
    {
        return (false, 0.0);
    }
    (in_range, ratio)
}

/// Calibration sample queue capacity (sliding window, includes the preseeded prior placeholder). 16 = the estimator's steady-state window:
/// a more stable median (real replay: 10→16 lowers the median error another ~0.6pp); adaptivity is delegated to recency weighting
/// and AR1 shrinkage (catches up in 2~3 samples after a shift); rationale in the [`cal_estimate`] docs
const CAL_QUEUE_CAP: usize = 16;
/// Recency weighting half-life (in samples): newest weight 1, halving every 3 samples back
const CAL_HALF_LIFE: f64 = 3.0;
/// Mild trim bounds (multiples of the trim center; center = untrimmed weighted median, see the `cal_estimate`
/// two-pass method): outlier samples are excluded from the weighted median. Bounds are wide, [0.4, 2.5] — true-value samples
/// fluctuate nearly 2x on their own; trimming harder would discard genuine magnitude shifts as outliers (adaptivity is handled by recency weights instead)
const CAL_TRIM_LO: f64 = 0.4;
const CAL_TRIM_HI: f64 = 2.5;
/// AR1 recency shrinkage strength (see `cal_estimate` point 4): logarithmic shrinkage ratio toward the newest sample.
/// The measured lag-1 autocorrelation ≈0.50 gives a theoretical optimum of ρ≈0.5; 0.3 is the robust value under chunked validation
/// — a larger ρ overfits the training segment while the test-segment median regresses (chasing single-sample noise); 0.3 keeps
/// all the tail gains (p75 -7pp) without degrading the median
const CAL_AR1_RHO: f64 = 0.3;
/// Ratio clamp for AR1 shrinkage (bound on last sample/estimate): a single sample's influence on the coefficient is bounded
/// (≤ CLAMP^ρ ≈ ×1.23); an outlier pulls one tick and cannot accumulate across ticks; real replay favors this
/// over "skip shrinkage when out of bounds" across the board (the clamped pull serves both shift head starts and steady-state tails)
const CAL_AR1_CLAMP: f64 = 2.0;
/// Sample count threshold for cold-start prior shrinkage: below this the estimate transitions proportionally toward the prior,
/// fully data-driven at 3 samples (preserving the historical property that a single anomalous sample cannot own the coefficient)
const CAL_SHRINK_N: usize = 3;

/// Startup hint decision (pure function): gate on, no streaming anchor yet (first byte not arrived), and still within the hint window
/// since the call started. No anchor past the window = silent-pipe call; the upper layer falls back to estimate display
pub(crate) fn awaiting_hint(
    inflight_started: Option<i64>,
    anchor: Option<i64>,
    now_ms: i64,
) -> bool {
    match (inflight_started, anchor) {
        (Some(started), None) => now_ms - started <= TTFT_HINT_MS,
        _ => false,
    }
}

/// Stop-detection fallback (pure function): gate on and a streaming anchor exists, but the last time the streaming threshold was reached
/// is older than the grace period → generation has actually stopped (no more lingering "generating" while waiting for completed to flush)
pub(crate) fn stale_stop(anchor: Option<i64>, last_stream_ms: Option<i64>, now_ms: i64) -> bool {
    anchor.is_some()
        && last_stream_ms.map_or(false, |t| now_ms - t > SILENT_STOP_MS)
}

/// Round average-speed drift detection (pure function for easy testing): feed in each round's mean displayed speed and compare against the mean of the
/// previous DRIFT_ROUNDS consecutive rounds; a difference ≥ DRIFT_RATIO in either direction declares a speed magnitude shift and
/// should trigger recalibration (`LiveIo::reset_calibration`). On trigger the history is cleared,
/// and the new magnitude rebuilds a baseline, preventing repeated triggers from the same shift
#[derive(Default)]
pub struct RoundDrift {
    history: VecDeque<f64>,
}

impl RoundDrift {
    pub fn new() -> Self {
        Self {
            history: VecDeque::with_capacity(DRIFT_ROUNDS),
        }
    }

    /// Observes one round's displayed mean. Returns Some(baseline mean) = recalibration triggered (baseline for logs);
    /// rounds with mean ≤0 (silent pipe, no measured ticks) are skipped and not stored in history
    pub fn observe(&mut self, round_avg: f64) -> Option<f64> {
        if round_avg <= 0.0 {
            return None;
        }
        let base = (self.history.len() == DRIFT_ROUNDS)
            .then(|| self.history.iter().sum::<f64>() / DRIFT_ROUNDS as f64);
        if let Some(b) = base {
            if round_avg / b >= DRIFT_RATIO || b / round_avg >= DRIFT_RATIO {
                self.history.clear();
                return Some(b);
            }
        }
        self.history.push_back(round_avg);
        while self.history.len() > DRIFT_ROUNDS {
            self.history.pop_front();
        }
        None
    }

    /// Clears history (reset in sync with manual recalibration; new baseline accumulates from zero)
    pub fn reset(&mut self) {
        self.history.clear();
    }
}

struct ProcRing {
    handle: platform::ProcHandle,
    samples: VecDeque<(i64, u64)>,
    /// This process's minimum per-tick delta (adaptive heartbeat noise floor, capped when used)
    min_delta: f64,
}

/// Platform process primitives: process discovery / opening handles / reading cumulative write bytes / tracked file totals.
/// Three implementations selected by cfg, exposed uniformly as `liveio::platform::*` (reused by examples).
/// All FFI failure paths return None/empty Vec; panics are forbidden
pub mod platform {
    /// Windows: Toolhelp enumeration + command-line read to filter CLI subprocesses; GetProcessIoCounters
    /// reads cumulative write bytes since process start (WriteTransferCount, kernel-maintained, authoritative)
    #[cfg(windows)]
    mod win {
        use std::ffi::c_void;

        #[link(name = "kernel32")]
        extern "system" {
            fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut c_void;
            fn CloseHandle(h: *mut c_void) -> i32;
            fn GetProcessIoCounters(h: *mut c_void, counters: *mut IoCounters) -> i32;
            fn ReadProcessMemory(
                h: *mut c_void,
                addr: *const c_void,
                buf: *mut c_void,
                size: usize,
                read: *mut usize,
            ) -> i32;
            fn CreateToolhelp32Snapshot(flags: u32, pid: u32) -> isize;
            fn Process32FirstW(snap: isize, entry: *mut ProcessEntry32W) -> i32;
            fn Process32NextW(snap: isize, entry: *mut ProcessEntry32W) -> i32;
        }
        #[repr(C)]
        struct IoCounters {
            read_ops: u64,
            write_ops: u64,
            other_ops: u64,
            read_bytes: u64,
            write_bytes: u64,
            other_bytes: u64,
        }

        #[repr(C)]
        struct ProcessEntry32W {
            size: u32,
            usage: u32,
            process_id: u32,
            default_heap_id: usize,
            module_id: u32,
            threads: u32,
            parent_process_id: u32,
            pri_class_base: i32,
            flags: u32,
            exe_file: [u16; 260],
        }

        const PROCESS_QUERY_LIMITED: u32 = 0x1410; // QUERY_INFORMATION | QUERY_LIMITED | VM_READ
        const TH32CS_SNAPPROCESS: u32 = 2;

        fn process_command_line(pid: u32) -> Option<String> {
            unsafe {
                let h = OpenProcess(PROCESS_QUERY_LIMITED, 0, pid);
                if h.is_null() {
                    return None;
                }
                // Classic approach: ProcessBasicInformation → PEB → ProcessParameters → CommandLine
                let rd = |addr: usize, buf: &mut [u8]| -> bool {
                    let mut n = 0usize;
                    ReadProcessMemory(h, addr as *const c_void, buf.as_mut_ptr().cast(), buf.len(), &mut n)
                        != 0
                };
                let mut pbi = [0u8; 48];
                let mut ret: u32 = 0;
                if NtQueryInformationProcess(h, 0, pbi.as_mut_ptr().cast(), 48, &mut ret) != 0 {
                    CloseHandle(h);
                    return None;
                }
                #[cfg(target_pointer_width = "64")]
                {
                    let peb = usize::from_ne_bytes(pbi[8..16].try_into().ok()?);
                    if peb == 0 {
                        CloseHandle(h);
                        return None;
                    }
                    let mut pp_ptr = [0u8; 8];
                    if !rd(peb + 0x20, &mut pp_ptr) {
                        CloseHandle(h);
                        return None;
                    }
                    let pp = usize::from_ne_bytes(pp_ptr.try_into().ok()?);
                    if pp == 0 {
                        CloseHandle(h);
                        return None;
                    }
                    // RTL_USER_PROCESS_PARAMETERS.CommandLine (UNICODE_STRING) @ 0x70
                    let mut us = [0u8; 16];
                    if !rd(pp + 0x70, &mut us) {
                        CloseHandle(h);
                        return None;
                    }
                    let len = u16::from_ne_bytes([us[0], us[1]]) as usize;
                    let buf_ptr = usize::from_ne_bytes(us[8..16].try_into().ok()?);
                    if len == 0 || buf_ptr == 0 {
                        CloseHandle(h);
                        return None;
                    }
                    let mut wbuf = vec![0u8; len];
                    if !rd(buf_ptr, &mut wbuf) {
                        CloseHandle(h);
                        return None;
                    }
                    let u16s: Vec<u16> = wbuf
                        .chunks_exact(2)
                        .map(|c| u16::from_ne_bytes([c[0], c[1]]))
                        .collect();
                    CloseHandle(h);
                    return Some(String::from_utf16_lossy(&u16s));
                }
                #[cfg(not(target_pointer_width = "64"))]
                {
                    CloseHandle(h);
                    None
                }
            }
        }

        pub fn discover_cli_pids() -> Vec<u32> {
            let mut pids = Vec::new();
            unsafe {
                let snap = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
                if snap == -1 {
                    return pids;
                }
                let mut entry = ProcessEntry32W {
                    size: std::mem::size_of::<ProcessEntry32W>() as u32,
                    usage: 0,
                    process_id: 0,
                    default_heap_id: 0,
                    module_id: 0,
                    threads: 0,
                    parent_process_id: 0,
                    pri_class_base: 0,
                    flags: 0,
                    exe_file: [0; 260],
                };
                let ok = Process32FirstW(snap, &mut entry);
                if ok != 0 {
                    loop {
                        let exe = String::from_utf16_lossy(
                            &entry.exe_file[..entry.exe_file.iter().position(|c| *c == 0).unwrap_or(260)],
                        );
                        if exe.eq_ignore_ascii_case("zcode.exe") {
                            if let Some(cmd) = process_command_line(entry.process_id) {
                                if cmd.contains("zcode.cjs") {
                                    pids.push(entry.process_id);
                                }
                            }
                        }
                        if Process32NextW(snap, &mut entry) == 0 {
                            break;
                        }
                    }
                }
                CloseHandle(snap as *mut c_void);
            }
            pids
        }

        /// Whether any ZCode process exists on the system (process name zcode.exe; the desktop shell and the CLI
        /// subprocess share the name — no command-line read, a name match counts). Used for the standby detection
        /// of the autostart follow mode (autostart.rs): either the desktop app or the CLI running counts as "ZCode
        /// is running". Difference from discover_cli_pids: that one reads command lines to precisely filter
        /// CLI subprocesses, while this only matches names — faster and broader
        pub fn any_zcode_process() -> bool {
            unsafe {
                let snap = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
                if snap == -1 {
                    return false;
                }
                let mut entry = ProcessEntry32W {
                    size: std::mem::size_of::<ProcessEntry32W>() as u32,
                    usage: 0,
                    process_id: 0,
                    default_heap_id: 0,
                    module_id: 0,
                    threads: 0,
                    parent_process_id: 0,
                    pri_class_base: 0,
                    flags: 0,
                    exe_file: [0; 260],
                };
                let mut found = false;
                let ok = Process32FirstW(snap, &mut entry);
                if ok != 0 {
                    loop {
                        let end = entry.exe_file.iter().position(|c| *c == 0).unwrap_or(260);
                        if String::from_utf16_lossy(&entry.exe_file[..end])
                            .eq_ignore_ascii_case("zcode.exe")
                        {
                            found = true;
                            break;
                        }
                        if Process32NextW(snap, &mut entry) == 0 {
                            break;
                        }
                    }
                }
                CloseHandle(snap as *mut c_void);
                found
            }
        }

        pub fn io_write_bytes(h: &ProcHandle) -> Option<u64> {
            let mut io = IoCounters {
                read_ops: 0,
                write_ops: 0,
                other_ops: 0,
                read_bytes: 0,
                write_bytes: 0,
                other_bytes: 0,
            };
            unsafe {
                if GetProcessIoCounters(h.handle as *mut c_void, &mut io) != 0 {
                    Some(io.write_bytes)
                } else {
                    None
                }
            }
        }
        #[link(name = "ntdll")]
        extern "system" {
            fn NtQueryInformationProcess(
                h: *mut c_void,
                class: u32,
                info: *mut c_void,
                len: u32,
                ret_len: *mut u32,
            ) -> i32;
        }

        /// Process handle: kernel handle opened via OpenProcess (kept resident and reused, not opened/closed per tick)
        pub struct ProcHandle {
            handle: isize,
        }

        pub fn open_proc(pid: u32) -> Option<ProcHandle> {
            let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED, 0, pid) } as isize;
            if handle == 0 {
                None
            } else {
                Some(ProcHandle { handle })
            }
        }

        /// Tracked file totals (all jsonl in the rollout dir + CLI log dir + db WAL,
        /// the source of flush-write spikes), used by the cleaned stream for flush deduction
        pub fn tracked_files_total() -> u64 {
            let mut total = 0u64;
            if let Some(home) = crate::metrics::home_dir() {
                for dir in [
                    home.join(".zcode/cli/rollout"),
                    home.join(".zcode/cli/log"),
                ] {
                    if let Ok(rd) = std::fs::read_dir(&dir) {
                        for e in rd.flatten() {
                            if let Ok(m) = e.metadata() {
                                total += m.len();
                            }
                        }
                    }
                }
                if let Ok(m) = std::fs::metadata(home.join(".zcode/cli/db/db.sqlite-wal")) {
                    total += m.len();
                }
            }
            total
        }
    }

    /// macOS: libproc process enumeration (KERN_PROCARGS2 argv contains the exact argument
    /// `zcode-cli`) + proc_pid_rusage's ri_diskio_byteswritten (kernel-maintained
    /// cumulative process disk write bytes, authoritative and zero-cost to read)
    #[cfg(target_os = "macos")]
    mod mac {
        use std::ffi::{c_int, c_void};

        // Link name "proc" (library file is /usr/lib/libproc.dylib; the link name drops the lib prefix)
        #[link(name = "proc")]
        extern "C" {
            /// Note buffersize is in **bytes** (not pid count); pass pid capacity × 4
            fn proc_listallpids(buffer: *mut c_void, buffersize: c_int) -> c_int;
            fn proc_pid_rusage(pid: c_int, flavor: c_int, buffer: *mut c_void) -> c_int;
        }
        #[link(name = "System")]
        extern "C" {
            fn sysctl(
                name: *const c_int,
                namelen: u32,
                oldp: *mut c_void,
                oldlenp: *mut usize,
                newp: *mut c_void,
                newlen: usize,
            ) -> c_int;
        }

        const CTL_KERN: c_int = 1;
        const KERN_PROCARGS2: c_int = 49;

        /// Field-by-field mirror of rusage_info_v4 (per the macOS SDK sys/resource.h).
        /// Note the newer kernel layout has ri_proc_exit_abstime after ri_proc_start_abstime,
        /// which determines the offset of ri_diskio_byteswritten — field offsets are pinned at compile time by the
        /// const assertions below; an SDK layout change fails compilation outright, do not remove the assertions
        #[repr(C)]
        struct RusageInfoV4 {
            ri_uuid: [u8; 16],
            ri_user_time: u64,
            ri_system_time: u64,
            ri_pkg_idle_wkups: u64,
            ri_interrupt_wkups: u64,
            ri_pageins: u64,
            ri_wired_size: u64,
            ri_resident_size: u64,
            ri_phys_footprint: u64,
            ri_proc_start_abstime: u64,
            ri_proc_exit_abstime: u64,
            ri_child_user_time: u64,
            ri_child_system_time: u64,
            ri_child_pkg_idle_wkups: u64,
            ri_child_interrupt_wkups: u64,
            ri_child_pageins: u64,
            ri_child_elapsed_abstime: u64,
            ri_diskio_bytesread: u64,
            ri_diskio_byteswritten: u64,
            ri_cpu_time_qos_default: u64,
            ri_cpu_time_qos_maintenance: u64,
            ri_cpu_time_qos_background: u64,
            ri_cpu_time_qos_utility: u64,
            ri_cpu_time_qos_legacy: u64,
            ri_cpu_time_qos_user_initiated: u64,
            ri_cpu_time_qos_user_interactive: u64,
            ri_billed_system_time: u64,
            ri_serviced_system_time: u64,
            ri_logical_writes: u64,
            ri_lifetime_max_phys_footprint: u64,
            ri_instructions: u64,
            ri_cycles: u64,
            ri_billed_energy: u64,
            ri_serviced_energy: u64,
            ri_interval_max_phys_footprint: u64,
            ri_runnable_time: u64,
        }

        /// Compile-time assertions that key field offsets match the local SDK header (offsetof measured with a C program:
        /// ri_proc_start_abstime=80, ri_diskio_byteswritten=152, sizeof=296);
        /// if an assertion fails, fix the struct layout — do not remove the assertions
        const _: () = {
            assert!(std::mem::offset_of!(RusageInfoV4, ri_proc_start_abstime) == 80);
            assert!(std::mem::offset_of!(RusageInfoV4, ri_diskio_byteswritten) == 152);
            assert!(std::mem::size_of::<RusageInfoV4>() == 296);
        };

        const RUSAGE_INFO_V4: c_int = 4;

        /// Process handle: pid + the process start time (absolute) captured at open.
        /// A mismatched start time at sampling = pid was reused; treat as exited and drop
        pub struct ProcHandle {
            pid: u32,
            start_abstime: u64,
        }

        fn read_rusage(pid: u32) -> Option<RusageInfoV4> {
            // 512-byte buffer ≥ sizeof(RusageInfoV4)=296, leaving room for future field growth
            let mut buf = [0u8; 512];
            let ok = unsafe {
                proc_pid_rusage(pid as c_int, RUSAGE_INFO_V4, buf.as_mut_ptr().cast::<c_void>())
            };
            if ok != 0 {
                return None;
            }
            Some(unsafe { buf.as_ptr().cast::<RusageInfoV4>().read_unaligned() })
        }

        /// Enumerates ZCode CLI processes (multiple may coexist). Identification methodology: an exact argument "zcode-cli" present in the
        /// KERN_PROCARGS2 argv (the CLI is forked from an Electron Helper;
        /// proc_pidpath only yields the "ZCode Helper" executable path and cannot distinguish it from other Helper
        /// processes; measured CLI processes have argv[1] == "zcode-cli").
        /// proc_listallpids is two-phase: first pass a null buffer to get the pid count, then fetch the list
        /// (buffersize is in bytes); m <= 0 is treated as failure and returns empty
        pub fn discover_cli_pids() -> Vec<u32> {
            let mut pids = Vec::new();
            unsafe {
                let n = proc_listallpids(std::ptr::null_mut(), 0);
                if n <= 0 {
                    return pids;
                }
                let cap = (n + 16) as usize;
                let mut buf = vec![0i32; cap];
                let m = proc_listallpids(buf.as_mut_ptr().cast::<c_void>(), (cap * 4) as c_int);
                if m <= 0 {
                    return pids;
                }
                let mut scratch = vec![0u8; 64 * 1024];
                for pid in &buf[..m as usize] {
                    if *pid > 0 && argv_has_cli_marker(*pid as u32, &mut scratch) {
                        pids.push(*pid as u32);
                    }
                }
            }
            pids
        }

        /// Whether the exact string "zcode-cli" exists in the KERN_PROCARGS2 packed area.
        /// The layout is [nargs: i32][argv0 … (alignment NUL padding follows argv0) argv1..][envp…];
        /// alignment padding is hard to distinguish from empty arguments, so argv boundaries are not reconstructed exactly — just scan all
        /// NUL-terminated strings for an exact match (measured: the CLI process's argument area contains a standalone
        /// "zcode-cli" string; envp strings are all KEY=VALUE and cannot collide; same broad methodology as the
        /// Windows "command line contains zcode.cjs"). 64KB covers typical processes;
        /// parse failures/permission errors are always treated as no match (no panic)
        fn argv_has_cli_marker(pid: u32, buf: &mut [u8]) -> bool {
            let mib = [CTL_KERN, KERN_PROCARGS2, pid as c_int];
            let mut len = buf.len();
            let ok = unsafe {
                sysctl(
                    mib.as_ptr(),
                    3,
                    buf.as_mut_ptr().cast::<c_void>(),
                    &mut len,
                    std::ptr::null_mut(),
                    0,
                )
            };
            if ok != 0 || len < 4 {
                return false;
            }
            let mut pos = 4usize;
            while pos < len {
                while pos < len && buf[pos] == 0 {
                    pos += 1; // skip NUL / alignment padding
                }
                if pos >= len {
                    break;
                }
                let start = pos;
                while pos < len && buf[pos] != 0 {
                    pos += 1;
                }
                if &buf[start..pos] == b"zcode-cli" {
                    return true;
                }
            }
            false
        }

        /// Whether any ZCode process exists on the system (either the desktop app or the CLI counts).
        /// Used for the standby detection of the autostart follow mode (autostart.rs)
        pub fn any_zcode_process() -> bool {
            unsafe {
                let n = proc_listallpids(std::ptr::null_mut(), 0);
                if n <= 0 {
                    return false;
                }
                let cap = (n + 16) as usize;
                let mut buf = vec![0i32; cap];
                let m = proc_listallpids(buf.as_mut_ptr().cast::<c_void>(), (cap * 4) as c_int);
                if m <= 0 {
                    return false;
                }
                let mut scratch = vec![0u8; 64 * 1024];
                for pid in &buf[..m as usize] {
                    if *pid > 0 && proc_is_zcode(*pid as u32, &mut scratch) {
                        return true;
                    }
                }
            }
            false
        }

        /// KERN_PROCARGS2 test for a "ZCode process": argv[0] (executable path, the first NUL-terminated string right after the 4-byte
        /// nargs header) ending in /ZCode = desktop main process;
        /// the argument area containing the exact string zcode-cli = CLI subprocess (same methodology as discover)
        fn proc_is_zcode(pid: u32, buf: &mut [u8]) -> bool {
            let mib = [CTL_KERN, KERN_PROCARGS2, pid as c_int];
            let mut len = buf.len();
            let ok = unsafe {
                sysctl(
                    mib.as_ptr(),
                    3,
                    buf.as_mut_ptr().cast::<c_void>(),
                    &mut len,
                    std::ptr::null_mut(),
                    0,
                )
            };
            if ok != 0 || len < 4 {
                return false;
            }
            if let Some(end) = buf[4..len].iter().position(|b| *b == 0).map(|p| p + 4) {
                if buf[4..end].to_ascii_lowercase().ends_with(b"/zcode") {
                    return true;
                }
            }
            argv_has_cli_marker(pid, buf)
        }

        pub fn open_proc(pid: u32) -> Option<ProcHandle> {
            let ru = read_rusage(pid)?;
            Some(ProcHandle {
                pid,
                start_abstime: ru.ri_proc_start_abstime,
            })
        }

        /// Cumulative write bytes since process start (ri_diskio_byteswritten). Returns None if the process has exited or
        /// the pid was reused (start_abstime changed); the upper layer drops it that tick
        pub fn io_write_bytes(h: &ProcHandle) -> Option<u64> {
            let ru = read_rusage(h.pid)?;
            if ru.ri_proc_start_abstime != h.start_abstime {
                return None;
            }
            Some(ru.ri_diskio_byteswritten)
        }

        /// macOS does no tracked file deduction: in the 120s probe, the rollout dir's du net change
        /// measured negative (CLI cleanup rotation), and negative deltas would backfire on the cleaned stream; a constant 0 also avoids a per-tick directory scan
        pub fn tracked_files_total() -> u64 {
            0
        }
    }

    /// Other platforms: IO probing unavailable (panel falls back to window/estimate display)
    #[cfg(not(any(windows, target_os = "macos")))]
    mod stub {
        pub struct ProcHandle;

        pub fn discover_cli_pids() -> Vec<u32> {
            Vec::new()
        }
        pub fn any_zcode_process() -> bool {
            false
        }
        pub fn open_proc(_pid: u32) -> Option<ProcHandle> {
            None
        }
        pub fn io_write_bytes(_h: &ProcHandle) -> Option<u64> {
            None
        }
        pub fn tracked_files_total() -> u64 {
            0
        }
    }

    #[cfg(windows)]
    pub use win::*;
    #[cfg(target_os = "macos")]
    pub use mac::*;
    #[cfg(not(any(windows, target_os = "macos")))]
    pub use stub::*;
}

pub struct LiveIo {
    procs: HashMap<u32, ProcRing>,
    last_refresh: Option<Instant>,
    /// (time ms, tracked file cumulative bytes)
    files_hist: VecDeque<(i64, u64)>,
    pending: VecDeque<Call>,
    cal: VecDeque<f64>,
    /// Cleaning/calibration parameters (parameterized per platform, locked at startup)
    params: CleanParams,
    bytes_per_token: f64,
    last_result: LiveNow,
    /// session → the CLI process that most recently generated output for it
    session_pid: HashMap<String, u32>,
    /// Last time the cleaned flow rate reached the streaming threshold (timer start for the stop-detection fallback)
    last_stream_ms: Option<i64>,
    /// Current session of interest = the session of the most recently completed call (only seeds the gate's criterion when it has no call baseline)
    current_session: Option<String>,
    attributed: HashSet<String>,
    history_done: bool,
    /// All in-progress calls (Engine decides via the message table): (session, call start time);
    /// with concurrent multi-tasking, live speed aggregates over the union of attributed processes
    inflight: Vec<(String, i64)>,
    /// Magnitude anchor of this call segment (first time the streaming threshold is reached, wall-clock ms)
    active_since: Option<i64>,
    /// Most recent calibration event (for debug logs to consume)
    pending_cal: Option<CalEvent>,
    /// Whether CLI processes were ever discovered in this process's lifetime (distinguishes "never available" from "all exited")
    ever_saw_procs: bool,
}

impl LiveIo {
    pub fn new() -> Self {
        // Preseed the coefficient queue with the default as a prior sample: during cold start (window < 3 samples) the estimator shrinks
        // toward the prior so a single anomalous sample cannot own the coefficient; the prior placeholder slides out naturally as the window moves
        let params = CleanParams::platform();
        let mut cal = VecDeque::with_capacity(CAL_QUEUE_CAP);
        cal.push_back(params.default_bpt);
        Self {
            procs: HashMap::new(),
            last_refresh: None,
            files_hist: VecDeque::new(),
            pending: VecDeque::new(),
            cal,
            params,
            bytes_per_token: params.default_bpt,
            last_result: LiveNow::default(),
            session_pid: HashMap::new(),
            last_stream_ms: None,
            current_session: None,
            attributed: HashSet::new(),
            history_done: false,
            inflight: Vec::new(),
            active_since: None,
            pending_cal: None,
            ever_saw_procs: false,
        }
    }

    /// Injects today's existing calls at startup, used to determine the current session
    pub fn ingest_history(&mut self, calls: &[Call]) {
        if let Some(latest) = calls.iter().max_by_key(|c| c.completed_ms) {
            self.current_session = Some(latest.session.clone());
        }
        self.history_done = true;
    }

    pub fn history_done(&self) -> bool {
        self.history_done
    }

    pub fn observe(&mut self, new_calls: &[Call]) {
        for c in new_calls {
            self.pending.push_back(c.clone());
            // The most recently completed call's session = the current session of interest
            self.current_session = Some(c.session.clone());
        }
        while self.pending.len() > 8 {
            self.pending.pop_front();
        }
    }

    /// Updates the "call in progress" signal every tick (Engine derives it by comparing the message table with completed rows;
    /// with concurrent multi-tasking this covers all in-progress sessions)
    pub fn set_inflight(&mut self, inflight: Vec<(String, i64)>) {
        self.inflight = inflight;
    }

    /// For debug logs: attribution map of in-progress sessions (session id → pid; unattributed sessions omitted)
    pub fn inflight_attr(&self) -> Vec<(String, u32)> {
        self.inflight
            .iter()
            .filter_map(|(s, _)| self.session_pid.get(s).map(|p| (s.clone(), *p)))
            .collect()
    }

    /// Current effective bytes→token coefficient (for debug logs)
    pub fn bytes_per_token(&self) -> f64 {
        self.bytes_per_token
    }

    /// Whether CLI processes were ever discovered. Distinguishes "never available" (IO probing unavailable in this environment, estimate fallback allowed)
    /// from "discovered then all exited" (the CLI is closed; generation/estimate display should stop)
    pub fn ever_saw_procs(&self) -> bool {
        self.ever_saw_procs
    }

    /// Takes the most recent calibration event (if any)
    pub fn take_calibration(&mut self) -> Option<CalEvent> {
        self.pending_cal.take()
    }

    /// Recalibrates (manual button on the current speed card / auto-triggered by round mean-speed drift): discards learned
    /// coefficient samples, returning to the platform prior's cold-start state (prior placeholder prevents single-sample capture), then re-converges
    /// from samples of subsequent completed calls. Pending calls are kept — session→process attribution still needs processing,
    /// and their old-magnitude samples get pushed out by new ones within 1~2 window rotations. Returns the coefficient after reset
    pub fn reset_calibration(&mut self) -> f64 {
        self.cal.clear();
        self.cal.push_back(self.params.default_bpt);
        self.bytes_per_token = self.params.default_bpt;
        self.bytes_per_token
    }

    /// Exports the coefficient sample queue (for persistence; after recalibration it is [prior], and persisting it on every queue change
    /// keeps the "recalibration intent" across restarts)
    pub fn cal_state(&self) -> Vec<f64> {
        self.cal.iter().copied().collect()
    }

    /// Restores coefficient samples from persistence: accepts only finite values within range, injecting them into the queue (capacity matches live
    /// calibration, oldest dropped on overflow); the effective coefficient is the estimator output over the restored queue (same
    /// measurement methodology as the calibration path). If empty/all invalid, the prior stays untouched; returns the number actually accepted
    pub fn restore_cal(&mut self, samples: Vec<f64>) -> usize {
        let valid: Vec<f64> = samples
            .into_iter()
            .filter(|v| v.is_finite() && *v >= self.params.cal_min && *v <= self.params.cal_max)
            .collect();
        let n = valid.len();
        for v in valid {
            self.cal.push_back(v);
        }
        while self.cal.len() > CAL_QUEUE_CAP {
            self.cal.pop_front();
        }
        self.bytes_per_token = cal_estimate(self.cal.make_contiguous(), self.params.default_bpt);
        n
    }

    /// Called once per polling cycle. now_ms is wall-clock milliseconds (same source as Engine snapshots)
    pub fn measure(&mut self, now_ms: i64) -> LiveNow {
        let now = Instant::now();
        // Periodically refresh the CLI process set; shorten to 2s instead of waiting the full 30s when no process has been found, when an in-progress
        // session has no attribution record yet (a newly opened second window — display falls back to the all-process sum, and if the new process
        // hasn't been discovered the reading reflects another window's speed), or when ≥2 sessions are in progress (concurrent multi-tasking;
        // attribution dedup needs to see the new process's byte distribution quickly)
        let unattributed_inflight =
            !self.inflight.is_empty() && self.inflight.iter().any(|(s, _)| !self.session_pid.contains_key(s));
        let refresh_due = self
            .last_refresh
            .map_or(true, |t| now.duration_since(t) > REFRESH_EVERY);
        let quick_due = self
            .last_refresh
            .map_or(true, |t| now.duration_since(t) > Duration::from_secs(2));
        if refresh_due
            || ((self.procs.is_empty() || unattributed_inflight || self.inflight.len() >= 2) && quick_due)
        {
            self.last_refresh = Some(now);
            let found = platform::discover_cli_pids();
            self.procs.retain(|pid, _| found.contains(pid));
            for pid in found {
                if let Some(handle) = platform::open_proc(pid) {
                    self.procs.entry(pid).or_insert_with(|| ProcRing {
                        handle,
                        samples: VecDeque::new(),
                        min_delta: f64::MAX,
                    });
                }
            }
            if !self.procs.is_empty() {
                self.ever_saw_procs = true;
            }
        }

        // Sample this round's write bytes and tracked file totals (paired same tick, identical timestamps)
        self.procs.retain(|_, ring| {
            match platform::io_write_bytes(&ring.handle) {
                Some(w) => {
                    ring.samples.push_back((now_ms, w));
                    while ring.samples.len() > RING_CAP {
                        ring.samples.pop_front();
                    }
                    true
                }
                None => false, // process has exited
            }
        });
        // Also clean up session mappings for stale PIDs, preventing memory leaks and dirty attribution from PID reuse
        let active_pids: HashSet<u32> = self.procs.keys().copied().collect();
        self.session_pid.retain(|_, pid| active_pids.contains(pid));
        if self.session_pid.len() > 200 {
            self.session_pid.clear();
        }
        let ft = platform::tracked_files_total();
        self.files_hist.push_back((now_ms, ft));
        while self.files_hist.len() > RING_CAP {
            self.files_hist.pop_front();
        }
        let files: Vec<(i64, u64)> = self.files_hist.iter().copied().collect();

        // Cleaned tick sequences (display and calibration share the same stream, guaranteeing a consistent measurement methodology)
        let mut rows_by_pid: HashMap<u32, Vec<TickRow>> = HashMap::new();
        for (pid, ring) in self.procs.iter_mut() {
            let samples: Vec<(i64, u64)> = ring.samples.iter().copied().collect();
            // A low quantile of per-tick minimum deltas × 2 as this process's heartbeat noise floor (bytes/tick);
            // skip the adaptive floor when samples are too few (avoiding a cold start swallowing the initial signal).
            // Capped inside build_rows when used, preventing self-poisoning by its own deltas during sustained streaming
            let mut deltas: Vec<f64> = samples
                .windows(2)
                .map(|w| w[1].1.saturating_sub(w[0].1) as f64)
                .collect();
            if !deltas.is_empty() {
                deltas.sort_by(|a: &f64, b: &f64| a.partial_cmp(b).unwrap());
                ring.min_delta = if deltas.len() >= 20 {
                    deltas[deltas.len() / 10] * 2.0
                } else {
                    0.0
                };
            }
            rows_by_pid.insert(*pid, build_rows(&samples, ring.min_delta, &self.params));
        }

        // ---- Calibration + session→process attribution (processed after call completion) ----
        // Attribution uses raw bytes (uncleaned): flush misalignment/noise deduction does not affect the "which process is writing" judgment.
        // The coefficient numerator integrates the exact cleaned stream used for display over [first_token, completed],
        // so systematic deductions cancel in the coefficient and the display converges to true t/s
        while let Some(call) = self.pending.front().cloned() {
            if now_ms - call.completed_ms > 120_000 {
                self.pending.pop_front();
                continue;
            }
            // Delayed-flush grace (mac): disk write bytes are a page-cache async flush count, so wait the full grace after
            // completed before integrating to let dirty pages enter the counter. Stays at the queue head during grace; the >120s
            // drop check runs first to prevent backlog. With Windows=0 this branch is skipped and same-tick processing is unchanged
            if self.params.cal_grace_ms > 0 && now_ms - call.completed_ms < self.params.cal_grace_ms
            {
                break;
            }
            let stream_start_ms = (call.completed_ms - call.gen_ms.min(300_000)).max(0);
            // Integral/stats window upper bound extended to completed + grace: mac's dirty pages flush late,
            // so numerator (clean) and denominator methodology (raw) relax together, while pred keeps using real gen_ms
            let window_end_ms = call.completed_ms + self.params.cal_grace_ms;
            let mut raw_by_pid: HashMap<u32, u64> = HashMap::new();
            for (pid, ring) in self.procs.iter() {
                let mut acc = 0u64;
                for (a, b) in ring.samples.iter().zip(ring.samples.iter().skip(1)) {
                    if b.0 >= stream_start_ms && a.0 <= window_end_ms {
                        acc += b.1.saturating_sub(a.1);
                    }
                }
                raw_by_pid.insert(*pid, acc);
            }
            let raw_total = raw_by_pid.values().sum::<u64>() as f64;
            // Candidates sorted by bytes descending, ties by pid ascending (deterministic); shared by attribution and event recording
            let mut cands: Vec<(u32, u64)> = raw_by_pid.iter().map(|(p, b)| (*p, *b)).collect();
            cands.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            let top_pid = cands.first().map(|(p, _)| *p);
            if !self.attributed.contains(&call.id) {
                self.attributed.insert(call.id.clone());
                if self.attributed.len() > 4_000 {
                    self.attributed.clear();
                }
                if raw_total > 20_000.0 {
                    // pids already owned by other in-progress sessions (concurrent dedup: don't squeeze onto the same process on a tie;
                    // when two sessions genuinely share one process the runner-up is only noise ratio, so it is unaffected)
                    let owned: HashSet<u32> = self
                        .inflight
                        .iter()
                        .filter(|(s, _)| s != &call.session)
                        .filter_map(|(s, _)| self.session_pid.get(s).copied())
                        .collect();
                    if let Some(pid) =
                        pick_attribution(self.session_pid.get(&call.session).copied(), &raw_by_pid, &owned)
                    {
                        self.session_pid.insert(call.session.clone(), pid);
                    }
                }
            }
            // Cleaned integral: prefer the attributed process (the same aggregated stream as the display path, tracked file
            // growth likewise deducted once); falls back to the all-process aggregate when unattributed
            let mut attr_pid = None;
            let clean_bytes = match self.session_pid.get(&call.session) {
                Some(pid) if rows_by_pid.contains_key(pid) => {
                    attr_pid = Some(*pid);
                    let streams = [&rows_by_pid[pid][..]];
                    integrate(
                        &merge_streams(&streams, &files),
                        stream_start_ms,
                        window_end_ms,
                    )
                    .0
                }
                _ => {
                    let streams: Vec<&[TickRow]> =
                        rows_by_pid.values().map(|v| &v[..]).collect();
                    integrate(
                        &merge_streams(&streams, &files),
                        stream_start_ms,
                        window_end_ms,
                    )
                    .0
                }
            };
            // Numerator/denominator pairing: when clean comes from the attributed process, the raw baseline uses that process too
            // (all-process raw would count other windows' bytes into the denominator and misjudge the clean/raw guard).
            // Cross-process guard baseline: the attributed process's bytes; in the unattributed all-process sum branch use the top process
            // as baseline — that branch's integral also mixes in other windows' bytes and must not pass
            let top_raw = top_pid
                .map_or(0.0, |p| *raw_by_pid.get(&p).unwrap_or(&0) as f64);
            let attr_raw = attr_pid
                .map_or(raw_total, |p| *raw_by_pid.get(&p).unwrap_or(&0) as f64);
            let guard_ref = attr_pid.map_or(top_raw, |_| attr_raw);
            let others_raw = (raw_total - guard_ref).max(0.0);
            let eff = call.effective_out();
            let true_tps = eff as f64 / (call.gen_ms.max(50) as f64 / 1000.0);
            let (mut in_cal, bpt_sample) =
                cal_sample(eff, clean_bytes, attr_raw, self.bytes_per_token, &self.params);
            if in_cal && !cross_pid_ok(others_raw, guard_ref) {
                in_cal = false;
            }
            if in_cal {
                self.cal.push_back(bpt_sample);
                while self.cal.len() > CAL_QUEUE_CAP {
                    self.cal.pop_front();
                }
                self.bytes_per_token = cal_estimate(self.cal.make_contiguous(), self.params.default_bpt);
            }
            self.pending_cal = Some(CalEvent {
                id: call.id.clone(),
                session: call.session.clone(),
                completed_ms: call.completed_ms,
                true_tps,
                gen_ms: call.gen_ms,
                eff,
                raw_bytes: raw_total,
                clean_bytes,
                bpt_sample: if in_cal { bpt_sample } else { 0.0 },
                bpt_now: self.bytes_per_token,
                cal_skipped: !in_cal,
                attr_pid,
                top_pid,
                others_bytes: others_raw,
            });
            self.pending.pop_front();
        }

        // Display process set: all in-progress sessions have live attribution → union of attributed pids (concurrent multi-tasking
        // aggregates into true total throughput); any session without attribution (new session/subagent whose first call has not completed)
        // → all-process sum fallback, so a new process without an attribution record is not missed or shown as another window's speed
        let pid_set = pick_pid_set(&self.inflight, &self.session_pid, &active_pids);
        // Aggregated stream: sum per-tick bytes over the set + deduct tracked file growth once overall (duration also counted
        // once — integrating per process would sum wall-clock time, diluting the rate into a cross-process mean)
        let display_rows: Vec<TickRow> = {
            let streams: Vec<&[TickRow]> = match &pid_set {
                Some(set) => set
                    .iter()
                    .filter_map(|p| rows_by_pid.get(p).map(|v| &v[..]))
                    .collect(),
                None => rows_by_pid.values().map(|v| &v[..]).collect(),
            };
            merge_streams(&streams, &files)
        };
        let span = |from_ms: i64| -> (f64, f64) { integrate(&display_rows, from_ms, now_ms) };

        // Start/stop decision (call gate): the message table's assistant rows commit at call start,
        // and the row's data.time.completed is backfilled at the end (including cancel/error) — the gate trusts
        // this signal directly without restricting to a session (the first call of a newly opened conversation lights up same tick, no need to wait for the first completed row),
        // and zeroes the same tick on end/cancel. Tool execution/idle periods also produce UI-state bursts on the pipe; the gate
        // reliably excludes them. Process guard: when all attributed processes of in-progress sessions have exited (crash/closed terminal,
        // completed never backfilled), force a stop rather than leave a zombie "generating"; sessions without attribution are not declared dead.
        // When the gate is unavailable (no completed calls yet to serve as baseline), fall back to pure byte-level detection.
        let proc_gone = {
            let mut any_alive = false;
            for (s, _) in &self.inflight {
                match self.session_pid.get(s) {
                    Some(pid) if self.procs.contains_key(pid) => any_alive = true,
                    Some(_) => {}
                    None => any_alive = true,
                }
            }
            !self.inflight.is_empty() && !any_alive
        };
        let (det_b, det_s) = span(now_ms - self.params.detect_ms);
        let detect_bps = if det_s > 0.0 { det_b / det_s } else { 0.0 };
        let gate_on = !proc_gone
            && (!self.inflight.is_empty()
                || (self.current_session.is_none() && detect_bps > STREAMING_BPS));
        if gate_on {
            // Magnitude anchor: the first time the cleaned flow rate reaches the streaming threshold within this streaming segment. Re-anchor after a
            // stream gap resumes (stop fallback fired/aggregation segment changed) — the 30s sliding window restarts from the new segment,
            // avoiding diluting the reading with the silent stretch
            if detect_bps > STREAMING_BPS {
                if !self.last_result.streaming || self.active_since.is_none() {
                    self.active_since = Some(now_ms);
                }
                // Stop-fallback timer: keeps renewing while the cleaned flow rate stays above the streaming threshold
                self.last_stream_ms = Some(now_ms);
            }
        } else {
            self.active_since = None;
            self.last_stream_ms = None;
        }

        // Magnitude: cleaned-stream integral over 30s sliding window ∩ [first-byte tick, now]. The first-byte tick yields a real reading
        // immediately (before that it is TTFT, shown as "measuring"); steady state covers the full 30s (smoothed); zeroed the tick after stop.
        // Negative ticks from flush misalignment cancel within the window; clamped non-negative only at aggregation
        let pipe_bps = if gate_on {
            match self.active_since {
                Some(anchor) => {
                    let from = anchor.max(now_ms - WINDOW_MS);
                    let (b, s) = span(from);
                    if s > 0.0 {
                        (b / s).max(0.0)
                    } else {
                        0.0
                    }
                }
                None => 0.0, // first byte not arrived (TTFT), show "measuring"
            }
        } else {
            0.0
        };
        // Stop-detection fallback: gate still on (completed not flushed) but the cleaned flow after the anchor has been silent past the grace →
        // generation has actually stopped, display zeroes; the upper layer's streaming=false takes the idle branch instead of the
        // window fallback keeping "generating" up. Self-heals the tick bytes resume (timer refreshes with the flow)
        let streaming =
            gate_on && !stale_stop(self.active_since, self.last_stream_ms, now_ms);
        let ramping = streaming
            && (self.active_since.is_none()
                || self
                    .active_since
                    .map_or(false, |a| now_ms - a < WINDOW_MS));
        // Startup hint: gate on but first byte not arrived (TTFT, from the most recently started session), bounded by the
        // hint window — show "measuring…" within it; past the window with still no bytes, the upper layer falls back to estimate
        // (silent-pipe call)
        let awaiting = streaming
            && awaiting_hint(
                self.inflight.iter().map(|(_, t)| *t).max(),
                self.active_since,
                now_ms,
            );

        // Per-task breakdown: each process in the display set's cleaned rate over the aggregation window. File growth is apportioned by each
        // process's share of **positive** bytes (negative-tick processes don't participate and their own values clamp to 0,
        // otherwise the breakdown sum would exceed the aggregate reading); empty when gate off/not streaming/startup phase (TTFT)
        let mut tasks: Vec<TaskLive> = Vec::new();
        let mut active_pid_count = 0usize;
        if streaming && !awaiting {
            let from = self
                .active_since
                .map_or(now_ms, |a| a.max(now_ms - WINDOW_MS));
            let (_, wall_s) = integrate(&display_rows, from, now_ms);
            let fg_w = integrate(&file_growth_rows(&files), from, now_ms).0;
            let pids: Vec<u32> = match &pid_set {
                Some(set) => set.clone(),
                None => rows_by_pid.keys().copied().collect(),
            };
            let mut per: Vec<(u32, f64, bool)> = Vec::with_capacity(pids.len());
            let mut sum_pos = 0.0;
            for pid in &pids {
                let Some(rows) = rows_by_pid.get(pid) else { continue };
                let (b, _) = integrate(rows, from, now_ms);
                sum_pos += b.max(0.0);
                let (db, ds) = integrate(rows, now_ms - self.params.detect_ms, now_ms);
                let dbps = if ds > 0.0 { db / ds } else { 0.0 };
                per.push((*pid, b, dbps > STREAMING_BPS));
            }
            // Number of processes whose window contribution reaches streaming magnitude (the drift detector's "single-process round" criterion;
            // idle processes' floor-noise leaks are not counted)
            active_pid_count = per
                .iter()
                .filter(|(_, b, _)| *b > STREAMING_BPS * wall_s)
                .count();
            for (pid, b, is_stream) in per {
                let share = if sum_pos > 0.0 { b.max(0.0) / sum_pos } else { 0.0 };
                let tps = if wall_s > 0.0 && self.bytes_per_token > 0.0 {
                    ((b - fg_w * share) / wall_s).max(0.0) / self.bytes_per_token
                } else {
                    0.0
                };
                tasks.push(TaskLive {
                    pid,
                    session: None,
                    n_sessions: 0,
                    tps,
                    streaming: is_stream,
                });
            }
            // Session labels and counts: in-progress sessions fill their attributed process; multiple sessions on one process
            // (a new task in the same ZCode window reusing the app-server) are honestly counted in n_sessions,
            // with the speed as that process's total — inseparable at the byte layer; the label takes the first session
            for (s, _) in &self.inflight {
                if let Some(pid) = self.session_pid.get(s) {
                    if let Some(t) = tasks.iter_mut().find(|t| t.pid == *pid) {
                        t.n_sessions += 1;
                        if t.session.is_none() {
                            t.session = Some(s.clone());
                        }
                    }
                }
            }
            // Processes that are idle and host no attributed session don't occupy breakdown slots; stably sorted by speed descending
            tasks.retain(|t| t.streaming || t.session.is_some());
            tasks.sort_by(|a, b| b.tps.partial_cmp(&a.tps).unwrap_or(std::cmp::Ordering::Equal));
        }
        let n_pids = if streaming && !awaiting {
            active_pid_count
        } else {
            0
        };
        // Diagnostics: each tracked process's detection-window cleaned rate (KB/s, for the tick debug log)
        let proc_bps: Vec<(u32, f64)> = rows_by_pid
            .iter()
            .map(|(pid, rows)| {
                let (db, ds) = integrate(rows, now_ms - self.params.detect_ms, now_ms);
                (*pid, if ds > 0.0 { (db / ds / 1024.0 * 10.0).round() / 10.0 } else { 0.0 })
            })
            .collect();

        let result = LiveNow {
            available: !self.procs.is_empty(),
            streaming,
            ramping,
            awaiting,
            tps: if streaming {
                pipe_bps / self.bytes_per_token
            } else {
                0.0
            },
            pipe_bps,
            n_pids,
            tasks,
            proc_bps,
        };
        self.last_result = result.clone();
        result
    }
}

// ============ Tests ============
#[cfg(test)]
mod tests {
    use super::*;

    const TICK: i64 = 700;

    /// Builds a (time, cumulative bytes) sampling series: each tick's delta is given by deltas
    fn series(start_ms: i64, deltas: &[f64]) -> Vec<(i64, u64)> {
        let mut out = Vec::with_capacity(deltas.len() + 1);
        let mut acc = 0u64;
        out.push((start_ms, acc));
        for (i, d) in deltas.iter().enumerate() {
            acc += *d as u64;
            out.push((start_ms + (i as i64 + 1) * TICK, acc));
        }
        out
    }

    #[test]
    fn burst_tick_dropped_entirely() {
        // The 190KB request-body burst tick is dropped whole (neither bytes nor duration integrate); 52KB real streaming ticks are kept
        let s = series(0, &[190_000.0, 52_000.0, 52_000.0]);
        let rows = build_rows(&s, 0.0, &CleanParams::windows());
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.bytes < 52_000.0));
        // The burst tick contributes no duration at all: the two kept ticks' dt totals 1.4s
        let secs = integrate(&rows, i64::MIN, i64::MAX).1;
        assert!((secs - 1.4).abs() < 1e-9);
    }

    #[test]
    fn floor_capped_against_ring_poisoning() {
        // The adaptive floor is poisoned to 50KB/tick (quantile lifted during sustained streaming);
        // after capping, at most FLOOR_CAP + BASE_NOISE×dt is deducted per tick, so 52KB streaming ticks keep the bulk
        let s = series(0, &[52_000.0; 4]);
        let rows = build_rows(&s, 50_000.0, &CleanParams::windows());
        let floor = FLOOR_CAP_BYTES + BASE_NOISE_BPS * (TICK as f64 / 1000.0);
        for r in &rows {
            assert!((r.bytes - (52_000.0 - floor)).abs() < 1e-6);
        }
    }

    /// Aggregated stream: file growth deducted once overall (deducting per process when summing would subtract N times),
    /// wall-clock duration counted once too (integrating per process would sum durations, diluting the rate into a mean)
    #[test]
    fn merge_streams_deducts_file_growth_once_and_counts_secs_once() {
        // Two processes each write 52KB same tick, files grow 10KB per tick
        let a = series(0, &[52_000.0; 4]);
        let b = series(0, &[52_000.0; 4]);
        let files: Vec<(i64, u64)> = (0..5)
            .map(|i| (a[0].0 + i as i64 * TICK, i as u64 * 10_000))
            .collect();
        let rows_a = build_rows(&a, 0.0, &CleanParams::windows());
        let rows_b = build_rows(&b, 0.0, &CleanParams::windows());
        let merged = merge_streams(&[&rows_a, &rows_b], &files);
        let floor = BASE_NOISE_BPS * (TICK as f64 / 1000.0);
        // Per tick = 2×(52_000 − floor) − 10_000 (file deducted once), not
        // 2×(52_000 − floor − 10_000)
        for r in &merged {
            assert!((r.bytes - (2.0 * (52_000.0 - floor) - 10_000.0)).abs() < 1e-6);
        }
        // Duration counted once: 4 ticks = 2.8s (integrating per process and summing would give 5.6s)
        let (b_sum, secs) = integrate(&merged, i64::MIN, i64::MAX);
        assert!((secs - 2.8).abs() < 1e-9, "wall-clock duration should be counted once: {secs}");
        // Byte total = 2 processes × 4 ticks × (52_000−floor) − 4 ticks of file growth (deducted once each)
        assert!((b_sum - (8.0 * (52_000.0 - floor) - 40_000.0)).abs() < 1e-6);
        // A single input stream is arithmetically equivalent to the historical "deduct files per row": misaligned flush ticks are negative and cancel over the interval
        let s = series(0, &[20_000.0, 60_000.0, 20_000.0]);
        let flush_files = vec![
            (s[0].0, 0u64),
            (s[1].0, 0u64),
            (s[2].0, 60_000u64),
            (s[3].0, 60_000u64),
        ];
        let single = merge_streams(&[&build_rows(&s, 0.0, &CleanParams::windows())], &flush_files);
        assert!(single[1].bytes < 0.0, "flush tick should be negative: {}", single[1].bytes);
        let (b2, _) = integrate(&single, i64::MIN, i64::MAX);
        assert!((b2 - (100_000.0 - 60_000.0 - 3.0 * floor)).abs() < 1e-6);
    }

    /// Display process set: all in-progress sessions have live attribution → union of attributed pids;
    /// any session without attribution (new session/subagent whose first call has not completed) → None = all-process sum fallback
    #[test]
    fn pick_pid_set_union_and_fallback() {
        let mut sp = HashMap::new();
        sp.insert("a".to_string(), 1u32);
        sp.insert("b".to_string(), 2u32);
        let active: HashSet<u32> = [1u32, 2u32, 3u32].into_iter().collect();
        // Two sessions attributed separately → union (deduped, ascending)
        let inflight = vec![("b".to_string(), 5i64), ("a".to_string(), 3i64)];
        assert_eq!(
            pick_pid_set(&inflight, &sp, &active),
            Some(vec![1u32, 2u32])
        );
        // Two sessions attributed to the same process (main session and its subagent) → single-element set
        sp.insert("c".to_string(), 1u32);
        let inflight = vec![("a".to_string(), 3i64), ("c".to_string(), 9i64)];
        assert_eq!(pick_pid_set(&inflight, &sp, &active), Some(vec![1u32]));
        // New session without attribution → all-process sum fallback (its own process must not be missed)
        let inflight = vec![("a".to_string(), 3i64), ("new".to_string(), 9i64)];
        assert_eq!(pick_pid_set(&inflight, &sp, &active), None);
        // Attributed process dead → same fallback
        let dead: HashSet<u32> = [2u32].into_iter().collect();
        let inflight = vec![("a".to_string(), 3i64)];
        assert_eq!(pick_pid_set(&inflight, &sp, &dead), None);
        // No in-progress sessions → None (gate off)
        assert_eq!(pick_pid_set(&[], &sp, &active), None);
    }

    /// Attribution switch hysteresis: with existing attribution, switch only when the candidate's window bytes are ≥ 2x,
    /// preventing per-call flips between concurrent windows (2026-09-18 incident: attribution for adjacent calls of one session oscillated,
    /// the coefficient was polluted and jumped between 262~764, readings off 2~3x)
    #[test]
    fn should_reattribute_hysteresis() {
        let raws: HashMap<u32, u64> = [(1u32, 100_000u64), (2u32, 150_000u64), (3u32, 300_000u64)]
            .into_iter()
            .collect();
        // No current attribution → trust top directly
        assert_eq!(should_reattribute(None, Some(2), &raws), Some(2));
        // top same as current attribution → unchanged
        assert_eq!(should_reattribute(Some(1), Some(1), &raws), Some(1));
        // top at only 1.5x (below hysteresis) → keep current attribution, no flip
        assert_eq!(should_reattribute(Some(1), Some(2), &raws), Some(1));
        // top at 3x (≥2x hysteresis) → switch
        assert_eq!(should_reattribute(Some(1), Some(3), &raws), Some(3));
        // Current attribution has zero window bytes (stale attribution) → self-heal by switching to top
        let raws0: HashMap<u32, u64> = [(9u32, 0u64), (3u32, 300_000u64)].into_iter().collect();
        assert_eq!(should_reattribute(Some(9), Some(3), &raws0), Some(3));
        // No top (all-zero bytes) → unchanged
        assert_eq!(should_reattribute(Some(1), None, &raws), None);
    }

    /// Attribution dedup (concurrent tie): on first attribution, if top is already owned by another in-progress session and the runner-up's
    /// bytes reach half → attribute to the runner-up so the two sessions don't squeeze onto the same process (display set collapsing to one);
    /// if the runner-up has only noise ratio (genuinely sharing one app-server process) → keep top.
    /// 2026-09-18 incident: a new task in the same ZCode window reused the same app-server (others only
    /// ~0.17 noise ratio), while cross-project tasks landed on different app-servers (concurrent tie ~1)
    #[test]
    fn pick_attribution_dedup_on_tie() {
        // First attribution: top(1) owned, runner-up(2) at 95% bytes → attribute to 2
        let raws: HashMap<u32, u64> = [(1u32, 4_000_000u64), (2u32, 3_800_000u64)]
            .into_iter()
            .collect();
        let owned: HashSet<u32> = [1u32].into_iter().collect();
        assert_eq!(pick_attribution(None, &raws, &owned), Some(2));
        // Runner-up only noise ratio (~0.17) → keep the shared top
        let raws: HashMap<u32, u64> = [(1u32, 4_000_000u64), (2u32, 700_000u64)]
            .into_iter()
            .collect();
        assert_eq!(pick_attribution(None, &raws, &owned), Some(1));
        // Tie with equal bytes → byte-descending tie broken by pid ascending; top=1 owned → attribute to 2
        let raws: HashMap<u32, u64> = [(2u32, 4_000_000u64), (1u32, 4_000_000u64)]
            .into_iter()
            .collect();
        assert_eq!(pick_attribution(None, &raws, &owned), Some(2));
        // top unowned → attribute directly to top (no dedup involved)
        let no_owned: HashSet<u32> = HashSet::new();
        assert_eq!(pick_attribution(None, &raws, &no_owned), Some(1));
        // Runner-up bytes too small (<20KB threshold) → keep top
        let raws: HashMap<u32, u64> = [(1u32, 4_000_000u64), (2u32, 30_000u64)]
            .into_iter()
            .collect();
        assert_eq!(pick_attribution(None, &raws, &owned), Some(1));
    }

    /// Attribution dedup (owned self-heal): when the current attribution is owned by another in-progress session, switch if top is unowned and
    /// its bytes reach half the current's (the owned scenario's 2x hysteresis would lock in historical mis-attribution);
    /// current attribution unowned → keep the original 2x hysteresis semantics
    #[test]
    fn pick_attribution_relaxed_switch_when_owned() {
        let owned: HashSet<u32> = [1u32].into_iter().collect();
        // Current attribution 1 owned; top 2 (highest window bytes) unowned at 95% → switch
        // (the original 2x hysteresis would lock in this historical mis-attribution)
        let raws: HashMap<u32, u64> = [(2u32, 4_000_000u64), (1u32, 3_800_000u64)]
            .into_iter()
            .collect();
        assert_eq!(pick_attribution(Some(1), &raws, &owned), Some(2));
        // top below half the current attribution → keep
        let raws: HashMap<u32, u64> = [(2u32, 1_900_000u64), (1u32, 4_000_000u64)]
            .into_iter()
            .collect();
        assert_eq!(pick_attribution(Some(1), &raws, &owned), Some(1));
        // Current attribution unowned → original 2x hysteresis (top at 95% is below 2x, no switch)
        let no_owned: HashSet<u32> = HashSet::new();
        let raws: HashMap<u32, u64> = [(2u32, 4_000_000u64), (1u32, 3_800_000u64)]
            .into_iter()
            .collect();
        assert_eq!(pick_attribution(Some(1), &raws, &no_owned), Some(1));
        // top also owned (both pids have owners) → nowhere to go, keep current attribution
        let both_owned: HashSet<u32> = [1u32, 2u32].into_iter().collect();
        assert_eq!(pick_attribution(Some(1), &raws, &both_owned), Some(1));
    }

    /// Calibration cross-process guard: admitted only when other processes' window bytes are ≤ 20% of the attributed process's
    #[test]
    fn cross_pid_guard_thresholds() {
        // Only the attributed process writes bytes → pass
        assert!(cross_pid_ok(0.0, 500_000.0));
        // Other processes at exactly 20% → pass (boundary inclusive)
        assert!(cross_pid_ok(100_000.0, 500_000.0));
        // Over 20% → reject (the other window streaming concurrently)
        assert!(!cross_pid_ok(100_001.0, 500_000.0));
        // Attributed process zero bytes → reject (no baseline to pair against)
        assert!(!cross_pid_ok(0.0, 0.0));
    }

    #[test]
    fn integrate_prorates_boundary_ticks() {
        let rows = vec![TickRow { dt_ms: 1000, bytes: 1000.0, end_ms: 10_000 }];
        // Take only the second half of the tick [9_500, 10_000]: half the bytes and half the duration
        let (b, s) = integrate(&rows, 9_500, 10_000);
        assert!((b - 500.0).abs() < 1e-9);
        assert!((s - 0.5).abs() < 1e-9);
        // A fully disjoint interval contributes nothing
        let (b, _) = integrate(&rows, 10_500, 11_000);
        assert_eq!(b, 0.0);
    }

    #[test]
    fn cal_estimate_weighted_median_basics() {
        // A full window of identical samples → exactly that value
        let s: Vec<f64> = vec![560.0; 16];
        assert!((cal_estimate(&s, 600.0) - 560.0).abs() < 1e-9);
        // Recency weighting: the trailing 3 new-magnitude samples (weights 1+.79+.63=2.42) outweigh 13 old-magnitude ones
        // (weight sum ~2.1) — after a magnitude shift 3 samples catch up; adaptivity is not slowed by the window
        let mut s: Vec<f64> = vec![560.0; 13];
        s.extend([200.0; 3]);
        assert!((cal_estimate(&s, 600.0) - 200.0).abs() < 1e-9);
        // Mild trim + AR1 bounded pull: a single outlier sample (0.23x, the half/silent garbage kind)
        // is trimmed out of the weighted median (anchor stays 560), but AR1 shrinkage still pulls one tick at the clamp bound:
        // 560 × 0.5^0.3 ≈ 454.9 — bounded impact that recovers the next tick; a tradeoff between outlier tolerance and
        // shift head start (real replay shows positive net gain)
        let mut s: Vec<f64> = vec![560.0; 15];
        s.push(130.0);
        let bounded = 560.0 * (1.0 / CAL_AR1_CLAMP).powf(CAL_AR1_RHO);
        assert!((cal_estimate(&s, 600.0) - bounded).abs() < 1e-9);
        // Empty window returns the prior
        assert!((cal_estimate(&[], 600.0) - 600.0).abs() < 1e-9);
    }

    /// Two boundary properties of AR1 shrinkage: the clamp caps a single sample's influence; the first post-shift sample gets a head start and
    /// convergence is no slower than the non-shrinking version
    #[test]
    fn cal_estimate_ar1_bounded_and_step_head_start() {
        // One ×9 outlier after steady state: impact ≤ CLAMP^ρ (ratio clamped to 2)
        let mut s: Vec<f64> = vec![550.0; 15];
        s.push(5_000.0);
        let e = cal_estimate(&s, 600.0);
        let bounded = 550.0 * CAL_AR1_CLAMP.powf(CAL_AR1_RHO);
        assert!((e - bounded).abs() < 1e-9, "{e} should equal the bounded pull {bounded}");
        // First sample of a magnitude shift (300→900): head-start pull (369 > 300); the median anchor only flips
        // once new-magnitude weight passes half (the 3rd sample), during which AR1 maintains a bounded head start
        let base = vec![300.0; 12];
        let e1 = cal_estimate(&[base.as_slice(), &[900.0]].concat(), 600.0);
        assert!((e1 - 300.0 * CAL_AR1_CLAMP.powf(CAL_AR1_RHO)).abs() < 1e-9, "first sample should pull ahead: {e1}");
        let e2 = cal_estimate(&[base.as_slice(), &[900.0, 900.0]].concat(), 600.0);
        assert!((e2 - e1).abs() < 1e-9, "maintains head start before the median flips: {e2}");
        let mut s = base.clone();
        s.extend([900.0; 3]);
        let e3 = cal_estimate(&s, 600.0);
        assert!((e3 - 900.0).abs() < 1e-9, "fully caught up once new-magnitude weight passes half: {e3}");
    }

    /// Cold-start protection keeps the historical property: with the prior preseeded, a single anomalous sample (e.g. bpt=179) only
    /// partially pulls the coefficient via prior shrinkage (≈319, not owning it at 179); after two consistent samples
    /// it fully follows the data, and trimming removes outlier samples from the weighted median
    #[test]
    fn cold_start_prior_shrinks_single_sample() {
        let mut s: Vec<f64> = vec![600.0];
        s.push(179.0);
        let e1 = cal_estimate(&s, 600.0);
        assert!((e1 - 179.0 * (2.0 / 3.0) - 600.0 / 3.0).abs() < 1e-9, "{e1}");
        s.push(552.0);
        let e2 = cal_estimate(&s, 600.0);
        assert!((e2 - 552.0).abs() < 1e-9, "with 3 samples it should fully follow the data: {e2}");
    }

    #[test]
    fn awaiting_hint_window() {
        // Gate on, first byte not arrived: true within the hint window (20s)
        assert!(awaiting_hint(Some(1_000), None, 5_000));
        assert!(awaiting_hint(Some(1_000), None, 21_000));
        // Past the window → no more hint (upper layer falls back to estimate: silent-pipe call)
        assert!(!awaiting_hint(Some(1_000), None, 21_001));
        // First-byte anchor exists → not in the startup phase
        assert!(!awaiting_hint(Some(1_000), Some(2_000), 5_000));
        // No in-progress calls → no hint
        assert!(!awaiting_hint(None, None, 5_000));
        assert!(!awaiting_hint(None, Some(1_000), 5_000));
    }

    /// Stop-detection fallback: gate on (completed not flushed) but the cleaned flow after the anchor silent past the grace → stop.
    /// Field example (2026-09-17 logs): the call had stopped, but before completed flushed the window fallback kept
    /// showing "generating + ≈ last round's speed"; the user perceived "stopped but still generating, slowly declining"
    #[test]
    fn stale_stop_after_silent_window() {
        // Anchor established, last flow time within grace → still streaming
        assert!(!stale_stop(Some(1_000), Some(16_000), 16_000));
        // Silent exactly 15s → not past (strict >), still streaming
        assert!(!stale_stop(Some(1_000), Some(1_000), 16_000));
        // Silent past 15s → stop
        assert!(stale_stop(Some(1_000), Some(1_000), 16_001));
        // No anchor (silent-pipe call, no bytes at all) → never stops; served by the window fallback
        assert!(!stale_stop(None, None, 100_000));
        assert!(!stale_stop(None, Some(1_000), 100_000));
        // Anchor present but no flow time ever recorded (theoretically unreachable: an anchor implies flow) → no stop
        assert!(!stale_stop(Some(1_000), None, 100_000));
    }

    /// Round mean-speed drift: last round's mean vs the mean of the previous 5 consecutive rounds; ≥3x in either direction triggers recalibration
    #[test]
    fn round_drift_triggers_on_threefold_jump() {
        let mut d = RoundDrift::new();
        for v in [40.0, 42.0, 38.0, 41.0, 39.0] {
            assert!(d.observe(v).is_none(), "should not trigger while the baseline accumulates");
        }
        // Last round 120 = exactly 3x the baseline mean 40 → triggers, returning the baseline for logs
        let base = d.observe(120.0).expect("a 3x upward jump should trigger");
        assert!((base - 40.0).abs() < 1e-9);
        // History cleared after trigger: the next round of the same magnitude does not trigger again
        assert!(d.observe(120.0).is_none());
    }

    #[test]
    fn round_drift_needs_five_round_history() {
        let mut d = RoundDrift::new();
        for _ in 0..4 {
            assert!(d.observe(40.0).is_none());
        }
        // History under 5 rounds: even an extreme jump does not trigger; the round still enters history
        assert!(d.observe(4_000.0).is_none());
        // Once 5 rounds are filled, the mixed baseline (4×40 + 4000 = 832) still differs from the old magnitude 40 by more than 3x
        assert!(d.observe(40.0).is_some());
    }

    /// Reverse direction (switching back from a faster model, or fast→slow): baseline 90 vs last round 30 = 1/3 → triggers too
    #[test]
    fn round_drift_downward_jump_triggers() {
        let mut d = RoundDrift::new();
        for _ in 0..5 {
            assert!(d.observe(90.0).is_none());
        }
        assert!(d.observe(30.0).is_some());
    }

    /// Normal fluctuation within 2.5x does not trigger; the sliding window keeps only the last 5 rounds, old magnitudes slide out naturally
    #[test]
    fn round_drift_moderate_change_and_window_cap() {
        let mut d = RoundDrift::new();
        for _ in 0..5 {
            assert!(d.observe(40.0).is_none());
        }
        assert!(d.observe(100.0).is_none(), "a 2.5x upward jump should not trigger");
        for _ in 0..5 {
            assert!(d.observe(100.0).is_none());
        }
        // Baseline all 100 now; a drop back to 40 is exactly 2.5x → no trigger
        assert!(d.observe(40.0).is_none());
    }

    /// Silent rounds with no measured ticks (mean 0) don't participate in detection and don't pollute the baseline
    #[test]
    fn round_drift_ignores_zero_round() {
        let mut d = RoundDrift::new();
        for v in [50.0, 0.0, 50.0, 0.0, 50.0, 0.0, 50.0] {
            assert!(d.observe(v).is_none());
        }
        // Four 50s enter history (all 0s ignored); the 5th 50 fills the baseline without triggering
        assert!(d.observe(50.0).is_none());
        assert!(d.observe(200.0).is_some());
    }

    /// Recalibration: the coefficient and sample queue return to the platform prior (cold-start state)
    #[test]
    fn reset_calibration_restores_prior() {
        let mut io = LiveIo::new();
        io.cal.clear();
        io.cal.extend([420.0, 380.0, 455.0]);
        io.bytes_per_token = 420.0;
        let bpt = io.reset_calibration();
        assert!((bpt - io.params.default_bpt).abs() < 1e-9);
        assert_eq!(io.cal.len(), 1, "queue should only hold the prior placeholder");
        assert!((io.bytes_per_token - io.params.default_bpt).abs() < 1e-9);
    }

    /// Restart restore: out-of-range/non-finite values rejected; the effective coefficient is recomputed as the estimator output over the restored
    /// queue (same measurement methodology as the calibration path); an empty restore leaves state untouched. With capacity 16, a queue mixing old
    /// samples and the prior placeholder does not trigger eviction
    #[test]
    fn restore_cal_filters_and_recomputes_median() {
        let mut io = LiveIo::new();
        // 30 below the lower bound, 99999 above the upper bound (beyond both platforms' CAL_MAX), NaN non-finite → reject;
        // 500/540 enqueue, giving [prior600,500,540]
        let n = io.restore_cal(vec![500.0, 540.0, 30.0, 99_999.0, f64::NAN]);
        assert_eq!(n, 2);
        // The weighted median (recency weights 1/.79/.63) lands on 540; with 3 samples there is no prior shrinkage
        assert!((io.bytes_per_token() - 540.0).abs() < 1e-9);
        // Empty restore leaves state untouched
        assert_eq!(io.restore_cal(vec![]), 0);
        assert!((io.bytes_per_token() - 540.0).abs() < 1e-9);
        // Inject 5 more valid values: within capacity 16, no eviction; queue = [600,500,540,450..490]
        io.restore_cal(vec![450.0, 460.0, 470.0, 480.0, 490.0]);
        assert_eq!(io.cal_state().len(), 8);
        assert!(io.cal_state().contains(&io.params.default_bpt), "prior placeholder retained within capacity");
        // The untrimmed weighted median 480 = trim center, unchanged after trimming; AR1 shrinks logarithmically toward the newest sample 490
        // (ratio within the clamp bound, (490/480)^0.3 ≈ 1.0062)
        let expect = 480.0 * (490.0f64 / 480.0).powf(CAL_AR1_RHO);
        assert!((io.bytes_per_token() - expect).abs() < 1e-9, "{est}", est = io.bytes_per_token());
        // Capacity eviction: inject 10 more; the queue keeps the newest 16 (prior600/500 evicted)
        io.restore_cal(vec![505.0, 510.0, 515.0, 520.0, 525.0, 512.0, 518.0, 524.0, 511.0, 517.0]);
        assert_eq!(io.cal_state().len(), CAL_QUEUE_CAP);
        assert!(!io.cal_state().contains(&io.params.default_bpt));
        // The newest samples 505~525 dominate the weights; the estimate lands among them
        let est = io.bytes_per_token();
        assert!((505.0..=525.0).contains(&est), "estimate should be within the new samples' magnitude: {est}");
    }

    /// Sample admission cases taken from real debug logs (2026-09-17 incident):
    /// silent-pipe calls produce ~7 B/token garbage samples, which must be rejected whole rather than clamped and enqueued
    #[test]
    fn cal_sample_rejects_silent_pipe_calls() {
        // bpt_now passes the Windows prior (outlier rejection is disabled on that platform; the value doesn't affect the result)
        // Silent call: 887 tokens, pipe integral only 5.9KB, raw bytes ~1MB → reject
        let (ok, _) = cal_sample(887, 5_939.0, 1_048_576.0, DEFAULT_BPT, &CleanParams::windows());
        assert!(!ok);
        // Normal call: 1028 tokens, clean 526.5KB / raw ~900KB → accept, sample ≈524
        let (ok, v) = cal_sample(
            1028,
            526.5 * 1024.0,
            900.0 * 1024.0,
            DEFAULT_BPT,
            &CleanParams::windows(),
        );
        assert!(ok);
        assert!((v - 524.0).abs() < 15.0);
        // Small call: 64 tokens → reject
        assert!(!cal_sample(64, 50_000.0, 80_000.0, DEFAULT_BPT, &CleanParams::windows()).0);
        // Lean but real: 449 tokens, clean 67.5KB (ratio ~150 B/token, 52% of raw) → accept
        let (ok, v) = cal_sample(
            449,
            67.5 * 1024.0,
            130.0 * 1024.0,
            DEFAULT_BPT,
            &CleanParams::windows(),
        );
        assert!(ok);
        assert!((v - 150.0).abs() < 5.0);
        // Ratio out of range (>6000) → reject
        assert!(!cal_sample(
            500,
            500.0 * 6000.0 * 1.1,
            500.0 * 6000.0 * 1.2,
            DEFAULT_BPT,
            &CleanParams::windows()
        )
        .0);
    }

    /// mac parameter literal (kept identical to `CleanParams::macos()`; literal construction ensures
    /// the tests compile and run on Windows too)
    fn mac_params() -> CleanParams {
        CleanParams {
            burst_tick_bytes: u64::MAX as f64,
            base_noise_bps: 0.0,
            floor_cap_bytes: FLOOR_CAP_BYTES,
            cal_min: CAL_MIN,
            cal_max: 12_000.0,
            cal_min_tokens: CAL_MIN_TOKENS,
            default_bpt: 700.0,
            detect_ms: DETECT_MS,
            cal_grace_ms: 15_000,
            cal_outlier_ratio: 3.0,
        }
    }

    /// mac outlier rejection (measured in the 2026-09-17 reconciliation): a delayed-flush half-sample (a 34s call integrating
    /// only half the bytes → 186 B/token) deviates from the effective coefficient 700 by more than the 3x bound and is rejected,
    /// staying out of the median; normal samples (around ground truth 614/724) are accepted as usual
    #[test]
    fn mac_outlier_sample_rejected() {
        let mac = mac_params();
        // Normal sample ≈650 B/token, inside [700/3, 700×3] → accept
        let (ok, v) = cal_sample(1_000, 650_000.0, 900_000.0, 700.0, &mac);
        assert!(ok);
        assert!((v - 650.0).abs() < 1e-6);
        // Half-sample 186 (>cal_min=100, clean/raw=62%, all existing checks pass):
        // 186 < 700/3≈233 → outlier rejection, sample recorded as 0
        let (ok, v) = cal_sample(1_000, 186_000.0, 300_000.0, 700.0, &mac);
        assert!(!ok);
        assert_eq!(v, 0.0);
        // High-side outlier: 2500 > 700×3=2100 (still within cal_max=12000) → reject
        assert!(!cal_sample(1_000, 2_500_000.0, 3_000_000.0, 700.0, &mac).0);
        // The same half-sample is not rejected on Windows (ratio=0 disables), equivalent to existing behavior
        assert!(cal_sample(1_000, 186_000.0, 300_000.0, DEFAULT_BPT, &CleanParams::windows()).0);
    }

    /// mac delayed-flush grace: disk write bytes are a page-cache async flush count, entering the counter seconds~
    /// tens of seconds after write() (a 117s long call measured 96% of bytes landing after completed, the user staring
    /// at 0.7 t/s for two minutes while ground truth was 65.3). grace=15s extends the calibration integral window to
    /// completed+15s, so lagging bytes enter the numerator and the sample recovers ground truth; the Windows-style
    /// [.., completed] window loses nearly everything
    #[test]
    fn mac_grace_window_captures_delayed_disk_writes() {
        const TRUE_TPS: f64 = 50.0;
        const BPT_TRUE: f64 = 3_900.0; // mac streaming pipe byte density
        let gen_ms = 30_000i64;
        let n = (gen_ms / TICK) as usize; // 43 ticks
        let total = TRUE_TPS * BPT_TRUE * (gen_ms as f64 / 1000.0); // 5.85MB
        let t0 = 1_000_000i64;
        // The disk counter barely moves during the call; dirty pages flush in a burst of 4 ticks ~7~9.8s after completed
        let mut deltas = vec![0.0; n];
        deltas.extend(std::iter::repeat(0.0).take(10)); // silent ~7s after completion
        let chunk = total / 4.0;
        deltas.extend(std::iter::repeat(chunk).take(4));
        let samples = series(t0, &deltas);
        // mac does no files deduction (tracked_files_total is constant 0)
        let files = samples.iter().map(|(t, _)| (*t, 0u64)).collect::<Vec<_>>();
        let rows = merge_streams(&[&build_rows(&samples, 0.0, &mac_params())], &files);
        let call_end = t0 + (n as i64) * TICK;
        let stream_start = call_end - gen_ms;
        let eff = (TRUE_TPS * (gen_ms as f64 / 1000.0)) as u64; // 1500 tok

        // Windows-style [.., completed]: bytes not yet flushed, nearly all lost
        let (no_grace, _) = integrate(&rows, stream_start, call_end);
        assert!(
            no_grace < total * 0.5,
            "the no-grace window should not see most of the bytes: {no_grace}"
        );
        // Grace window [.., completed+15s]: all lagging flush bytes counted, sample recovers ground truth
        let (clean, _) = integrate(&rows, stream_start, call_end + mac_params().cal_grace_ms);
        let bpt = clean / eff as f64;
        assert!(
            (bpt - BPT_TRUE).abs() / BPT_TRUE < 0.05,
            "grace-window sample {bpt:.0} should be near ground truth {BPT_TRUE:.0}"
        );
    }

    /// Synthetic end-to-end: drive cleaning/integration/calibration per measure()'s measurement methodology,
    /// asserting "post-calibration display ≈ ground truth".
    ///
    /// Scenario aligned with the measured field: 30s call, ground truth 50 t/s, UI pipe 600 B/token,
    /// flush mirroring 50% of streaming bytes, heartbeat noise floor, poisoned adaptive floor, first-tick request burst.
    #[test]
    fn synthetic_call_converges_to_true_tps() {
        const TRUE_TPS: f64 = 50.0;
        const BPT_TRUE: f64 = 600.0;
        const FLUSH_RATIO: f64 = 0.5; // mirrors half the streaming bytes to disk
        const NOISE: f64 = 1_500.0; // heartbeat/log noise floor (merged into write bytes)

        let gen_ms = 30_000i64;
        let n = (gen_ms / TICK) as usize; // 43 ticks
        let stream_per_tick = TRUE_TPS * BPT_TRUE * (TICK as f64 / 1000.0); // 21_000 B
        let mut deltas = Vec::with_capacity(n + 1);
        deltas.push(190_000.0); // request-body upload (first-tick burst, should be dropped whole)
        for _ in 0..n {
            deltas.push(stream_per_tick + NOISE);
        }
        let t0 = 1_000_000i64;
        let samples = series(t0, &deltas);
        // File growth: visible in the same tick as the writes
        let mut files = vec![(samples[0].0, 0u64)];
        for (i, d) in deltas.iter().enumerate() {
            let prev = files[i].1;
            files.push((samples[i + 1].0, prev + (*d * FLUSH_RATIO) as u64));
        }

        let rows = merge_streams(
            &[&build_rows(&samples, 40_000.0 /* poisoned noise floor */, &CleanParams::windows())],
            &files,
        );
        let call_end = t0 + (deltas.len() as i64) * TICK;
        let stream_start = call_end - gen_ms;

        // Consistency calibration: numerator = the same cleaned stream integrated over the call span
        let (clean, cov_s) = integrate(&rows, stream_start, call_end);
        let eff = (TRUE_TPS * (gen_ms as f64 / 1000.0)) as u64; // 1500 tok
        let bpt = (clean / eff as f64).clamp(CAL_MIN, CAL_MAX);
        assert!(
            cov_s > (gen_ms as f64 / 1000.0) * 0.9,
            "the integral should cover the call span: {cov_s}"
        );

        // Steady-state display: full 30s sliding window
        let (wb, ws) = integrate(&rows, call_end - WINDOW_MS, call_end);
        let pipe_bps = (wb / ws).max(0.0);
        let shown = pipe_bps / bpt;
        assert!(
            (shown - TRUE_TPS).abs() / TRUE_TPS < 0.05,
            "post-calibration display {shown:.1} should be near ground truth {TRUE_TPS}"
        );
    }

    // ============ Benchmarks: new vs old coefficient estimator (synthetic series, deterministic seeds) ============
    // `cargo test --release -p zcode-speed-panel --lib -- benchmark --nocapture`
    // prints the error table. For absolute numbers trust the real-data replay (scripts/cal_bench.py, 249
    // real calibration samples on this machine: old 5-sample median error 24.3% → new estimator 20.8%);
    // the assertions here only lock relative relations (new ≤ old / convergence not degraded / outlier resistant), with fixed reproducible seeds

    /// Deterministic pseudo-random (LCG), keeping tests reproducible
    struct Lcg(u64);
    impl Lcg {
        fn next_f64(&mut self) -> f64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 11) as f64 / (1u64 << 53) as f64
        }
        /// Approximate standard normal (sum of 3 uniforms, standardized)
        fn next_normal(&mut self) -> f64 {
            (self.next_f64() + self.next_f64() + self.next_f64() - 1.5) * 2.0
        }
    }

    /// Old estimator replay: plain median of the last 5 samples (historical methodology including the prior placeholder)
    fn replay_old(samples: &[f64]) -> Vec<f64> {
        let mut errs = Vec::new();
        for i in 1..samples.len() {
            let mut q: Vec<f64> = samples[..i].to_vec();
            if q.len() > 5 {
                q = q[q.len() - 5..].to_vec();
            }
            q.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let f = q[q.len() / 2];
            errs.push((f - samples[i]).abs() / samples[i]);
        }
        errs
    }

    /// New estimator replay: cal_estimate (prior 600)
    fn replay_new(samples: &[f64]) -> Vec<f64> {
        let mut errs = Vec::new();
        for i in 1..samples.len() {
            let f = cal_estimate(&samples[..i], DEFAULT_BPT);
            errs.push((f - samples[i]).abs() / samples[i]);
        }
        errs
    }

    fn summarize(name: &str, errs: &mut [f64]) {
        errs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = errs[errs.len() / 2];
        let p75 = errs[errs.len() * 3 / 4];
        let mean = errs.iter().sum::<f64>() / errs.len() as f64;
        println!("{name:<26} med={:>5.1}%  p75={:>5.1}%  mean={:>5.1}%", med * 100.0, p75 * 100.0, mean * 100.0);
    }

    /// Steady-state noise + short-range correlation: the sample distribution aligns with real logs — lognormal margin σ≈0.39
    /// (reproduces p25~p75 = 478~773 / median 563) plus lag-1 autocorrelation ρ=0.5 (measured
    /// 0.50, 0.54 within 10 minutes: consecutive calls share content style). Recency weighting wins precisely through this
    /// correlation; assert the new estimator's error is never above the old
    #[test]
    fn benchmark_steady_state_new_beats_old() {
        let mut rng = Lcg(0x5EED_2026_0924);
        let base = 560.0f64;
        let (rho, sigma) = (0.5f64, 0.39f64);
        let innov = sigma * (1.0 - rho * rho).sqrt();
        let mut z = 0.0f64;
        let samples: Vec<f64> = (0..200)
            .map(|_| {
                z = rho * z + innov * rng.next_normal();
                base * z.exp()
            })
            .collect();
        let mut old = replay_old(&samples);
        let mut new = replay_new(&samples);
        summarize("steady-state old median-5", &mut old);
        summarize("steady-state new cal_estimate", &mut new);
        old.sort_by(|a, b| a.partial_cmp(b).unwrap());
        new.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let (om, on) = (old[old.len() / 2], new[new.len() / 2]);
        assert!(
            on <= om + 1e-9,
            "new estimator median error {on:.3} should be ≤ old {om:.3}"
        );
    }

    /// Magnitude shift (model/tokenizer change: 560 → 200): the new estimator converges no worse than the old 5-window
    /// (guaranteed by recency weighting), with 2 samples of slack
    #[test]
    fn benchmark_regime_shift_convergence() {
        let mut rng = Lcg(0xA11CE);
        let mut samples: Vec<f64> = (0..60)
            .map(|_| 560.0 * (0.2 * rng.next_normal()).exp())
            .collect();
        samples.extend((0..60).map(|_| 200.0 * (0.2 * rng.next_normal()).exp()));
        let conv = |errs: &[f64]| -> usize {
            // After the shift point (60), the first position with 3 consecutive ticks of error <15%
            for (k, w) in errs[60..].windows(3).enumerate() {
                if w.iter().all(|e| *e < 0.15) {
                    return 60 + k;
                }
            }
            usize::MAX
        };
        let (old, new) = (replay_old(&samples), replay_new(&samples));
        let (co, cn) = (conv(&old), conv(&new));
        println!("shift convergence: old={} ticks new={} ticks (the shift is sample 60)", co.saturating_sub(60), cn.saturating_sub(60));
        assert!(cn != usize::MAX && co != usize::MAX, "both should converge");
        assert!(cn <= co + 2, "new estimator convergence should not be notably slower than old: new{cn} old{co}");
    }

    /// Outlier pollution: every 10 samples mixes in one ×0.45 garbage sample (the half-sample form admission can pass).
    /// Trimming guarantees the weighted median anchor is not dragged (med error no worse than the old
    /// 5-window plain median); AR1 shrinkage's one-tick pull on garbage samples is capped by CLAMP^ρ and
    /// good samples pull back same tick — assert the coefficient never collapses toward the garbage magnitude (bounded-drag invariant).
    /// Note that under sustained pollution the coefficient's center sits ~10% lower than the non-shrinking version (the cost of the clamped pull);
    /// adopted only because the real-replay net gain is positive (see the scripts/cal_bench.py table)
    #[test]
    fn benchmark_outlier_resistance() {
        let mut rng = Lcg(0xBEEF);
        let mut samples: Vec<f64> = (0..200)
            .map(|i| {
                let s = 560.0 * (0.15 * rng.next_normal()).exp();
                if i % 10 == 7 { s * 0.45 } else { s }
            })
            .collect();
        samples[0] = 560.0; // pin the first sample, avoiding pure random jitter
        let mut coeffs = Vec::new();
        for i in 1..samples.len() {
            coeffs.push(cal_estimate(&samples[..i], DEFAULT_BPT));
        }
        let mut new: Vec<f64> = coeffs
            .iter()
            .zip(samples[1..].iter())
            .map(|(f, s)| (f - *s).abs() / s)
            .collect();
        let mut old = replay_old(&samples);
        summarize("outlier old median-5", &mut old);
        summarize("outlier new cal_estimate", &mut new);
        let om = old[old.len() / 2];
        let nm = new[new.len() / 2];
        // med allows a bounded regression of ≤2pp: the known cost of AR1's clamped pull in a pure-pollution scenario
        // (measured ~1pp); the real-data net gain is positive (see the cal_bench table). A regression notably
        // past the bound means the drag is out of control
        assert!(nm <= om + 0.02, "outlier-scenario med regression past the bound: {nm:.3} vs {om:.3}");
        // Bounded drag: the garbage magnitude is ≈252 (×0.45); the coefficient fails if dragged near it;
        // under the clamp cap it must stay within [0.6, 1.4]×560 throughout
        for (k, c) in coeffs.iter().enumerate() {
            assert!(
                (0.6..=1.4).contains(&(c / 560.0)),
                "tick {k} coefficient {c:.0} collapsed out of the bounded range — AR1 drag out of control"
            );
        }
    }


    /// Disk flush delayed into one large block (worst misalignment case): positive and negative ticks cancel in the interval sum,
    /// and consistency calibration still converges to ground truth
    #[test]
    fn delayed_flush_still_converges() {
        const TRUE_TPS: f64 = 50.0;
        const BPT_TRUE: f64 = 600.0;
        let gen_ms = 30_000i64;
        let n = (gen_ms / TICK) as usize;
        let stream_per_tick = TRUE_TPS * BPT_TRUE * (TICK as f64 / 1000.0);
        let deltas: Vec<f64> = std::iter::once(190_000.0)
            .chain(std::iter::repeat(stream_per_tick).take(n))
            .collect();
        let t0 = 1_000_000i64;
        let samples = series(t0, &deltas);
        // All flushed bytes become visible at once on the last tick (a single large flush)
        let total_flush: u64 = (deltas.iter().sum::<f64>() * 0.5) as u64;
        let mut files = Vec::with_capacity(deltas.len() + 1);
        for (i, (t, _)) in samples.iter().enumerate() {
            let v = if i == samples.len() - 1 { total_flush } else { 0 };
            files.push((*t, v));
        }
        let rows = merge_streams(&[&build_rows(&samples, 0.0, &CleanParams::windows())], &files);
        let call_end = t0 + (deltas.len() as i64) * TICK;
        let (clean, _) = integrate(&rows, call_end - gen_ms, call_end);
        let eff = (TRUE_TPS * (gen_ms as f64 / 1000.0)) as u64;
        let bpt = (clean / eff as f64).clamp(CAL_MIN, CAL_MAX);
        let (wb, ws) = integrate(&rows, call_end - WINDOW_MS, call_end);
        let shown = ((wb / ws).max(0.0)) / bpt;
        assert!(
            (shown - TRUE_TPS).abs() / TRUE_TPS < 0.05,
            "delayed-flush scenario display {shown:.1} should be near ground truth {TRUE_TPS}"
        );
    }
}
