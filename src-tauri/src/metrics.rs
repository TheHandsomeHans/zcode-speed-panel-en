use chrono::{Datelike, Days, Local, NaiveTime, TimeZone, Utc};
use rusqlite::OpenFlags;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// One completed model call (from the ZCode usage database model_usage table)
#[derive(Clone, Debug)]
pub struct Call {
    /// model_usage primary key
    #[allow(dead_code)]
    pub id: String,
    pub started_ms: i64,
    #[allow(dead_code)]
    pub first_token_ms: Option<i64>,
    pub completed_ms: i64,
    /// Pure generation time: completed_at - first_token_at (falls back to duration_ms when missing)
    pub gen_ms: i64,
    pub output: u64,
    pub reasoning: u64,
    pub input: u64,
    pub cache_creation: u64,
    pub cache_read: u64,
    pub session: String,
}

impl Call {
    /// Rate numerator: output tokens + reasoning tokens (reasoning content is streamed output too)
    pub fn effective_out(&self) -> u64 {
        self.output + self.reasoning
    }
}

/// Per-task live details (multiple only with concurrent tasks): one CLI process = one task row.
/// Subagents running in parallel within one process cannot be split at the byte layer; shown truthfully as that process's total
#[derive(Serialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskStat {
    pub pid: u32,
    /// Owning in-progress session id (empty = streaming process not yet attributed)
    pub session: String,
    /// Number of in-progress sessions this process carries (≥2 = multiple tasks in one process; speed is the total)
    pub n_sessions: u32,
    pub tps: f64,
    /// Whether this process is currently streaming (probe-window rate above threshold)
    pub streaming: bool,
}

/// ZCode connection detail row (filled by netio): one ESTABLISHED connection + its owning process
#[derive(Serialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnStat {
    /// Remote "ip:port" ("[addr]:port" for v6)
    pub remote: String,
    /// Owning process pid
    pub pid: u32,
    /// Process type label ("CLI session process" / "main process" / "renderer process" / "GPU process" /
    /// "utility process" / "crash reporter process") — both groups are ZCode's own processes, distinguished by role
    pub proc: String,
}

/// Metrics snapshot pushed to the frontend
#[derive(Serialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    pub current_tps: f64,
    pub avg_tps: f64,
    pub total_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
    pub input_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cache_read_tokens: u64,
    pub calls_today: u64,
    pub sessions_today: u64,
    pub is_live: bool,
    /// Call not yet persisted but inferred still generating from call intervals; speed is the window fallback value
    pub is_estimating: bool,
    /// Streaming measured as started but the 30s sliding window is not yet full (readings come from the active interval; the frontend shows "measuring")
    pub ramping: bool,
    /// Call started but no first byte output yet (TTFT/pipeline saw no increments): show a "measuring…" hint
    /// instead of a misleading estimate; the frontend gauge/desktop pet displays …
    pub is_starting: bool,
    /// Real speed of completed calls over the last 10 minutes (persisted-data methodology, same source as the speed chart).
    /// During parts of a call the UI pipeline has no incremental bytes (IO measurement unavailable); used as fallback display
    pub window_tps: f64,
    /// Real speed of the most recent completed call (persisted-data methodology: (output+reasoning) ÷ pure generation time).
    /// 0 when no call completed today; the small gauge at the top-right of the current speed card uses it for "last round"
    pub last_call_tps: f64,
    /// Highest single-call speed in the last 7 days (t/s). Admission criteria in HistoryStats (valid first_token,
    /// pure generation ≥1s, effective output ≥300 tokens — tiny calls have noisy millisecond timestamps and are excluded).
    /// 0 when no qualifying call is inside the window; shown by the "max" small gauge at the bottom-right of the current speed card
    pub hist_max_tps: f64,
    /// Average speed over the last 7 days: completed calls in the window Σeff ÷ Σgen_s (same methodology as today's average,
    /// unfiltered; sliding expiry by local day). Shown by the "history" small gauge at the top-right of the today-average card
    pub hist_avg_tps: f64,
    /// Source of the current speed: "io" = measured from process streams / "window" = window fallback / "idle" = standby
    pub live_source: String,
    pub last_activity_ms: i64,
    pub now_ms: i64,
    pub rollout_dir: String,
    pub spark: Vec<f64>,
    /// Per-process details of concurrent tasks (filled by the live pipeline; the frontend shows a task list when ≥2)
    pub tasks: Vec<TaskStat>,
    // ---- Network traffic monitoring (filled by netio.rs; methodology in that module's comments) ----
    /// Whether system-wide interface counters are available (false on stub platforms; the frontend hides the network card)
    pub net_available: bool,
    /// System-wide live upload/download speed (B/s, measured by interface counters over a ~1s sliding window)
    pub net_up_bps: f64,
    pub net_down_bps: f64,
    /// System-wide upload/download totals for today (real values, persisted and continued across restarts)
    pub net_up_today: u64,
    pub net_down_today: u64,
    /// Session traffic estimate (≈, tokens × factor): today's upload (request bodies) / download (streamed responses)
    pub net_sess_up_today: u64,
    pub net_sess_down_today: u64,
    /// Whether connection attribution is available (Windows only)
    pub net_conns_available: bool,
    /// Connection counts and details for the session group (CLI processes) / desktop-app group (other zcode.exe)
    /// (each entry has the remote endpoint + owning pid + process type label; both groups are ZCode's own processes)
    pub net_cli_conns: u32,
    pub net_app_conns: u32,
    pub net_cli_conn_list: Vec<ConnStat>,
    pub net_app_conn_list: Vec<ConnStat>,
}

/// Current-speed statistics window
const LIVE_WINDOW_MS: i64 = 10 * 60 * 1000;
/// If more than this long since the last call completed, treat as standby and zero the current speed
/// Estimate window: infer "still generating" from the median of today's call intervals; beyond it, standby
const ESTIMATE_MIN_MS: i64 = 20 * 1000;
const ESTIMATE_MAX_MS: i64 = 240 * 1000;
const ESTIMATE_DEFAULT_MS: i64 = 60 * 1000;
/// Lower bound for very short generation times, avoiding division by zero / extreme spikes
const MIN_DUR_MS: i64 = 50;
/// Speed chart: 15 minutes, 10 seconds per bucket (wall-clock aligned so the frontend can scroll smoothly)
const SPARK_BUCKETS: usize = 90;
const SPARK_BUCKET_MS: i64 = 10_000;

pub fn home_dir() -> Option<PathBuf> {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
}

pub fn usage_db_path() -> Option<PathBuf> {
    home_dir().map(|h| h.join(".zcode").join("cli").join("db").join("db.sqlite"))
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn local_midnight_utc_ms() -> i64 {
    local_day_start_ms(Utc::now().timestamp_millis())
}

/// Local-day midnight (UTC ms) for a timestamp. DST-ambiguous/nonexistent times fall back to the UTC day boundary
fn local_day_start_ms(ts_ms: i64) -> i64 {
    let fallback = || ts_ms - ts_ms.rem_euclid(86_400_000);
    let Some(dt) = Local.timestamp_millis_opt(ts_ms).single() else {
        return fallback();
    };
    let tz = dt.timezone();
    dt.date_naive()
        .and_time(NaiveTime::MIN)
        .and_local_timezone(tz)
        .single()
        .map(|d| d.with_timezone(&Utc).timestamp_millis())
        .unwrap_or_else(fallback)
}

/// History window start (inclusive): midnight of HIST_WINDOW_DAYS-1 local days before today.
/// Uses calendar-day subtraction (Days::new) rather than a millisecond difference — DST would shift a millisecond difference into the adjacent day
fn hist_window_cutoff(today_start_ms: i64) -> i64 {
    let fallback = || today_start_ms - (HIST_WINDOW_DAYS - 1) * 86_400_000;
    let Some(today) = Local.timestamp_millis_opt(today_start_ms).single() else {
        return fallback();
    };
    let tz = today.timezone();
    let Some(day) = today.date_naive().checked_sub_days(Days::new((HIST_WINDOW_DAYS - 1) as u64))
    else {
        return fallback();
    };
    day.and_time(NaiveTime::MIN)
        .and_local_timezone(tz)
        .single()
        .map(|d| d.with_timezone(&Utc).timestamp_millis())
        .unwrap_or_else(fallback)
}

/// Today's aggregator: holds all of today's calls and computes all metrics (pure functions, easy to test)
pub struct Aggregator {
    pub calls: Vec<Call>,
    pub today_ymd: (i32, u32, u32),
}

/// Historical stats (the last HIST_WINDOW_DAYS local calendar days, including today): at startup a
/// baseline scan covers rows before today inside the window; afterwards each poll accumulates
/// incrementally with sliding expiry by local day —
/// once midnight is crossed, the oldest whole-day bucket is dropped. The historical average uses the
/// same methodology as today's average (unfiltered);
/// the historical max has an admission threshold — tiny calls like 24ms/79 tokens observed in the real
/// database produce fake records of thousands of t/s from millisecond-level timestamp
/// noise (same reason calibration samples exclude calls <300 tokens).
/// Bucketing by day rather than a plain accumulator lets old calls be withdrawn from Σ and the peak on expiry
#[derive(Clone, Debug, Default)]
pub struct HistoryStats {
    /// One bucket per day (ascending day_start; baseline ordered by completed_at ASC, incremental appends only new days),
    /// keeping only days inside the window. Aggregation/eviction is independent of bucket order
    pub days: Vec<HistDay>,
}

/// Single-day aggregation bucket: peak/average accumulate per day; expired buckets are dropped whole
#[derive(Clone, Copy, Debug, Default)]
pub struct HistDay {
    /// Local-day midnight (UTC ms)
    pub day_start: i64,
    /// Σeff of completed calls that day (historical average numerator)
    pub total_eff: u64,
    /// Σgen_ms for the day (historical average denominator; missing first_token falls back to duration then max(50))
    pub total_gen_ms: i64,
    /// Highest single-call speed that day (t/s): only calls with valid first_token, gen≥1s and eff≥300 count
    pub max_tps: f64,
}

/// History window: the last 7 local calendar days (including today), sliding expiry at each day's midnight
pub const HIST_WINDOW_DAYS: i64 = 7;

/// Historical max admission: pure generation time lower bound (ms) — short calls have noisy timestamps
const HIST_MAX_MIN_GEN_MS: i64 = 1000;
/// Historical max admission: effective output token lower bound (same value as calibration sample admission)
const HIST_MAX_MIN_EFF: u64 = 300;

impl HistoryStats {
    /// Expiry cleanup: drop whole-day buckets before (and including) the window start (called every tick; bucket count ≤ window days)
    pub fn prune(&mut self, cutoff_day_start: i64) {
        self.days.retain(|b| b.day_start >= cutoff_day_start);
    }

    /// Fold one completed call into its local-day bucket (shared by the baseline scan and per-tick increments).
    /// gen_ms is the final poll-methodology value (completed-ft when ft is valid, otherwise duration fallback then max(50))
    pub fn fold(&mut self, day_start: i64, ft: Option<i64>, completed_ms: i64, gen_ms: i64, eff: u64) {
        let idx = match self.days.iter().position(|b| b.day_start == day_start) {
            Some(i) => i,
            None => {
                self.days.push(HistDay { day_start, ..Default::default() });
                self.days.len() - 1
            }
        };
        let b = &mut self.days[idx];
        b.total_eff += eff;
        b.total_gen_ms += gen_ms.max(MIN_DUR_MS);
        // Peak record admission: ft must be genuinely valid (duration-fallback rows are untrustworthy) + both lower bounds
        let real_ft = matches!(ft, Some(f) if completed_ms > f);
        if real_ft && gen_ms >= HIST_MAX_MIN_GEN_MS && eff >= HIST_MAX_MIN_EFF {
            let tps = eff as f64 * 1000.0 / gen_ms as f64;
            if tps > b.max_tps {
                b.max_tps = tps;
            }
        }
    }

    pub fn total_eff(&self) -> u64 {
        self.days.iter().map(|b| b.total_eff).sum()
    }

    pub fn total_gen_ms(&self) -> i64 {
        self.days.iter().map(|b| b.total_gen_ms).sum()
    }

    pub fn avg_tps(&self) -> f64 {
        let gen = self.total_gen_ms();
        if gen > 0 {
            self.total_eff() as f64 / (gen as f64 / 1000.0)
        } else {
            0.0
        }
    }

    pub fn max_tps(&self) -> f64 {
        self.days.iter().map(|b| b.max_tps).fold(0.0, f64::max)
    }
}

/// Poll-methodology pure generation time: completed-ft when ft is valid, otherwise duration_ms (only if >0),
/// finally max(50) as a floor. Shared by the baseline scan and incremental ingestion so both paths use one methodology
fn gen_ms_from(ft: Option<i64>, completed: i64, dur: Option<i64>) -> i64 {
    match ft {
        Some(f) if completed > f => completed - f,
        _ => dur.filter(|d| *d > 0).unwrap_or(MIN_DUR_MS),
    }
    .max(MIN_DUR_MS)
}


impl Aggregator {
    pub fn new() -> Self {
        Self {
            calls: Vec::new(),
            today_ymd: {
                let n = Local::now();
                (n.year(), n.month(), n.day())
            },
        }
    }

    pub fn ingest(&mut self, call: Call) {
        self.calls.push(call);
    }

    /// Day rollover: clear today's accumulations
    pub fn rollover_if_needed(&mut self) {
        let n = Local::now();
        let ymd = (n.year(), n.month(), n.day());
        if ymd != self.today_ymd {
            self.today_ymd = ymd;
            self.calls.clear();
        }
    }

    pub fn calls(&self) -> &[Call] {
        &self.calls
    }

    pub fn snapshot(&self) -> Snapshot {
        let now = now_ms();
        let mut out_total = 0u64;
        let mut reason_total = 0u64;
        let mut input_total = 0u64;
        let mut cc_total = 0u64;
        let mut cr_total = 0u64;
        let mut dur_total = 0i64;
        let mut w_out = 0u64;
        let mut w_dur = 0i64;
        let mut last_completed = 0i64;
        // Numerator/denominator of the most recent completed call, for the "last call speed" badge
        let mut last_eff = 0u64;
        let mut last_gen = 0i64;
        let mut sessions: HashSet<&str> = HashSet::new();
        // Buckets align to 10s wall-clock boundaries: bucket index = the completion time's slot minus the current slot
        let now_slot = now.div_euclid(SPARK_BUCKET_MS);
        let mut buckets = vec![(0u64, 0i64); SPARK_BUCKETS];

        for c in &self.calls {
            out_total += c.output;
            reason_total += c.reasoning;
            input_total += c.input;
            cc_total += c.cache_creation;
            cr_total += c.cache_read;
            dur_total += c.gen_ms.max(MIN_DUR_MS);
            if !c.session.is_empty() {
                sessions.insert(c.session.as_str());
            }
            if c.completed_ms >= last_completed {
                last_completed = c.completed_ms;
                last_eff = c.effective_out();
                last_gen = c.gen_ms.max(MIN_DUR_MS);
            }
            if c.completed_ms >= now - LIVE_WINDOW_MS {
                w_out += c.effective_out();
                w_dur += c.gen_ms.max(MIN_DUR_MS);
            }
            let slot = (now_slot - c.completed_ms.div_euclid(SPARK_BUCKET_MS)) as usize;
            if slot < SPARK_BUCKETS {
                let b = &mut buckets[SPARK_BUCKETS - 1 - slot];
                b.0 += c.effective_out();
                b.1 += c.gen_ms.max(MIN_DUR_MS);
            }
        }

        // Estimate window: median of intervals between adjacent completions today (clamped to 20s~240s),
        // used to keep showing the fallback at the recent speed during long reasoning/long output (call not yet persisted)
        let mut comps: Vec<i64> = self.calls.iter().map(|c| c.completed_ms).collect();
        comps.sort_unstable();
        comps.dedup();
        let mut gaps: Vec<i64> = comps
            .windows(2)
            .map(|w| w[1] - w[0])
            .filter(|g| *g > 0 && *g < 600_000)
            .collect();
        let grace_ms = if gaps.len() >= 3 {
            let start = gaps.len().saturating_sub(10);
            let tail = &mut gaps[start..];
            tail.sort_unstable();
            (tail[tail.len() / 2]).clamp(ESTIMATE_MIN_MS, ESTIMATE_MAX_MS)
        } else {
            ESTIMATE_DEFAULT_MS
        };

        let since = now - last_completed;
        // is_live is decided solely by live IO measurement (overridden in main); here the window fallback is based on call intervals
        let is_estimating = last_completed > 0 && since <= grace_ms && w_dur > 0;
        let current_tps = if is_estimating && w_dur > 0 {
            w_out as f64 / (w_dur as f64 / 1000.0)
        } else {
            0.0
        };
        let avg_tps = if dur_total > 0 {
            (out_total + reason_total) as f64 / (dur_total as f64 / 1000.0)
        } else {
            0.0
        };
        let mut spark: Vec<f64> = buckets
            .iter()
            .map(|(o, d)| {
                if *d > 0 {
                    *o as f64 / (*d as f64 / 1000.0)
                } else {
                    0.0
                }
            })
            .collect();
        // While estimating, temporarily fill the rightmost bucket (the not-yet-persisted call) with the fallback value; real data replaces it on completion
        if is_estimating {
            if let Some(last) = spark.last_mut() {
                if *last <= 0.0 {
                    *last = current_tps;
                }
            }
        }
        let window_tps = if w_dur > 0 {
            w_out as f64 / (w_dur as f64 / 1000.0)
        } else {
            0.0
        };
        let last_call_tps = if last_gen > 0 {
            last_eff as f64 / (last_gen as f64 / 1000.0)
        } else {
            0.0
        };

        // Totals follow ZCode's official statistics methodology: input + output + reasoning + cache_creation;
        // cache hits (cache_read) are prompt reuse, not new usage — displayed separately, not counted
        Snapshot {
            current_tps,
            avg_tps,
            total_tokens: out_total + reason_total + input_total + cc_total,
            output_tokens: out_total,
            reasoning_tokens: reason_total,
            input_tokens: input_total,
            cache_creation_tokens: cc_total,
            cache_read_tokens: cr_total,
            calls_today: self.calls.len() as u64,
            sessions_today: sessions.len() as u64,
            is_live: false,
            is_estimating,
            ramping: false,
            is_starting: false,
            window_tps,
            last_call_tps,
            hist_max_tps: 0.0,
            hist_avg_tps: 0.0,
            live_source: if is_estimating {
                "window".to_string()
            } else {
                "idle".to_string()
            },
            last_activity_ms: last_completed,
            now_ms: now,
            rollout_dir: String::new(),
            spark,
            tasks: Vec::new(),
            net_available: false,
            net_up_bps: 0.0,
            net_down_bps: 0.0,
            net_up_today: 0,
            net_down_today: 0,
            net_sess_up_today: 0,
            net_sess_down_today: 0,
            net_conns_available: false,
            net_cli_conns: 0,
            net_app_conns: 0,
            net_cli_conn_list: Vec::new(),
            net_app_conn_list: Vec::new(),
        }
    }
}

/// Polling engine for the ZCode usage database (read-only WAL)
pub struct Engine {
    conn: Option<rusqlite::Connection>,
    agg: Aggregator,
    ingested: HashSet<String>,
    /// Historical stats (last 7 local calendar days): the first poll does a baseline scan of rows before
    /// today inside the window; afterwards each tick's new calls accumulate incrementally with sliding expiry
    hist: HistoryStats,
    hist_loaded: bool,
    pub db_path: Option<PathBuf>,
}

impl Engine {
    pub fn new() -> Self {
        let db_path = usage_db_path();
        let conn = db_path.as_ref().and_then(|p| {
            match rusqlite::Connection::open_with_flags(
                p,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            ) {
                Ok(c) => {
                    // Key optimizations for WAL concurrency and read performance:
                    // 1. Set busy_timeout to 3 seconds to avoid immediate SQLITE_BUSY while the ZCode CLI writes transactions or checkpoints
                    // 2. Enable query_only to guarantee read-only access
                    let _ = c.busy_timeout(std::time::Duration::from_millis(3000));
                    let _ = c.execute_batch("PRAGMA query_only = ON;");
                    Some(c)
                }
                Err(e) => {
                    eprintln!("[zcode-speed-panel] usage DB open failed: {e}");
                    None
                }
            }
        });
        Self {
            conn,
            agg: Aggregator::new(),
            ingested: HashSet::new(),
            hist: HistoryStats::default(),
            hist_loaded: false,
            db_path,
        }
    }

    pub fn data_source_label(&self) -> String {
        self.db_path
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| "(~/.zcode/cli/db/db.sqlite not found)".into())
    }

    /// Poll the usage database, incrementally ingesting calls completed today. Returns this round's new calls (for the live IO module's calibration).
    pub fn poll(&mut self) -> Vec<Call> {
        self.agg.rollover_if_needed();
        let today_start_ms = local_midnight_utc_ms();
        // Reset the ingested set on day rollover
        if self.agg.calls.is_empty() && !self.ingested.is_empty() {
            self.ingested.clear();
        }
        let Some(conn) = &self.conn else {
            return Vec::new();
        };
        // Historical baseline: the first poll scans completed rows before today inside the window (a single local SQLite
        // read, milliseconds; today's rows are accumulated by the incremental path below and not double-counted). conn and hist
        // are borrowed as separate fields to avoid a mutable/immutable borrow conflict across all of self
        if !self.hist_loaded {
            self.hist_loaded = true;
            Self::scan_history_before(conn, &mut self.hist, today_start_ms);
        }
        // Sliding expiry: drop the oldest whole-day buckets once midnight is crossed (every tick; bucket count ≤ window days)
        self.hist.prune(hist_window_cutoff(today_start_ms));
        let mut new_calls = Vec::new();
        let sql = concat!(
            "SELECT id, started_at, first_token_at, completed_at, duration_ms, ",
            "output_tokens, reasoning_tokens, input_tokens, ",
            "cache_creation_input_tokens, cache_read_input_tokens, session_id ",
            "FROM model_usage WHERE status='completed' AND completed_at >= ?1 ",
            "ORDER BY completed_at ASC"
        );
        let mut stmt = match conn.prepare_cached(sql) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[zcode-speed-panel] usage DB prepare failed: {e}");
                return Vec::new();
            }
        };
        let rows = stmt
            .query_map([today_start_ms], |r| {
            let id: String = r.get(0)?;
            let started: i64 = r.get(1)?;
            let ft: Option<i64> = r.get(2)?;
            let completed: i64 = r.get(3)?;
            let dur: Option<i64> = r.get(4)?;
            // rusqlite cannot read u64 columns; read as i64 and convert
            let out: i64 = r.get::<_, Option<i64>>(5)?.unwrap_or(0);
            let reason: i64 = r.get::<_, Option<i64>>(6)?.unwrap_or(0);
            let input: i64 = r.get::<_, Option<i64>>(7)?.unwrap_or(0);
            let cc: i64 = r.get::<_, Option<i64>>(8)?.unwrap_or(0);
            let cr: i64 = r.get::<_, Option<i64>>(9)?.unwrap_or(0);
            let session: String = r.get(10)?;
            Ok((
                id,
                started,
                ft,
                completed,
                dur,
                out.max(0) as u64,
                reason.max(0) as u64,
                input.max(0) as u64,
                cc.max(0) as u64,
                cr.max(0) as u64,
                session,
            ))
        });
        let rows = match rows {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[zcode-speed-panel] usage DB query failed: {e}");
                return Vec::new();
            }
        };
        for row in rows.flatten() {
            let (id, started, ft, completed, dur, out, reason, input, cc, cr, session) = row;
            if self.ingested.contains(&id) {
                continue;
            }
            self.ingested.insert(id.clone());
            let gen_ms = gen_ms_from(ft, completed, dur);
            // Incrementally accumulate historical stats (today's calls are always inside the window; the peak follows
            // HistoryStats::fold's admission criteria)
            let eff = out + reason;
            let day = local_day_start_ms(completed);
            self.hist.fold(day, ft, completed, gen_ms, eff);
            self.agg.ingest(Call {
                id,
                started_ms: started,
                first_token_ms: ft,
                completed_ms: completed,
                gen_ms,
                output: out,
                reasoning: reason,
                input,
                cache_creation: cc,
                cache_read: cr,
                session,
            });
            new_calls.push(self.agg.calls.last().unwrap().clone());
        }
        // Today's aggregation keeps only today's data (the ingested set resets on day rollover)
        self.agg.calls.retain(|c| c.started_ms >= today_start_ms);
        new_calls
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut s = self.agg.snapshot();
        s.rollout_dir = self.data_source_label();
        s.hist_max_tps = self.hist.max_tps();
        s.hist_avg_tps = self.hist.avg_tps();
        s
    }

    /// Historical baseline scan: fold completed rows from the window start (inclusive) up to today's midnight.
    /// Window start = midnight of HIST_WINDOW_DAYS-1 local days before today (DST-safe),
    /// exactly a local-day midnight, so completed_at >= start means "the local day is inside the window".
    /// Failures are only logged, not panicked (history badges show 0; today's incremental path proceeds normally)
    fn scan_history_before(
        conn: &rusqlite::Connection,
        hist: &mut HistoryStats,
        today_start_ms: i64,
    ) {
        let cutoff = hist_window_cutoff(today_start_ms);
        let sql = concat!(
            "SELECT first_token_at, completed_at, duration_ms, ",
            "output_tokens, reasoning_tokens FROM model_usage ",
            "WHERE status='completed' AND completed_at < ?1 AND completed_at >= ?2"
        );
        let mut query = || -> rusqlite::Result<()> {
            let mut stmt = conn.prepare_cached(sql)?;
            let rows = stmt.query_map([today_start_ms, cutoff], |r| {
                Ok((
                    r.get::<_, Option<i64>>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Option<i64>>(2)?,
                    r.get::<_, Option<i64>>(3)?.unwrap_or(0).max(0) as u64,
                    r.get::<_, Option<i64>>(4)?.unwrap_or(0).max(0) as u64,
                ))
            })?;
            for row in rows.flatten() {
                let (ft, completed, dur, out, reason) = row;
                let gen = gen_ms_from(ft, completed, dur);
                let day = local_day_start_ms(completed);
                hist.fold(day, ft, completed, gen, out + reason);
            }
            Ok(())
        };
        if let Err(e) = query() {
            eprintln!("[zcode-speed-panel] history baseline scan failed: {e}");
        }
    }

    /// All calls ingested today (lets the live module determine the current session)
    pub fn calls(&self) -> &[Call] {
        &self.agg.calls()
    }

    /// Whether a call is in progress: inspect the newest assistant message row of recently active sessions.
    /// A message row commits the instant a call starts (readable within ≤200ms); the time object in the
    /// row's data gets completed backfilled when the call ends (including cancel/error) — faster than the
    /// model_usage completion row, and covers status='cancelled'/'error' (those calls never get a
    /// completed status row; under the old methodology they stuck at "generating" until the 10-min fallback).
    /// Returns all in-progress (session, call start time) pairs, sorted by start time descending — with
    /// concurrent tasks (multi-window / subagent sessions) live speed aggregates over the process set
    /// instead of picking only the newest one; the 10-minute cap covers crash-orphaned rows.
    pub fn call_in_flight(&self) -> Vec<(String, i64)> {
        let Some(conn) = self.conn.as_ref() else {
            return Vec::new();
        };
        // Recently active sessions (the session table has ~1k rows; scanning this small table in reverse by time_updated is acceptable;
        // limit 16: multi-task aggregation must cover all in-progress sessions, so >6 concurrent subagents are not missed);
        // the message table lacks a single-column index on time_created, so no global ORDER BY (measured ~200ms per query)
        let mut stmt = match conn.prepare_cached(
            "SELECT id FROM session ORDER BY time_updated DESC LIMIT 16",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let sessions: Vec<String> = match stmt.query_map([], |r| r.get::<_, String>(0)) {
            Ok(rows) => rows.flatten().collect(),
            Err(_) => return Vec::new(),
        };
        drop(stmt);

        let mut cands: Vec<(String, i64, bool)> = Vec::new();
        for sess in &sessions {
            // Per session, look only at the newest assistant row (uses the (session_id, time_created) composite index)
            let Ok(mut stmt) = conn.prepare_cached(
                "SELECT time_created, substr(data,1,120) FROM message \
                 WHERE session_id = ?1 ORDER BY time_created DESC LIMIT 8",
            ) else {
                continue;
            };
            let Ok(rows) = stmt.query_map([sess], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            }) else {
                continue;
            };
            for (created, prefix) in rows.flatten() {
                if !prefix.contains("\"assistant\"") {
                    continue;
                }
                let done = prefix.contains("\"completed\"");
                cands.push((sess.clone(), created, done));
                break;
            }
        }
        inflight_from_rows(&cands, Utc::now().timestamp_millis())
    }

    /// Model speed trends: read-only query of completed model_usage rows inside the window, aggregated per model × time bucket.
    /// Query-and-compute only (zero local storage, no indexes/writes to the usage DB); missing conn or query failure
    /// returns an empty payload (no panic). Aggregation methodology: see aggregate_model_stats
    pub fn model_stats(&self, window_min: i64) -> ModelStatsPayload {
        let window_min = clamp_chart_window(window_min);
        let now = now_ms();
        let Some(conn) = &self.conn else {
            eprintln!("[zcode-speed-panel] model_stats: usage DB unavailable");
            return empty_model_stats(window_min, now);
        };
        // Note: the model column in model_usage is actually named model_id (verified via PRAGMA table_info; there is no model column)
        let sql = concat!(
            "SELECT model_id, first_token_at, completed_at, duration_ms, ",
            "output_tokens, reasoning_tokens FROM model_usage ",
            "WHERE status='completed' AND completed_at >= ?1 ORDER BY completed_at ASC"
        );
        let cutoff = now - window_min * 60_000;
        let query = || -> rusqlite::Result<Vec<ModelUsageRow>> {
            let mut stmt = conn.prepare_cached(sql)?;
            let rows = stmt.query_map([cutoff], |r| {
                let model: String = r.get(0)?;
                let ft: Option<i64> = r.get(1)?;
                let completed: i64 = r.get(2)?;
                let dur: Option<i64> = r.get(3)?;
                // rusqlite cannot read u64 columns; read as i64 and convert (same methodology as poll)
                let out: i64 = r.get::<_, Option<i64>>(4)?.unwrap_or(0);
                let reason: i64 = r.get::<_, Option<i64>>(5)?.unwrap_or(0);
                Ok(ModelUsageRow {
                    model,
                    first_token_at: ft,
                    completed_at: completed,
                    duration_ms: dur,
                    output_tokens: out.max(0) as u64,
                    reasoning_tokens: reason.max(0) as u64,
                })
            })?;
            Ok(rows.flatten().collect())
        };
        match query() {
            Ok(rows) => aggregate_model_stats(rows, window_min, now),
            Err(e) => {
                eprintln!("[zcode-speed-panel] model_stats query failed: {e}");
                empty_model_stats(window_min, now)
            }
        }
    }
}

/// Pure message-gate logic: candidates (session, assistant row creation time, whether completed is present).
/// Each session counts only its newest assistant row (older unfinished rows are crash residue, superseded by newer rows);
/// sessions whose newest row is unfinished and fresh are **all** treated as in progress (each counted under multi-task
/// concurrency so the live pipeline can aggregate over the process set), returned by creation time descending
pub(crate) fn inflight_from_rows(
    cands: &[(String, i64, bool)],
    now_ms: i64,
) -> Vec<(String, i64)> {
    let mut newest: HashMap<&str, &(String, i64, bool)> = HashMap::new();
    for row in cands {
        match newest.get(row.0.as_str()) {
            Some(prev) if prev.1 >= row.1 => {}
            _ => {
                newest.insert(row.0.as_str(), row);
            }
        }
    }
    let mut out: Vec<(String, i64)> = newest
        .values()
        .filter(|(_, created, done)| !done && now_ms - *created <= 600_000)
        .map(|(s, c, _)| (s.clone(), *c))
        .collect();
    out.sort_unstable_by(|a, b| b.1.cmp(&a.1));
    out
}

// ============ Model speed trends (aggregated per model × time bucket, for the chart card's model detail view, zero local storage) ============
// Time spec is fully shared with chart_stats below: the same window choices (15/60/360/1440 minutes),
// the same bucket count (CHART_BUCKETS=90) and bucket width — switching views aligns the x-axis pixel by pixel, swapping series but not ticks

/// Per-model per-bucket aggregation
#[derive(Serialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ModelBucket {
    /// Bucket tps = Σ(output+reasoning) ÷ Σ pure generation seconds; 0 with no calls
    pub tps: f64,
    pub calls: u64,
    pub tokens: u64,
}

/// One trend line per model + window totals
#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ModelSeries {
    pub model: String,
    /// Always CHART_BUCKETS (90) entries; 0 = newest bucket
    pub buckets: Vec<ModelBucket>,
    pub total_calls: u64,
    pub total_tokens: u64,
    /// Σeff ÷ Σgen_s over the whole window
    pub avg_tps: f64,
    /// Maximum of the per-bucket tps values
    pub peak_tps: f64,
    /// This model's eff as a fraction of all models' eff (0~1)
    pub share: f64,
}

/// Payload returned by the model_stats command
#[derive(Serialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct ModelStatsPayload {
    pub window_min: i64,
    pub bucket_ms: i64,
    pub now_ms: i64,
    /// Sorted by total_tokens descending
    pub series: Vec<ModelSeries>,
}

/// model_usage query row (input to the aggregation pure functions)
struct ModelUsageRow {
    model: String,
    first_token_at: Option<i64>,
    completed_at: i64,
    duration_ms: Option<i64>,
    output_tokens: u64,
    reasoning_tokens: u64,
}

/// Raw per-model per-bucket accumulators: (effective output tokens, generation ms, call count)
struct BucketAcc {
    eff: u64,
    gen_ms: i64,
    calls: u64,
}

/// Invalid window values snap to the nearest legal choice: reuse the chart's clamp_chart_window directly
/// (both views share one set of choices, see CHART_WINDOW_CHOICES)

/// Empty payload (returned when conn is missing / the query fails; no panic)
fn empty_model_stats(window_min: i64, now_ms: i64) -> ModelStatsPayload {
    ModelStatsPayload {
        window_min,
        bucket_ms: window_min * 60_000 / CHART_BUCKETS as i64,
        now_ms,
        series: Vec::new(),
    }
}

/// Pure function: aggregate completed call rows inside the window into per-model 90-bucket trends and totals (unit-testable).
/// Time spec (window choices / bucket count / bucket width) is identical to aggregate_chart_stats.
/// - Bucket index = now ÷ bucket_ms − completed ÷ bucket_ms (div_euclid, absolute wall-clock slot
///   alignment, same methodology as chart_stats / today's spark: bucket boundaries are pinned to whole
///   multiples of real time, so two queries in the same slot yield identical buckets — trends only shift, never deform),
///   0 = newest bucket, 89 = oldest bucket; out-of-range (including more than one slot into the future) rows are dropped;
/// - gen_ms = completed − first_token; when first_token is missing or non-positive, fall back to duration_ms,
///   then max(50) as a floor (same methodology as today's aggregation);
/// - eff = output + reasoning; bucket tps = Σeff ÷ Σgen_s, 0 with no calls;
/// - series sorted by total_tokens descending.
fn aggregate_model_stats(
    rows: Vec<ModelUsageRow>,
    window_min: i64,
    now_ms: i64,
) -> ModelStatsPayload {
    let window_min = clamp_chart_window(window_min);
    let bucket_ms = window_min * 60_000 / CHART_BUCKETS as i64;
    // model -> (per-bucket accumulators, total eff, total gen_ms, total call count)
    let mut per_model: HashMap<String, (Vec<BucketAcc>, u64, i64, u64)> = HashMap::new();
    for r in rows {
        let gen = match r.first_token_at {
            Some(f) if r.completed_at > f => r.completed_at - f,
            _ => r.duration_ms.filter(|d| *d > 0).unwrap_or(MIN_DUR_MS),
        }
        .max(MIN_DUR_MS);
        let eff = r.output_tokens + r.reasoning_tokens;
        let slot = now_ms.div_euclid(bucket_ms) - r.completed_at.div_euclid(bucket_ms);
        if slot < 0 || slot as usize >= CHART_BUCKETS {
            continue;
        }
        let entry = per_model.entry(r.model).or_insert_with(|| {
            (
                (0..CHART_BUCKETS)
                    .map(|_| BucketAcc { eff: 0, gen_ms: 0, calls: 0 })
                    .collect(),
                0,
                0,
                0,
            )
        });
        let b = &mut entry.0[slot as usize];
        b.eff += eff;
        b.gen_ms += gen;
        b.calls += 1;
        entry.1 += eff;
        entry.2 += gen;
        entry.3 += 1;
    }

    let grand_eff: u64 = per_model.values().map(|e| e.1).sum();
    let mut series: Vec<ModelSeries> = per_model
        .into_iter()
        .map(|(model, (buckets, total_eff, total_gen, total_calls))| {
            let mut peak = 0.0f64;
            let buckets: Vec<ModelBucket> = buckets
                .into_iter()
                .map(|b| {
                    let tps = if b.gen_ms > 0 {
                        b.eff as f64 / (b.gen_ms as f64 / 1000.0)
                    } else {
                        0.0
                    };
                    if tps > peak {
                        peak = tps;
                    }
                    ModelBucket { tps, calls: b.calls, tokens: b.eff }
                })
                .collect();
            ModelSeries {
                model,
                buckets,
                total_calls,
                total_tokens: total_eff,
                avg_tps: if total_gen > 0 {
                    total_eff as f64 / (total_gen as f64 / 1000.0)
                } else {
                    0.0
                },
                peak_tps: peak,
                share: if grand_eff > 0 {
                    total_eff as f64 / grand_eff as f64
                } else {
                    0.0
                },
            }
        })
        .collect();
    series.sort_by(|a, b| b.total_tokens.cmp(&a.total_tokens).then(a.model.cmp(&b.model)));
    ModelStatsPayload { window_min, bucket_ms, now_ms, series }
}

// ============ Output speed chart (selectable time range, for the chart card, zero local storage) ============

/// Legal chart time ranges (minutes): 15 min / 1 hour / 6 hours / 24 hours
const CHART_WINDOW_CHOICES: [i64; 4] = [15, 60, 360, 1440];
/// The chart always uses 90 buckets (same density as today's spark): 15m→10s, 1h→40s, 6h→4min, 24h→16min
const CHART_BUCKETS: usize = 90;

/// Payload returned by the chart_stats command: a single series (all models merged) of tps per time bucket
#[derive(Serialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct ChartStatsPayload {
    pub window_min: i64,
    pub bucket_ms: i64,
    pub now_ms: i64,
    /// Always 90 entries, oldest→newest (0 = oldest bucket, last = newest bucket)
    pub buckets: Vec<f64>,
}

/// Invalid window values snap to the nearest legal choice (15 / 60 / 360 / 1440 minutes)
fn clamp_chart_window(window_min: i64) -> i64 {
    CHART_WINDOW_CHOICES
        .iter()
        .copied()
        .min_by_key(|&w| (w - window_min).abs())
        .unwrap_or(15)
}

/// Pure function: aggregate completed call rows inside the window into 90-bucket tps (same methodology as today's spark:
/// gen = the poll-methodology fallback chain, bucket tps = Σeff ÷ Σgen_s). Unit-testable
fn aggregate_chart_stats(
    rows: Vec<ModelUsageRow>,
    window_min: i64,
    now_ms: i64,
) -> ChartStatsPayload {
    let window_min = clamp_chart_window(window_min);
    let bucket_ms = window_min * 60_000 / CHART_BUCKETS as i64;
    let mut acc = vec![(0u64, 0i64); CHART_BUCKETS]; // (Σeff, Σgen_ms)
    for r in rows {
        let gen = gen_ms_from(r.first_token_at, r.completed_at, r.duration_ms);
        let slot = now_ms.div_euclid(bucket_ms) - r.completed_at.div_euclid(bucket_ms);
        if slot < 0 || slot as usize >= CHART_BUCKETS {
            continue;
        }
        let b = &mut acc[CHART_BUCKETS - 1 - slot as usize];
        b.0 += r.output_tokens + r.reasoning_tokens;
        b.1 += gen;
    }
    ChartStatsPayload {
        window_min,
        bucket_ms,
        now_ms,
        buckets: acc
            .into_iter()
            .map(|(eff, gen)| {
                if gen > 0 {
                    eff as f64 / (gen as f64 / 1000.0)
                } else {
                    0.0
                }
            })
            .collect(),
    }
}

impl Engine {
    /// Output speed chart: read-only query of completed model_usage rows inside the window, aggregated into 90-bucket tps
    /// (all models merged into one series, oldest→newest). Returns an all-zero payload when conn is missing or the query fails
    pub fn chart_stats(&self, window_min: i64) -> ChartStatsPayload {
        let window_min = clamp_chart_window(window_min);
        let now = now_ms();
        let empty = ChartStatsPayload {
            window_min,
            bucket_ms: window_min * 60_000 / CHART_BUCKETS as i64,
            now_ms: now,
            buckets: vec![0.0; CHART_BUCKETS],
        };
        let Some(conn) = &self.conn else {
            return empty;
        };
        // Same query as model_stats (fetches one extra model_id column, ignored in aggregation — keeps
        // the SQL and row structure consistent so the prepare_cached statement is shared)
        let sql = concat!(
            "SELECT model_id, first_token_at, completed_at, duration_ms, ",
            "output_tokens, reasoning_tokens FROM model_usage ",
            "WHERE status='completed' AND completed_at >= ?1 ORDER BY completed_at ASC"
        );
        let cutoff = now - window_min * 60_000;
        let query = || -> rusqlite::Result<Vec<ModelUsageRow>> {
            let mut stmt = conn.prepare_cached(sql)?;
            let rows = stmt.query_map([cutoff], |r| {
                let model: String = r.get(0)?;
                let ft: Option<i64> = r.get(1)?;
                let completed: i64 = r.get(2)?;
                let dur: Option<i64> = r.get(3)?;
                let out: i64 = r.get::<_, Option<i64>>(4)?.unwrap_or(0);
                let reason: i64 = r.get::<_, Option<i64>>(5)?.unwrap_or(0);
                Ok(ModelUsageRow {
                    model,
                    first_token_at: ft,
                    completed_at: completed,
                    duration_ms: dur,
                    output_tokens: out.max(0) as u64,
                    reasoning_tokens: reason.max(0) as u64,
                })
            })?;
            Ok(rows.flatten().collect())
        };
        match query() {
            Ok(rows) => aggregate_chart_stats(rows, window_min, now),
            Err(e) => {
                eprintln!("[zcode-speed-panel] chart_stats query failed: {e}");
                empty
            }
        }
    }
}

// ============ Tests ============
#[cfg(test)]
mod tests {
    use super::*;

    /// Message gating: newest assistant row without completed → in progress;
    /// completed rows, over-aged zombie rows (crash fallback), and "a newer completed row in the same session" don't count;
    /// all concurrent multi-session (multi-window/subagent) entries are returned
    #[test]
    fn inflight_from_rows_gating() {
        let now = 1_000_000i64;
        // First call of a new session: row unfinished → in progress
        let r = inflight_from_rows(&[("new".into(), now - 3_000, false)], now);
        assert_eq!(r, vec![("new".to_string(), now - 3_000)]);
        // A newer completed assistant row exists in the same session (old zombie row first) → doesn't count
        assert_eq!(
            inflight_from_rows(
                &[
                    ("a".into(), now - 60_000, false),      // crash residue
                    ("a".into(), now - 30_000, true),       // session a's newest assistant row
                ],
                now
            ),
            Vec::new()
        );
        // Multi-session concurrency: all in-progress sessions are returned, sorted by start time descending
        // (subagent sessions b/c started later than main session a, whose current round is complete)
        let r = inflight_from_rows(
            &[
                ("a".into(), now - 40_000, true),
                ("b".into(), now - 5_000, false),
                ("c".into(), now - 20_000, false),
            ],
            now,
        );
        assert_eq!(
            r,
            vec![
                ("b".to_string(), now - 5_000),
                ("c".to_string(), now - 20_000),
            ]
        );
        // Unfinished but past the 10-minute fallback → judged stopped
        assert_eq!(
            inflight_from_rows(&[("z".into(), now - 601_000, false)], now),
            Vec::new()
        );
        assert_eq!(inflight_from_rows(&[], now), Vec::new());
    }

    fn call(completed: i64, gen_ms: i64, out: u64, reason: u64, input: u64, session: &str) -> Call {
        Call {
            id: format!("{}-{}", completed, out),
            started_ms: completed - gen_ms - 1000,
            first_token_ms: Some(completed - gen_ms),
            completed_ms: completed,
            gen_ms,
            output: out,
            reasoning: reason,
            input,
            cache_creation: 0,
            cache_read: 0,
            session: session.into(),
        }
    }

    #[test]
    fn snapshot_computes_speeds() {
        let now = now_ms();
        let mut agg = Aggregator::new();
        agg.ingest(call(now - 10_000, 10_000, 500, 40, 100, "a"));
        agg.ingest(call(now - 1_000, 8_000, 240, 60, 100, "a"));
        let s = agg.snapshot();
        // Pure generation rate: (500+40 + 240+60) / 18s = 46.7
        assert!((s.avg_tps - 840.0 / 18.0).abs() < 1e-9);
        assert!((s.current_tps - 840.0 / 18.0).abs() < 1e-9);
        assert!(!s.is_live); // is_live is overridden in main solely by live IO measurement
        // Total tokens = output 740 + reasoning 100 + input 200
        assert_eq!(s.total_tokens, 1040);
        assert_eq!(s.output_tokens, 740);
        assert_eq!(s.reasoning_tokens, 100);
        assert_eq!(s.sessions_today, 1);
        assert_eq!(s.spark.len(), SPARK_BUCKETS);
        assert_eq!(s.live_source, "window"); // no IO probing, is_live=false → window fallback
        // Last call speed = eff/gen of the most recently completed call (now-1s) = 300 / 8s
        assert!((s.last_call_tps - 37.5).abs() < 1e-9);
    }

    /// "Last call speed" takes the row with the latest completion time, independent of ingest order (DB query ordering can vary)
    #[test]
    fn last_call_tps_uses_latest_completed() {
        let now = now_ms();
        let mut agg = Aggregator::new();
        agg.ingest(call(now - 1_000, 4_000, 400, 0, 0, "a")); // 100 t/s
        agg.ingest(call(now - 30_000, 2_000, 100, 0, 0, "b")); // 50 t/s, completed earlier
        agg.ingest(call(now - 20_000, 5_000, 250, 50, 0, "c")); // 60 t/s, still earlier than now-1s
        let s = agg.snapshot();
        assert!((s.last_call_tps - 100.0).abs() < 1e-9);
        // Average speed and "last call" are two different methodologies: total eff 800 / total 11s ≠ 100
        assert!((s.avg_tps - 800.0 / 11.0).abs() < 1e-9);
    }

    #[test]
    fn speed_excludes_time_before_first_token() {
        let now = now_ms();
        let mut agg = Aggregator::new();
        agg.ingest(call(now - 5_000, 5_000, 500, 0, 0, "a"));
        let s1 = agg.snapshot();
        assert!((s1.avg_tps - 100.0).abs() < 1e-9);
        // Call B: request sent 30s ago (long TTFT/queuing), but pure generation is 5s with 500 output
        agg.ingest(Call {
            id: "b".into(),
            started_ms: now - 30_000,
            first_token_ms: Some(now - 6_000),
            completed_ms: now - 1_000,
            gen_ms: 5_000,
            output: 500,
            reasoning: 0,
            input: 0,
            cache_creation: 0,
            cache_read: 0,
            session: "b".into(),
        });
        let s2 = agg.snapshot();
        // Denominator uses completed - first_token (excludes waiting before the first token), still 100 t/s
        assert!((s2.avg_tps - 100.0).abs() < 1e-9);
    }

    fn mrow(
        model: &str,
        completed: i64,
        ft: Option<i64>,
        dur: Option<i64>,
        out: u64,
        reason: u64,
    ) -> ModelUsageRow {
        ModelUsageRow {
            model: model.into(),
            first_token_at: ft,
            completed_at: completed,
            duration_ms: dur,
            output_tokens: out,
            reasoning_tokens: reason,
        }
    }

    /// Two models × two buckets: correct tps/calls/tokens/avg/peak/share, bucket alignment, empty buckets are 0,
    /// series sorted by total_tokens descending, out-of-window (too old / future-crossing-slot) rows dropped.
    /// now is mid-slot (not on a boundary), the same convention as the chart_stats test
    #[test]
    fn model_stats_two_models_two_buckets() {
        let now = 1_700_000_005_000i64; // 5s into a 10s slot (15-minute window bucket width is 10s)
        let rows = vec![
            // Model A: bucket 0 (gen 4s, eff 400 → 100 t/s), bucket 1 (gen 1s, eff 100 → 100 t/s)
            mrow("model-a", now - 5_000, Some(now - 9_000), Some(9_000), 300, 100),
            mrow("model-a", now - 15_000, Some(now - 16_000), Some(6_000), 100, 0),
            // Model B: bucket 0 (first token missing, falls back to duration 2s, eff 400 → 200 t/s),
            // bucket 2 (gen 3s, eff 1200 → 400 t/s)
            mrow("model-b", now - 5_000, None, Some(2_000), 400, 0),
            mrow("model-b", now - 25_000, Some(now - 28_000), None, 900, 300),
            // Out of range: older than the window (slot 100 ≥ 90) and future-crossing slot (negative slot); both should be dropped
            mrow("model-a", now - 1_000_000, Some(now - 1_001_000), None, 999, 0),
            mrow("model-a", now + 6_000, Some(now + 5_000), None, 999, 0),
        ];
        let p = aggregate_model_stats(rows, 15, now);
        assert_eq!(p.window_min, 15);
        assert_eq!(p.bucket_ms, 10_000);
        assert_eq!(p.now_ms, now);
        // Sorted by total_tokens descending: B(1600) first, A(500) after
        assert_eq!(p.series.len(), 2);
        assert_eq!(p.series[0].model, "model-b");
        assert_eq!(p.series[1].model, "model-a");

        let b = &p.series[0];
        assert_eq!(b.buckets.len(), 90);
        assert_eq!(b.total_calls, 2);
        assert_eq!(b.total_tokens, 1600);
        assert!((b.avg_tps - 1600.0 / 5.0).abs() < 1e-9); // Σeff 1600 ÷ 5s
        assert!((b.peak_tps - 400.0).abs() < 1e-9);
        assert!((b.share - 1600.0 / 2100.0).abs() < 1e-9);
        assert!((b.buckets[0].tps - 200.0).abs() < 1e-9);
        assert_eq!(b.buckets[0].calls, 1);
        assert_eq!(b.buckets[0].tokens, 400);
        assert_eq!(b.buckets[1].calls, 0); // empty bucket
        assert_eq!(b.buckets[1].tps, 0.0);
        assert_eq!(b.buckets[1].tokens, 0);
        assert!((b.buckets[2].tps - 400.0).abs() < 1e-9);
        assert_eq!(b.buckets[2].tokens, 1200);
        // Out-of-range rows are counted in no bucket
        assert_eq!(b.buckets.iter().map(|x| x.calls).sum::<u64>(), 2);

        let a = &p.series[1];
        assert_eq!(a.total_calls, 2);
        assert_eq!(a.total_tokens, 500);
        assert!((a.avg_tps - 100.0).abs() < 1e-9); // 500 ÷ 5s
        assert!((a.peak_tps - 100.0).abs() < 1e-9);
        assert!((a.share - 500.0 / 2100.0).abs() < 1e-9);
        assert!((a.buckets[0].tps - 100.0).abs() < 1e-9);
        assert!((a.buckets[1].tps - 100.0).abs() < 1e-9);
        assert_eq!(a.buckets[2].calls, 0);
    }

    /// Wall-clock alignment guard (same methodology as chart_stats): while now slides within one absolute slot, bucket contents
    /// stay completely unchanged — trends should only shift, never deform, over time (previously bucketing used the (now−completed)
    /// relative offset, so bucket boundaries drifted with query time and calls jumped between adjacent buckets, slightly warping the chart every tick)
    #[test]
    fn model_stats_wall_clock_aligned_buckets() {
        let mk = || {
            vec![
                mrow("m", 1_700_000_002_000, Some(1_699_999_990_000), Some(12_000), 600, 0),
                mrow("m", 1_699_999_990_000, Some(1_699_999_986_000), Some(4_000), 200, 0),
            ]
        };
        // The two now values differ by 5s but share the absolute slot [1_700_000_000_000, 1_700_000_010_000)
        let a = aggregate_model_stats(mk(), 15, 1_700_000_003_000);
        let b = aggregate_model_stats(mk(), 15, 1_700_000_008_000);
        assert_eq!(a.bucket_ms, 10_000);
        assert_eq!(a.series[0].buckets, b.series[0].buckets);
        // Consistent with absolute-slot alignment: the two completed values land in the newest bucket (0) and second-newest (1)
        assert!(a.series[0].buckets[0].tps > 0.0);
        assert!(a.series[0].buckets[1].tps > 0.0);
        assert_eq!(a.series[0].buckets[2].calls, 0);
    }

    /// Window clamp (shared with the chart's clamp_chart_window: 999→1440, 0→15, 40→60, 400→360)
    /// and gen_ms missing fallback (first_token None → duration_ms → max(50) floor)
    #[test]
    fn model_stats_window_clamp_and_gen_fallback() {
        assert_eq!(clamp_chart_window(999), 1440);
        assert_eq!(clamp_chart_window(0), 15);
        assert_eq!(clamp_chart_window(40), 60);
        assert_eq!(clamp_chart_window(400), 360);
        assert_eq!(clamp_chart_window(60), 60);
        assert_eq!(clamp_chart_window(360), 360);

        let now = 1_700_000_000_000i64;
        let rows = vec![
            mrow("m", now - 5_000, None, Some(5_000), 500, 0), // gen=5000ms
            mrow("m", now - 6_000, None, None, 100, 0),        // duration missing → 50ms
            mrow("m", now - 7_000, Some(now - 7_000), Some(0), 100, 0), // ft non-positive (=completed) → dur 0 non-positive → 50ms
        ];
        // window_min=999 clamps to 1440 (24 hours): bucket width 960_000ms, all three rows land in the newest bucket
        let p = aggregate_model_stats(rows, 999, now);
        assert_eq!(p.window_min, 1440);
        assert_eq!(p.bucket_ms, 960_000);
        assert_eq!(p.series.len(), 1);
        let s = &p.series[0];
        assert_eq!(s.buckets.len(), 90);
        assert_eq!(s.total_calls, 3);
        assert_eq!(s.total_tokens, 700);
        // Σeff 700 ÷ (5s + 50ms + 50ms)
        assert!((s.avg_tps - 700.0 / 5.1).abs() < 1e-9);
        assert!((s.buckets[0].tps - 700.0 / 5.1).abs() < 1e-9);
        assert_eq!(s.peak_tps, s.buckets[0].tps);
        assert!((s.share - 1.0).abs() < 1e-9);
        // Empty input → empty series
        let p = aggregate_model_stats(Vec::new(), 15, now);
        assert!(p.series.is_empty());
    }

    /// Historical stats: average unfiltered (includes duration-fallback rows); peak record admission —
    /// valid first_token + gen≥1s + eff≥300, all three required
    #[test]
    fn history_stats_fold_and_admission() {
        let now = 1_700_000_000_000i64;
        let day = 1_700_000_000_000i64 - now.rem_euclid(86_400_000); // arbitrary local-day placeholder
        let mut h = HistoryStats::default();
        // Normal large call: 1000 tok / 4s = 250 t/s, enters the peak
        h.fold(day, Some(now - 5_000), now - 1_000, 4_000, 1_000);
        // Faster small call: 300 tok / 1.05s ≈ 285.7 t/s, eff=300 qualifies → should refresh the peak
        h.fold(day, Some(now - 3_000), now - 1_950, 1_050, 300);
        assert!((h.max_tps() - 300.0 * 1000.0 / 1050.0).abs() < 1e-9);
        // Fake-record trap: 79 tok / 24ms (shape observed in the real DB, 1580 t/s) — fails both the eff and duration thresholds
        h.fold(day, Some(now - 100), now - 76, 24, 79);
        assert!((h.max_tps() - 300.0 * 1000.0 / 1050.0).abs() < 1e-9);
        // eff qualifies but duration does not (500 tok / 200ms = 2500 t/s) → excluded
        h.fold(day, Some(now - 300), now - 100, 200, 500);
        assert!((h.max_tps() - 300.0 * 1000.0 / 1050.0).abs() < 1e-9);
        // Duration-fallback row (ft missing) counts toward the average but not the peak
        h.fold(day, None, now - 60_000, 10_000, 2_000);
        assert!((h.max_tps() - 300.0 * 1000.0 / 1050.0).abs() < 1e-9);
        // Average = Σeff 3800 ÷ Σgen (the 24ms row rounds up to MIN_DUR_MS=50)
        let total_eff = 1_000 + 300 + 79 + 500 + 2_000;
        let total_gen = 4_000 + 1_050 + 50 + 200 + 10_000;
        assert!((h.avg_tps() - total_eff as f64 / (total_gen as f64 / 1000.0)).abs() < 1e-9);
        assert_eq!(h.total_eff(), total_eff);
        assert_eq!(h.total_gen_ms(), total_gen);
        assert_eq!(h.days.len(), 1);
        // Empty database
        assert_eq!(HistoryStats::default().avg_tps(), 0.0);
        assert_eq!(HistoryStats::default().max_tps(), 0.0);
    }

    /// Week-window sliding expiry: the earliest whole-day buckets before the window start are dropped; peak/average aggregate only
    /// from day buckets inside the window (when the start lands exactly on a day's midnight, that day is kept)
    #[test]
    fn history_stats_week_window_prune() {
        let day = 86_400_000i64 * 20_000; // arbitrary placeholder day midnight aligned to a day boundary
        let mut h = HistoryStats::default();
        // One qualifying call per day for three days: day 1 is fastest at 400 t/s (should vanish once out of window), days 2-3 at 250 t/s
        h.fold(day, Some(day + 1_000), day + 3_500, 2_500, 1_000);
        h.fold(day + 86_400_000, Some(day + 86_400_001), day + 86_400_005, 4_000, 1_000);
        h.fold(day + 2 * 86_400_000, Some(day + 2 * 86_400_001), day + 2 * 86_400_005, 4_000, 1_000);
        assert_eq!(h.days.len(), 3);
        assert!((h.max_tps() - 400.0).abs() < 1e-9);
        // Window start = day 2's midnight: day 1's whole bucket leaves the window (its 400 t/s peak disappears with it),
        // the day containing the start is kept
        h.prune(day + 86_400_000);
        assert_eq!(h.days.len(), 2);
        assert!((h.max_tps() - 250.0).abs() < 1e-9);
        assert!((h.avg_tps() - 250.0).abs() < 1e-9);
        // Average recomputed from the remaining buckets: Σeff 2000 ÷ Σgen 8000ms
        assert_eq!(h.total_eff(), 2_000);
        assert_eq!(h.total_gen_ms(), 8_000);
        // Start pushed three days later: everything leaves the window → zeroed (no stale peak left behind)
        h.prune(day + 3 * 86_400_000);
        assert!(h.days.is_empty());
        assert_eq!(h.max_tps(), 0.0);
        assert_eq!(h.avg_tps(), 0.0);
    }

    /// Chart aggregation: 90 buckets, oldest→newest, out-of-range dropped, gen fallback methodology matches today's spark.
    /// now is mid-slot (not on a boundary) so "completed a few seconds ago" lands stably in the newest bucket
    #[test]
    fn chart_stats_buckets_and_order() {
        let now = 1_700_000_005_000i64; // 5s into a 10s slot
        let rows = vec![
            // 15-minute window bucket width 10s: completed 3s ago → same slot as now → newest bucket (last).
            // gen = ft difference 12s, eff 1000 → 83.3 t/s
            mrow("m", now - 3_000, Some(now - 15_000), Some(15_000), 1_000, 0),
            // Completed 95s ago → 9 buckets from the newest slot → index 89-9=80; gen 4s eff 3000 → 750 t/s
            mrow("m", now - 95_000, Some(now - 99_000), Some(9_000), 3_000, 0),
            // Out of window (20 minutes ago) → dropped
            mrow("m", now - 1_200_000, Some(now - 1_204_000), None, 9_999, 0),
        ];
        let p = aggregate_chart_stats(rows, 15, now);
        assert_eq!(p.window_min, 15);
        assert_eq!(p.bucket_ms, 10_000);
        assert_eq!(p.buckets.len(), 90);
        assert!((p.buckets[89] - 1_000.0 * 1000.0 / 12_000.0).abs() < 1e-9); // newest bucket
        assert!((p.buckets[80] - 750.0).abs() < 1e-9);
        assert!((p.buckets[0] - 0.0).abs() < 1e-9); // empty bucket
    }

    /// Chart window clamp: 999→1440, 30→15, 90→60, 720→360; 1h window bucket width 40s
    #[test]
    fn chart_stats_window_clamp() {
        assert_eq!(clamp_chart_window(999), 1440);
        assert_eq!(clamp_chart_window(30), 15);
        assert_eq!(clamp_chart_window(90), 60);
        assert_eq!(clamp_chart_window(720), 360);
        assert_eq!(clamp_chart_window(15), 15);
        let p = aggregate_chart_stats(Vec::new(), 60, 1_700_000_000_000i64);
        assert_eq!(p.bucket_ms, 60 * 60_000 / 90);
        assert_eq!(p.buckets.len(), 90);
        assert!(p.buckets.iter().all(|v| *v == 0.0));
    }
}
