#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod autostart;
mod liveio;
mod metrics;
mod netio;
mod updater;

use liveio::{LiveIo, RoundDrift};
use metrics::{home_dir, Engine, ModelStatsPayload, Snapshot};
use updater::Release;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;
use tauri::menu::{Menu, MenuItem, PredefinedMenuItem};
use tauri::tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};
use tauri::{AppHandle, Emitter, LogicalSize, Manager, PhysicalPosition, PhysicalSize, WindowEvent};

/// Window display mode: full panel / floating window
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Full,
    Float,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Full => "full",
            Mode::Float => "float",
        }
    }
    fn parse(s: &str) -> Mode {
        if s.trim() == "float" {
            Mode::Float
        } else {
            Mode::Full
        }
    }
}

/// Floating window style: mini gauge / speed pill / desktop pet
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FloatStyle {
    Gauge,
    Pill,
    Pet,
}

impl FloatStyle {
    fn as_str(self) -> &'static str {
        match self {
            FloatStyle::Gauge => "gauge",
            FloatStyle::Pill => "pill",
            FloatStyle::Pet => "pet",
        }
    }
    fn parse(s: &str) -> FloatStyle {
        match s.trim() {
            "pill" => FloatStyle::Pill,
            "pet" => FloatStyle::Pet,
            _ => FloatStyle::Gauge,
        }
    }
}

/// Persisted state: mode, style, and the window position / desktop pet size each mode remembers separately.
/// The pet position is independent of the full panel position — collapsing to the pet returns the pet to its own last position
/// (anchored to the window center when there is no memory, not the window's top-left corner); expanding returns the window to its own old position.
#[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
struct Persisted {
    mode: String,
    style: String,
    /// Full panel's last position (physical pixels)
    #[serde(default)]
    full_pos: Option<(i32, i32)>,
    /// Floating window's last position (physical pixels)
    #[serde(default)]
    float_pos: Option<(i32, i32)>,
    /// Desktop pet floating window side length (logical pixels)
    #[serde(default)]
    pet_size: Option<f64>,
}

struct AppState {
    engine: Mutex<Engine>,
    mode: Mutex<Mode>,
    style: Mutex<FloatStyle>,
    live: Mutex<LiveIo>,
    /// Network traffic monitoring (netio.rs: system-wide interface counters + connection attribution)
    net: Mutex<netio::NetIo>,
    debug: Mutex<DebugLog>,
    persist: Mutex<Persisted>,
    /// Position persistence throttle (every 2s while dragging; persisted immediately on close/exit)
    last_pos_save: Mutex<Option<std::time::Instant>>,
    /// Status item at the top of the tray menu (disabled; only displays generation status)
    tray_status: Mutex<Option<tauri::menu::MenuItem<tauri::Wry>>>,
    /// Status text last written to the status item/tray tooltip (updated only on change, avoiding churn every tick)
    tray_status_last: Mutex<String>,
    /// Whether the mac startup hint is pending claim (one-shot): setup runs before the event loop,
    /// where emit is necessarily dropped before page load; the frontend claims it via invoke once ready
    tray_hint_pending: Mutex<bool>,
    /// macOS frameless multi-monitor safe-maximize memory: (restore physical position, restore physical size)
    saved_max_rect: Mutex<Option<(PhysicalPosition<i32>, PhysicalSize<u32>)>>,
    /// Current round (a continuous gated "in progress" segment) displayed-speed accumulator: (Σtps, measured tick count, whether a multi-process aggregated tick was seen)
    round_tps: Mutex<(f64, u32, bool)>,
    /// Whether the previous tick had an in-progress call (a true→false edge = one round ended; settle the mean and feed drift detection)
    round_was_inflight: Mutex<bool>,
    /// Round mean-speed drift detection: last round's mean vs the mean of the previous 5 consecutive rounds ≥3x (either direction) → auto recalibration
    drift: Mutex<RoundDrift>,
    /// Coefficient sample queue last persisted (write speed-panel-cal.json only on change)
    cal_saved: Mutex<Vec<f64>>,
    /// Desktop pet multi-task heightening debounce counter (≥2 tasks +1 / <2 tasks -1, confirmed over 3 ticks)
    pet_task_streak: Mutex<u32>,
    /// The multi-task heightening the pet window should currently have (0 or PET_TASK_EXTRA; the poller
    /// corrects any difference from the actual window size each tick, so it also self-heals after mode/style switches)
    pet_task_extra: Mutex<f64>,
    /// In-app update (updater.rs): latest Release, pre-downloaded artifact, and concurrency gate flag.
    /// All network operations run on background threads; automatic-check failures are always silent (see the updater.rs module comments)
    update: Mutex<UpdateMem>,
}

/// In-memory state of the update flow (not persisted: a check runs on every launch, so the last check time need not survive restarts)
#[derive(Default)]
struct UpdateMem {
    /// Time of the last successful Release lookup (network failures are not recorded; it retries the next hour)
    last_check_ms: i64,
    checking: bool,
    downloading: bool,
    /// Discovered new version (Some means an update exists)
    latest: Option<Release>,
    /// Pre-downloaded installer (tag, path)
    downloaded: Option<(String, PathBuf)>,
    /// Launch the install automatically once the download completes (the user already clicked "Update now"; waiting for the download)
    install_when_ready: bool,
}

/// Debug log: records live display values, statistics, and the ground truth after each round of calls completes,
/// for deviation analysis of "live readings vs persisted statistics". Appended as JSONL; rotated keeping one generation when oversized;
/// rotated-out old files older than 7 days are cleaned up automatically at startup.
struct DebugLog {
    file: Option<fs::File>,
    written: u64,
    last_heartbeat: std::time::Instant,
}

const DEBUG_LOG_MAX: u64 = 8 * 1024 * 1024;
/// Retention period for rotated old logs
const DEBUG_LOG_KEEP: std::time::Duration = std::time::Duration::from_secs(7 * 86400);

impl DebugLog {
    fn new() -> Self {
        let mut log = DebugLog { file: None, written: 0, last_heartbeat: std::time::Instant::now() };
        log.cleanup_rotated();
        log.reopen();
        log
    }

    fn path() -> Option<PathBuf> {
        home_dir().map(|h| h.join(".zcode").join("speed-panel-debug.jsonl"))
    }

    /// Auto cleanup: delete rotated logs (speed-panel-debug.jsonl.N) past the retention period
    fn cleanup_rotated(&mut self) {
        let Some(p) = DebugLog::path() else { return };
        let Some(dir) = p.parent() else { return };
        let Ok(rd) = fs::read_dir(dir) else { return };
        for e in rd.flatten() {
            if !e.file_name().to_string_lossy().starts_with("speed-panel-debug.jsonl.") {
                continue;
            }
            let Ok(meta) = e.metadata() else { continue };
            if let Ok(mtime) = meta.modified() {
                if mtime < std::time::SystemTime::now() - DEBUG_LOG_KEEP {
                    let _ = fs::remove_file(e.path());
                }
            }
        }
    }

    fn reopen(&mut self) {
        if let Some(p) = DebugLog::path() {
            if let Ok(meta) = fs::metadata(&p) {
                self.written = meta.len();
            }
            self.file = fs::OpenOptions::new().create(true).append(true).open(&p).ok();
        }
    }

    fn write(&mut self, value: serde_json::Value) {
        use std::io::Write;
        if self.written > DEBUG_LOG_MAX {
            self.file = None;
            if let Some(p) = DebugLog::path() {
                let _ = fs::rename(&p, p.with_extension("jsonl.1"));
            }
            self.written = 0;
            self.reopen();
        }
        if let Some(f) = self.file.as_mut() {
            let _ = writeln!(f, "{}", value);
            self.written += value.to_string().len() as u64 + 1;
        }
    }
}

/// Full panel default size (logical pixels): height 800 lets every card (including the bottom chart card)
/// be fully visible without scrolling on open (natural content height ~760)
const FULL_SIZE: (f64, f64) = (1000.0, 800.0);
/// Gauge floating window 148×118: height 118 keeps the main ring's arc bottom 8px above the window bottom, symmetric with the
/// top-right "last round" small ring's top:8px (the main ring canvas is 116px wide, the radius derives from the canvas, and the arc
/// bottom is anchored in gauges.ts MiniGauge — change style.css #mini-gauge and the docs in sync)
const FLOAT_GAUGE_SIZE: (f64, f64) = (148.0, 118.0);
const FLOAT_PILL_SIZE: (f64, f64) = (172.0, 72.0);
/// Desktop pet default side length (logical pixels); mouse-wheel zoom range [100, 480]
const FLOAT_PET_SIZE: f64 = 200.0;
const PET_SIZE_MIN: f64 = 100.0;
const PET_SIZE_MAX: f64 = 480.0;
/// Reserved bubble height at the top of the pet window (logical pixels): two bubble lines max ~51px (10 + 18×2 + 3 at
/// fs=15) plus margin. Window = side × (side + reserve); the bubble's bottom edge anchors near the sprite's
/// head and grows upward, so the sprite no longer shrinks to make room for bubbles (pet.ts lays out by the bottom square area;
/// changing this value requires syncing both set_size calls and pet.ts layout logic)
const PET_BUBBLE_RESERVE: f64 = 56.0;
/// Desktop pet multi-task heightening (logical pixels): with ≥2 in-progress tasks (debounced over 3 ticks) the window grows
/// upward by this much to make room for the bubble's per-task rows (bottom edge stays put: it moves up by exactly the added height).
/// 96px fits 6 bubble lines at the default 200 size (live + 6 tasks + last round). Collapsing back is debounced the same way
const PET_TASK_EXTRA: f64 = 96.0;

fn mode_file() -> Option<PathBuf> {
    home_dir().map(|h| h.join(".zcode").join("speed-panel-mode.txt"))
}

/// Coefficient sample persistence: warm start after restarts instead of re-converging from the prior 600 every time (measured: for a high-speed
/// session with a ground-truth coefficient of ~160, cold-start readings are 2~3x low and convergence takes ~25 minutes)
fn cal_file() -> Option<PathBuf> {
    home_dir().map(|h| h.join(".zcode").join("speed-panel-cal.json"))
}

/// Restore validity period: after a model/tokenizer change, old samples become stale noise; past the deadline, revert to the prior and re-converge
const CAL_STALE_MS: i64 = 14 * 24 * 3600 * 1000;

fn load_cal_samples() -> Vec<f64> {
    let raw = cal_file().and_then(|p| fs::read_to_string(p).ok());
    let Some(s) = raw else { return Vec::new() };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&s) else {
        eprintln!("[zcode-speed-panel] cal sample file corrupted, reverting to prior");
        return Vec::new();
    };
    let updated = v.get("updated_ms").and_then(|x| x.as_i64()).unwrap_or(0);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    if now_ms - updated > CAL_STALE_MS {
        eprintln!("[zcode-speed-panel] cal samples expired after 14 days, reverting to prior");
        return Vec::new();
    }
    v.get("samples")
        .and_then(|x| x.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_f64())
                .collect::<Vec<f64>>()
        })
        .unwrap_or_default()
}

fn save_cal_samples(samples: &[f64]) {
    if let Some(path) = cal_file() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let json = serde_json::json!({ "updated_ms": now_ms, "samples": samples });
        if let Err(e) = fs::write(path, json.to_string()) {
            eprintln!("[zcode-speed-panel] failed to persist cal samples: {e}");
        }
    }
}

fn load_persisted() -> Persisted {
    let raw = mode_file().and_then(|p| fs::read_to_string(p).ok());
    match raw {
        Some(s) => match serde_json::from_str::<Persisted>(&s) {
            Ok(p) => p,
            // Legacy format: plain-text "full"/"float"
            Err(_) => Persisted {
                mode: s,
                ..Default::default()
            },
        },
        None => Persisted::default(),
    }
}

fn save_all(app: &AppHandle) {
    let state = app.state::<AppState>();
    let mode = *state.mode.lock().unwrap();
    let style = *state.style.lock().unwrap();
    let p = state.persist.lock().unwrap().clone();
    // Persist today's network totals too (shared by the exit/position-save paths)
    state.net.lock().unwrap().save_forced();
    if let Some(path) = mode_file() {
        let json = serde_json::json!({
            "mode": mode.as_str(),
            "style": style.as_str(),
            "full_pos": p.full_pos,
            "float_pos": p.float_pos,
            "pet_size": p.pet_size,
        });
        let _ = fs::write(path, json.to_string());
    }
}

/// Pull the window fully back into its monitor's visible area (on multi-monitor, locate by the window's current point)
fn clamp_to_screen(window: &tauri::WebviewWindow, x: i32, y: i32, w: u32, h: u32) -> (i32, i32) {
    let monitor = window
        .monitor_from_point(x as f64, y as f64)
        .ok()
        .flatten()
        .or_else(|| window.current_monitor().ok().flatten())
        .or_else(|| window.primary_monitor().ok().flatten());
    let Some(m) = monitor else {
        return (x, y);
    };
    let mp = m.position();
    let ms = m.size();
    let max_x = (mp.x + ms.width as i32 - w as i32).max(mp.x);
    let max_y = (mp.y + ms.height as i32 - h as i32).max(mp.y);
    (x.clamp(mp.x, max_x), y.clamp(mp.y, max_y))
}


fn apply_mode(window: &tauri::WebviewWindow, mode: Mode, style: FloatStyle, p: &Persisted, pet_extra: f64) {
    let scale = window.scale_factor().unwrap_or(1.0);
    match mode {
        Mode::Full => {
            let _ = window.set_min_size(Some(LogicalSize::new(720.0, 520.0)));
            let _ = window.set_size(LogicalSize::new(FULL_SIZE.0, FULL_SIZE.1));
            // Top bar: on mac restore the native overlay title bar — real system traffic lights (native
            // animation for red close / yellow minimize / green fullscreen), content extends under the title bar,
            // the frontend leaves space on the left; also switch to the Regular policy to show the Dock icon
            // (only Regular apps can natively fullscreen; see the setup comments). Windows stays frameless + frontend-drawn — ▢ ✕ (the floating window must be frameless)
            #[cfg(target_os = "macos")]
            {
                let _ = window
                    .app_handle()
                    .set_activation_policy(tauri::ActivationPolicy::Regular);
                let _ = window.set_decorations(true);
            }
            #[cfg(not(target_os = "macos"))]
            let _ = window.set_decorations(false);
            let _ = window.set_resizable(true);
            let _ = window.set_always_on_top(false);
            let _ = window.set_skip_taskbar(false);
            let _ = window.set_shadow(true);
            // Return to the full panel's own old position (keep the current top-left when there's no memory, clamped into the visible area)
            if let Some((x, y)) = p.full_pos {
                let (px, py) = clamp_to_screen(
                    window,
                    x,
                    y,
                    (FULL_SIZE.0 * scale) as u32,
                    (FULL_SIZE.1 * scale) as u32,
                );
                let _ = window.set_position(PhysicalPosition::new(px, py));
            }
        }
        Mode::Float => {
            let (w, h) = match style {
                FloatStyle::Gauge => FLOAT_GAUGE_SIZE,
                FloatStyle::Pill => FLOAT_PILL_SIZE,
                FloatStyle::Pet => {
                    let s = p.pet_size.unwrap_or(FLOAT_PET_SIZE).clamp(PET_SIZE_MIN, PET_SIZE_MAX);
                    // The top reserve band gives room for two bubble lines: the sprite doesn't shrink, bubbles grow upward;
                    // the multi-task heightening (pet_task_extra) gives per-task rows somewhere to grow too
                    (s, s + PET_BUBBLE_RESERVE + pet_extra)
                }
            };
            let _ = window.set_min_size(None::<LogicalSize<f64>>);
            let _ = window.set_size(LogicalSize::new(w, h));
            let _ = window.set_decorations(false);
            // mac: switch back to Accessory — hide the Dock icon and live in the menu bar (the app doesn't quit;
            // this and the Regular, Dock-showing full panel are two states of each other, see the setup comments)
            #[cfg(target_os = "macos")]
            let _ = window
                .app_handle()
                .set_activation_policy(tauri::ActivationPolicy::Accessory);
            let _ = window.set_resizable(false);
            // Floating window: always on top, not in the taskbar, no native shadow (a shadow would cover the transparent area outside the rounded corners)
            let _ = window.set_always_on_top(true);
            let _ = window.set_skip_taskbar(true);
            let _ = window.set_shadow(false);
            // Position: the pet/floating window's own last position; with no memory, anchor to the current window center
            // (not the top-left corner — fixes the old issue where the pet always landed at the original window's top-left after collapsing)
            let (pw, ph) = ((w * scale) as u32, (h * scale) as u32);
            let target = match p.float_pos {
                Some((x, y)) => clamp_to_screen(window, x, y, pw, ph),
                None => {
                    let cur = window.outer_position().unwrap_or_default();
                    let sz = window.outer_size().unwrap_or_default();
                    let cx = cur.x + sz.width as i32 / 2;
                    let cy = cur.y + sz.height as i32 / 2;
                    clamp_to_screen(window, cx - pw as i32 / 2, cy - ph as i32 / 2, pw, ph)
                }
            };
            let _ = window.set_position(PhysicalPosition::new(target.0, target.1));
        }
    }
}

fn switch_mode(app: &AppHandle, mode: Mode) {
    let state = app.state::<AppState>();
    let style = *state.style.lock().unwrap();
    let prev = *state.mode.lock().unwrap();
    if mode == Mode::Float {
        *state.saved_max_rect.lock().unwrap() = None;
    }
    // Remember the window's position in the old mode (each mode remembers independently)
    if let Some(win) = app.get_webview_window("main") {
        if let Ok(pos) = win.outer_position() {
            let mut p = state.persist.lock().unwrap();
            match prev {
                Mode::Full => p.full_pos = Some((pos.x, pos.y)),
                Mode::Float => p.float_pos = Some((pos.x, pos.y)),
            }
        }
    }
    // Update the mode before applying the new size/position: Moved events triggered during application write back under the new mode
    *state.mode.lock().unwrap() = mode;
    let p = state.persist.lock().unwrap().clone();
    let pet_extra = *state.pet_task_extra.lock().unwrap();
    if let Some(window) = app.get_webview_window("main") {
        apply_mode(&window, mode, style, &p, pet_extra);
    }
    save_all(app);
    let _ = app.emit("mode", mode.as_str());
}

/// Collapse to the floating window: full panel → switch to floating mode; already floating → raise and focus.
/// Shared by the CloseRequested / mac menu-bar Cmd+Q / ExitRequested fallbacks
fn collapse_to_float(app: &AppHandle) {
    let mode = *app.state::<AppState>().mode.lock().unwrap();
    if mode == Mode::Full {
        switch_mode(app, Mode::Float);
    } else {
        show_main(app);
    }
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotPayload {
    snapshot: Snapshot,
    rollout_dir: String,
    mode: String,
    float_style: String,
}

fn build_payload(app: &AppHandle) -> SnapshotPayload {
    let state = app.state::<AppState>();
    let mode = *state.mode.lock().unwrap();
    let style = *state.style.lock().unwrap();
    let rollout_dir;
    let new_calls;
    let engine_calls: Vec<metrics::Call>;
    let inflight: Vec<(String, i64)>;
    let mut snapshot;
    {
        let mut engine = state.engine.lock().unwrap();
        new_calls = engine.poll();
        snapshot = engine.snapshot();
        rollout_dir = engine.data_source_label();
        engine_calls = engine.calls().to_vec();
        inflight = engine.call_in_flight();
    }
    // Live measurement: process IO write byte streams (real values). With concurrent tasks (multi-window/subagents),
    // aggregate in parallel over the processes owning the in-progress sessions; current speed = true total throughput
    let now_ms = snapshot.now_ms;
    // Network traffic monitoring: system-wide interface counter differencing + connection attribution
    let net_now = state.net.lock().unwrap().tick(now_ms);
    snapshot.net_available = net_now.available;
    snapshot.net_up_bps = net_now.up_bps;
    snapshot.net_down_bps = net_now.down_bps;
    snapshot.net_up_today = net_now.up_today;
    snapshot.net_down_today = net_now.down_today;
    // Session traffic estimate (≈): the upload numerator uses the uncached prompt (cache hits aren't resent; measured system-wide
    // upload for the day is only tens of KB), download is output tokens × the SSE density factor
    let uncached_prompt = snapshot
        .input_tokens
        .saturating_add(snapshot.cache_creation_tokens)
        .saturating_sub(snapshot.cache_read_tokens);
    let (sess_up, sess_down) = netio::sess_bytes_est(
        uncached_prompt,
        snapshot.output_tokens + snapshot.reasoning_tokens,
    );
    snapshot.net_sess_up_today = sess_up;
    snapshot.net_sess_down_today = sess_down;
    snapshot.net_conns_available = net_now.conns_available;
    snapshot.net_cli_conns = net_now.cli_conns;
    snapshot.net_app_conns = net_now.app_conns;
    snapshot.net_cli_conn_list = net_now.cli_conn_list.clone();
    snapshot.net_app_conn_list = net_now.app_conn_list.clone();
    let cal_event;
    let bpt_now;
    let pipe_bps;
    let npids;
    let proc_bps_log: Vec<(u32, f64)>;
    let infl_attr_log: Vec<(String, u32)>;
    {
        let mut live = state.live.lock().unwrap();
        if !live.history_done() {
            live.ingest_history(engine_calls.as_slice());
        }
        live.observe(&new_calls);
        live.set_inflight(inflight.clone());
        let live_now = live.measure(now_ms);
        cal_event = live.take_calibration();
        bpt_now = live.bytes_per_token();
        pipe_bps = live_now.pipe_bps;
        npids = live_now.n_pids;
        proc_bps_log = live_now.proc_bps.clone();
        infl_attr_log = live.inflight_attr();
        snapshot.tasks = live_now
            .tasks
            .iter()
            .map(|t| metrics::TaskStat {
                pid: t.pid,
                session: t.session.clone().unwrap_or_default(),
                n_sessions: t.n_sessions as u32,
                tps: t.tps,
                streaming: t.streaming,
            })
            .collect();
        // Persist whenever the coefficient sample queue changes (new sample added / manual or drift recalibration) for warm restarts
        {
            let q = live.cal_state();
            let mut saved = state.cal_saved.lock().unwrap();
            if *saved != q {
                save_cal_samples(&q);
                *saved = q;
            }
        }
        let ever_saw = live.ever_saw_procs();
        if live_now.available {
            if live_now.streaming {
                snapshot.is_live = true;
                snapshot.is_estimating = false;
                snapshot.ramping = live_now.ramping;
                snapshot.is_starting = live_now.awaiting;
                snapshot.live_source = "io".into();
                if live_now.awaiting {
                    // Startup phase (gate open, first byte not yet seen): show the "measuring…" hint,
                    // not a misleading estimate
                    snapshot.current_tps = 0.0;
                } else if live_now.tps < 1.0 && snapshot.window_tps > 0.0 {
                    // During parts of a call the UI pipeline has no incremental bytes (IO measurement reads 0): fall back to the real
                    // speed of recently completed calls (same methodology as the speed chart), marked ≈ estimated.
                    // ≈ is a global value under the persisted-data methodology with no per-task breakdown, so details are cleared
                    snapshot.current_tps = snapshot.window_tps;
                    snapshot.is_estimating = true;
                    snapshot.live_source = "window".into();
                    snapshot.tasks.clear();
                } else {
                    snapshot.current_tps = live_now.tps;
                }
                if let Some(last) = snapshot.spark.last_mut() {
                    *last = snapshot.current_tps;
                }
            } else {
                // IO available but the gate says no calls → truthful standby (real values take priority; estimates never mask them)
                snapshot.is_estimating = false;
                snapshot.ramping = false;
                snapshot.current_tps = 0.0;
                snapshot.live_source = "idle".into();
                if let Some(last) = snapshot.spark.last_mut() {
                    *last = 0.0;
                }
            }
        } else if !inflight.is_empty() && !ever_saw {
            // IO never became available (IO probing environment unavailable / panel just started, process not found):
            // decide by the message gate instead of blindly estimating from call intervals — display only while a call is in progress
            // (estimate ≈ when recent ground truth exists, otherwise show the "measuring…" hint), and zero immediately once the gate stops.
            // The old methodology inferred from the interval median and kept spinning "estimating" for up to 240s after the call ended
            if !(snapshot.is_estimating && snapshot.current_tps > 0.0) {
                snapshot.is_estimating = false;
                snapshot.is_starting = true;
                snapshot.current_tps = 0.0;
                snapshot.live_source = "window".into();
                if let Some(last) = snapshot.spark.last_mut() {
                    *last = 0.0;
                }
            }
        } else {
            // Processes were seen but are currently unavailable (all CLIs exited), or the gate has stopped → truthful standby
            snapshot.is_live = false;
            snapshot.is_estimating = false;
            snapshot.ramping = false;
            snapshot.current_tps = 0.0;
            snapshot.live_source = "idle".into();
            if let Some(last) = snapshot.spark.last_mut() {
                *last = 0.0;
            }
        }

    // ---- Debug log: live display values / statistics / ground truth after each round completes ----
    {
        let state = app.state::<AppState>();
        let mut log = state.debug.lock().unwrap();
        for c in &new_calls {
            log.write(serde_json::json!({
                "kind": "call",
                "t": now_ms,
                "id": c.id,
                "sess": &c.session[c.session.len().saturating_sub(8)..],
                "done": c.completed_ms,
                "gen_ms": c.gen_ms,
                "eff": c.effective_out(),
                "true_tps": (c.effective_out() as f64) / (c.gen_ms.max(50) as f64 / 1000.0),
            }));
        }
        if let Some(cal) = &cal_event {
            // Reconciliation: integrating the cleaned stream over this call's interval ÷ generation seconds ÷ the current coefficient =
            // the predicted average displayed t/s for the call; compare it with the persisted ground truth true_tps to assess live accuracy
            let pred_tps = if cal.gen_ms > 0 && cal.bpt_now > 0.0 {
                cal.clean_bytes / (cal.gen_ms as f64 / 1000.0) / cal.bpt_now
            } else {
                0.0
            };
            log.write(serde_json::json!({
                "kind": "cal",
                "t": now_ms,
                "id": cal.id,
                "gen_ms": cal.gen_ms,
                "eff": cal.eff,
                "true_tps": (cal.true_tps * 10.0).round() / 10.0,
                "raw_kb": (cal.raw_bytes / 1024.0 * 10.0).round() / 10.0,
                "clean_kb": (cal.clean_bytes / 1024.0 * 10.0).round() / 10.0,
                "attr_pid": cal.attr_pid,
                "top_pid": cal.top_pid,
                "others_kb": (cal.others_bytes / 1024.0 * 10.0).round() / 10.0,
                "bpt_sample": (cal.bpt_sample * 10.0).round() / 10.0,
                "bpt_now": (cal.bpt_now * 10.0).round() / 10.0,
                "pred_tps": (pred_tps * 10.0).round() / 10.0,
                "skipped": cal.cal_skipped,
            }));
        }
        let active = snapshot.is_live || snapshot.is_estimating || snapshot.is_starting;
        let heartbeat = log.last_heartbeat.elapsed() > std::time::Duration::from_secs(30);
        if active || heartbeat {
            log.last_heartbeat = std::time::Instant::now();
            let tail: Vec<f64> = snapshot
                .spark
                .iter()
                .rev()
                .take(3)
                .rev()
                .map(|v| (v * 10.0).round() / 10.0)
                .collect();
            // The three multi-task diagnostics: tracked processes' probe-window rates / in-progress session counts / session-to-process attribution map
            let pids_json: serde_json::Map<String, serde_json::Value> = proc_bps_log
                .iter()
                .map(|(pid, kbps)| (pid.to_string(), serde_json::json!(kbps)))
                .collect();
            let attr_json: Vec<String> = infl_attr_log
                .iter()
                .map(|(s, pid)| format!("{}…{}", &s[s.len().saturating_sub(4)..], pid))
                .collect();
            log.write(serde_json::json!({
                "kind": "tick",
                "t": now_ms,
                "src": snapshot.live_source,
                "tps": (snapshot.current_tps * 10.0).round() / 10.0,
                "pipe": (pipe_bps / 10.0).round() * 10.0,
                "stream": snapshot.is_live,
                "ramp": snapshot.ramping,
                "start": snapshot.is_starting,
                "est": snapshot.is_estimating,
                "bpt": (bpt_now * 10.0).round() / 10.0,
                "avg": (snapshot.avg_tps * 10.0).round() / 10.0,
                "spark_tail": tail,
                "calls": snapshot.calls_today,
                "npids": npids,
                "infl": inflight.len(),
                "pids": pids_json,
                "attr": attr_json,
                "net_up": (net_now.up_bps / 1024.0 * 10.0).round() / 10.0,
                "net_dn": (net_now.down_bps / 1024.0 * 10.0).round() / 10.0,
                "cli_conn": net_now.cli_conns,
                "app_conn": net_now.app_conns,
            }));
        }
    }

    }

    // ---- Round mean-speed drift auto-recalibration: a round = a continuous gated "in progress" segment; average the displayed
    //      speeds (io-measured ticks) within the round; last round's mean vs the mean of the previous 5 consecutive rounds ≥3x (either
    //      direction) judges an order-of-magnitude shift (model/tokenizer change, stale old coefficients) → drop the coefficient samples and revert to the prior.
    //      Multi-process aggregated rounds (multi-task concurrency) don't participate: throughput differences from a changing task count are not coefficient drift ----
    {
        let state = app.state::<AppState>();
        let now_inflight = !inflight.is_empty();
        let was_inflight = {
            let mut flag = state.round_was_inflight.lock().unwrap();
            std::mem::replace(&mut *flag, now_inflight)
        };
        if snapshot.is_live && snapshot.current_tps > 0.0 {
            let mut acc = state.round_tps.lock().unwrap();
            acc.0 += snapshot.current_tps;
            acc.1 += 1;
            if npids != 1 {
                acc.2 = true;
            }
        }
        if was_inflight && !now_inflight {
            // A round ended: settle the mean. Silent/estimating rounds (no measured ticks) and multi-process aggregated rounds don't participate in drift detection
            let (sum, n, saw_multi) = {
                let mut acc = state.round_tps.lock().unwrap();
                std::mem::take(&mut *acc)
            };
            if n > 0 && !saw_multi {
                let avg = sum / n as f64;
                let mut drift = state.drift.lock().unwrap();
                if let Some(base) = drift.observe(avg) {
                    let (bpt_old, bpt_new) = {
                        let mut live = state.live.lock().unwrap();
                        let old = live.bytes_per_token();
                        (old, live.reset_calibration())
                    };
                    state.debug.lock().unwrap().write(serde_json::json!({
                        "kind": "cal_reset",
                        "t": now_ms,
                        "reason": "auto",
                        "round_avg": (avg * 10.0).round() / 10.0,
                        "base_avg": (base * 10.0).round() / 10.0,
                        "bpt_old": (bpt_old * 10.0).round() / 10.0,
                        "bpt_new": (bpt_new * 10.0).round() / 10.0,
                    }));
                    // Same feedback as the manual trigger (⟳ button flashes ✓): an automatic trigger comes with a large
                    // coefficient shift, exactly when the user needs this hint
                    let _ = app.emit("recalibrated", ());
                }
            }
        }
    }
    SnapshotPayload {
        rollout_dir,
        snapshot,
        mode: mode.as_str().to_string(),
        float_style: style.as_str().to_string(),
    }
}

#[tauri::command]
fn snapshot(app: AppHandle) -> SnapshotPayload {
    build_payload(&app)
}

/// Model speed trends: read-only query of the usage DB aggregated per model × bucket (the frontend polls every 5s while the detail dialog is open).
/// Aggregation is computed on the fly inside the Engine; zero local storage, no writes to the usage DB
#[tauri::command]
fn model_stats(app: AppHandle, window_min: i64) -> ModelStatsPayload {
    let state = app.state::<AppState>();
    let engine = state.engine.lock().unwrap();
    engine.model_stats(window_min)
}

/// Output speed chart (time range selectable: 15m/1h/6h/24h): read-only query of the usage DB aggregated into 90 tps
/// buckets (all models merged, oldest→newest). The 15-minute window shares its methodology with today's spark in the metrics payload;
/// the frontend polls every 5s on longer windows and blends the live speed into the newest bucket
#[tauri::command]
fn chart_stats(app: AppHandle, window_min: i64) -> metrics::ChartStatsPayload {
    let state = app.state::<AppState>();
    let engine = state.engine.lock().unwrap();
    engine.chart_stats(window_min)
}

#[tauri::command]
fn set_mode(app: AppHandle, mode: String, style: Option<String>) {
    if let Some(s) = style {
        let st = FloatStyle::parse(&s);
        *app.state::<AppState>().style.lock().unwrap() = st;
    }
    switch_mode(&app, Mode::parse(&mode));
}

#[tauri::command]
fn set_float_style(app: AppHandle, style: String) {
    let st = FloatStyle::parse(&style);
    {
        let state = app.state::<AppState>();
        *state.style.lock().unwrap() = st;
        let mode = *state.mode.lock().unwrap();
    if mode == Mode::Float {
        if let Some(window) = app.get_webview_window("main") {
            let p = state.persist.lock().unwrap().clone();
            let pet_extra = *state.pet_task_extra.lock().unwrap();
            apply_mode(&window, mode, st, &p, pet_extra);
        }
    }
    }
    save_all(&app);
    let _ = app.emit("float-style", st.as_str());
}

/// Desktop pet mouse-wheel zoom: adjust the floating window side length (logical pixels) and persist
#[tauri::command]
fn set_float_size(app: AppHandle, size: f64) {
    let size = size.clamp(PET_SIZE_MIN, PET_SIZE_MAX);
    {
        let state = app.state::<AppState>();
        state.persist.lock().unwrap().pet_size = Some(size);
        let mode = *state.mode.lock().unwrap();
        let style = *state.style.lock().unwrap();
        if mode == Mode::Float && style == FloatStyle::Pet {
            if let Some(window) = app.get_webview_window("main") {
                // Height includes the top bubble reserve band and the multi-task heightening (same methodology as apply_mode)
                let extra = *state.pet_task_extra.lock().unwrap();
                let _ = window.set_size(LogicalSize::new(size, size + PET_BUBBLE_RESERVE + extra));
            }
        }
    }
    save_all(&app);
}

/// Debounce and apply the desktop pet's multi-task heightening: ≥2 in-progress tasks for 3 consecutive ticks → raise by
/// PET_TASK_EXTRA; below 2 for 3 consecutive ticks → retract (the same 3-tick debounce as the full panel's task card).
/// Returns immediately when want equals the applied value, without churning the window size
fn update_pet_task_extra(app: &AppHandle, multi_now: bool) {
    let state = app.state::<AppState>();
    let streak = {
        let mut s = state.pet_task_streak.lock().unwrap();
        *s = if multi_now {
            (*s + 1).min(3)
        } else {
            s.saturating_sub(1)
        };
        *s
    };
    let want = if streak >= 3 { PET_TASK_EXTRA } else { 0.0 };
    let cur = *state.pet_task_extra.lock().unwrap();
    if (want - cur).abs() < f64::EPSILON {
        return;
    }
    *state.pet_task_extra.lock().unwrap() = want;
    apply_pet_size(app);
}

/// Set the window size to the current pet side length + bubble reserve + multi-task heightening, and shift the whole
/// window up/down by the height delta so the bottom edge (the sprite's feet) stays in place on screen; effective only in pet floating mode —
/// other modes/styles only update state values, and apply_mode / this function reconciles them when switching back
fn apply_pet_size(app: &AppHandle) {
    let state = app.state::<AppState>();
    let mode = *state.mode.lock().unwrap();
    let style = *state.style.lock().unwrap();
    if mode != Mode::Float || style != FloatStyle::Pet {
        return;
    }
    let extra = *state.pet_task_extra.lock().unwrap();
    let size = state
        .persist
        .lock()
        .unwrap()
        .pet_size
        .unwrap_or(FLOAT_PET_SIZE)
        .clamp(PET_SIZE_MIN, PET_SIZE_MAX);
    let Some(w) = app.get_webview_window("main") else {
        return;
    };
    let scale = w.scale_factor().unwrap_or(1.0);
    let Ok(outer) = w.outer_size() else {
        return;
    };
    let new_h = size + PET_BUBBLE_RESERVE + extra;
    let dy = ((new_h - outer.height as f64 / scale) * scale).round() as i32;
    let pos = w.outer_position().unwrap_or_default();
    let (nx, ny) = clamp_to_screen(&w, pos.x, pos.y - dy, outer.width, (new_h * scale) as u32);
    let _ = w.set_size(LogicalSize::new(size, new_h));
    let _ = w.set_position(PhysicalPosition::new(nx, ny));
}

/// Floating window context-menu "Quit": save state, then exit the app
#[tauri::command]
fn quit_app(app: AppHandle) {
    save_all(&app);
    app.exit(0);
}

/// Manual recalibration (the ⟳ button at the top-left of the full panel's current speed card): discard the learned coefficient samples,
/// return to the platform prior and re-converge from subsequent calls; the drift-detection history resets too
#[tauri::command]
fn recalibrate(app: AppHandle) {
    let state = app.state::<AppState>();
    let (bpt_old, bpt_new) = {
        let mut live = state.live.lock().unwrap();
        let old = live.bytes_per_token();
        (old, live.reset_calibration())
    };
    state.drift.lock().unwrap().reset();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    state.debug.lock().unwrap().write(serde_json::json!({
        "kind": "cal_reset",
        "t": now,
        "reason": "manual",
        "bpt_old": (bpt_old * 10.0).round() / 10.0,
        "bpt_new": (bpt_new * 10.0).round() / 10.0,
    }));
    let _ = app.emit("recalibrated", ());
}

/// mac startup hint (one-shot): claimed by the frontend via invoke once the page is ready —
/// emit inside setup is necessarily dropped before page load, so the frontend claims it via invoke when ready. Always returns false on non-mac
#[tauri::command]
fn tray_hint_once(app: AppHandle) -> bool {
    let state = app.state::<AppState>();
    let mut guard = state.tray_hint_pending.lock().unwrap();
    let pending = *guard;
    *guard = false;
    pending
}

fn toggle_window_maximize(window: &tauri::WebviewWindow) {
    if window.is_maximized().unwrap_or(false) {
        let _ = window.unmaximize();
    } else {
        let _ = window.maximize();
    }
}

/// Multi-monitor safe maximize/restore: on macOS a frameless window's native toggle_maximize jumps back to the main screen,
/// so here we fill the monitor under the window's center point (avoiding the menu bar); Windows calls the system maximize directly
#[tauri::command]
fn toggle_maximize_safe(window: tauri::WebviewWindow, state: tauri::State<'_, AppState>) {
    #[cfg(windows)]
    {
        let _ = &state; // state is used only in the mac branch; silences the Windows unused warning
        toggle_window_maximize(&window);
    }
    #[cfg(target_os = "macos")]
    {
        let mut saved = state.saved_max_rect.lock().unwrap();
        if let Some((pos, size)) = saved.take() {
            // Already maximized: restore
            let _ = window.set_size(size);
            let _ = window.set_position(pos);
        } else {
            // Not maximized: perform the safe maximize
            let cur_pos = window.outer_position().unwrap_or_default();
            let cur_size = window.outer_size().unwrap_or_default();
            *saved = Some((cur_pos, cur_size));

            let cx = cur_pos.x + cur_size.width as i32 / 2;
            let cy = cur_pos.y + cur_size.height as i32 / 2;
            let monitor = window
                .monitor_from_point(cx as f64, cy as f64)
                .ok()
                .flatten()
                .or_else(|| window.current_monitor().ok().flatten())
                .or_else(|| window.primary_monitor().ok().flatten());

            if let Some(m) = monitor {
                let scale = m.scale_factor();
                let mp = m.position();
                let ms = m.size();
                // Avoid the macOS top menu bar, about 28pt tall
                let top_margin = (28.0 * scale) as i32;
                let target_x = mp.x;
                let target_y = mp.y + top_margin;
                let target_w = ms.width;
                let target_h = ms.height.saturating_sub(top_margin as u32);

                let _ = window.set_position(PhysicalPosition::new(target_x, target_y));
                let _ = window.set_size(PhysicalSize::new(target_w, target_h));
            } else {
                toggle_window_maximize(&window);
            }
        }
    }
    #[cfg(not(any(windows, target_os = "macos")))]
    {
        toggle_window_maximize(&window);
    }
}

// ---- In-app update (updater.rs): check / pre-download / install orchestration, driving the frontend card via events ----

/// Payload of the frontend "update" event: a flat structure branching on state (available / downloading /
/// ready / launching / error), unused fields left empty
#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdateEvent {
    state: &'static str,
    current_version: String,
    new_version: String,
    release_url: String,
    notes: String,
    downloaded_bytes: u64,
    total_bytes: u64,
    message: String,
}

/// Synchronous return of the manual check (check_update command); the frontend shows a toast from it
#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "kind")]
enum CheckOutcome {
    UpToDate { current: String },
    Available { current: String, new_version: String },
    Failed { message: String },
}

fn current_version(app: &AppHandle) -> String {
    app.package_info().version.to_string()
}

/// Truncate release notes (counted in characters, so an over-long body can't blow up the frontend card; the frontend also has max-height)
fn truncate_chars(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        format!("{}…", s.chars().take(n).collect::<String>())
    }
}

fn update_event(state: &'static str, current: &str, rel: Option<&Release>) -> UpdateEvent {
    UpdateEvent {
        state,
        current_version: current.to_string(),
        new_version: rel.map(|r| r.version.clone()).unwrap_or_default(),
        release_url: rel.map(|r| r.url.clone()).unwrap_or_default(),
        notes: rel.map(|r| truncate_chars(&r.notes, 400)).unwrap_or_default(),
        downloaded_bytes: 0,
        total_bytes: rel.map(|r| r.asset_size).unwrap_or(0),
        message: String::new(),
    }
}

/// Run one check (shared by manual/automatic): record last_check only when a Release is fetched (network failures
/// are not recorded; retry the next hour); on a new version, emit + silently pre-download (no waiting at install time).
/// Failures are returned only to the manual caller for display; the automatic path discards them
fn do_check(app: &AppHandle) -> CheckOutcome {
    let state = app.state::<AppState>();
    {
        let mut u = state.update.lock().unwrap();
        if u.checking {
            return CheckOutcome::Failed { message: "A check is already in progress".into() };
        }
        u.checking = true;
    }
    let current = current_version(app);
    let outcome = match updater::fetch_latest(&format!("zcode-speed-panel/{current}")) {
        None => CheckOutcome::Failed { message: "Network error or Release information unavailable".into() },
        Some(rel) => {
            {
                let mut u = state.update.lock().unwrap();
                u.last_check_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
            }
            if updater::is_newer(&rel.tag, &current) {
                let ev = update_event("available", &current, Some(&rel));
                state.update.lock().unwrap().latest = Some(rel.clone());
                let _ = app.emit("update", ev);
                let new_version = rel.version.clone();
                spawn_download(app.clone(), rel);
                CheckOutcome::Available { current, new_version }
            } else {
                CheckOutcome::UpToDate { current }
            }
        }
    };
    state.update.lock().unwrap().checking = false;
    outcome
}

/// Background pre-download of the installer: progress is pushed as "update" events (250ms throttle); on completion emit
/// ready; if the user already clicked "Update now" (install_when_ready), launch the install right away.
/// Automatic pre-download failures are fully silent (retry at install time); failures while waiting to install are the only ones that emit error
fn spawn_download(app: AppHandle, rel: Release) {
    let current = current_version(&app);
    {
        let state = app.state::<AppState>();
        let mut u = state.update.lock().unwrap();
        if let Some((tag, _)) = &u.downloaded {
            if *tag == rel.tag {
                // This version was already pre-downloaded (the frontend also hits this path to restore state after a refresh)
                drop(u);
                let _ = app.emit("update", update_event("ready", &current, Some(&rel)));
                return;
            }
        }
        if u.downloading {
            return;
        }
        u.downloading = true;
    }
    std::thread::spawn(move || {
        let mut ev = update_event("downloading", &current, Some(&rel));
        let mut last_emit = std::time::Instant::now();
        let progress_app = app.clone();
        let result = updater::download(&rel, &format!("zcode-speed-panel/{current}"), &mut |done, total| {
            if last_emit.elapsed() >= Duration::from_millis(250) {
                last_emit = std::time::Instant::now();
                ev.downloaded_bytes = done;
                ev.total_bytes = total;
                let _ = progress_app.emit("update", ev.clone());
            }
        });
        match result {
            Ok(path) => {
                let mut ev_ready = update_event("ready", &current, Some(&rel));
                ev_ready.downloaded_bytes = rel.asset_size;
                let launch = {
                    let state = app.state::<AppState>();
                    let mut u = state.update.lock().unwrap();
                    u.downloading = false;
                    u.downloaded = Some((rel.tag.clone(), path));
                    u.install_when_ready
                };
                let _ = app.emit("update", ev_ready);
                if launch {
                    launch_update(&app);
                }
            }
            Err(msg) => {
                let wait = {
                    let state = app.state::<AppState>();
                    let mut u = state.update.lock().unwrap();
                    u.downloading = false;
                    let wait = u.install_when_ready;
                    u.install_when_ready = false; // after a failure, wait for the user to click again; no automatic retry
                    wait
                };
                if wait {
                    // The user is waiting for the install but it can't start: tell them truthfully (the only case we interrupt for;
                    // silence would make the "Update now" button seem broken)
                    let mut ev = update_event("error", &current, Some(&rel));
                    ev.message = msg;
                    let _ = app.emit("update", ev);
                }
            }
        }
    });
}

/// Launch the install: on Windows run the NSIS installer then exit the app (the installer takes over; wait 600ms
/// before exiting so the installer doesn't hit the still-exiting process lock); on macOS open the dmg for the user to drag into
/// Applications (the app doesn't exit; the old version keeps running until the user restarts)
fn launch_update(app: &AppHandle) {
    let state = app.state::<AppState>();
    let rel = state.update.lock().unwrap().latest.clone();
    let downloaded = state.update.lock().unwrap().downloaded.clone();
    let (Some(rel), Some((tag, path))) = (rel, downloaded) else {
        return;
    };
    if tag != rel.tag {
        return; // don't install a stale artifact (pre-download re-fetches a new package when latest changes)
    }
    let mut ev = update_event("launching", &current_version(app), Some(&rel));
    #[cfg(target_os = "windows")]
    let msg = "Installer launched; the app will exit shortly…".to_string();
    #[cfg(target_os = "macos")]
    let msg = "Disk image opened: drag zcode-speed-panel into Applications to overwrite-install".to_string();
    ev.message = msg;
    let _ = app.emit("update", ev);
    if updater::launch_installer(&path).is_err() {
        let mut ev = update_event("error", &current_version(app), Some(&rel));
        ev.message = "Failed to launch the installer".into();
        let _ = app.emit("update", ev);
        return;
    }
    #[cfg(target_os = "windows")]
    {
        save_all(app);
        let handle = app.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(600));
            handle.exit(0);
        });
    }
}

/// Manual check (clicking the version number at the bottom-right of the footer): synchronously returns the result for a frontend toast;
/// when an update exists, the card is rendered by the "update" event (this command only handles the result toast)
#[tauri::command]
fn check_update(app: AppHandle) -> CheckOutcome {
    do_check(&app)
}

/// Frontend "Update now" button: already pre-downloaded → launch the install directly; otherwise mark it pending and make sure
/// the download thread is running (installs automatically when ready, no second click needed)
#[tauri::command]
fn install_update(app: AppHandle) -> Result<(), String> {
    let state = app.state::<AppState>();
    let rel = state.update.lock().unwrap().latest.clone().ok_or("No update available")?;
    let ready = {
        let u = state.update.lock().unwrap();
        matches!(&u.downloaded, Some((tag, _)) if *tag == rel.tag)
    };
    if ready {
        launch_update(&app);
        return Ok(());
    }
    state.update.lock().unwrap().install_when_ready = true;
    spawn_download(app.clone(), rel); // internal no-op if already downloading
    Ok(())
}

/// Current version (shown at the bottom-right of the footer, sourced from tauri.conf.json)
#[tauri::command]
fn app_version(app: AppHandle) -> String {
    current_version(&app)
}

// ---- Autostart (autostart.rs): read/write commands for the settings dialog's "Autostart" section ----
// The registry / LaunchAgent is the source of truth; get reads back the real state (the frontend cache is not trusted)

#[tauri::command]
fn autostart_get() -> String {
    autostart::current_mode().as_str().to_string()
}

/// Returns the mode actually in effect (read back after writing; on failure the error is passed truthfully to the frontend)
#[tauri::command]
fn autostart_set(mode: String) -> Result<String, String> {
    let parsed = autostart::AutostartMode::parse(&mode);
    autostart::set_mode(parsed)?;
    Ok(autostart::current_mode().as_str().to_string())
}

/// Open a link in the system default browser (the release notes page). <a> navigation inside the WebView is uncontrollable,
/// so the backend always opens it; only https is accepted, preventing the frontend from injecting protocols like file://
#[tauri::command]
fn open_url(url: String) -> Result<(), String> {
    if !url.starts_with("https://") {
        return Err("Only https links are supported".into());
    }
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        // CREATE_NO_WINDOW: fixes the cmd window flashing briefly
        std::process::Command::new("cmd")
            .args(["/C", "start", "", &url])
            .creation_flags(0x0800_0000)
            .spawn()
            .map(|_| ())
            .map_err(|e| format!("Failed to open link: {e}"))
    }
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open")
            .arg(&url)
            .spawn()
            .map(|_| ())
            .map_err(|e| format!("Failed to open link: {e}"))
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        let _ = url;
        Err("Not supported on this platform".into())
    }
}

/// Background update-check thread: sleeps 8s after startup (avoiding the startup SQLite/IO peak), then checks once;
/// afterwards it wakes hourly but only sends a real request ≥24h after the last successful check (once a day).
/// Failures are swallowed inside fetch_latest; the thread never disturbs the user
fn update_loop(app: AppHandle) {
    std::thread::sleep(Duration::from_secs(8));
    let _ = do_check(&app);
    loop {
        std::thread::sleep(Duration::from_secs(3600));
        let due = {
            let state = app.state::<AppState>();
            let u = state.update.lock().unwrap();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            now - u.last_check_ms >= 24 * 3600 * 1000
        };
        if due {
            let _ = do_check(&app);
        }
    }
}

fn show_main(app: &AppHandle) {
    if let Some(win) = app.get_webview_window("main") {
        // mac: when the window goes hidden→shown, hint "the app lives in the menu bar" (no Dock icon, so the hint
        // helps the user find the entry after closing the window); don't disturb when already visible (e.g., re-launch raise)
        #[cfg(target_os = "macos")]
        let was_hidden = !win.is_visible().unwrap_or(true);
        let _ = win.unminimize();
        let _ = win.show();
        let _ = win.set_focus();
        #[cfg(target_os = "macos")]
        if was_hidden {
            let _ = app.emit("tray-hint", ());
        }
    }
}

fn hide_main(app: &AppHandle) {
    if let Some(win) = app.get_webview_window("main") {
        let _ = win.hide();
    }
}

fn toggle_main(app: &AppHandle) {
    if let Some(win) = app.get_webview_window("main") {
        if win.is_visible().unwrap_or(false) && !win.is_minimized().unwrap_or(false) {
            let _ = win.hide();
        } else {
            show_main(app);
        }
    }
}

/// Window moved: write the position back to memory per the current mode, throttled persistence (at most once per 2s while dragging)
fn on_window_moved(app: &AppHandle, pos: PhysicalPosition<i32>) {
    let state = app.state::<AppState>();
    let mode = *state.mode.lock().unwrap();
    {
        let mut p = state.persist.lock().unwrap();
        match mode {
            Mode::Full => p.full_pos = Some((pos.x, pos.y)),
            Mode::Float => p.float_pos = Some((pos.x, pos.y)),
        }
    }
    let due = {
        let mut last = state.last_pos_save.lock().unwrap();
        let due = last.map_or(true, |t| t.elapsed() > Duration::from_secs(2));
        if due {
            *last = Some(std::time::Instant::now());
        }
        due
    };
    if due {
        save_all(app);
    }
}

/// Tray status: the top menu status item text + tray tooltip. Generated from the snapshot state
/// (generating/estimating/standby); written only when the text changes (avoids re-setting every 700ms)
fn update_tray_status(app: &AppHandle, s: &Snapshot) {
    let state_word = if s.is_live || s.is_starting {
        "Generating"
    } else if s.is_estimating {
        "Estimating"
    } else {
        "Standby"
    };
    let text = if s.is_live || s.is_starting {
        format!("Generating {:.1} t/s", s.current_tps)
    } else if s.is_estimating {
        format!("Estimating ≈{:.1} t/s", s.current_tps)
    } else {
        "Standby".to_string()
    };
    let state = app.state::<AppState>();
    {
        let mut last = state.tray_status_last.lock().unwrap();
        if *last == text {
            return;
        }
        *last = text.clone();
    }
    if let Some(item) = state.tray_status.lock().unwrap().as_ref() {
        let _ = item.set_text(text);
    }
    if let Some(tray) = app.tray_by_id("main-tray") {
        let _ = tray.set_tooltip(Some(&format!("ZCode Speed Dashboard · {state_word}")));
    }
}

/// Background polling thread: incrementally parses the model-io files and pushes snapshots
fn poller(app: AppHandle) {
    loop {
        let payload = build_payload(&app);
        update_tray_status(&app, &payload.snapshot);
        // After the multi-task (≥2 processes) debounce, raise/collapse the pet window's per-task row space
        update_pet_task_extra(&app, payload.snapshot.tasks.len() >= 2);
        let _ = app.emit("metrics", &payload);
        std::thread::sleep(Duration::from_millis(700));
    }
}

fn main() {
    tauri::Builder::default()
        // Raise the existing window on duplicate launch
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            show_main(app);
        }))
        .manage(AppState {
            engine: Mutex::new(Engine::new()),
            mode: Mutex::new(Mode::Full),
            style: Mutex::new(FloatStyle::Gauge),
            live: Mutex::new(LiveIo::new()),
            net: Mutex::new(netio::NetIo::new()),
            debug: Mutex::new(DebugLog::new()),
            persist: Mutex::new(Persisted::default()),
            last_pos_save: Mutex::new(None),
            tray_status: Mutex::new(None),
            tray_status_last: Mutex::new(String::new()),
            tray_hint_pending: Mutex::new(cfg!(target_os = "macos")),
            saved_max_rect: Mutex::new(None),
            round_tps: Mutex::new((0.0, 0, false)),
            round_was_inflight: Mutex::new(false),
            drift: Mutex::new(RoundDrift::new()),
            cal_saved: Mutex::new(Vec::new()),
            pet_task_streak: Mutex::new(0),
            pet_task_extra: Mutex::new(0.0),
            update: Mutex::new(UpdateMem::default()),
        })
        .invoke_handler(tauri::generate_handler![
            snapshot,
            model_stats,
            chart_stats,
            set_mode,
            set_float_style,
            set_float_size,
            quit_app,
            toggle_maximize_safe,
            recalibrate,
            tray_hint_once,
            check_update,
            install_update,
            app_version,
            open_url,
            autostart_get,
            autostart_set
        ])
        .setup(|app| {
            // mac activation policy **dynamic switching** (apply_mode sets it per mode, no longer fixed):
            // full panel = Regular (has a Dock icon — macOS treats only Regular apps as
            // "proper apps", where the green traffic light gets a native fullscreen Space; Accessory is always assistant
            // fullscreen: fills the screen but the menu bar remains, concluded by testing on 2026-09-19); collapsed floating window =
            // Accessory (hides the Dock icon and lives in the menu bar; the app doesn't quit)

            // mac: the custom app menu intercepts Cmd+Q as "collapse to floating window" (no system
            // quit item registered), plus an edit menu to keep the WebView's Cmd+C/V/X/A shortcuts
            #[cfg(target_os = "macos")]
            {
                macos_ui::install(app)?;
                app.on_menu_event(|app, ev| {
                    if ev.id().as_ref() == "collapse-to-float" {
                        save_all(app);
                        collapse_to_float(app);
                    }
                });
            }

            // ---- System tray ----
            // Top status item (disabled and unclickable; the poller refreshes its text from the snapshot each tick)
            let status = MenuItem::with_id(app, "status", "Standby", false, None::<&str>)?;
            let show = MenuItem::with_id(app, "show", "Show Panel", true, None::<&str>)?;
            let hide = MenuItem::with_id(app, "hide", "Hide to Tray", true, None::<&str>)?;
            let toggle_float =
                MenuItem::with_id(app, "toggle-float", "Floating Window / Full Panel", true, None::<&str>)?;
            let quit = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
            let sep = PredefinedMenuItem::separator(app)?;
            let menu = Menu::with_items(
                app,
                &[&status, &sep, &show, &hide, &toggle_float, &quit],
            )?;
            app.state::<AppState>().tray_status.lock().unwrap().replace(status);

            let icon = tauri::image::Image::from_bytes(include_bytes!("../icons/32x32.png"))?;
            TrayIconBuilder::with_id("main-tray")
                .icon(icon)
                .tooltip("ZCode Speed Dashboard")
                .menu(&menu)
                .show_menu_on_left_click(false)
                .on_menu_event(|app, ev| match ev.id().as_ref() {
                    "show" => show_main(app),
                    "hide" => hide_main(app),
                    "toggle-float" => {
                        let cur = *app.state::<AppState>().mode.lock().unwrap();
                        switch_mode(app, if cur == Mode::Float { Mode::Full } else { Mode::Float });
                    }
                    "quit" => {
                        save_all(app);
                        app.exit(0);
                    }
                    _ => {}
                })
                .on_tray_icon_event(|tray, ev| {
                    if let TrayIconEvent::Click {
                        button: MouseButton::Left,
                        button_state: MouseButtonState::Up,
                        ..
                    } = ev
                    {
                        toggle_main(tray.app_handle());
                    }
                })
                .build(app)?;

            // ---- Close button = collapse to floating window; remember position on move (each mode independently) ----
            let win_handle = app.handle().clone();
            app.get_webview_window("main")
                .unwrap()
                .on_window_event(move |event| match event {
                    WindowEvent::CloseRequested { api, .. } => {
                        api.prevent_close();
                        // Clicking close = immediately become the floating window (don't hide the tray; full exit goes through the tray/context menu)
                        collapse_to_float(&win_handle);
                    }
                    WindowEvent::Moved(pos) => on_window_moved(&win_handle, *pos),
                    _ => {}
                });

            // ---- Show the window only after restoring the last display mode, style, and position, avoiding a full-size flash ----
            let persisted = load_persisted();
            let mode = Mode::parse(&persisted.mode);
            let style = FloatStyle::parse(&persisted.style);
            {
                let state = app.state::<AppState>();
                *state.mode.lock().unwrap() = mode;
                *state.style.lock().unwrap() = style;
                *state.persist.lock().unwrap() = persisted;
            }
            // Restore the previously learned coefficient samples (missing/corrupt/over-14-day-old file → keep the prior 600):
            // readings are usable right after a dev hot restart or boot, instead of re-converging from cold start each time
            {
                let state = app.state::<AppState>();
                let samples = load_cal_samples();
                if !samples.is_empty() {
                    let n = state.live.lock().unwrap().restore_cal(samples);
                    eprintln!("[zcode-speed-panel] restored {n} calibration samples");
                }
                *state.cal_saved.lock().unwrap() = state.live.lock().unwrap().cal_state();
            }
            let window = app.get_webview_window("main").unwrap();
            let p = app.state::<AppState>().persist.lock().unwrap().clone();
            let pet_extra = *app.state::<AppState>().pet_task_extra.lock().unwrap();
            apply_mode(&window, mode, style, &p, pet_extra);
            // Follow ZCode launch (autostart.rs follow mode, boot with --zcode-follow):
            // silent standby — no window shown, tray only; the watcher thread shows the window once it detects the ZCode process.
            // The single-instance plugin ensures this flag only affects the "first instance at boot" (with an existing instance, this process
            // never reaches setup, and the raise callback directly shows the old instance's window)
            let follow_boot = autostart::follow_requested();
            if follow_boot {
                eprintln!("[zcode-speed-panel] following ZCode launch: silent standby (tray resident)");
                let watch = app.handle().clone();
                std::thread::spawn(move || {
                    // The system is busy right after boot; wait 3s before starting detection
                    std::thread::sleep(Duration::from_secs(3));
                    loop {
                        if autostart::zcode_running() {
                            eprintln!("[zcode-speed-panel] ZCode process detected, showing panel");
                            show_main(&watch);
                            break;
                        }
                        std::thread::sleep(Duration::from_secs(2));
                    }
                });
            } else {
                let _ = window.show();
            }
            // The mac startup hint is not emitted here: setup runs before the event loop/WKWebView load,
            // so it would be dropped immediately — the frontend instead claims it via invoke `tray_hint_once` once ready (one-shot)

            // ---- Start the polling thread ----
            let poll_handle = app.handle().clone();
            std::thread::spawn(move || poller(poll_handle));

            // ---- Start the update-check thread (once 8s after startup, once a day while resident, silent) ----
            let update_handle = app.handle().clone();
            std::thread::spawn(move || update_loop(update_handle));
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building zcode-speed-panel")
        .run(|app, event| match event {
            // Safety net: exit requests without an explicit exit(0) (e.g., the last window closing on mac,
            // or an exit before system logout) are always prevented and collapsed to the floating window — the only real exits are the tray
            // "Quit" and the floating window context-menu "Quit" (on app.exit, code=Some, let it through)
            tauri::RunEvent::ExitRequested { code: None, api, .. } => {
                api.prevent_exit();
                save_all(app);
                collapse_to_float(app);
            }
            // Save once more before the real exit (best-effort)
            tauri::RunEvent::Exit => {
                save_all(app);
            }
            _ => {}
        });
}

/// macOS-only UI: the app menu bar. Cmd+Q is intercepted as "collapse to floating window" (in Accessory mode the
/// app has no Dock/Cmd+Tab entry, and quitting directly would make users think the app vanished); no system
/// quit item is registered in the menu, so quitting only goes through the tray and the floating window context menu.
/// The edit submenu keeps Cmd+C/V/X/A, otherwise the WebView's text-editing shortcuts stop working
#[cfg(target_os = "macos")]
mod macos_ui {
    use super::*;
    use tauri::menu::{MenuItem, PredefinedMenuItem, Submenu};

    pub fn install(app: &tauri::App) -> tauri::Result<()> {
        let collapse = MenuItem::with_id(
            app,
            "collapse-to-float",
            "Hide as Floating Window",
            true,
            Some("CmdOrCtrl+Q"),
        )?;
        let app_menu = Submenu::with_id_and_items(
            app,
            "app",
            "zcode-speed-panel",
            true,
            &[&collapse],
        )?;
        let edit_menu = Submenu::with_id_and_items(
            app,
            "edit",
            "Edit",
            true,
            &[
                &PredefinedMenuItem::cut(app, None)?,
                &PredefinedMenuItem::copy(app, None)?,
                &PredefinedMenuItem::paste(app, None)?,
                &PredefinedMenuItem::select_all(app, None)?,
            ],
        )?;
        let menu = Menu::with_items(app, &[&app_menu, &edit_menu])?;
        app.set_menu(menu)?;
        Ok(())
    }
}
