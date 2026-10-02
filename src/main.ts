import "./style.css";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { ArcGauge, BadgeGauge, MiniGauge, SPEED_TIERS, drawSpark, fmtBps, fmtBytes, fmtClock, fmtTokens, fmtTps, speedColor } from "./gauges";
import { PetWidget } from "./pet";
import { startMock, mockModelStats, type ConnStat, type Snapshot } from "./mock";
import { initModelStats } from "./model_stats";

interface SnapshotPayload {
  snapshot: Snapshot;
  rolloutDir: string;
  mode: string;
  floatStyle: string;
}

const $ = <T extends HTMLElement>(id: string): T => {
  const el = document.getElementById(id);
  if (!el) throw new Error(`missing #${id}`);
  return el as T;
};

const hasTauri = typeof (window as unknown as Record<string, unknown>).__TAURI_INTERNALS__ !== "undefined";

const isMac = navigator.userAgent.includes("Mac");
if (isMac) {
  document.body.classList.add("platform-mac");
}

async function tauriInvoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T | undefined> {
  if (!hasTauri) return undefined;
  const { invoke } = await import("@tauri-apps/api/core");
  return invoke<T>(cmd, args);
}

const gCurrent = new ArcGauge($("g-current"), {
  label: "Current Output Speed",
  unit: "token / s",
  color: "#22d3ee",
  color2: "#0ea5e9",
  kind: "speed",
  minScale: 60, // minimum scale 60 t/s so common speeds sit in the middle of the arc for readability
  tiers: SPEED_TIERS, // six tiers (0–40/40–80/80–160/160–240/240–320/320+), color follows the current speed
});

const gAvg = new ArcGauge($("g-avg"), {
  label: "Today's Average Speed",
  unit: "token / s",
  color: "#a78bfa",
  color2: "#8b5cf6",
  kind: "speed",
});

const gTotal = new ArcGauge($("g-total"), {
  label: "Today's Total Tokens",
  unit: "Today's Cumulative",
  color: "#34d399",
  color2: "#10b981",
  kind: "tokens",
});

// Top-right badge on the current-speed card: speed of the most recent completed call (on-disk basis, not realtime)
const gLast = new BadgeGauge($("g-last"), { tiers: SPEED_TIERS });
// Bottom-right badge on the current-speed card: fastest single call in the last 7 days (window and admission criteria: see the tooltip and metrics.rs)
const gPeak = new BadgeGauge($("g-peak"), { tiers: SPEED_TIERS, label: "Peak" });
// Top-right badge on the today-average card: average speed over the last 7 days (calls in window Σeff ÷ Σgen, same basis as today's average)
const gHistAvg = new BadgeGauge($("g-histavg"), { tiers: SPEED_TIERS, label: "History" });

const miniGauge = new MiniGauge($("mini-gauge"), { tiers: SPEED_TIERS });
// Top-right last-call ring on the mini gauge floating window (same as the full panel's corner badge, just smaller)
const miniLast = new BadgeGauge($("mini-last"), { tiers: SPEED_TIERS });
// Storage key bumped to v2 so existing users also get the new default (maid-deepseek-whale) once; later choices are remembered as usual
const PET_PACK_KEY = "petPack.v2";
let currentPetPack = localStorage.getItem(PET_PACK_KEY) ?? "maid-deepseek-whale";
const petWidget = new PetWidget($<HTMLCanvasElement>("pet-canvas"), currentPetPack);
petWidget.start();

// ---- Desktop pet wheel zoom: scroll up/down to resize the floating window (remembered by the backend, survives restarts) ----
const PET_BASE_SIZE = 200;
const PET_SIZE_MIN = 100;
const PET_SIZE_MAX = 480;
let petSize = Math.min(PET_SIZE_MAX, Math.max(PET_SIZE_MIN, Number(localStorage.getItem("petSize.v1")) || PET_BASE_SIZE));
$("float-pet").addEventListener(
  "wheel",
  (e) => {
    e.preventDefault();
    const next = petSize * (e.deltaY < 0 ? 1.08 : 1 / 1.08);
    const clamped = Math.min(PET_SIZE_MAX, Math.max(PET_SIZE_MIN, next));
    if (Math.round(clamped) === Math.round(petSize)) return;
    petSize = clamped;
    localStorage.setItem("petSize.v1", String(Math.round(clamped)));
    if (hasTauri) {
      tauriInvoke("set_float_size", { size: Math.round(clamped) }).catch(() => {});
    }
  },
  { passive: false }
);

// ---- Floating window / desktop pet context menu: restore window / quit ----
const floatMenu = $("float-menu");
const showFloatMenu = (x: number, y: number) => {
  floatMenu.style.display = "flex";
  const mw = floatMenu.offsetWidth || 110;
  const mh = floatMenu.offsetHeight || 60;
  floatMenu.style.left = `${Math.max(0, Math.min(x, window.innerWidth - mw - 2))}px`;
  floatMenu.style.top = `${Math.max(0, Math.min(y, window.innerHeight - mh - 2))}px`;
};
const hideFloatMenu = () => {
  floatMenu.style.display = "none";
};
for (const id of ["float-pet", "float-gauge", "float-pill"]) {
  $(id).addEventListener("contextmenu", (e) => {
    e.preventDefault();
    showFloatMenu(e.clientX, e.clientY);
  });
}
window.addEventListener("mousedown", (e) => {
  if (!floatMenu.contains(e.target as Node)) hideFloatMenu();
  if (!styleDropdown.contains(e.target as Node)) setStyleDropdownOpen(false);
});
window.addEventListener("blur", () => {
  hideFloatMenu();
  setStyleDropdownOpen(false);
});
window.addEventListener("keydown", (e) => {
  if (e.key === "Escape") setStyleDropdownOpen(false);
});
$("float-menu-restore").addEventListener("click", () => {
  hideFloatMenu();
  requestMode("full");
});
$("float-menu-quit").addEventListener("click", () => {
  hideFloatMenu();
  tauriInvoke("quit_app");
});

// ---- Desktop pet "always show last-call avg speed": pet context-menu checkbox + full-panel header toggle (same state) ----
// When checked the bubble always shows two lines (expands with the realtime speed while generating, no hover needed); visibility still follows
// the generation state, hidden while idle. Clicking the menu item does not close the menu so the checkbox state stays visible; clicking anywhere
// outside the menu closes it as usual.
// The header toggle is shown by CSS only for the desktop pet style
const PET_LAST_KEY = "petLastAlways.v1";
let petLastAlways = localStorage.getItem(PET_LAST_KEY) === "1";
const petLastControls = [$("float-menu-pet-last"), $("pet-last-toggle")];
const applyPetLast = () => {
  for (const el of petLastControls) el.classList.toggle("pet-last-on", petLastAlways);
  petWidget.setAlwaysLast(petLastAlways);
};
applyPetLast();
for (const el of petLastControls) {
  el.addEventListener("click", () => {
    petLastAlways = !petLastAlways;
    localStorage.setItem(PET_LAST_KEY, petLastAlways ? "1" : "0");
    applyPetLast();
  });
}

const sparkCanvas = $<HTMLCanvasElement>("spark");
const liveDot = $("live-dot");
const liveText = $("live-text");
const updatedAt = $("updated-at");
const subCurrent = $("sub-current");
const subAvg = $("sub-avg");
const subTotal = $("sub-total");
const stDir = $("st-dir");
const stCalls = $("st-calls");
const stSessions = $("st-sessions");
const stLast = $("st-last");
const chartMax = $("chart-max");
const taskCard = $("task-card");
const taskList = $("task-list");
const netCard = $("net-card");
const netScope = $("net-scope");
const netUpBpsEl = $("net-up-bps");
const netDownBpsEl = $("net-down-bps");
const netConnsEl = $("net-conns");
const netConnCli = $("net-conn-cli");
const netConnApp = $("net-conn-app");
const netCliConnsEl = $("net-cli-conns");
const netAppConnsEl = $("net-app-conns");
const netSessUpEl = $("net-sess-up");
const netSessDownEl = $("net-sess-down");
const netUpTodayEl = $("net-up-today");
const netDownTodayEl = $("net-down-today");
const floatTps = $("float-tps");
const floatDot = $("float-dot");
const floatLast = $("float-last");

let lastSpark: number[] = [];
let lastNowMs = 0;
let sparkColor = "#22d3ee";
// ---- Chart time range (15m/1h/6h/24h, default 15 minutes, choice remembered in localStorage) ----
// The 15-minute range uses the today spark from the metrics payload (the backend mixes realtime speed into the tail bucket, zero extra
// queries); longer ranges use the chart_stats command (the usage library computes 90 buckets on the fly, fetched every 5s),
// and the frontend mixes realtime speed into the newest bucket — both ranges share the same tail-bucket semantics, so switching causes no jump
type ChartRange = 15 | 60 | 360 | 1440;
const CHART_RANGES: { value: ChartRange; label: string; bucketLabel: string; gridMs: number }[] = [
  { value: 15, label: "15 min", bucketLabel: "10s buckets", gridMs: 5 * 60_000 },
  { value: 60, label: "1 hour", bucketLabel: "40s buckets", gridMs: 10 * 60_000 },
  { value: 360, label: "6 hours", bucketLabel: "4min buckets", gridMs: 60 * 60_000 },
  { value: 1440, label: "24 hours", bucketLabel: "16min buckets", gridMs: 4 * 3_600_000 },
];
const CHART_RANGE_KEY = "chartRange.v1";
const storedChartRange = Number(localStorage.getItem(CHART_RANGE_KEY));
let chartRange: ChartRange = CHART_RANGES.some((r) => r.value === storedChartRange)
  ? (storedChartRange as ChartRange)
  : 15;
let chartCache: { buckets: number[]; bucketMs: number; nowMs: number } | null = null;
let chartTimer = 0;
// Latest tick's realtime state (used for mixing into the long-range tail bucket)
let liveTpsNow = 0;
let liveActive = false;
// ---- Chart card view (overall speed chart / model details, mutually exclusive toggle, choice remembered) ----
type ChartView = "total" | "model";
const CHART_VIEW_KEY = "chartView.v1";
let chartView: ChartView = localStorage.getItem(CHART_VIEW_KEY) === "model" ? "model" : "total";

function chartRangeCfg(): (typeof CHART_RANGES)[number] {
  return CHART_RANGES.find((r) => r.value === chartRange) ?? CHART_RANGES[0];
}

/** Long-range data fetch: on failure keep the old cache silently (browser preview without Tauri also stays silent) */
async function refreshChartStats() {
  const p = await tauriInvoke<{
    windowMin: number;
    bucketMs: number;
    nowMs: number;
    buckets: number[];
  }>("chart_stats", { windowMin: chartRange });
  if (p && p.windowMin === chartRange) {
    chartCache = { buckets: p.buckets, bucketMs: p.bucketMs, nowMs: p.nowMs };
    redrawSpark();
  }
}

function redrawSpark() {
  if (chartRange === 15) {
    if (lastSpark.length) drawSpark(sparkCanvas, lastSpark, sparkColor, lastNowMs);
    return;
  }
  if (chartCache && chartCache.buckets.length >= 2) {
    const values = chartCache.buckets.slice();
    // Realtime tail-bucket mixing (matches the 15-minute range's backend behavior): while generating/estimating the newest bucket is
    // temporarily filled with the current speed; the next 5s fetch replaces it with real on-disk data
    if (liveActive && liveTpsNow > 0) values[values.length - 1] = liveTpsNow;
    drawSpark(sparkCanvas, values, sparkColor, lastNowMs, {
      bucketMs: chartCache.bucketMs,
      gridMs: chartRangeCfg().gridMs,
    });
    return;
  }
  // Browser preview (no Tauri): long ranges have no data source yet, so reuse the 15-minute mock data for styling
  if (!hasTauri && lastSpark.length) {
    drawSpark(sparkCanvas, lastSpark, sparkColor, lastNowMs, { gridMs: chartRangeCfg().gridMs });
  }
}

// Task card hide hysteresis: when the task count flickers around the 1↔2 boundary (subagents starting/stopping, streaming threshold edge),
// wait for 3 consecutive ticks (~2s) below 2 rows before hiding, so the chart card below doesn't jump up and down
let taskHideStreak = 3;

function statusClass(s: Snapshot): string {
  if (s.isLive || s.isStarting) return "dot live";
  if (s.isEstimating) return "dot est";
  return "dot idle";
}

/** Network Monitor card: system-wide measured speed + session upload estimate breakdown.
 *  System-wide = real interface counter values; session = token × coefficient estimate (with ≈).
 *  The whole card is hidden when the interface is unavailable (stub platforms) */
function renderNet(s: Snapshot) {
  if (!s.netAvailable) {
    netCard.hidden = true;
    return;
  }
  netCard.hidden = false;
  netUpBpsEl.textContent = fmtBps(s.netUpBps);
  netDownBpsEl.textContent = fmtBps(s.netDownBps);
  netSessUpEl.textContent = fmtBytes(s.netSessUpToday);
  netSessDownEl.textContent = fmtBytes(s.netSessDownToday);
  netUpTodayEl.textContent = fmtBytes(s.netUpToday);
  netDownTodayEl.textContent = fmtBytes(s.netDownToday);

  // Connection attribution (Windows only): each connection's remote endpoint + owning process (type + pid) attached as a tooltip.
  // Both groups are ZCode's own processes: session = CLI (conversation API traffic),
  // desktop = Electron shell (telemetry and other non-conversation traffic), no other apps
  netConnsEl.style.display = s.netConnsAvailable ? "" : "none";
  if (s.netConnsAvailable) {
    netCliConnsEl.textContent = String(s.netCliConns);
    netAppConnsEl.textContent = String(s.netAppConns);
    const connLines = (list: ConnStat[]) => list.map((r) => `${r.remote} · ${r.proc || "?"}(${r.pid})`);
    netConnCli.title = s.netCliConnList.length
      ? `ZCode session process (CLI, conversation API traffic) connections:\n${connLines(s.netCliConnList).join("\n")}`
      : "ZCode session process has no outgoing connections";
    netConnApp.title = s.netAppConnList.length
      ? `ZCode desktop process (Electron main/render/GPU/utility — telemetry, updates, and other non-conversation traffic) connections:\n${connLines(s.netAppConnList).join("\n")}`
      : "ZCode desktop process has no outgoing connections";
  }
  netScope.textContent = s.netConnsAvailable ? "System-wide = all apps on this machine (not just ZCode)" : "System-wide = all apps on this machine";
}

/** Cache hit rate = cache_read ÷ input (in the usage library, input already counts all prompt tokens,
 *  cache reads included; adding cache_read to the denominator again would double-count; cache_creation is
 *  always 0 across libraries, kept in the denominator defensively for providers that may report it separately) */
const cacheHitRate = (s: Snapshot): string => {
  const prompt = s.inputTokens + s.cacheCreationTokens;
  if (prompt <= 0) return "0%";
  return ((s.cacheReadTokens / prompt) * 100).toFixed(1) + "%";
};

function onSnapshot(s: Snapshot) {
  gCurrent.setTarget(s.currentTps, s.isEstimating, s.isStarting);
  gAvg.setTarget(s.avgTps);
  gLast.setTarget(s.lastCallTps);
  gPeak.setTarget(s.histMaxTps);
  gHistAvg.setTarget(s.histAvgTps);
  gTotal.setTarget(s.totalTokens);
  miniGauge.setTarget(s.currentTps, s.isEstimating, s.isStarting);
  miniLast.setTarget(s.lastCallTps);
  renderNet(s);

  // Concurrent task details: shown when ≥2 tasks (hidden for a single task, takes no layout space).
  // One CLI process = one row, row totals = the current speed gauge (file growth apportioned by byte share);
  // one process carrying multiple sessions (a new task in the same ZCode window reuses the app-server process,
  // inseparable at the byte level) counts as one combined row and joins the task total via n_sessions
  const tasks = s.tasks ?? [];
  const taskCount = tasks.reduce((n, t) => n + Math.max(1, t.nSessions || 0), 0);
  if (taskCount >= 2) {
    taskHideStreak = 0;
    taskCard.hidden = false;
    taskList.textContent = "";
    for (const t of tasks) {
      const row = document.createElement("div");
      row.className = "task-row";
      const dot = document.createElement("span");
      dot.className = t.streaming ? "dot live" : "dot idle";
      const label = document.createElement("span");
      label.className = "task-sess";
      label.textContent =
        t.nSessions >= 2
          ? `${t.nSessions} sessions (combined) · PID ${t.pid}`
          : t.session
            ? `Session …${t.session.slice(-6)} · PID ${t.pid}`
            : `Unattributed process ${t.pid}`;
      const tps = document.createElement("span");
      tps.className = "task-tps";
      tps.textContent = t.streaming ? `${fmtTps(t.tps)} t/s` : "Idle";
      if (t.streaming) tps.style.color = speedColor(t.tps, SPEED_TIERS);
      row.append(dot, label, tps);
      taskList.append(row);
    }
  } else if (taskHideStreak < 3) {
    taskHideStreak++;
    if (taskHideStreak >= 3) taskCard.hidden = true;
  }

  subCurrent.textContent = s.isStarting
    ? "Generation started · Waiting for model output (Collecting…)"
    : s.liveSource === "io"
      ? s.ramping
        ? "Measured live · Collecting… (30s sliding window establishing)"
        : `Measured live · Process streaming output (30s sliding window${taskCount >= 2 ? ` · ${taskCount} tasks aggregated` : ""})`
      : s.isEstimating
        ? "Generating · No incremental bytes in this segment, estimating from recent real speed ≈"
        : "Idle · No active generation tasks";
  subAvg.textContent = `Σoutput ÷ Σgeneration time · ${s.callsToday} calls today`;
  subTotal.textContent = `Output ${fmtTokens(s.outputTokens)} · Input ${fmtTokens(s.inputTokens)} · Cache hit rate ${cacheHitRate(s)}`;
  document.body.classList.toggle("live", s.isLive || s.isStarting);
  document.body.classList.toggle("est", s.isEstimating);
  const petState: "idle" | "running" | "estimating" | "starting" = s.isStarting
    ? "starting"
    : s.liveSource === "io"
      ? "running"
      : "idle";
  petWidget.setLive(s.currentTps, petState);
  // Per-process multi-task details: the pet bubble expands to per-task rows when ≥2 tasks (same semantics as the full
  // panel's task card; pass empty for single task / fallback / startup, the bubble only shows the aggregate)
  petWidget.setTasks(
    taskCount >= 2
      ? tasks.map((t) => ({
          label:
            t.nSessions >= 2
              ? `${t.nSessions}sessions·${t.pid}`
              : t.session
                ? `…${t.session.slice(-6)}`
                : `PID ${t.pid}`,
          tps: t.tps,
          streaming: t.streaming,
        }))
      : []
  );
  liveDot.className = statusClass(s);
  liveText.textContent = s.isLive || s.isStarting ? "Generating" : s.isEstimating ? "Estimating" : "Idle";
  updatedAt.textContent = `Updated at ${fmtClock(s.nowMs)}`;
  floatDot.className = statusClass(s);
  floatTps.textContent = s.isStarting
    ? "…"
    : (s.isEstimating && s.liveSource !== "io" ? "≈" : "") + fmtTps(s.currentTps);
  petWidget.setLast(s.lastCallTps);
  // Pill second line: last-call avg speed (on-disk basis), colored by speed tier, shows -- when there is no data
  floatLast.textContent = s.lastCallTps > 0 ? fmtTps(s.lastCallTps) : "--";
  floatLast.style.color = speedColor(s.lastCallTps, SPEED_TIERS);

  // Window title mirrors the realtime speed, directly visible in the taskbar / Alt+Tab
  const title = `${s.isLive || s.isStarting ? "▶" : s.isEstimating ? "≈" : "⏸"} ${s.isStarting ? "…" : fmtTps(s.currentTps)} t/s · ${s.callsToday} calls · ZCode Speed Panel`;
  document.title = title;
  try {
    getCurrentWindow().setTitle(title).catch(() => {});
  } catch {
    // Browser preview mode has no Tauri API
  }

  stDir.textContent = `Monitoring ${s.rolloutDir}`;
  stCalls.textContent = `Today's calls: ${s.callsToday}`;
  stSessions.textContent = `${s.sessionsToday} sessions`;
  stLast.textContent = `Last activity ${fmtClock(s.lastActivityMs)}`;

  lastSpark = s.spark;
  lastNowMs = s.nowMs;
  liveTpsNow = s.currentTps;
  liveActive = s.isLive || s.isEstimating;
  sparkColor = s.isLive ? "#22d3ee" : s.isEstimating ? "#fbbf24" : "#64748b";
  // Peak label reads from the currently shown range (15m = payload spark; long ranges = 5s cache + realtime)
  const shown = chartRange === 15 ? s.spark : (chartCache?.buckets ?? []);
  const peak = Math.max(10, ...shown, s.currentTps);
  chartMax.textContent = `Peak ${fmtTps(peak)} t/s`;
  redrawSpark();
}

window.addEventListener("resize", redrawSpark);

// ---- Chart time range dropdown (custom dropdown, same interaction as the floating-window style dropdown) ----
const chartDropdown = $("chart-window");
const chartRangeOptions = Array.from(
  $<HTMLElement>("chart-window-list").querySelectorAll<HTMLButtonElement>("button[data-value]"),
);

/** Chart card title built from the current view + current range (the range dropdown is shared by both views) */
function updateChartTitle() {
  const cfg = chartRangeCfg();
  $("chart-title").textContent =
    chartView === "model"
      ? `Model speed trends — last ${cfg.label} (${cfg.bucketLabel} · by model · token/s)`
      : `Output speed — last ${cfg.label} (${cfg.bucketLabel} · token/s, x-axis is real time)`;
}

function applyChartRangeUi() {
  const cfg = chartRangeCfg();
  $("chart-window-label").textContent = cfg.label;
  updateChartTitle();
  for (const opt of chartRangeOptions) {
    opt.classList.toggle("selected", Number(opt.dataset.value) === chartRange);
  }
}

function setChartDropdownOpen(open: boolean) {
  chartDropdown.classList.toggle("open", open);
  $<HTMLButtonElement>("chart-window-btn").setAttribute("aria-expanded", String(open));
}

function selectChartRange(r: ChartRange) {
  if (r === chartRange) {
    setChartDropdownOpen(false);
    return;
  }
  setChartDropdownOpen(false);
  chartRange = r;
  localStorage.setItem(CHART_RANGE_KEY, String(r));
  if (r === 15) chartCache = null;
  applyChartRangeUi();
  window.clearInterval(chartTimer);
  chartTimer = 0;
  if (r !== 15) {
    void refreshChartStats();
    chartTimer = window.setInterval(() => void refreshChartStats(), 5000);
  }
  // The model details view shares the time range with the overall chart: switching range refetches model stats immediately
  if (chartView === "model") modelStats.refresh();
  redrawSpark();
}

$<HTMLButtonElement>("chart-window-btn").addEventListener("click", () =>
  setChartDropdownOpen(!chartDropdown.classList.contains("open")),
);
for (const opt of chartRangeOptions) {
  opt.addEventListener("click", () => selectChartRange(Number(opt.dataset.value) as ChartRange));
}
window.addEventListener("mousedown", (e) => {
  if (chartDropdown.classList.contains("open") && !chartDropdown.contains(e.target as Node)) {
    setChartDropdownOpen(false);
  }
});
window.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && chartDropdown.classList.contains("open")) setChartDropdownOpen(false);
});
// On startup restore the previously selected range (long ranges fetch once immediately and start the 5s timer)
applyChartRangeUi();
if (chartRange !== 15) {
  void refreshChartStats();
  chartTimer = window.setInterval(() => void refreshChartStats(), 5000);
}

// ---- Mode and floating window style (custom dropdown replacing the native select: WebView2 popups are unreadable on light system themes) ----
function applyModeUi(mode: string) {
  document.body.classList.toggle("float-mode", mode === "float");
}

const STYLE_LABELS: Record<string, string> = {
  pet: "Pet",
  gauge: "Mini Gauge",
  pill: "Speed Pill",
};

let currentStyle = localStorage.getItem("floatStyle") ?? "gauge";
const styleDropdown = $("float-style");
const styleOptions = Array.from(
  $<HTMLElement>("float-style-list").querySelectorAll<HTMLButtonElement>("button[data-value]"),
);

function applyStyleUi(style: string) {
  currentStyle = style;
  document.body.classList.toggle("style-pet", style === "pet");
  document.body.classList.toggle("style-gauge", style === "gauge");
  document.body.classList.toggle("style-pill", style === "pill");
  $("float-style-label").textContent = STYLE_LABELS[style] ?? STYLE_LABELS.gauge;
  for (const opt of styleOptions) {
    opt.classList.toggle("selected", opt.dataset.value === style);
  }
}

function setStyleDropdownOpen(open: boolean) {
  styleDropdown.classList.toggle("open", open);
  $<HTMLButtonElement>("float-style-btn").setAttribute("aria-expanded", String(open));
}

function selectFloatStyle(style: string) {
  setStyleDropdownOpen(false);
  localStorage.setItem("floatStyle", style);
  applyStyleUi(style);
  if (document.body.classList.contains("float-mode")) {
    tauriInvoke("set_float_style", { style }).catch(() => {});
  }
}

function requestMode(mode: "full" | "float") {
  if (!hasTauri) {
    applyModeUi(mode);
    return;
  }
  tauriInvoke("set_mode", { mode, style: currentStyle }).catch(() => {});
}

$("btn-float").addEventListener("click", () => requestMode("float"));
$("float-gauge-expand").addEventListener("click", () => requestMode("full"));
$("float-pill-expand").addEventListener("click", () => requestMode("full"));
$("float-pet-expand").addEventListener("click", () => requestMode("full"));
$("float-pet-cycle").addEventListener("click", () => {
  currentPetPack = petWidget.cyclePack();
  localStorage.setItem(PET_PACK_KEY, currentPetPack);
});

// ---- Module visibility and ordering (header ⚙ settings modal) ----
// A module = a group of cards in the full panel main that can be toggled/reordered as a whole; the concurrent tasks
// card follows the Dashboard (not listed separately).
// Dashboard / Network Monitor / Speed Chart are shown by default. Config is stored in localStorage and survives restarts.
type ModuleId = "gauges" | "net" | "chart";
const MODULE_DEFS: { id: ModuleId; name: string; desc: string }[] = [
  { id: "gauges", name: "Dashboard", desc: "Current speed / Today's average / Today's total (includes concurrent task details when multiple tasks)" },
  { id: "net", name: "Network Monitor", desc: "System-wide upload/download speed · ZCode connection attribution · Today's totals" },
  { id: "chart", name: "Speed Chart", desc: "Overall speed chart (four time ranges) · Toggle to model speed trends" },
];
const MODULES_KEY = "modules.v1";
const MODULES_DEFAULT_ORDER: ModuleId[] = ["gauges", "net", "chart"];
const MODULES_DEFAULT_HIDDEN: ModuleId[] = [];

interface ModulesConfig {
  /** Global order of all modules (including hidden ones — re-checking puts them back in their original position; ordering applies to hidden rows too) */
  order: ModuleId[];
  /** Hidden module ids */
  hidden: ModuleId[];
}

/** Read localStorage with defensive sanitizing: corrupted JSON falls back to defaults; unknown ids are dropped, missing ids are appended in
 *  default order, duplicates removed — hand-edited/outdated configs can neither lose modules nor throw */
function loadModulesConfig(): ModulesConfig {
  const fallback = (): ModulesConfig => ({
    order: [...MODULES_DEFAULT_ORDER],
    hidden: [...MODULES_DEFAULT_HIDDEN],
  });
  try {
    const raw = localStorage.getItem(MODULES_KEY);
    if (!raw) return fallback();
    const parsed = JSON.parse(raw) as Partial<{ order: unknown; hidden: unknown }>;
    const known = new Set<string>(MODULES_DEFAULT_ORDER);
    const clean = (v: unknown): ModuleId[] => [
      ...new Set(Array.isArray(v) ? v.filter((x): x is ModuleId => typeof x === "string" && known.has(x)) : []),
    ];
    const order = clean(parsed.order);
    for (const id of MODULES_DEFAULT_ORDER) if (!order.includes(id)) order.push(id);
    return { order, hidden: clean(parsed.hidden) };
  } catch {
    return fallback();
  }
}

const moduleWraps = new Map<ModuleId, HTMLElement>(
  MODULE_DEFS.map((m) => [m.id, document.querySelector<HTMLElement>(`.module-wrap[data-module="${m.id}"]`)!]),
);
const mainEl = document.querySelector("main")!;
const footerEl = $("statusbar");
const modulesEmpty = $("modules-empty");
let modulesCfg = loadModulesConfig();

/** Reorder/hide modules per config: the wrapper is display:contents, cards remain flex items of main,
 *  order = the order array minus hidden ids; the footer always stays last. When everything is hidden show a
 *  placeholder (the header ⚙ can always reopen settings, but a blank page with no explanation looks broken).
 *  The network card has its own data-availability hidden logic (renderNet), independent of the module
 *  toggles; display requires both */
function applyModules() {
  for (const id of modulesCfg.order) {
    const wrap = moduleWraps.get(id);
    if (!wrap) continue;
    mainEl.insertBefore(wrap, footerEl);
    wrap.hidden = modulesCfg.hidden.includes(id);
  }
  modulesEmpty.hidden = modulesCfg.order.some((id) => !modulesCfg.hidden.includes(id));
  mainEl.insertBefore(modulesEmpty, footerEl);
}

const saveModulesConfig = () => {
  localStorage.setItem(MODULES_KEY, JSON.stringify(modulesCfg));
};
applyModules();

// ---- Settings modal: custom checkboxes (same style as .pet-chk, native checkbox disabled) + ↑↓ reordering, takes effect immediately ----
const settingsModal = $("settings-modal");
const settingsList = $("settings-modules");
const setSettingsOpen = (open: boolean) => {
  settingsModal.style.display = open ? "flex" : "none";
};

function renderSettingsRows() {
  settingsList.textContent = "";
  modulesCfg.order.forEach((id, idx) => {
    const def = MODULE_DEFS.find((m) => m.id === id)!;
    const shown = !modulesCfg.hidden.includes(id);
    const row = document.createElement("div");
    row.className = shown ? "settings-row" : "settings-row off";

    const toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = `ghost-btn settings-toggle${shown ? " on" : ""}`;
    toggle.title = shown ? "Hide this module" : "Show this module";
    const chk = document.createElement("span");
    chk.className = "pet-chk";
    chk.setAttribute("aria-hidden", "true");
    toggle.append(chk, document.createTextNode("Show"));
    toggle.addEventListener("click", () => {
      modulesCfg.hidden = shown
        ? [...modulesCfg.hidden, id]
        : modulesCfg.hidden.filter((x) => x !== id);
      saveModulesConfig();
      applyModules();
      renderSettingsRows();
    });

    const info = document.createElement("div");
    info.className = "settings-info";
    const name = document.createElement("span");
    name.className = "settings-name";
    name.textContent = def.name;
    const desc = document.createElement("span");
    desc.className = "settings-desc";
    desc.textContent = def.desc;
    info.append(name, desc);

    const move = (dir: -1 | 1) => {
      const j = idx + dir;
      if (j < 0 || j >= modulesCfg.order.length) return;
      [modulesCfg.order[idx], modulesCfg.order[j]] = [modulesCfg.order[j], modulesCfg.order[idx]];
      saveModulesConfig();
      applyModules();
      renderSettingsRows();
    };
    const up = document.createElement("button");
    up.type = "button";
    up.className = "ghost-btn settings-move";
    up.textContent = "↑";
    up.title = "Move up";
    up.disabled = idx === 0;
    up.addEventListener("click", () => move(-1));
    const down = document.createElement("button");
    down.type = "button";
    down.className = "ghost-btn settings-move";
    down.textContent = "↓";
    down.title = "Move down";
    down.disabled = idx === modulesCfg.order.length - 1;
    down.addEventListener("click", () => move(1));
    const orderBtns = document.createElement("div");
    orderBtns.className = "settings-order";
    orderBtns.append(up, down);

    row.append(toggle, info, orderBtns);
    settingsList.append(row);
  });
}

$("btn-settings").addEventListener("click", () => {
  renderSettingsRows();
  void refreshAutostart();
  setSettingsOpen(true);
});
$("settings-close").addEventListener("click", () => setSettingsOpen(false));
$("settings-reset").addEventListener("click", () => {
  modulesCfg = {
    order: [...MODULES_DEFAULT_ORDER],
    hidden: [...MODULES_DEFAULT_HIDDEN],
  };
  saveModulesConfig();
  applyModules();
  renderSettingsRows();
});
// Click on the backdrop / Esc closes (same convention as the model details modal)
settingsModal.addEventListener("mousedown", (e) => {
  if (e.target === settingsModal) setSettingsOpen(false);
});
window.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && settingsModal.style.display === "flex") setSettingsOpen(false);
});

// ---- Auto-start (the settings modal's "Auto-start" section): three states off / boot / follow ----
// Unlike module visibility, the source of truth is the backend (Windows registry / mac LaunchAgent, see
// autostart.rs): the real state is read back when the modal opens, clicks write immediately and the selection is
// refreshed from a post-write read-back.
// Not stored in localStorage — the registry/plist is the single source of truth, avoiding state drift between the two.
// Browser preview (no Tauri) is display-only
type AutostartMode = "off" | "boot" | "follow";
const AUTOSTART_DEFS: { id: AutostartMode; name: string; desc: string }[] = [
  { id: "off", name: "Off", desc: "Do not auto-start, open manually when needed" },
  { id: "boot", name: "Launch at login", desc: "Always start after login, display in last-used form (full panel / floating window)" },
  { id: "follow", name: "Follow ZCode startup", desc: "Wait silently after login (tray icon only, no window), automatically show panel when ZCode is running" },
];
const autostartList = $("settings-autostart");
/** null = reading/preview mode (no row shows as selected) */
let autostartCurrent: AutostartMode | null = null;
const autostartError = document.createElement("div");
autostartError.className = "autostart-error";

function renderAutostartRows() {
  autostartList.textContent = "";
  for (const def of AUTOSTART_DEFS) {
    const on = autostartCurrent === def.id;
    const row = document.createElement("div");
    row.className = on ? "settings-row" : "settings-row off";

    // Radio semantics: the selected row reuses the module row's custom checkbox style (.pet-chk); unselected rows appear off
    const toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = `ghost-btn settings-toggle${on ? " on" : ""}`;
    toggle.title = on ? "Current mode" : "Switch to this mode";
    const chk = document.createElement("span");
    chk.className = "pet-chk";
    chk.setAttribute("aria-hidden", "true");
    toggle.append(chk, document.createTextNode(on ? "Selected" : "Select"));
    toggle.addEventListener("click", () => void applyAutostart(def.id));

    const info = document.createElement("div");
    info.className = "settings-info";
    const name = document.createElement("span");
    name.className = "settings-name";
    name.textContent = def.name;
    const desc = document.createElement("span");
    desc.className = "settings-desc";
    desc.textContent = def.desc;
    info.append(name, desc);

    row.append(toggle, info);
    autostartList.append(row);
  }
  autostartList.append(autostartError);
}

/** On modal open read the real state back from the backend (the registry/plist is the source of truth; don't trust the last in-memory value) */
const refreshAutostart = async () => {
  const mode = await tauriInvoke<string>("autostart_get");
  autostartCurrent = mode === "boot" || mode === "follow" ? mode : "off";
  autostartError.textContent = "";
  renderAutostartRows();
};

const applyAutostart = async (mode: AutostartMode) => {
  if (!hasTauri || mode === autostartCurrent) return;
  const prev = autostartCurrent;
  autostartCurrent = mode;
  autostartError.textContent = "";
  renderAutostartRows();
  try {
    // Read back the effective value after the backend writes (a failed write throws; the frontend rolls back the selection and shows the reason)
    const applied = await tauriInvoke<string>("autostart_set", { mode });
    autostartCurrent = applied === "boot" || applied === "follow" ? applied : "off";
  } catch (e) {
    autostartCurrent = prev;
    autostartError.textContent = `Settings failed: ${e}`;
  }
  renderAutostartRows();
};
renderAutostartRows();

// ---- Recalibrate (⟳ at the top-left of the current-speed card): discard the byte→token coefficient samples and return to the prior ----
const btnRecal = $<HTMLButtonElement>("btn-recal");
if (!hasTauri) btnRecal.style.display = "none"; // no real calibration in browser preview
let recalTimer = 0;
const flashRecal = () => {
  btnRecal.classList.add("done");
  window.clearTimeout(recalTimer);
  recalTimer = window.setTimeout(() => btnRecal.classList.remove("done"), 1500);
};
btnRecal.addEventListener("click", () => {
  tauriInvoke("recalibrate").catch((err) => console.warn("recalibrate failed:", err));
});

// ---- In-app update: version number at the footer's bottom right (click = manual check); the backend checks silently at startup + daily,
//      auto-pre-downloads new versions and pops this card; silent when there is no update / network error, never nags ----
interface UpdateEvent {
  state: "available" | "downloading" | "ready" | "launching" | "error";
  currentVersion: string;
  newVersion: string;
  releaseUrl: string;
  notes: string;
  downloadedBytes: number;
  totalBytes: number;
  message: string;
}
type CheckOutcome =
  | { kind: "upToDate"; current: string }
  | { kind: "available"; current: string; newVersion: string }
  | { kind: "failed"; message: string };

const DISMISS_KEY = "updateDismissed.v1";
const stVersion = $("st-version");
const updateCard = $("update-card");
const updateVersion = $("update-version");
const updateCurrent = $("update-current");
const updateNotes = $("update-notes");
const updateLink = $("update-link");
const updateProgress = $("update-progress");
const updateBarFill = $("update-bar-fill");
const updateProgressText = $("update-progress-text");
const updateInstall = $<HTMLButtonElement>("update-install");
const updateStatus = $("update-status");
const updateToast = $("update-toast");
let currentVersion = "";
let updateDismissed = localStorage.getItem(DISMISS_KEY) ?? "";
let toastTimer = 0;
let checkingUpdate = false;

if (!hasTauri) {
  stVersion.style.display = "none"; // no backend in browser preview, hide the entry point
} else {
  tauriInvoke<string>("app_version").then((v) => {
    if (v) {
      currentVersion = v;
      stVersion.textContent = `v${v}`;
    }
  });
}

const toast = (msg: string) => {
  updateToast.textContent = msg;
  updateToast.classList.add("show");
  window.clearTimeout(toastTimer);
  toastTimer = window.setTimeout(() => updateToast.classList.remove("show"), 2600);
};

const setUpdateProgress = (done: number, total: number) => {
  const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;
  updateBarFill.style.width = `${pct}%`;
  updateProgressText.textContent =
    total > 0 ? `${pct}% · ${(done / 1048576).toFixed(1)}/${(total / 1048576).toFixed(1)} MB` : "Downloading…";
};

/** Pop the card (user hasn't dismissed this version's notice); if dismissed, just add a small dot to the version number */
const maybeOpenCard = (e: UpdateEvent) => {
  if (updateDismissed && updateDismissed === e.newVersion) {
    stVersion.classList.add("has-update");
  } else {
    updateCard.classList.add("show");
  }
};

function applyUpdateEvent(e: UpdateEvent) {
  if (e.currentVersion) currentVersion = e.currentVersion;
  if (e.newVersion) {
    updateVersion.textContent = `v${e.newVersion}`;
    updateCurrent.textContent = currentVersion ? `v${currentVersion}` : "";
    updateNotes.textContent = e.notes;
    updateLink.dataset.url = e.releaseUrl;
  }
  updateStatus.classList.toggle("error", e.state === "error");
  updateStatus.textContent = e.message;
  switch (e.state) {
    case "available":
      updateProgress.style.display = "none";
      updateInstall.disabled = false;
      updateInstall.textContent = "⤓ Update Now";
      maybeOpenCard(e);
      break;
    case "downloading":
      updateProgress.style.display = "";
      setUpdateProgress(e.downloadedBytes, e.totalBytes);
      updateInstall.disabled = true;
      updateInstall.textContent = "⤓ Downloading…";
      maybeOpenCard(e);
      break;
    case "ready":
      updateProgress.style.display = "none";
      updateInstall.disabled = false;
      updateInstall.textContent = "⤓ Install Now";
      maybeOpenCard(e);
      break;
    case "launching":
      updateInstall.disabled = true;
      updateInstall.textContent = "Installing…";
      updateCard.classList.add("show");
      break;
    case "error":
      updateInstall.disabled = false;
      updateInstall.textContent = "Retry";
      updateCard.classList.add("show");
      break;
  }
}

async function manualCheck() {
  if (!hasTauri || checkingUpdate) return;
  checkingUpdate = true;
  // Clear the "dismissed notice" before checking: a manual check counts as renewed interest, so the card pops normally
  // when the "available" event arrives (it may arrive before the invoke returns)
  updateDismissed = "";
  localStorage.removeItem(DISMISS_KEY);
  stVersion.classList.remove("has-update");
  stVersion.classList.add("checking");
  try {
    const r = await tauriInvoke<CheckOutcome>("check_update");
    if (r?.kind === "upToDate") toast(`Already up to date v${r.current}`);
    else if (r?.kind === "failed") toast("Update check failed: network error, please try again later");
    // available → the card is rendered by the "update" event
  } finally {
    stVersion.classList.remove("checking");
    checkingUpdate = false;
  }
}

stVersion.addEventListener("click", () => manualCheck());
updateInstall.addEventListener("click", () => {
  updateInstall.disabled = true;
  updateInstall.textContent = "Preparing…";
  tauriInvoke("install_update").catch((err) => {
    updateStatus.classList.add("error");
    updateStatus.textContent = String(err);
    updateInstall.disabled = false;
    updateInstall.textContent = "Retry";
  });
});
$("update-close").addEventListener("click", () => {
  updateCard.classList.remove("show");
  const v = updateVersion.textContent?.replace(/^v/, "") ?? "";
  if (v) {
    updateDismissed = v;
    localStorage.setItem(DISMISS_KEY, v);
  }
});
updateLink.addEventListener("click", (e) => {
  e.preventDefault();
  const url = updateLink.dataset.url;
  if (url) tauriInvoke("open_url", { url }).catch(() => {});
});
// Manual download: self-service path besides in-app installation (goes straight to the Releases page when install fails or auto-install is unwanted)
const RELEASES_URL = "https://github.com/Masterchiefm/zcode-speed-panel/releases";
$("update-manual").addEventListener("click", () => {
  const url = updateLink.dataset.url || RELEASES_URL;
  tauriInvoke("open_url", { url }).catch(() => {});
});

$("float-style-btn").addEventListener("click", () => {
  setStyleDropdownOpen(!styleDropdown.classList.contains("open"));
});
for (const opt of styleOptions) {
  opt.addEventListener("click", () => selectFloatStyle(opt.dataset.value!));
}

// ---- mac onboarding hint: the backend emits "tray-hint" when the main window goes hidden→visible (never emitted on Windows, the frontend never shows it) ----
const trayHint = $("tray-hint");
let trayHintTimer = 0;
const showTrayHint = () => {
  trayHint.classList.add("show");
  window.clearTimeout(trayHintTimer);
  trayHintTimer = window.setTimeout(() => trayHint.classList.remove("show"), 6000);
};
trayHint.addEventListener("click", () => {
  window.clearTimeout(trayHintTimer);
  trayHint.classList.remove("show");
});

// Header / floating window dragging: mousedown calls startDragging (buttons and dropdowns excepted).
// If the target itself carries data-tauri-drag-region, the Tauri core handles it directly (skip, to avoid double dragging).
// Double-click must fire directly on the detail>=2 mousedown; it cannot be bound as a DOM dblclick:
// the first mousedown's startDragging enters the Windows native drag loop and swallows the mouse sequence, and mouseup is only
// re-delivered via WM_EXITSIZEMOVE, so two complete click pairs never line up → dblclick never fires (the gauge/pill
// legacy bindings therefore silently never worked). The Tauri core's drag.js double-click maximize also calls IPC directly on detail===2.
function enableDrag(el: HTMLElement, onDoubleClick?: () => void) {
  el.addEventListener("mousedown", (e) => {
    const target = e.target as HTMLElement;
    if (target.closest("button, select, input, .dropdown")) return;
    if (target.hasAttribute("data-tauri-drag-region")) return;
    e.preventDefault();
    if (onDoubleClick && e.detail >= 2) {
      onDoubleClick();
      return;
    }
    import("@tauri-apps/api/window")
      .then(({ getCurrentWindow: g }) => g().startDragging().catch(() => {}))
      .catch(() => {});
  });
}
enableDrag($("app-header"));
enableDrag($("float-gauge"), () => requestMode("full"));
enableDrag($("float-pill"), () => requestMode("full"));
enableDrag($("float-pet"), () => requestMode("full"));

// ---- Custom title bar: drag to move, double-click to maximize, — / ▢ / ✕ window controls ----
const currentWindow = () => import("@tauri-apps/api/window").then((m) => m.getCurrentWindow());
$("app-header").addEventListener("dblclick", (e) => {
  if ((e.target as HTMLElement).closest("button, select, input, .dropdown")) return;
  if (hasTauri) tauriInvoke("toggle_maximize_safe").catch(() => {});
});
if (hasTauri) {
  $("wc-min").addEventListener("click", () => {
    currentWindow().then((w) => w.minimize()).catch(() => {});
  });
  $("wc-max").addEventListener("click", () => {
    // mac uses the native overlay title bar (real traffic lights, green dot = native fullscreen), so this button is hidden;
    // Windows ▢ = safe maximize (same as double-clicking the header)
    tauriInvoke("toggle_maximize_safe").catch(() => {});
  });
  $("wc-close").addEventListener("click", () => requestMode("float"));
} else {
  // No window controls in browser preview
  ($("win-controls") as HTMLElement).style.display = "none";
}

applyStyleUi(localStorage.getItem("floatStyle") ?? "gauge");

// ---- Chart card "Model Details" view (mutually exclusive with the overall speed chart via the toggle) ----
/** Browser preview (no Tauri): model_stats uses the mock generator (the same call stream the overall chart's mock
 *  draws from, split by model), all other commands still go through tauriInvoke and silently return undefined */
async function modelStatsInvoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T | undefined> {
  if (!hasTauri && cmd === "model_stats") {
    return mockModelStats(Number(args?.windowMin ?? 60)) as T;
  }
  return tauriInvoke<T>(cmd, args);
}
const modelStats = initModelStats(modelStatsInvoke, () => ({
  windowMin: chartRange,
  gridMs: chartRangeCfg().gridMs,
}));

// Segmented toggle: overall chart / model details are mutually exclusive views (the color block slides to the active side);
// visibility is driven by body.chart-view-model via CSS (#spark and #model-view switch as a pair),
// data polling starts/stops with the view (modelStats.setActive). The time range is shared by both views and unaffected by the toggle
const viewToggle = $<HTMLButtonElement>("chart-view-toggle");

function applyChartViewUi() {
  const model = chartView === "model";
  document.body.classList.toggle("chart-view-model", model);
  viewToggle.classList.toggle("on", model);
  viewToggle.setAttribute("aria-checked", String(model));
  updateChartTitle();
  if (!model) redrawSpark(); // redraw immediately when switching back to the overall chart (the canvas skipped all drawing while hidden)
  modelStats.setActive(model);
}

viewToggle.addEventListener("click", () => {
  chartView = chartView === "model" ? "total" : "model";
  localStorage.setItem(CHART_VIEW_KEY, chartView);
  applyChartViewUi();
});
applyChartViewUi();

if (hasTauri) {
  (async () => {
    const { listen } = await import("@tauri-apps/api/event");
    await listen<SnapshotPayload>("metrics", (e) => {
      onSnapshot({ ...e.payload.snapshot, rolloutDir: e.payload.rolloutDir });
    });
    await listen<string>("mode", (e) => applyModeUi(e.payload));
    await listen("tray-hint", () => showTrayHint());
    // Recalibration finished (manual or auto-triggered by drift): the button flashes ✓ for feedback
    await listen("recalibrated", flashRecal);
    await listen<string>("float-style", (e) => {
      localStorage.setItem("floatStyle", e.payload);
      applyStyleUi(e.payload);
    });
    await listen<UpdateEvent>("update", (e) => applyUpdateEvent(e.payload));
    const p = await tauriInvoke<SnapshotPayload>("snapshot");
    if (p) {
      applyModeUi(p.mode);
      if (p.floatStyle) {
        localStorage.setItem("floatStyle", p.floatStyle);
        applyStyleUi(p.floatStyle);
      }
      onSnapshot({ ...p.snapshot, rolloutDir: p.rolloutDir });
    }
    // mac onboarding (one-shot): claim it proactively once the page is ready, so an emit inside setup isn't dropped for arriving before the page loads
    if (await tauriInvoke<boolean>("tray_hint_once")) showTrayHint();
  })().catch((err) => {
    document.title = `Initialization failed · ZCode Speed Panel`;
    subCurrent.textContent = `Tauri initialization failed: ${err}`;
  });
} else {
  startMock(onSnapshot);
}
