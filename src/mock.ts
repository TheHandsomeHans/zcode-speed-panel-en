// Browser preview mode: simulates ZCode's model-io call stream so the UI can be previewed without Tauri

import type { ModelStatsPayload } from "./model_stats";

/** Per-task realtime details (multiple only when tasks run concurrently): one CLI process = one row */
export interface TaskStat {
  pid: number;
  /** Owning in-progress session id (empty = streaming process not yet attributed) */
  session: string;
  /** Number of in-progress sessions carried by this process (≥2 = multiple tasks in one process, speed is the combined total) */
  nSessions: number;
  tps: number;
  streaming: boolean;
}

/** ZCode connection detail row (same shape as the backend's ConnStat): both groups are ZCode's own processes */
export interface ConnStat {
  remote: string;
  pid: number;
  /** Process type label: CLI session process / main process / render process / GPU process / utility process / crash reporter process */
  proc: string;
}

export interface Snapshot {
  currentTps: number;
  avgTps: number;
  totalTokens: number;
  outputTokens: number;
  inputTokens: number;
  cacheCreationTokens: number;
  cacheReadTokens: number;
  callsToday: number;
  sessionsToday: number;
  isLive: boolean;
  isEstimating: boolean;
  /** Call started but first byte not yet arrived (TTFT): shows the "Collecting…" notice instead of an estimate */
  isStarting: boolean;
  /** Measured streaming started but the 30s sliding window isn't full yet (shows "Collecting") */
  ramping: boolean;
  /** Real speed of calls completed in the last 10 minutes (on-disk basis) */
  windowTps: number;
  /** Real speed of the most recent completed call (on-disk basis), used by the current speed card's top-right badge */
  lastCallTps: number;
  /** Fastest single call in the last 7 days (window and admission criteria: see metrics.rs; the mock uses a plausible peak) */
  histMaxTps: number;
  /** Average speed over the last 7 days (calls in window Σeff ÷ Σgen, same basis as today's average) */
  histAvgTps: number;
  liveSource: string;
  lastActivityMs: number;
  nowMs: number;
  rolloutDir: string;
  spark: number[];
  /** Concurrent per-process task details (frontend shows the task list when ≥2) */
  tasks: TaskStat[];
  // ---- Network traffic monitoring (netio.rs; simulated values in browser preview) ----
  netAvailable: boolean;
  netUpBps: number;
  netDownBps: number;
  netUpToday: number;
  netDownToday: number;
  netSessUpToday: number;
  netSessDownToday: number;
  netConnsAvailable: boolean;
  netCliConns: number;
  netAppConns: number;
  /** Connection details (each with remote endpoint + owning pid + process type label) */
  netCliConnList: ConnStat[];
  netAppConnList: ConnStat[];
}

interface MockCall {
  completed: number;
  duration: number;
  output: number;
  input: number;
  cache: number;
  session: string;
  /** Pipe-silent call: no incremental bytes for the whole segment, displayed as a ≈ estimate */
  silent: boolean;
  /** Fixed speed of the second process during concurrent tasks (0 = single task; fixed per call, not re-rolled each tick) */
  second: number;
  /** Model name (the model details view groups by it; same role as the real library's model_id) */
  model: string;
}

const MIN_DUR = 50;
const WINDOW = 10 * 60 * 1000;
const BUCKETS = 90;
const BUCKET = 10_000;

const rnd = (a: number, b: number) => a + Math.random() * (b - a);

/** Simulated model pool: the main model at high frequency, two secondary models at low frequency (demonstrates multiple lines in the model details view) */
const MODEL_POOL = ["claude-sonnet-4-5", "claude-sonnet-4-5", "glm-4.6", "deepseek-v3.2"];

let calls: MockCall[] = [];
let sessionNo = 1;
// Network monitor simulation state: today's totals accumulate monotonically
let netUpToday = rnd(2e8, 6e8);
let netDownToday = rnd(1e9, 4e9);

function newCall(now: number): MockCall {
  if (Math.random() < 0.18) sessionNo++;
  const duration = Math.exp(rnd(Math.log(12000), Math.log(180000)));
  const tps = rnd(18, 70);
  const output = Math.max(60, Math.round((duration / 1000) * tps));
  // usage library semantics: cache_read is a subset of input, the hit rate is normally 90%+
  const input = Math.round(rnd(15000, 60000));
  return {
    completed: now + duration,
    duration,
    output,
    input,
    cache: Math.round(input * rnd(0.8, 0.99)),
    session: `mock-sess-${sessionNo}`,
    silent: Math.random() < 0.22,
    second: Math.random() < 0.3 ? rnd(15, 90) : 0,
    model: MODEL_POOL[Math.floor(Math.random() * MODEL_POOL.length)],
  };
}

function seedHistory(now: number) {
  let t = now - 3 * 3600 * 1000;
  while (t < now) {
    if (Math.random() < 0.72) {
      const burst = Math.round(rnd(3, 14));
      for (let i = 0; i < burst && t < now; i++) {
        const c = newCall(t);
        if (c.completed > now) break;
        calls.push(c);
        t = c.completed + rnd(300, 2500);
      }
      t += rnd(20000, 240000);
    } else {
      t += rnd(60000, 300000);
    }
  }
  sessionNo = Math.max(sessionNo, 6);
}

function snapshot(now: number, pending: MockCall | null): Snapshot {
  let out = 0,
    input = 0,
    cache = 0,
    dur = 0,
    wOut = 0,
    wDur = 0,
    last = 0,
    lastTps = 0;
  const sessions = new Set<string>();
  const buckets = new Array<number>(BUCKETS).fill(0);
  const bucketDur = new Array<number>(BUCKETS).fill(0);
  const nowSlot = Math.floor(now / BUCKET);
  for (const c of calls) {
    const d = Math.max(MIN_DUR, c.duration);
    out += c.output;
    input += c.input;
    cache += c.cache;
    dur += d;
    if (c.completed >= last) {
      last = c.completed;
      lastTps = c.output / (d / 1000); // matches the backend: use the call with the latest completion time, eff ÷ pure generation time
    }
    sessions.add(c.session);
    if (c.completed >= now - WINDOW) {
      wOut += c.output;
      wDur += d;
    }
    const slot = nowSlot - Math.floor(c.completed / BUCKET);
    if (slot >= 0 && slot < BUCKETS) {
      buckets[BUCKETS - 1 - slot] += c.output;
      bucketDur[BUCKETS - 1 - slot] += d;
    }
  }
  // Gating model matches the backend: the in-progress call (pending) decides.
  // Simulated TTFT ~2.5s: the starting phase shows "Collecting…"; about 1 in 5 calls is pipe-silent (whole segment shown as a ≈ estimate)
  const pendingStart = pending ? pending.completed - pending.duration : 0;
  const ageSec = pending ? (now - pendingStart) / 1000 : Infinity;
  const isStarting = !!pending && ageSec < 2.5 && !pending.silent;
  const isLive = !!pending && ageSec >= 2.5 && !pending.silent;
  const isEstimating = !!pending && pending.silent && wDur > 0;
  const currentTps = isStarting
    ? 0
    : isLive || isEstimating
      ? wDur > 0
        ? wOut / (wDur / 1000)
        : 0
      : 0;
  // Simulated concurrent tasks: during some calls a second CLI process streams concurrently — the current speed is the
  // aggregate throughput and the task list shows two rows (previews the multi-task UI; the second task's speed is fixed per call)
  const ownTps = currentTps;
  const secondTps = pending && pending.second > 0 && isLive ? pending.second : 0;
  const currentAgg = currentTps + secondTps;
  const spark = buckets.map((o, i) => (bucketDur[i] > 0 ? o / (bucketDur[i] / 1000) : 0));
  if (isEstimating && spark[BUCKETS - 1] <= 0) {
    spark[BUCKETS - 1] = currentTps;
  }
  const tasks: TaskStat[] = [];
  if (pending && (isLive || isStarting)) {
    tasks.push({
      pid: 4000 + sessionNo,
      session: pending.session,
      nSessions: 1,
      tps: ownTps,
      streaming: isLive,
    });
    if (secondTps > 0) {
      tasks.push({
        pid: 7000 + sessionNo,
        session: `mock-sess-${sessionNo + 1}`,
        nSessions: 1,
        tps: secondTps,
        streaming: true,
      });
    }
  }
  return {
    currentTps: currentAgg,
    avgTps: dur > 0 ? out / (dur / 1000) : 0,
    totalTokens: out + input,
    outputTokens: out,
    inputTokens: input,
    cacheCreationTokens: 0,
    cacheReadTokens: cache,
    callsToday: calls.length,
    sessionsToday: sessions.size,
    isLive,
    isEstimating,
    isStarting,
    ramping: isLive && ageSec < 30,
    windowTps: wDur > 0 ? wOut / (wDur / 1000) : 0,
    lastCallTps: lastTps,
    // Last-7-days mock stats: peak = today's peak × 1.2, 7-day average slightly below today's average (diluted across days)
    histMaxTps: Math.max(...spark, lastTps) * 1.2 || 312,
    histAvgTps: dur > 0 ? (out / (dur / 1000)) * 0.92 : 0,
    liveSource: isStarting || isLive ? "io" : isEstimating ? "window" : "idle",
    lastActivityMs: last,
    nowMs: now,
    rolloutDir: "(Browser preview · simulated data)",
    spark,
    tasks,
    // Network monitor simulation: speed rises and falls with call activity, today's totals accumulate monotonically
    netAvailable: true,
    netUpBps: isLive ? rnd(20_000, 90_000) : rnd(0, 3_000),
    netDownBps: isLive ? rnd(80_000, 400_000) : rnd(0, 8_000),
    netUpToday: netUpToday,
    netDownToday: netDownToday,
    // Same basis as the backend: upload = uncached prompt (input−cache_read) × 5, download = output × 400
    netSessUpToday: Math.max(0, input - cache) * 5,
    netSessDownToday: out * 400,
    netConnsAvailable: true,
    netCliConns: isLive ? 2 : 1,
    netAppConns: 1,
    // Connection details simulation: both groups are ZCode's own processes (CLI / Electron shell), labeled per process
    netCliConnList: [
      { remote: "61.170.79.24:443", pid: 41092, proc: "CLI Session" },
      { remote: "61.170.79.31:443", pid: 41092, proc: "CLI Session" },
    ],
    netAppConnList: [{ remote: "61.151.230.245:443", pid: 18104, proc: "Main Process" }],
  };
}

/** Mock data for the model details view: aggregates the call stream by model × absolute wall-clock slot (same semantics as the
 *  backend's aggregate_model_stats: slot = now÷bucketMs − completed÷bucketMs, 90 buckets, the four windows are shared with
 *  the chart, out-of-range slots dropped, bucket tps = Σeff ÷ Σgen_s), for preview without Tauri */
export function mockModelStats(windowMin: number): ModelStatsPayload {
  const win = [15, 60, 360, 1440].reduce((a, b) => (Math.abs(b - windowMin) < Math.abs(a - windowMin) ? b : a));
  const now = Date.now();
  const bucketMs = (win * 60_000) / 90;
  const cutoff = now - win * 60_000;
  interface Acc {
    eff: number;
    gen: number;
    calls: number;
  }
  const per = new Map<string, { slots: Acc[]; total: Acc }>();
  for (const c of calls) {
    if (c.completed < cutoff || c.completed > now) continue;
    const gen = Math.max(MIN_DUR, c.duration);
    const slot = Math.floor(now / bucketMs) - Math.floor(c.completed / bucketMs);
    if (slot < 0 || slot >= 90) continue;
    let e = per.get(c.model);
    if (!e) {
      e = { slots: Array.from({ length: 90 }, () => ({ eff: 0, gen: 0, calls: 0 })), total: { eff: 0, gen: 0, calls: 0 } };
      per.set(c.model, e);
    }
    const b = e.slots[slot];
    b.eff += c.output;
    b.gen += gen;
    b.calls += 1;
    e.total.eff += c.output;
    e.total.gen += gen;
    e.total.calls += 1;
  }
  const grandEff = [...per.values()].reduce((t, e) => t + e.total.eff, 0);
  const tpsOf = (b: Acc) => (b.gen > 0 ? b.eff / (b.gen / 1000) : 0);
  const series = [...per.entries()]
    .sort((a, b) => b[1].total.eff - a[1].total.eff || a[0].localeCompare(b[0]))
    .map(([model, e]) => ({
      model,
      buckets: e.slots.map((b) => ({ tps: tpsOf(b), calls: b.calls, tokens: b.eff })),
      totalCalls: e.total.calls,
      totalTokens: e.total.eff,
      avgTps: e.total.gen > 0 ? e.total.eff / (e.total.gen / 1000) : 0,
      peakTps: Math.max(0, ...e.slots.map(tpsOf)),
      share: grandEff > 0 ? e.total.eff / grandEff : 0,
    }));
  return { windowMin: win, bucketMs, nowMs: now, series };
}

export function startMock(onData: (s: Snapshot) => void) {
  const now = Date.now();
  seedHistory(now);
  let pending: MockCall | null = null;
  let nextStart = now + rnd(1000, 4000);

  const tick = () => {
    const t = Date.now();
    if (pending && t >= pending.completed) {
      calls.push(pending);
      // Keep only the last 30 minutes
      const cutoff = t - 30 * 60 * 1000;
      calls = calls.filter((c) => c.completed >= cutoff);
      pending = null;
      nextStart = t + rnd(200, 2500);
    }
    if (!pending && t >= nextStart) {
      pending = newCall(t);
    }
    // Network monitor simulation: totals advance at the simulated speed
    netUpToday += rnd(500, 120_000) * 0.4;
    netDownToday += rnd(2_000, 500_000) * 0.4;
    onData(snapshot(t, pending));
  };

  tick();
  setInterval(tick, 400);
}
