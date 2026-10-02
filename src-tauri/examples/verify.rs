//! Verification tool: the real engine (Engine+LiveIo, same code as the panel)
//! + an independent raw per-process write-bytes sampler, emitting JSONL for
//! afterwards reconciling against the real tokens in the usage DB and
//! measuring how far live speed and total figures deviate.
//! Usage: cargo run --example verify -- [seconds] [output file]
#[path = "../src/metrics.rs"]
mod metrics;
#[path = "../src/liveio.rs"]
mod liveio;

use metrics::Engine;
use std::collections::HashMap;
use std::io::Write;

mod raw {
    use super::*;
    use super::liveio::platform;

    /// Independent sampling: enumerate CLI processes -> pid -> cumulative write bytes.
    /// Reuses liveio's platform primitives (process identification identical
    /// to the panel's), but the sampling cadence is independent of the engine
    pub fn sample_cli_writes() -> HashMap<u32, u64> {
        let mut out = HashMap::new();
        for pid in platform::discover_cli_pids() {
            if let Some(h) = platform::open_proc(pid) {
                if let Some(w) = platform::io_write_bytes(&h) {
                    out.insert(pid, w);
                }
            }
        }
        out
    }
}

fn tracked_files_total() -> u64 {
    liveio::platform::tracked_files_total()
}

fn main() {
    let secs: u64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(600);
    let out_path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "target/verify-log.jsonl".into());
    let mut out = std::io::BufWriter::new(std::fs::File::create(&out_path).expect("create log"));

    let mut e = Engine::new();
    let mut li = liveio::LiveIo::new();
    let start = std::time::Instant::now();
    eprintln!("verify: sampling {}s -> {}", secs, out_path);

    while start.elapsed().as_secs() < secs {
        let tick = std::time::Instant::now();
        let new_calls = e.poll();
        let inflight = e.call_in_flight();
        let s = e.snapshot();
        let live = {
            li.observe(&new_calls);
            li.set_inflight(inflight);
            li.measure(s.now_ms)
        };
        let cal = li.take_calibration();
        let raw = raw::sample_cli_writes();
        let files = tracked_files_total();

        // Ground-truth stats for completed calls (back-computed from persisted data)
        for c in &new_calls {
            let line = serde_json::json!({
                "kind": "call",
                "t": s.now_ms,
                "id": c.id,
                "sess": &c.session[c.session.len().saturating_sub(8)..],
                "done": c.completed_ms,
                "gen_ms": c.gen_ms,
                "eff": c.effective_out(),
                "true_tps": (c.effective_out() as f64) / (c.gen_ms.max(50) as f64 / 1000.0),
            });
            writeln!(out, "{}", line).ok();
        }
        // Calibration event (byte-side per-call estimate and sample, including
        // reconciliation with the same cleaned-integration figures the display uses)
        if let Some(cal) = &cal {
            let pred_tps = if cal.gen_ms > 0 && cal.bpt_now > 0.0 {
                cal.clean_bytes / (cal.gen_ms as f64 / 1000.0) / cal.bpt_now
            } else {
                0.0
            };
            let line = serde_json::json!({
                "kind": "cal",
                "t": s.now_ms,
                "id": cal.id,
                "gen_ms": cal.gen_ms,
                "eff": cal.eff,
                "true_tps": (cal.true_tps * 10.0).round() / 10.0,
                "raw_kb": (cal.raw_bytes / 1024.0 * 10.0).round() / 10.0,
                "clean_kb": (cal.clean_bytes / 1024.0 * 10.0).round() / 10.0,
                "attr_pid": cal.attr_pid,
                "top_pid": cal.top_pid,
                "bpt_sample": (cal.bpt_sample * 10.0).round() / 10.0,
                "bpt_now": (cal.bpt_now * 10.0).round() / 10.0,
                "pred_tps": (pred_tps * 10.0).round() / 10.0,
                "skipped": cal.cal_skipped,
            });
            writeln!(out, "{}", line).ok();
        }

        let calls_json: Vec<serde_json::Value> = new_calls
            .iter()
            .map(|c| {
                serde_json::json!({
                    "id": c.id, "session": c.session,
                    "completed_ms": c.completed_ms, "gen_ms": c.gen_ms,
                    "eff_out": c.effective_out(), "out": c.output, "rea": c.reasoning,
                    "inp": c.input, "cc": c.cache_creation, "cr": c.cache_read,
                })
            })
            .collect();
        let spark_tail: Vec<f64> = s.spark.iter().rev().take(3).rev().copied().collect();
        let line = serde_json::json!({
            "kind": "tick",
            "t": s.now_ms,
            "files": files,
            "raw": raw,
            "calls": calls_json,
            "snap": {
                "total": s.total_tokens, "out": s.output_tokens, "rea": s.reasoning_tokens,
                "inp": s.input_tokens, "cc": s.cache_creation_tokens, "cr": s.cache_read_tokens,
                "calls": s.calls_today, "tps": s.current_tps, "avg": s.avg_tps,
                "is_live": s.is_live, "est": s.is_estimating, "src": s.live_source, "win": (s.window_tps*10.0).round()/10.0,
            },
            "spark_tail": spark_tail,
            "bpt": (li.bytes_per_token() * 10.0).round() / 10.0,
            "live": { "avail": live.available, "stream": live.streaming, "ramp": live.ramping, "tps": live.tps },
        });
        writeln!(out, "{}", line).ok();
        out.flush().ok();

        let dt = tick.elapsed();
        if dt < std::time::Duration::from_millis(500) {
            std::thread::sleep(std::time::Duration::from_millis(500) - dt);
        }
    }
    eprintln!("verify: done");
}
