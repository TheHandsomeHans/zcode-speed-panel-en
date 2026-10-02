//! Debug tool: runs Engine code identical to the main app against real data and prints snapshots.
//! Usage: cargo run --example dump
#[path = "../src/metrics.rs"]
mod metrics;
#[path = "../src/liveio.rs"]
mod liveio;

use metrics::Engine;

fn main() {
    let mut e = Engine::new();
    let mut li = liveio::LiveIo::new();
    println!("Data source: {}", e.data_source_label());
    for i in 0..8 {
        let calls = e.poll();
        li.observe(&calls);
        li.set_inflight(e.call_in_flight());
        let live = li.measure(e.snapshot().now_ms);
        let s = e.snapshot();
        let tasks: Vec<String> = live
            .tasks
            .iter()
            .map(|t| {
                format!(
                    "{}:{:.0}{}",
                    t.pid,
                    t.tps,
                    t.session.as_deref().unwrap_or("?")
                )
            })
            .collect();
        println!(
            "[round {}] current {:.1} t/s | today avg {:.1} t/s | today total {} tok (out {}/in {}/cache write {}/cache read {}) | {} calls / {} sessions | live available={} streaming={} tps={:.1} npids={} tasks=[{}]",
            i,
            s.current_tps,
            s.avg_tps,
            s.total_tokens,
            s.output_tokens,
            s.input_tokens,
            s.cache_creation_tokens,
            s.cache_read_tokens,
            s.calls_today,
            s.sessions_today,
            live.available,
            live.streaming,
            live.tps,
            live.n_pids,
            tasks.join(" "),
        );
        std::thread::sleep(std::time::Duration::from_millis(600));
    }
}
