//! Measures the per-emit cost the way a caller experiences it: back-to-back
//! emits timed with the cycle counter, reported as a latency distribution in
//! nanoseconds, single-threaded and with every core emitting at once.
//!
//! ```text
//! cargo run --release --example cost [--threads N] [--iters N]
//! ```

use std::time::Instant;

use fasttrace::{arg, ArgKind, Event};

static E0: Event = Event::new("cost.e0", &[]);
static E2: Event = Event::new("cost.e2", &[arg("a", ArgKind::U64), arg("b", ArgKind::Hex)]);
static E5: Event = Event::new(
    "cost.e5",
    &[
        arg("a", ArgKind::U64),
        arg("b", ArgKind::U64),
        arg("c", ArgKind::U64),
        arg("d", ArgKind::U64),
        arg("e", ArgKind::U64),
    ],
);

fn ns_per_tick() -> f64 {
    let t0 = fasttrace::clock::now();
    let i0 = Instant::now();
    while i0.elapsed().as_millis() < 200 {
        std::hint::spin_loop();
    }
    let t1 = fasttrace::clock::now();
    i0.elapsed().as_nanos() as f64 / (t1 - t0) as f64
}

struct Dist {
    samples: Vec<u64>,
}

impl Dist {
    fn report(&mut self, label: &str, nspt: f64) {
        self.samples.sort_unstable();
        let n = self.samples.len();
        let q = |p: f64| self.samples[((n as f64 * p) as usize).min(n - 1)] as f64 * nspt;
        let mean = self.samples.iter().sum::<u64>() as f64 * nspt / n as f64;
        println!(
            "{label:<34} min {:6.1}  p50 {:6.1}  p90 {:6.1}  p99 {:6.1}  p99.9 {:6.1}  max {:9.1}  mean {:6.1} ns",
            q(0.0),
            q(0.5),
            q(0.9),
            q(0.99),
            q(0.999),
            q(1.0),
            mean
        );
    }
}

/// Time `iters` calls of `f` individually (rdtsc before/after each call,
/// minus the measured rdtsc pair overhead) and also as a throughput loop.
fn measure(label: &str, iters: usize, nspt: f64, rdtsc_pair: u64, mut f: impl FnMut(u64)) {
    let mut d = Dist {
        samples: Vec::with_capacity(iters),
    };
    for i in 0..iters {
        let t0 = fasttrace::clock::now();
        f(i as u64);
        let t1 = fasttrace::clock::now();
        d.samples.push((t1 - t0).saturating_sub(rdtsc_pair));
    }
    let t0 = fasttrace::clock::now();
    for i in 0..iters {
        f(i as u64);
    }
    let t1 = fasttrace::clock::now();
    d.report(&format!("{label} (per-call)"), nspt);
    println!(
        "{:<34} {:6.1} ns/emit",
        format!("{label} (loop avg)"),
        (t1 - t0) as f64 * nspt / iters as f64
    );
}

fn run_set(iters: usize, nspt: f64, rdtsc_pair: u64) {
    measure("instant 0 args", iters, nspt, rdtsc_pair, |_| {
        E0.instant([])
    });
    measure("instant 2 args", iters, nspt, rdtsc_pair, |i| {
        E2.instant([i, i * 7])
    });
    measure("instant 5 args", iters, nspt, rdtsc_pair, |i| {
        E5.instant([i, i, i, i, i])
    });
    measure("span begin+end (2 records)", iters, nspt, rdtsc_pair, |i| {
        let _s = E2.span([i, 1]);
    });
}

fn main() {
    let mut threads = 0usize;
    let mut iters = 2_000_000usize;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--threads" => threads = args.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "--iters" => iters = args.next().and_then(|v| v.parse().ok()).unwrap_or(iters),
            _ => {}
        }
    }
    if threads == 0 {
        threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
    }
    let nspt = ns_per_tick();
    // Cost of the two counter reads bracketing each measured call.
    let mut pair = Vec::with_capacity(100_000);
    for _ in 0..100_000 {
        let t0 = fasttrace::clock::now();
        let t1 = fasttrace::clock::now();
        pair.push(t1 - t0);
    }
    pair.sort_unstable();
    let rdtsc_pair = pair[pair.len() / 2];
    println!(
        "clock: {} ns/tick ({:.3} GHz), rdtsc-pair overhead {} ticks = {:.1} ns (subtracted from per-call numbers)",
        nspt,
        1.0 / nspt,
        rdtsc_pair,
        rdtsc_pair as f64 * nspt
    );
    println!(
        "ring: {} entries/thread",
        fasttrace::Config::from_env().entries_per_thread
    );

    println!("\n== single thread, {iters} iterations ==");
    run_set(iters, nspt, rdtsc_pair);

    println!("\n== {threads} threads emitting concurrently, {iters} iterations each ==");
    let per = iters;
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            std::thread::Builder::new()
                .name(format!("emitter-{t}"))
                .spawn(move || {
                    let mut d = Dist {
                        samples: Vec::with_capacity(per),
                    };
                    let t0 = fasttrace::clock::now();
                    for i in 0..per as u64 {
                        let a = fasttrace::clock::now();
                        E2.instant([i, t as u64]);
                        let b = fasttrace::clock::now();
                        d.samples.push((b - a).saturating_sub(rdtsc_pair));
                    }
                    let t1 = fasttrace::clock::now();
                    (d, (t1 - t0) as f64 * nspt / per as f64)
                })
                .unwrap()
        })
        .collect();
    let mut all = Dist {
        samples: Vec::new(),
    };
    let mut loop_avgs = Vec::new();
    for h in handles {
        let (d, avg) = h.join().unwrap();
        all.samples.extend(d.samples);
        loop_avgs.push(avg);
    }
    all.report("instant 2 args, all threads", nspt);
    loop_avgs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "{:<34} min {:.1}  median {:.1}  max {:.1} ns/emit",
        "per-thread loop avg",
        loop_avgs[0],
        loop_avgs[loop_avgs.len() / 2],
        loop_avgs[loop_avgs.len() - 1]
    );

    let s0 = Instant::now();
    let snap = fasttrace::snapshot();
    let took = s0.elapsed();
    println!(
        "\nsnapshot: {} threads, {} records in {:?} ({:.1} ns/record)",
        snap.threads.len(),
        snap.total_entries(),
        took,
        took.as_nanos() as f64 / snap.total_entries().max(1) as f64
    );
}
