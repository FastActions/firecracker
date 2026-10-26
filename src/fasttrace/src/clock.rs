//! Timestamp source. The hot path reads the CPU cycle counter (`rdtsc` on
//! x86_64, `cntvct_el0` on aarch64) because it is ~5-10 ns and needs no call;
//! conversion to nanoseconds happens once, at snapshot time, from a
//! calibration against `CLOCK_MONOTONIC`.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

#[inline(always)]
pub fn now() -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: rdtsc has no preconditions on x86_64.
        unsafe { std::arch::x86_64::_rdtsc() }
    }
    #[cfg(target_arch = "aarch64")]
    {
        let v: u64;
        // SAFETY: reading the virtual counter is always permitted in EL0.
        unsafe { std::arch::asm!("mrs {}, cntvct_el0", out(reg) v, options(nomem, nostack)) };
        v
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        monotonic_ns()
    }
}

/// `CLOCK_MONOTONIC` in nanoseconds.
pub fn monotonic_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid pointer to a timespec.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// `CLOCK_REALTIME` in nanoseconds since the Unix epoch.
pub fn realtime_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid pointer to a timespec.
    unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// Everything a consumer needs to turn raw ticks into wall-clock time.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ClockInfo {
    /// Which counter `ts` values come from.
    pub source: String,
    /// Counter ticks per second, measured against `CLOCK_MONOTONIC`.
    pub ticks_per_sec: f64,
    /// A (tick, CLOCK_MONOTONIC ns, CLOCK_REALTIME ns) sample taken together.
    pub anchor_ticks: u64,
    pub anchor_monotonic_ns: u64,
    pub anchor_realtime_ns: u64,
}

impl ClockInfo {
    pub fn ticks_to_monotonic_ns(&self, ticks: u64) -> f64 {
        let dt = ticks as f64 - self.anchor_ticks as f64;
        self.anchor_monotonic_ns as f64 + dt * 1e9 / self.ticks_per_sec
    }

    pub fn ticks_to_realtime_ns(&self, ticks: u64) -> f64 {
        let dt = ticks as f64 - self.anchor_ticks as f64;
        self.anchor_realtime_ns as f64 + dt * 1e9 / self.ticks_per_sec
    }

    pub fn ticks_to_ns(&self, dticks: u64) -> f64 {
        dticks as f64 * 1e9 / self.ticks_per_sec
    }
}

struct Sample {
    ticks: u64,
    mono: u64,
    real: u64,
}

fn sample() -> Sample {
    // Bracket the tick read with two monotonic reads and keep the midpoint so
    // the clock_gettime cost does not bias the anchor.
    let m0 = monotonic_ns();
    let t = now();
    let m1 = monotonic_ns();
    let real = realtime_ns();
    Sample {
        ticks: t,
        mono: m0 + (m1 - m0) / 2,
        real: real - (m1 - m0) / 2,
    }
}

static FIRST: OnceLock<Sample> = OnceLock::new();

/// Record the first calibration point. Called from the slow path of the
/// first emit on any thread; calling it early (at process start) makes the
/// snapshot-time frequency estimate more precise because the baseline is
/// longer.
pub fn calibrate() {
    FIRST.get_or_init(sample);
}

/// Build the conversion for a snapshot. Uses the span since [`calibrate`]
/// when it is at least 10 ms long, otherwise spins for 10 ms to measure.
pub fn info() -> ClockInfo {
    let first = FIRST.get_or_init(sample);
    let mut last = sample();
    if last.mono.saturating_sub(first.mono) < 10_000_000 {
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(10) {
            std::hint::spin_loop();
        }
        last = sample();
    }
    let dt_mono = last.mono.saturating_sub(first.mono).max(1) as f64;
    let dticks = last.ticks.wrapping_sub(first.ticks) as f64;
    ClockInfo {
        source: SOURCE.to_owned(),
        ticks_per_sec: dticks * 1e9 / dt_mono,
        anchor_ticks: last.ticks,
        anchor_monotonic_ns: last.mono,
        anchor_realtime_ns: last.real,
    }
}

#[cfg(target_arch = "x86_64")]
const SOURCE: &str = "rdtsc";
#[cfg(target_arch = "aarch64")]
const SOURCE: &str = "cntvct_el0";
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
const SOURCE: &str = "clock_monotonic_ns";
