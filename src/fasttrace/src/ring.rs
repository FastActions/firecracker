//! Per-thread rings and the hot path.
//!
//! Each thread that emits owns one ring: a power-of-two array of 64-byte
//! [`Slot`]s. The owning thread is the only writer, so it keeps its write
//! position in thread-local storage and never touches a shared cache line
//! on emit. Consumers read slots through a per-slot seqlock, so a snapshot
//! never needs to stop the writers and never sees a half-written record.

use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::clock;
use crate::entry::{Entry, Slot};

/// Ring sizing. Set with [`init`] before the first emit; afterwards the
/// values are fixed for the process.
#[derive(Clone, Debug)]
pub struct Config {
    /// Slots per thread ring; rounded up to a power of two. 64 bytes each,
    /// so the default of 16384 is 1 MiB per emitting thread.
    pub entries_per_thread: usize,
    /// Rings of exited threads kept for the next snapshot; the oldest is
    /// dropped when more threads than this have exited.
    pub retain_finished_threads: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            entries_per_thread: 1 << 14,
            retain_finished_threads: 256,
        }
    }
}

impl Config {
    /// Defaults overridden by `FASTTRACE_RING_ENTRIES` and
    /// `FASTTRACE_RETAIN_FINISHED` when set.
    pub fn from_env() -> Config {
        let mut c = Config::default();
        if let Some(v) = std::env::var("FASTTRACE_RING_ENTRIES")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            c.entries_per_thread = v;
        }
        if let Some(v) = std::env::var("FASTTRACE_RETAIN_FINISHED")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            c.retain_finished_threads = v;
        }
        c
    }
}

static CONFIG: OnceLock<Config> = OnceLock::new();

/// Fix the configuration. Returns `false` if a ring already exists (the
/// configuration in force stays).
pub fn init(config: Config) -> bool {
    clock::calibrate();
    CONFIG.set(config).is_ok()
}

fn config() -> &'static Config {
    CONFIG.get_or_init(Config::from_env)
}

pub(crate) struct Ring {
    slots: Box<[Slot]>,
    mask: u64,
    pub(crate) tid: u32,
    pub(crate) name: Option<String>,
    /// 0 while the owning thread is alive; otherwise the order in which the
    /// thread exited (1-based), so retention evicts by exit order and not by
    /// creation order. Contents are final once nonzero.
    pub(crate) finished: AtomicU64,
    /// Records the writer skipped because of ... nothing: the ring never
    /// drops on the write side. Kept as the writer's total so consumers can
    /// size rings; updated at most once per `HEAD_PUBLISH` records.
    pub(crate) published_head: AtomicU64,
}

/// Each `Local` publishes its head to the ring every this many records so a
/// snapshot can report writer progress without the writer paying a shared
/// store per emit.
const HEAD_PUBLISH: u64 = 1 << 10;

impl Ring {
    fn new(entries: usize) -> Ring {
        let n = entries.max(2).next_power_of_two();
        let slots: Box<[Slot]> = (0..n).map(|_| Slot::new()).collect();
        Ring {
            slots,
            mask: (n - 1) as u64,
            // SAFETY: gettid has no preconditions.
            tid: unsafe { libc::gettid() } as u32,
            name: std::thread::current().name().map(str::to_owned),
            finished: AtomicU64::new(0),
            published_head: AtomicU64::new(0),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Every complete record currently in the ring, oldest first. Slots the
    /// writer is in the middle of overwriting are skipped, so the result can
    /// have gaps in `index`; it never has torn records.
    pub(crate) fn read_all(&self) -> Vec<Entry> {
        let mut out: Vec<Entry> = self.slots.iter().filter_map(Slot::read).collect();
        out.sort_unstable_by_key(|e| e.index);
        out
    }

    #[inline(always)]
    fn write<const N: usize>(&self, index: u64, ts: u64, meta: u64, args: [u64; N]) {
        let slot = &self.slots[(index & self.mask) as usize];
        let seq = (index + 1) * 2;
        // Seqlock write: odd marks the slot in progress. The release fence
        // keeps the payload stores from becoming visible before the odd
        // marker on weakly ordered targets; on x86 both fences are free.
        slot.0[0].store(seq | 1, Ordering::Relaxed);
        std::sync::atomic::fence(Ordering::Release);
        slot.0[1].store(ts, Ordering::Relaxed);
        slot.0[2].store(meta, Ordering::Relaxed);
        let mut i = 0;
        while i < N {
            slot.0[3 + i].store(args[i], Ordering::Relaxed);
            i += 1;
        }
        slot.0[0].store(seq, Ordering::Release);
    }
}

struct Local {
    ring: Cell<*const Ring>,
    head: Cell<u64>,
}

thread_local! {
    static LOCAL: Local = const { Local { ring: Cell::new(ptr::null()), head: Cell::new(0) } };
    // Separate from LOCAL so the hot path has no lazy-init or destructor
    // bookkeeping; this one is touched once per thread.
    static GUARD: RingGuard = const { RingGuard { ring: Cell::new(None) } };
}

struct RingGuard {
    ring: Cell<Option<Arc<Ring>>>,
}

impl Drop for RingGuard {
    fn drop(&mut self) {
        if let Some(ring) = self.ring.take() {
            LOCAL.with(|l| {
                ring.published_head.store(l.head.get(), Ordering::Release);
                l.ring.set(ptr::null());
            });
            let order = FINISH_ORDER.fetch_add(1, Ordering::Relaxed) + 1;
            ring.finished.store(order, Ordering::Release);
            retire_finished();
        }
    }
}

static FINISH_ORDER: AtomicU64 = AtomicU64::new(0);

struct Rings {
    live: Vec<Arc<Ring>>,
}

fn rings() -> &'static Mutex<Rings> {
    static RINGS: Mutex<Rings> = Mutex::new(Rings { live: Vec::new() });
    &RINGS
}

/// Drop the earliest-exited rings beyond the retention limit.
fn retire_finished() {
    let keep = config().retain_finished_threads;
    let mut r = rings().lock().unwrap_or_else(|e| e.into_inner());
    let mut orders: Vec<u64> = r
        .live
        .iter()
        .map(|x| x.finished.load(Ordering::Acquire))
        .filter(|&o| o != 0)
        .collect();
    if orders.len() <= keep {
        return;
    }
    orders.sort_unstable();
    let cutoff = orders[orders.len() - keep - 1];
    r.live.retain(|x| {
        let o = x.finished.load(Ordering::Acquire);
        o == 0 || o > cutoff
    });
}

pub(crate) fn all_rings() -> Vec<Arc<Ring>> {
    rings()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .live
        .clone()
}

/// Count of rings ever created; a cheap health signal for tests and stats.
pub(crate) static RINGS_CREATED: AtomicUsize = AtomicUsize::new(0);

#[cold]
#[inline(never)]
fn attach(l: &Local) -> *const Ring {
    clock::calibrate();
    let ring = Arc::new(Ring::new(config().entries_per_thread));
    let p = Arc::as_ptr(&ring);
    {
        let mut r = rings().lock().unwrap_or_else(|e| e.into_inner());
        r.live.push(ring.clone());
    }
    RINGS_CREATED.fetch_add(1, Ordering::Relaxed);
    // The guard keeps the Arc alive for this thread's lifetime and marks the
    // ring finished on exit. If TLS is already being torn down, the guard
    // cannot be created; the ring then stays unfinished, which only means a
    // snapshot reports it as live.
    let _ = GUARD.try_with(|g| g.ring.set(Some(ring)));
    l.ring.set(p);
    p
}

/// The hot path: one record into the calling thread's ring.
#[inline(always)]
pub(crate) fn emit<const N: usize>(meta: u64, args: [u64; N]) {
    let ts = clock::now();
    // try_with: a destructor running after TLS teardown must not panic; it
    // just loses the record.
    let _ = LOCAL.try_with(|l| {
        let mut p = l.ring.get();
        if p.is_null() {
            p = attach(l);
        }
        let head = l.head.get();
        l.head.set(head + 1);
        // SAFETY: p points into an Arc<Ring> held by this thread's GUARD (or
        // by the global list, which never drops a ring that is not finished).
        let ring = unsafe { &*p };
        ring.write(head, ts, meta, args);
        if (head + 1) % HEAD_PUBLISH == 0 {
            ring.published_head.store(head + 1, Ordering::Relaxed);
        }
    });
}

/// Records this thread has emitted so far.
pub fn thread_emitted() -> u64 {
    LOCAL.try_with(|l| l.head.get()).unwrap_or(0)
}
