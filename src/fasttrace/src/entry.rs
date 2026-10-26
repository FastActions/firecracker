//! The 64-byte trace record.

use std::sync::atomic::{AtomicU64, Ordering};

/// Number of integer arguments one record carries.
pub const MAX_ARGS: usize = 5;

/// Cacheline size the ring is laid out for. Every slot starts on a boundary so
/// one emit touches exactly one line.
pub const SLOT_BYTES: usize = 64;

/// What an emission means for the consumer. Stored in the low byte of `meta`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    /// Point event.
    Instant = 0,
    /// Opens a span on this thread; closed by the next `End` of the same event
    /// (spans nest LIFO per thread, like Chrome `B`/`E`).
    Begin = 1,
    /// Closes the innermost open span of this event on this thread.
    End = 2,
    /// Sampled values; each argument is one counter series.
    Counter = 3,
}

impl Kind {
    pub fn from_u8(v: u8) -> Option<Kind> {
        match v {
            0 => Some(Kind::Instant),
            1 => Some(Kind::Begin),
            2 => Some(Kind::End),
            3 => Some(Kind::Counter),
            _ => None,
        }
    }
}

/// A decoded record, as handed to consumers. The on-ring layout is
/// [`Slot`]: word 0 is the seqlock word, word 1 the timestamp, word 2 `meta`,
/// words 3..8 the arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C, align(64))]
pub struct Entry {
    /// Absolute index of this record in its thread's stream (0-based, never
    /// wraps in practice). Gaps between consecutive snapshot entries are
    /// records lost to overwrite.
    pub index: u64,
    /// Raw clock ticks; convert with the snapshot's [`crate::clock::ClockInfo`].
    pub ts: u64,
    /// Registry id of the event (1-based).
    pub event: u16,
    pub kind: Kind,
    pub nargs: u8,
    pub args: [u64; MAX_ARGS],
}

pub(crate) const fn pack_meta(event: u16, kind: Kind, nargs: u8) -> u64 {
    (kind as u64) | ((nargs as u64) << 8) | ((event as u64) << 16)
}

/// One cacheline of the ring: eight atomics so the consumer can read a slot
/// the writer may be overwriting without UB. Word 0 is `2*(index+1)` once the
/// record is complete and odd while it is being written; 0 means never used.
#[repr(C, align(64))]
pub(crate) struct Slot(pub(crate) [AtomicU64; 8]);

const _: () = assert!(std::mem::size_of::<Slot>() == SLOT_BYTES);
const _: () = assert!(std::mem::size_of::<Entry>() == SLOT_BYTES);

impl Slot {
    pub(crate) const fn new() -> Slot {
        Slot([
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
        ])
    }

    /// Seqlock read. Returns the record if the slot held a complete record
    /// for the whole read, `None` if it was empty or the writer raced us.
    pub(crate) fn read(&self) -> Option<Entry> {
        let s1 = self.0[0].load(Ordering::Acquire);
        if s1 == 0 || s1 & 1 == 1 {
            return None;
        }
        let mut w = [0u64; 7];
        for (i, word) in w.iter_mut().enumerate() {
            *word = self.0[i + 1].load(Ordering::Relaxed);
        }
        std::sync::atomic::fence(Ordering::Acquire);
        let s2 = self.0[0].load(Ordering::Relaxed);
        if s1 != s2 {
            return None;
        }
        let meta = w[1];
        let nargs = (meta >> 8) as u8;
        let mut args = [w[2], w[3], w[4], w[5], w[6]];
        // The writer only stores its own arguments, so words past `nargs`
        // still hold the overwritten record's values; clear them here rather
        // than on the hot path so a snapshot never leaks them.
        for a in args.iter_mut().skip((nargs as usize).min(MAX_ARGS)) {
            *a = 0;
        }
        Some(Entry {
            index: s1 / 2 - 1,
            ts: w[0],
            event: (meta >> 16) as u16,
            kind: Kind::from_u8(meta as u8)?,
            nargs,
            args,
        })
    }
}
