//! Sub-100 ns in-process tracing.
//!
//! A trace record is one cacheline: a sequence word, a cycle-counter
//! timestamp, a packed `event/kind/nargs` word and up to five `u64`
//! arguments. Each thread writes into its own ring with plain stores, so an
//! emit is a TLS read, an `rdtsc`, eight stores to one cacheline and no
//! shared state. What the integers mean lives in a registry of
//! [`Event`] statics (name, argument names and kinds) that travels with every
//! [`snapshot`], so the raw records stay small and the consumer renders them.
//!
//! ```
//! use fasttrace::{arg, ArgKind, Event};
//!
//! static READ: Event = Event::new(
//!     "proxy.read",
//!     &[arg("offset", ArgKind::Hex), arg("len", ArgKind::Bytes)],
//! );
//!
//! fn read(offset: u64, len: u64) {
//!     let _span = READ.span([offset, len]);
//!     // ... work ...
//! }
//!
//! read(0x1000, 4096);
//! let snap = fasttrace::snapshot();
//! let mut json = Vec::new();
//! snap.write_chrome_json(&mut json).unwrap(); // open in https://ui.perfetto.dev
//! ```
//!
//! Dump a raw file with [`Snapshot::write_raw`] and convert it offline with
//! the `fasttrace-dump` binary.

pub mod clock;
mod entry;
mod registry;
mod ring;
mod snapshot;

pub use entry::{Entry, Kind, MAX_ARGS};
pub use registry::{
    arg, intern, Arg, ArgDef, ArgKind, Event, EventDef, RegistrySnapshot, Render, RenderDef, Span,
};
pub use ring::{init, thread_emitted, Config};
pub use snapshot::{snapshot, write_chrome_json_merged, Header, Snapshot, ThreadInfo, ThreadTrace};
