# fasttrace

In-process tracing where one emit costs about 15 ns.

## What a record is

Every trace record is exactly one cacheline (64 B), eight `u64` words:

| word | meaning                                                           |
| ---- | ----------------------------------------------------------------- |
| 0    | per-slot sequence (seqlock: odd = being written, even = complete) |
| 1    | cycle counter (`rdtsc` / `cntvct_el0`)                            |
| 2    | packed \`event id (u16)                                           |
| 3..7 | up to five `u64` arguments                                        |

Each emitting thread owns a ring of these slots (default `1 << 14`, 1 MiB per
thread) and writes with plain stores. An emit is a TLS read, one `rdtsc` and
eight stores to a single line; there is no lock, no shared counter, no
allocation and no formatting. Rings overwrite when full. Readers use the
sequence word to skip torn or in-flight slots and never block the writer.

## What the ints mean

Events are statics; the registry assigns each a `u16` id on first use and
remembers its name plus the name and render kind of every argument. The registry
(and clock calibration) travels inside every snapshot, so the raw records stay
small and the consumer renders them.

```rust
use fasttrace::{arg, ArgKind, Event};

static READ: Event = Event::new(
    "proxy.read",
    &[arg("offset", ArgKind::Hex), arg("len", ArgKind::Bytes)],
);

fn read(offset: u64, len: u64) {
    let _span = READ.span([offset, len]); // Begin now, End on drop
}
```

`ArgKind` covers `U64`, `I64`, `Hex`, `Bool`, `Bytes`, `Nanos`, `Ticks` and
`Str` (an id from `fasttrace::intern`, which is a slow-path call for use at
setup time, never inside the hot path). Kinds are `instant`, `begin`/`end` (or
`span`) and `counter`.

## Getting the data out

```rust
let snap = fasttrace::snapshot();            // copies every thread ring
snap.write_raw(File::create("trace.ftrc")?)?; // header JSON + 64 B records
snap.write_chrome_json(&mut out)?;            // open in https://ui.perfetto.dev
```

Offline: `fasttrace-dump trace.ftrc [-o trace.json | --text | --header]`.

Raw file layout:
`"FTRC" | u32 version | u32 header_len | header JSON (clock, registry, threads) | 64 B records per thread`.

## Config

`fasttrace::init(Config)` before the first emit, or `FASTTRACE_RING_ENTRIES`
(power of two) and `FASTTRACE_RETAIN_FINISHED` (rings kept for exited threads,
default 256).

## Cost

`cargo run --release --example cost` on an 8 vCPU Xeon VM (2.4 GHz TSC), 1 M
iterations per case, `rdtsc` pair overhead (11.7 ns) subtracted from per-call
numbers:

| case                               | p50     | p99     | p99.9   | loop avg |
| ---------------------------------- | ------- | ------- | ------- | -------- |
| instant, 0 args                    | 13.3 ns | 14.2 ns | 56.7 ns | 13.8 ns  |
| instant, 2 args                    | 13.3 ns | 14.2 ns | 46.7 ns | 14.2 ns  |
| instant, 5 args                    | 13.3 ns | 14.2 ns | 24.2 ns | 14.4 ns  |
| span begin+end (2 records)         | 26.7 ns | 27.5 ns | 39.2 ns | 27.7 ns  |
| instant, 2 args, 8 threads at once | 13.3 ns | 15.8 ns | 46.7 ns | 42-89 ns |

The 8-thread loop average is store-bandwidth bound (eight cores each writing a
fresh cacheline every ~14 ns into 8 MiB of rings); the per-call distribution is
unchanged. Snapshot of 147 k records takes ~5 ms. `cargo bench` runs the same
cases under Criterion.
