//! Consumer side: copy every ring out, carry the registry and clock along,
//! and write it as a raw file or as Chrome/Perfetto trace JSON.
//!
//! Raw file layout (all little-endian):
//!
//! ```text
//! magic "FTRC" | u32 version=1 | u32 header_len | header JSON (clock, registry, threads)
//! then for each thread, in header order: thread.entries * 64-byte records
//! ```
//!
//! Records are the decoded [`Entry`] layout (`index, ts, meta, args[5]`), so
//! a consumer in any language can read them with a fixed-size struct.

use std::io::{self, Read, Write};
use std::sync::atomic::Ordering;

use crate::clock::{self, ClockInfo};
use crate::entry::{Entry, Kind, MAX_ARGS, SLOT_BYTES};
use std::collections::BTreeMap;

use crate::registry::{ArgKind, RegistrySnapshot, RenderDef};
use crate::ring;

const MAGIC: &[u8; 4] = b"FTRC";
/// Upper bound on the header JSON a reader will allocate for.
const MAX_HEADER_BYTES: usize = 64 << 20;
const VERSION: u32 = 1;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ThreadInfo {
    pub tid: u32,
    pub name: Option<String>,
    /// Ring capacity in records.
    pub capacity: usize,
    /// Records the writer had published at snapshot time (lower bound, in
    /// steps of 1024); `max(index)+1` of the entries is the exact value.
    pub published_head: u64,
    pub finished: bool,
    /// Records present in this snapshot for the thread.
    pub entries: usize,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Header {
    pub version: u32,
    pub pid: u32,
    pub process: Option<String>,
    pub clock: ClockInfo,
    pub registry: RegistrySnapshot,
    pub threads: Vec<ThreadInfo>,
}

/// One thread's slice of a snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct ThreadTrace {
    pub info: ThreadInfo,
    /// Oldest first, strictly increasing `index`.
    pub entries: Vec<Entry>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    pub pid: u32,
    pub process: Option<String>,
    pub clock: ClockInfo,
    pub registry: RegistrySnapshot,
    pub threads: Vec<ThreadTrace>,
}

/// Copy every ring in the process. Writers keep running; records they
/// overwrite during the copy are dropped, never torn.
pub fn snapshot() -> Snapshot {
    let clock = clock::info();
    let threads = ring::all_rings()
        .iter()
        .map(|r| {
            let entries = r.read_all();
            ThreadTrace {
                info: ThreadInfo {
                    tid: r.tid,
                    name: r.name.clone(),
                    capacity: r.capacity(),
                    published_head: r.published_head.load(Ordering::Acquire),
                    finished: r.finished.load(Ordering::Acquire) != 0,
                    entries: entries.len(),
                },
                entries,
            }
        })
        .collect();
    // Captured after the rings so every event id copied above has a
    // definition: ids are assigned on first emit, which may race the copy.
    let registry = RegistrySnapshot::capture();
    Snapshot {
        pid: std::process::id(),
        process: std::env::current_exe()
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned())),
        clock,
        registry,
        threads,
    }
}

impl Snapshot {
    pub fn header(&self) -> Header {
        Header {
            version: VERSION,
            pid: self.pid,
            process: self.process.clone(),
            clock: self.clock.clone(),
            registry: self.registry.clone(),
            threads: self.threads.iter().map(|t| t.info.clone()).collect(),
        }
    }

    pub fn total_entries(&self) -> usize {
        self.threads.iter().map(|t| t.entries.len()).sum()
    }

    /// Raw format: header JSON plus verbatim 64-byte records.
    pub fn write_raw<W: Write>(&self, mut w: W) -> io::Result<()> {
        let header = serde_json::to_vec(&self.header())?;
        w.write_all(MAGIC)?;
        w.write_all(&VERSION.to_le_bytes())?;
        w.write_all(&(header.len() as u32).to_le_bytes())?;
        w.write_all(&header)?;
        let mut buf = [0u8; SLOT_BYTES];
        for t in &self.threads {
            for e in &t.entries {
                encode(e, &mut buf);
                w.write_all(&buf)?;
            }
        }
        w.flush()
    }

    pub fn read_raw<R: Read>(mut r: R) -> io::Result<Snapshot> {
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a fasttrace file",
            ));
        }
        let mut u = [0u8; 4];
        r.read_exact(&mut u)?;
        let version = u32::from_le_bytes(u);
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("fasttrace file version {version}, expected {VERSION}"),
            ));
        }
        r.read_exact(&mut u)?;
        let header_len = u32::from_le_bytes(u) as usize;
        if header_len > MAX_HEADER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("fasttrace header of {header_len} bytes exceeds {MAX_HEADER_BYTES}"),
            ));
        }
        let mut hdr = vec![0u8; header_len];
        r.read_exact(&mut hdr)?;
        let header: Header = serde_json::from_slice(&hdr)?;
        let mut threads = Vec::with_capacity(header.threads.len());
        let mut buf = [0u8; SLOT_BYTES];
        for info in header.threads {
            // Sized by what is actually read, not by the untrusted count.
            let mut entries = Vec::new();
            for _ in 0..info.entries {
                r.read_exact(&mut buf)?;
                entries.push(decode(&buf)?);
            }
            threads.push(ThreadTrace { info, entries });
        }
        Ok(Snapshot {
            pid: header.pid,
            process: header.process,
            clock: header.clock,
            registry: header.registry,
            threads,
        })
    }

    /// Chrome trace event format, which https://ui.perfetto.dev and
    /// chrome://tracing open directly. Timestamps are `CLOCK_MONOTONIC`
    /// microseconds (fractional, so nanosecond resolution survives).
    pub fn write_chrome_json<W: Write>(&self, w: W) -> io::Result<()> {
        write_chrome_json_merged(std::slice::from_ref(self), w)
    }

    /// Appends this snapshot's process, thread and trace events to `events`
    /// under the Perfetto process id `pid`.
    fn chrome_events(&self, pid: u32, events: &mut Vec<serde_json::Value>) {
        events.push(serde_json::json!({
            "name": "process_name", "ph": "M", "pid": pid,
            "args": {"name": self.process.clone().unwrap_or_else(|| format!("pid {pid}"))}
        }));
        for t in &self.threads {
            let tid = t.info.tid;
            let tname = match (&t.info.name, t.info.finished) {
                (Some(n), false) => n.clone(),
                (Some(n), true) => format!("{n} (exited)"),
                (None, false) => format!("tid {tid}"),
                (None, true) => format!("tid {tid} (exited)"),
            };
            events.push(serde_json::json!({
                "name": "thread_name", "ph": "M", "pid": pid, "tid": tid, "args": {"name": tname}
            }));
            let mut tracks: BTreeMap<String, u32> = BTreeMap::new();
            let mut prev_index: Option<u64> = None;
            for e in &t.entries {
                if let Some(p) = prev_index {
                    if e.index != p + 1 {
                        events.push(serde_json::json!({
                            "name": "fasttrace.lost", "cat": "fasttrace", "ph": "i", "s": "t",
                            "pid": pid, "tid": tid, "ts": self.ts_us(e.ts),
                            "args": {"records": e.index - p - 1}
                        }));
                    }
                }
                prev_index = Some(e.index);
                let def = self.registry.event(e.event);
                // Sub-tracks are synthetic tids under the same pid, named
                // after the thread so Perfetto groups them with it.
                let track_tid = match def.and_then(|d| d.track.as_deref()) {
                    None => tid,
                    Some(track) => {
                        let n = tracks.len() as u32;
                        *tracks.entry(track.to_owned()).or_insert_with(|| {
                            let synth = 0x4000_0000 | (tid << 8) | (n + 1);
                            events.push(serde_json::json!({
                                "name": "thread_name", "ph": "M", "pid": pid, "tid": synth,
                                "args": {"name": format!("{tname}: {track}")}
                            }));
                            synth
                        })
                    }
                };
                match def.map(|d| &d.render).unwrap_or(&RenderDef::Plain) {
                    RenderDef::Plain => events.push(self.chrome_event(pid, track_tid, e)),
                    RenderDef::SpanBegin { span, key } | RenderDef::SpanEnd { span, key } => {
                        let begin =
                            matches!(def.map(|d| &d.render), Some(RenderDef::SpanBegin { .. }));
                        let key = *key;
                        let name = def.map(|d| d.name.as_str()).unwrap_or("unknown");
                        let args: serde_json::Map<String, serde_json::Value> =
                            self.render_args(e).into_iter().collect();
                        events.push(serde_json::json!({
                            "name": span, "cat": span.split('.').next().unwrap_or(span),
                            "ph": if begin { "b" } else { "e" },
                            "id2": {"local": format!("{:#x}", e.args[(key as usize).min(MAX_ARGS - 1)])},
                            "pid": pid, "tid": track_tid, "ts": self.ts_us(e.ts),
                            "args": {name: args}
                        }));
                    }
                }
            }
        }
    }

    fn ts_us(&self, ticks: u64) -> f64 {
        self.clock.ticks_to_monotonic_ns(ticks) / 1000.0
    }

    fn chrome_event(&self, pid: u32, tid: u32, e: &Entry) -> serde_json::Value {
        let def = self.registry.event(e.event);
        let name = def.map(|d| d.name.as_str()).unwrap_or("unknown");
        let cat = name.split('.').next().unwrap_or(name);
        let ts = self.ts_us(e.ts);
        let args: serde_json::Map<String, serde_json::Value> =
            self.render_args(e).into_iter().collect();
        match e.kind {
            Kind::Instant => serde_json::json!({
                "name": name, "cat": cat, "ph": "i", "s": "t", "pid": pid, "tid": tid, "ts": ts, "args": args
            }),
            Kind::Begin => serde_json::json!({
                "name": name, "cat": cat, "ph": "B", "pid": pid, "tid": tid, "ts": ts, "args": args
            }),
            Kind::End => serde_json::json!({
                "name": name, "cat": cat, "ph": "E", "pid": pid, "tid": tid, "ts": ts, "args": args
            }),
            Kind::Counter => serde_json::json!({
                "name": name, "cat": cat, "ph": "C", "pid": pid, "tid": tid, "ts": ts, "args": args
            }),
        }
    }

    /// Render a record's arguments through the registry, in declaration order.
    pub fn render_args(&self, e: &Entry) -> Vec<(String, serde_json::Value)> {
        let def = self.registry.event(e.event);
        let mut m = Vec::with_capacity(e.nargs as usize);
        for i in 0..(e.nargs as usize).min(MAX_ARGS) {
            let v = e.args[i];
            let (name, kind) = def
                .and_then(|d| d.args.get(i))
                .map(|a| (a.name.clone(), a.kind))
                .unwrap_or_else(|| (format!("arg{i}"), ArgKind::U64));
            let value = match kind {
                ArgKind::U64 | ArgKind::Bytes => serde_json::json!(v),
                ArgKind::I64 => serde_json::json!(v as i64),
                ArgKind::Hex => serde_json::json!(format!("{v:#x}")),
                ArgKind::Bool => serde_json::json!(v != 0),
                ArgKind::Nanos => serde_json::json!(v),
                ArgKind::Ticks => {
                    serde_json::json!(self.clock.ticks_to_ns(e.ts.wrapping_sub(v)).round())
                }
                ArgKind::Str => match self.registry.string(v) {
                    Some(s) => serde_json::json!(s),
                    None => serde_json::json!(format!("str#{v}")),
                },
            };
            m.push((name, value));
        }
        m
    }

    /// One line per record, for eyeballing.
    pub fn write_text<W: Write>(&self, mut w: W) -> io::Result<()> {
        for t in &self.threads {
            let name = t.info.name.as_deref().unwrap_or("-");
            writeln!(
                w,
                "# thread tid={} name={} entries={} capacity={} finished={}",
                t.info.tid,
                name,
                t.entries.len(),
                t.info.capacity,
                t.info.finished
            )?;
            for e in &t.entries {
                let def = self.registry.event(e.event);
                let ev = def.map(|d| d.name.as_str()).unwrap_or("unknown");
                let args = self.render_args(e);
                let args: Vec<String> = args.iter().map(|(k, v)| format!("{k}={v}")).collect();
                writeln!(
                    w,
                    "{:>12} {:>18.0} {:<8} {:<7} {} {}",
                    e.index,
                    self.clock.ticks_to_monotonic_ns(e.ts),
                    t.info.tid,
                    format!("{:?}", e.kind).to_lowercase(),
                    ev,
                    args.join(" ")
                )?;
            }
        }
        w.flush()
    }
}

fn encode(e: &Entry, buf: &mut [u8; SLOT_BYTES]) {
    let meta = crate::entry::pack_meta(e.event, e.kind, e.nargs);
    let words = [
        e.index, e.ts, meta, e.args[0], e.args[1], e.args[2], e.args[3], e.args[4],
    ];
    for (i, wv) in words.iter().enumerate() {
        buf[i * 8..i * 8 + 8].copy_from_slice(&wv.to_le_bytes());
    }
}

fn decode(buf: &[u8; SLOT_BYTES]) -> io::Result<Entry> {
    let mut words = [0u64; 8];
    for (i, wv) in words.iter_mut().enumerate() {
        *wv = u64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap());
    }
    let meta = words[2];
    Ok(Entry {
        index: words[0],
        ts: words[1],
        event: (meta >> 16) as u16,
        kind: Kind::from_u8(meta as u8)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad record kind"))?,
        nargs: (meta >> 8) as u8,
        args: [words[3], words[4], words[5], words[6], words[7]],
    })
}

/// Writes the Chrome/Perfetto JSON for several snapshots on one timeline, one
/// Perfetto process per snapshot. Timestamps are each snapshot's
/// `CLOCK_MONOTONIC` microseconds, so dumps taken on the same host line up.
///
/// The Perfetto pid is the recorded pid unless an earlier snapshot already
/// uses it (two dumps of one process, or a reused pid), in which case the
/// next free number is taken so the dumps stay on separate tracks;
/// `metadata.fasttrace[i].pid` always carries the recorded pid.
pub fn write_chrome_json_merged<W: Write>(snaps: &[Snapshot], mut w: W) -> io::Result<()> {
    let mut events: Vec<serde_json::Value> =
        Vec::with_capacity(snaps.iter().map(|s| s.total_entries() + 8).sum());
    let mut used: Vec<u32> = Vec::with_capacity(snaps.len());
    let mut meta = Vec::with_capacity(snaps.len());
    for s in snaps {
        let mut pid = s.pid;
        while used.contains(&pid) {
            pid = pid.wrapping_add(1);
        }
        used.push(pid);
        s.chrome_events(pid, &mut events);
        meta.push(serde_json::json!({
            "pid": s.pid,
            "perfetto_pid": pid,
            "clock": s.clock,
            "threads": s.threads.iter().map(|t| &t.info).collect::<Vec<_>>(),
        }));
    }
    let doc = serde_json::json!({
        "displayTimeUnit": "ns",
        "traceEvents": events,
        "metadata": { "fasttrace": meta }
    });
    serde_json::to_writer(&mut w, &doc)?;
    w.flush()
}
