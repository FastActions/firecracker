use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fasttrace::{arg, ArgKind, Entry, Event, Kind, Snapshot};

static READ: Event = Event::new(
    "test.read",
    &[arg("offset", ArgKind::Hex), arg("len", ArgKind::Bytes)],
);
static TICK: Event = Event::new(
    "test.tick",
    &[arg("i", ArgKind::U64), arg("check", ArgKind::U64)],
);
static NAMED: Event = Event::new(
    "test.named",
    &[arg("who", ArgKind::Str), arg("ok", ArgKind::Bool)],
);
static GAUGE: Event = Event::new("test.gauge", &[arg("depth", ArgKind::U64)]);
static ELAPSED: Event = Event::new("test.elapsed", &[arg("since", ArgKind::Ticks)]);

fn thread_entries<'a>(snap: &'a Snapshot, name: &str) -> &'a [Entry] {
    snap.threads
        .iter()
        .find(|t| t.info.name.as_deref() == Some(name))
        .map(|t| t.entries.as_slice())
        .unwrap_or_else(|| panic!("no thread named {name} in snapshot"))
}

#[test]
fn records_round_trip_through_registry_and_raw_file() {
    let who = fasttrace::intern("volume-a");
    let before = fasttrace::clock::now();
    std::thread::Builder::new()
        .name("rt-writer".into())
        .spawn(move || {
            {
                let _s = READ.span([0x1000, 4096]);
                NAMED.instant([who, 1]);
                GAUGE.counter([3]);
            }
            ELAPSED.instant([before]);
        })
        .unwrap()
        .join()
        .unwrap();

    let snap = fasttrace::snapshot();
    let entries = thread_entries(&snap, "rt-writer");
    assert_eq!(entries.len(), 5);
    assert_eq!(
        entries.iter().map(|e| e.index).collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4]
    );
    let kinds: Vec<Kind> = entries.iter().map(|e| e.kind).collect();
    assert_eq!(
        kinds,
        vec![
            Kind::Begin,
            Kind::Instant,
            Kind::Counter,
            Kind::End,
            Kind::Instant
        ]
    );
    assert!(
        entries.windows(2).all(|w| w[0].ts <= w[1].ts),
        "timestamps are monotonic per thread"
    );

    let read_def = snap.registry.event(entries[0].event).unwrap();
    assert_eq!(read_def.name, "test.read");
    assert_eq!(read_def.args[0].name, "offset");
    assert_eq!(read_def.args[0].kind, ArgKind::Hex);
    let args = snap.render_args(&entries[0]);
    assert_eq!(args[0], ("offset".to_owned(), serde_json::json!("0x1000")));
    assert_eq!(args[1], ("len".to_owned(), serde_json::json!(4096)));
    let args = snap.render_args(&entries[1]);
    assert_eq!(args[0], ("who".to_owned(), serde_json::json!("volume-a")));
    assert_eq!(args[1], ("ok".to_owned(), serde_json::json!(true)));
    let args = snap.render_args(&entries[4]);
    assert!(args[0].1.as_f64().unwrap() >= 0.0);

    let t = snap
        .threads
        .iter()
        .find(|t| t.info.name.as_deref() == Some("rt-writer"))
        .unwrap();
    assert!(t.info.finished, "exited thread's ring is marked finished");

    let mut raw = Vec::new();
    snap.write_raw(&mut raw).unwrap();
    let back = Snapshot::read_raw(Cursor::new(raw)).unwrap();
    assert_eq!(back, snap);

    let mut json = Vec::new();
    snap.write_chrome_json(&mut json).unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&json).unwrap();
    let events = doc["traceEvents"].as_array().unwrap();
    let read_b = events
        .iter()
        .find(|e| e["name"] == "test.read" && e["ph"] == "B")
        .expect("span begin exported as Chrome B event");
    assert_eq!(read_b["args"]["offset"], "0x1000");
    assert!(events
        .iter()
        .any(|e| e["name"] == "test.read" && e["ph"] == "E"));
    assert!(events
        .iter()
        .any(|e| e["name"] == "test.gauge" && e["ph"] == "C"));
    assert!(events.iter().any(|e| e["ph"] == "M"
        && e["name"] == "thread_name"
        && e["args"]["name"] == "rt-writer (exited)"));

    let mut text = Vec::new();
    snap.write_text(&mut text).unwrap();
    let text = String::from_utf8(text).unwrap();
    assert!(
        text.contains("test.read offset=\"0x1000\" len=4096"),
        "{text}"
    );
}

/// A writer spinning through the ring far faster than the reader can copy
/// it: every record the snapshot returns must be internally consistent
/// (never a mix of two writes) and in order.
#[test]
fn concurrent_snapshot_never_yields_torn_records() {
    let stop = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    let writer = {
        let stop = stop.clone();
        let started = started.clone();
        std::thread::Builder::new()
            .name("torn-writer".into())
            .spawn(move || {
                let mut i = 0u64;
                TICK.instant([i, i.wrapping_mul(0x9E37_79B9_7F4A_7C15)]);
                i += 1;
                started.store(true, Ordering::Release);
                while !stop.load(Ordering::Relaxed) {
                    TICK.instant([i, i.wrapping_mul(0x9E37_79B9_7F4A_7C15)]);
                    i += 1;
                }
                i
            })
            .unwrap()
    };
    // The ring attaches on the first emit; snapshot only once it exists.
    while !started.load(Ordering::Acquire) {
        std::thread::yield_now();
    }
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut snapshots = 0;
    let mut records = 0usize;
    let mut gaps = 0usize;
    while Instant::now() < deadline {
        let snap = fasttrace::snapshot();
        let entries = thread_entries(&snap, "torn-writer");
        for w in entries.windows(2) {
            assert!(w[0].index < w[1].index, "snapshot is ordered by index");
            assert!(w[0].ts <= w[1].ts, "timestamps follow index order");
            if w[1].index != w[0].index + 1 {
                gaps += 1;
            }
        }
        for e in entries {
            assert_eq!(e.event, TICK.id(), "event id intact");
            assert_eq!(e.kind, Kind::Instant);
            assert_eq!(e.nargs, 2);
            let i = e.args[0];
            assert_eq!(
                e.args[1],
                i.wrapping_mul(0x9E37_79B9_7F4A_7C15),
                "args from one write"
            );
            assert_eq!(e.index, i, "index matches the writer's own counter");
        }
        snapshots += 1;
        records += entries.len();
    }
    stop.store(true, Ordering::Relaxed);
    let written = writer.join().unwrap();
    assert!(snapshots > 0 && records > 0);
    assert!(
        written as usize > records,
        "writer outran the reader: {written} written"
    );
    eprintln!("{snapshots} snapshots, {records} records checked, {gaps} overwrite gaps, {written} written");
}

#[test]
fn ring_overwrites_oldest_and_keeps_capacity_newest() {
    let cap = fasttrace::Config::from_env()
        .entries_per_thread
        .next_power_of_two() as u64;
    let n = cap * 3 + 17;
    std::thread::Builder::new()
        .name("wrap-writer".into())
        .spawn(move || {
            for i in 0..n {
                READ.instant([i, 1]);
            }
            assert_eq!(fasttrace::thread_emitted(), n);
        })
        .unwrap()
        .join()
        .unwrap();
    let snap = fasttrace::snapshot();
    let entries = thread_entries(&snap, "wrap-writer");
    assert_eq!(entries.len() as u64, cap);
    assert_eq!(entries[0].index, n - cap);
    assert_eq!(entries[cap as usize - 1].index, n - 1);
    assert!(entries
        .iter()
        .enumerate()
        .all(|(k, e)| e.index == n - cap + k as u64));
}

#[test]
fn clock_converts_ticks_to_wall_time() {
    let t0 = fasttrace::clock::now();
    let m0 = fasttrace::clock::monotonic_ns();
    std::thread::sleep(Duration::from_millis(20));
    let t1 = fasttrace::clock::now();
    let m1 = fasttrace::clock::monotonic_ns();
    let info = fasttrace::clock::info();
    let dt = info.ticks_to_ns(t1 - t0);
    let dm = (m1 - m0) as f64;
    assert!(
        (dt - dm).abs() / dm < 0.05,
        "tick->ns conversion within 5%: {dt} vs {dm}"
    );
    let abs = info.ticks_to_monotonic_ns(t1);
    assert!(
        (abs - m1 as f64).abs() < 2_000_000.0,
        "absolute conversion within 2 ms: {abs} vs {m1}"
    );
}

#[test]
fn retention_evicts_by_exit_order_not_creation_order() {
    // Rings are shared process-wide across tests, so this only checks that
    // the thread which exited *last* survives the retention sweep even though
    // it was created first.
    static E: Event = Event::new("test.retain", &[]);
    let n = fasttrace::Config::from_env().retain_finished_threads + 8;
    let handles: Vec<_> = (0..n)
        .map(|i| {
            std::thread::Builder::new()
                .name(format!("retain-{i}"))
                .spawn(move || {
                    E.instant([]);
                    if i == 0 {
                        std::thread::sleep(std::time::Duration::from_millis(200));
                    }
                })
                .unwrap()
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let snap = fasttrace::snapshot();
    assert!(
        snap.threads
            .iter()
            .any(|t| t.info.name.as_deref() == Some("retain-0") && t.info.finished),
        "the last thread to exit must survive retention"
    );
}

#[test]
fn shorter_record_does_not_expose_overwritten_arguments() {
    static WIDE: Event = Event::new("test.wide", &[arg("a", ArgKind::U64); 5]);
    static NARROW: Event = Event::new("test.narrow", &[arg("a", ArgKind::U64)]);
    let entries = fasttrace::Config::from_env().entries_per_thread;
    std::thread::Builder::new()
        .name("overwrite-args".into())
        .spawn(move || {
            for _ in 0..entries {
                WIDE.instant([0xdead, 0xdead, 0xdead, 0xdead, 0xdead]);
            }
            for _ in 0..entries {
                NARROW.instant([1]);
            }
        })
        .unwrap()
        .join()
        .unwrap();
    let snap = fasttrace::snapshot();
    let t = snap
        .threads
        .iter()
        .find(|t| t.info.name.as_deref() == Some("overwrite-args"))
        .unwrap();
    assert!(!t.entries.is_empty());
    for e in &t.entries {
        assert_eq!(e.args, [1, 0, 0, 0, 0], "stale words must read as zero");
    }
}

#[test]
fn header_json_round_trips_the_tsc_rate_exactly() {
    // A calibrated rate like this one does not survive serde_json's default
    // float parser; the header relies on the float_roundtrip feature.
    let mut snap = fasttrace::snapshot();
    snap.clock.ticks_per_sec = 4291881836.1193533;
    let hdr = serde_json::to_string(&snap.header()).unwrap();
    let back: fasttrace::Header = serde_json::from_str(&hdr).unwrap();
    assert_eq!(back.clock, snap.clock);
}

#[test]
fn render_hints_shape_chrome_json() {
    use fasttrace::Render;
    static OPEN: Event = Event::new(
        "rh.open",
        &[arg("key", ArgKind::U64), arg("len", ArgKind::Bytes)],
    )
    .render(Render::SpanBegin {
        span: "rh.span",
        key: 0,
    });
    static CLOSE: Event = Event::new(
        "rh.close",
        &[arg("status", ArgKind::U64), arg("key", ArgKind::U64)],
    )
    .render(Render::SpanEnd {
        span: "rh.span",
        key: 1,
    });
    static HITS: Event = Event::new(
        "rh.hits",
        &[arg("hit", ArgKind::U64), arg("miss", ArgKind::U64)],
    )
    .track("cache");
    static SIDE: Event = Event::new("rh.side", &[]).track("writeout");

    let done = Arc::new(AtomicBool::new(false));
    let d = done.clone();
    let t = std::thread::spawn(move || {
        OPEN.instant([7, 4096]);
        HITS.counter([1, 0]);
        HITS.counter([1, 1]);
        SIDE.instant([]);
        CLOSE.instant([0, 7]);
        d.store(true, Ordering::Release);
        while !d.load(Ordering::Acquire) {}
    });
    while !done.load(Ordering::Acquire) {
        std::thread::yield_now();
    }
    let snap = fasttrace::snapshot();
    t.join().unwrap();

    let defs = &snap.registry.events;
    let open = defs.iter().find(|d| d.name == "rh.open").unwrap();
    assert_eq!(
        open.render,
        fasttrace::RenderDef::SpanBegin {
            span: "rh.span".into(),
            key: 0
        }
    );
    let hits = defs.iter().find(|d| d.name == "rh.hits").unwrap();
    assert_eq!(hits.track.as_deref(), Some("cache"));
    let back: fasttrace::RegistrySnapshot =
        serde_json::from_str(&serde_json::to_string(&snap.registry).unwrap()).unwrap();
    assert_eq!(back.events, snap.registry.events);
    let mut unknown = serde_json::to_value(&snap.registry).unwrap();
    unknown["events"][0]["render"] = serde_json::json!({"Flow": {"key": 0}});
    let back: fasttrace::RegistrySnapshot = serde_json::from_value(unknown).unwrap();
    assert_eq!(
        back.events[0].render,
        fasttrace::RenderDef::Plain,
        "unknown hints read as plain"
    );

    let mut json = Vec::new();
    snap.write_chrome_json(&mut json).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
    let ev = v["traceEvents"].as_array().unwrap();
    let of = |name: &str, ph: &str| -> Vec<&serde_json::Value> {
        ev.iter()
            .filter(|e| e["name"] == name && e["ph"] == ph)
            .collect()
    };
    let b = of("rh.span", "b");
    let e = of("rh.span", "e");
    assert_eq!((b.len(), e.len()), (1, 1));
    assert_eq!(b[0]["id2"]["local"], "0x7");
    assert_eq!(e[0]["id2"]["local"], "0x7");
    assert_eq!(b[0]["pid"], e[0]["pid"]);

    let c = of("rh.hits", "C");
    assert_eq!(c.len(), 2);
    assert_eq!(c[1]["args"]["hit"], 1);
    assert_eq!(c[1]["args"]["miss"], 1);
    let side = of("rh.side", "i");
    assert_ne!(
        c[0]["tid"], side[0]["tid"],
        "tracks get distinct synthetic tids"
    );
    let names: Vec<String> = ev
        .iter()
        .filter(|e| e["name"] == "thread_name")
        .map(|e| e["args"]["name"].as_str().unwrap().to_owned())
        .collect();
    assert!(names.iter().any(|n| n.ends_with(": cache")), "{names:?}");
    assert!(names.iter().any(|n| n.ends_with(": writeout")), "{names:?}");
}

#[test]
fn merged_chrome_json_keeps_one_pid_per_snapshot() {
    // Two dumps of the same process: same recorded pid and thread ids, yet
    // they must land on separate Perfetto processes.
    let a = fasttrace::snapshot();
    let mut b = fasttrace::snapshot();
    b.process = Some("other".to_string());
    let mut json = Vec::new();
    fasttrace::write_chrome_json_merged(&[a.clone(), b.clone()], &mut json).unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&json).unwrap();
    let events = doc["traceEvents"].as_array().unwrap();
    let names: Vec<_> = events
        .iter()
        .filter(|e| e["name"] == "process_name")
        .map(|e| (e["pid"].as_u64().unwrap(), e["args"]["name"].clone()))
        .collect();
    let b_pid = u64::from(a.pid) + 1;
    assert_eq!(names.len(), 2);
    assert_eq!(names[0].0, u64::from(a.pid));
    assert_eq!(names[1], (b_pid, "other".into()));
    assert!(events
        .iter()
        .all(|e| e["pid"] == a.pid || e["pid"] == b_pid));
    assert!(events
        .iter()
        .any(|e| e["name"] == "thread_name" && e["pid"] == b_pid));
    let meta = doc["metadata"]["fasttrace"].as_array().unwrap();
    assert_eq!(meta.len(), 2);
    assert_eq!(meta[1]["pid"], a.pid);
    assert_eq!(meta[1]["perfetto_pid"], b_pid);

    let mut single = Vec::new();
    a.write_chrome_json(&mut single).unwrap();
    let single: serde_json::Value = serde_json::from_slice(&single).unwrap();
    assert_eq!(single["metadata"]["fasttrace"].as_array().unwrap().len(), 1);
}
