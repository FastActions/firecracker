//! Events and the registry that gives the ring's integers meaning.
//!
//! An [`Event`] is a `static`: its name, argument names and argument kinds
//! live in the binary, and its 16-bit id is assigned on the first emit. The
//! consumer gets the whole registry with every snapshot, so a raw record
//! (`event=7, args=[4096, 131072]`) can be rendered as
//! `storageproxy.read{offset=0x1000, len=131072}`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::entry::{pack_meta, Kind, MAX_ARGS};
use crate::ring;

/// How a consumer should render an argument.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArgKind {
    U64,
    I64,
    Hex,
    Bool,
    /// Byte count.
    Bytes,
    /// Nanoseconds.
    Nanos,
    /// Raw clock ticks (another `clock::now()` reading); rendered as a
    /// duration relative to the record's own timestamp.
    Ticks,
    /// A string id from [`intern`].
    Str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Arg {
    pub name: &'static str,
    pub kind: ArgKind,
}

pub const fn arg(name: &'static str, kind: ArgKind) -> Arg {
    Arg { name, kind }
}

/// A trace point. Declare one `static` per thing you want to see.
///
/// ```
/// use fasttrace::{arg, ArgKind, Event};
/// static READ: Event = Event::new("proxy.read", &[arg("offset", ArgKind::Hex), arg("len", ArgKind::Bytes)]);
/// READ.instant([0x1000, 4096]);
/// ```
pub struct Event {
    name: &'static str,
    args: &'static [Arg],
    render: Render,
    track: Option<&'static str>,
    id: AtomicU16,
}

/// How a viewer should draw an event's records. The hot path never looks
/// at this; it travels in the registry so `write_chrome_json` can pair
/// records into spans or sum them into counters offline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Render {
    /// Drawn as the record's own [`Kind`] on the emitting thread's track.
    Plain,
    /// Opens an async span named `span`, identified by the value of argument
    /// `key`; closed by the next `SpanEnd` with the same `span` and key value.
    SpanBegin {
        span: &'static str,
        key: u8,
    },
    SpanEnd {
        span: &'static str,
        key: u8,
    },
}

impl Event {
    pub const fn new(name: &'static str, args: &'static [Arg]) -> Event {
        assert!(
            args.len() <= MAX_ARGS,
            "an event carries at most MAX_ARGS arguments"
        );
        Event {
            name,
            args,
            render: Render::Plain,
            track: None,
            id: AtomicU16::new(0),
        }
    }

    /// Set the viewer hint; usable in a `static` initializer.
    pub const fn render(mut self, render: Render) -> Event {
        if let Render::SpanBegin { key, .. } | Render::SpanEnd { key, .. } = render {
            assert!(
                (key as usize) < self.args.len(),
                "span key must name an argument"
            );
        }
        self.render = render;
        self
    }

    /// Draw the event on a named sub-track of its thread instead of the
    /// thread's own track.
    pub const fn track(mut self, track: &'static str) -> Event {
        self.track = Some(track);
        self
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn args(&self) -> &'static [Arg] {
        self.args
    }

    /// Registry id, assigning one on first use.
    #[inline(always)]
    pub fn id(&'static self) -> u16 {
        let id = self.id.load(Ordering::Relaxed);
        if id != 0 {
            id
        } else {
            self.register()
        }
    }

    #[cold]
    #[inline(never)]
    fn register(&'static self) -> u16 {
        let mut reg = registry().lock().unwrap_or_else(|e| e.into_inner());
        // Another thread may have won the race between our load and the lock.
        let id = self.id.load(Ordering::Relaxed);
        if id != 0 {
            return id;
        }
        assert!(
            reg.events.len() < u16::MAX as usize,
            "fasttrace: more than 65534 events"
        );
        reg.events.push(self);
        let id = reg.events.len() as u16;
        self.id.store(id, Ordering::Relaxed);
        id
    }

    #[inline(always)]
    fn emit<const N: usize>(&'static self, kind: Kind, args: [u64; N]) {
        const { assert!(N <= MAX_ARGS, "an emit carries at most MAX_ARGS arguments") };
        let meta = pack_meta(self.id(), kind, N as u8);
        ring::emit(meta, args);
    }

    /// Point event.
    #[inline(always)]
    pub fn instant<const N: usize>(&'static self, args: [u64; N]) {
        self.emit(Kind::Instant, args)
    }

    #[inline(always)]
    pub fn begin<const N: usize>(&'static self, args: [u64; N]) {
        self.emit(Kind::Begin, args)
    }

    #[inline(always)]
    pub fn end<const N: usize>(&'static self, args: [u64; N]) {
        self.emit(Kind::End, args)
    }

    /// Counter sample; argument `i` is series `args[i].name`.
    #[inline(always)]
    pub fn counter<const N: usize>(&'static self, args: [u64; N]) {
        self.emit(Kind::Counter, args)
    }

    /// `begin` now, `end` (with no arguments) when the guard drops.
    #[inline(always)]
    pub fn span<const N: usize>(&'static self, args: [u64; N]) -> Span {
        self.begin(args);
        Span {
            event: self,
            _thread_local: std::marker::PhantomData,
        }
    }
}

impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Event")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("id", &self.id.load(Ordering::Relaxed))
            .finish()
    }
}

/// Guard returned by [`Event::span`].
///
/// `Begin` and `End` are paired per thread, so the guard is `!Send`: it must
/// drop on the thread that created it. Work that migrates between threads
/// should emit `begin`/`end` explicitly with a correlating argument.
///
/// ```compile_fail
/// static E: fasttrace::Event = fasttrace::Event::new("e", &[]);
/// let span = E.span([]);
/// std::thread::spawn(move || drop(span));
/// ```
#[must_use = "a span ends when the guard drops; bind it to a name"]
pub struct Span {
    event: &'static Event,
    _thread_local: std::marker::PhantomData<*const ()>,
}

impl Span {
    /// End the span now, attaching arguments to the `End` record.
    #[inline(always)]
    pub fn end<const N: usize>(self, args: [u64; N]) {
        self.event.end(args);
        std::mem::forget(self);
    }
}

impl Drop for Span {
    #[inline(always)]
    fn drop(&mut self) {
        self.event.end([]);
    }
}

#[derive(Default)]
pub(crate) struct Registry {
    pub(crate) events: Vec<&'static Event>,
    pub(crate) strings: Vec<String>,
    string_ids: HashMap<String, u64>,
}

pub(crate) fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Registry::default()))
}

/// Register a string once and get the integer to put in an [`ArgKind::Str`]
/// argument. Takes a lock; call it when the string comes into existence
/// (a volume name at expose time), not on the hot path.
pub fn intern(s: &str) -> u64 {
    let mut reg = registry().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(&id) = reg.string_ids.get(s) {
        return id;
    }
    reg.strings.push(s.to_owned());
    let id = reg.strings.len() as u64;
    reg.string_ids.insert(s.to_owned(), id);
    id
}

/// Serializable view of the registry, embedded in every snapshot.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct RegistrySnapshot {
    /// `events[i]` has id `i + 1`.
    pub events: Vec<EventDef>,
    /// `strings[i]` has id `i + 1`.
    pub strings: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct EventDef {
    pub name: String,
    pub args: Vec<ArgDef>,
    #[serde(default = "render_plain", deserialize_with = "render_lenient")]
    pub render: RenderDef,
    #[serde(default)]
    pub track: Option<String>,
}

fn render_plain() -> RenderDef {
    RenderDef::Plain
}

/// A hint this reader does not know renders as plain records, so a trace
/// written by a newer fasttrace still dumps with an older `fasttrace-dump`.
fn render_lenient<'de, D: serde::Deserializer<'de>>(d: D) -> Result<RenderDef, D::Error> {
    let v = <serde_json::Value as serde::Deserialize>::deserialize(d)?;
    Ok(serde_json::from_value(v).unwrap_or(RenderDef::Plain))
}

/// Owned, serializable form of [`Render`] as stored in a snapshot.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RenderDef {
    Plain,
    SpanBegin { span: String, key: u8 },
    SpanEnd { span: String, key: u8 },
}

impl From<Render> for RenderDef {
    fn from(r: Render) -> RenderDef {
        match r {
            Render::Plain => RenderDef::Plain,
            Render::SpanBegin { span, key } => RenderDef::SpanBegin {
                span: span.to_owned(),
                key,
            },
            Render::SpanEnd { span, key } => RenderDef::SpanEnd {
                span: span.to_owned(),
                key,
            },
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ArgDef {
    pub name: String,
    pub kind: ArgKind,
}

impl RegistrySnapshot {
    pub(crate) fn capture() -> RegistrySnapshot {
        let reg = registry().lock().unwrap_or_else(|e| e.into_inner());
        RegistrySnapshot {
            events: reg
                .events
                .iter()
                .map(|e| EventDef {
                    name: e.name.to_owned(),
                    render: e.render.into(),
                    track: e.track.map(str::to_owned),
                    args: e
                        .args
                        .iter()
                        .map(|a| ArgDef {
                            name: a.name.to_owned(),
                            kind: a.kind,
                        })
                        .collect(),
                })
                .collect(),
            strings: reg.strings.clone(),
        }
    }

    pub fn event(&self, id: u16) -> Option<&EventDef> {
        (id as usize)
            .checked_sub(1)
            .and_then(|i| self.events.get(i))
    }

    pub fn string(&self, id: u64) -> Option<&str> {
        (id as usize)
            .checked_sub(1)
            .and_then(|i| self.strings.get(i))
            .map(String::as_str)
    }
}
