// Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Fixed-cost trace points on the guest IO path.
//!
//! Every point is a `static` [`fasttrace::Event`]: an emit is one 64-byte
//! store into the emitting thread's lock-free ring, with no formatting,
//! allocation or lock. The ring holds the most recent `FASTTRACE_RING_ENTRIES`
//! records per thread; [`dump_if_requested`] writes all rings out as a raw
//! `.ftrc` file when `FIRECRACKER_FASTTRACE_DUMP` names a path
//! (`fasttrace-dump` turns it into Perfetto JSON). Timestamps are the raw TSC,
//! so a dump taken on the same host as a storageproxy dump lines up on one
//! timeline.
//!
//! Without the `fasttrace` cargo feature every point compiles to nothing.

#[cfg(feature = "fasttrace")]
pub use fasttrace::{ArgKind, Event, Render, Span, arg};
use kvm_ioctls::VcpuExit;
#[cfg(not(feature = "fasttrace"))]
pub use noop::{ArgKind, Event, Render, Span, arg};
use vmm_sys_util::errno;

#[cfg(not(feature = "fasttrace"))]
mod noop {
    //! Same surface as `fasttrace::{arg, ArgKind, Event, Render, Span}`, all
    //! inlined to nothing.
    #![allow(missing_docs)]
    #[derive(Debug, Clone, Copy)]
    pub enum ArgKind {
        U64,
        I64,
        Hex,
        Bool,
        Bytes,
        Nanos,
        Ticks,
        Str,
    }
    #[derive(Debug, Clone, Copy)]
    pub struct Arg;
    pub const fn arg(_name: &'static str, _kind: ArgKind) -> Arg {
        Arg
    }
    #[derive(Debug)]
    pub struct Event;
    #[derive(Debug)]
    #[must_use]
    pub struct Span;
    #[derive(Debug, Clone, Copy)]
    pub enum Render {
        Plain,
        SpanBegin { span: &'static str, key: u8 },
        SpanEnd { span: &'static str, key: u8 },
    }
    impl Event {
        pub const fn new(_name: &'static str, _args: &'static [Arg]) -> Event {
            Event
        }
        pub const fn render(self, _r: Render) -> Event {
            Event
        }
        pub const fn track(self, _t: &'static str) -> Event {
            Event
        }
        #[inline(always)]
        pub fn instant<const N: usize>(&'static self, _args: [u64; N]) {}
        #[inline(always)]
        pub fn begin<const N: usize>(&'static self, _args: [u64; N]) {}
        #[inline(always)]
        pub fn end<const N: usize>(&'static self, _args: [u64; N]) {}
        #[inline(always)]
        pub fn counter<const N: usize>(&'static self, _args: [u64; N]) {}
        #[inline(always)]
        pub fn span<const N: usize>(&'static self, _args: [u64; N]) -> Span {
            Span
        }
    }
    impl Span {
        #[inline(always)]
        pub fn end<const N: usize>(self, _args: [u64; N]) {}
    }
}

use ArgKind::{Bool, Bytes, Hex, U64};

/// Key of one in-flight block request: `device << 32 | queue << 16 | head`.
/// `device` is the address of the device struct (stable while it is alive,
/// distinct across drives); the head index is unique while the chain is in
/// flight, so the key identifies the request between `fc.blk.pop` and
/// `fc.blk.used`.
pub fn blk_key(device: usize, queue: usize, head: u16) -> u64 {
    (((device >> 4) as u64 & 0xffff_ffff) << 32) | ((queue as u64) << 16) | u64::from(head)
}

// In-process virtio-blk (devices/virtio/block/virtio). One kick drains the
// avail ring; each popped chain is an async span keyed by `req` that the
// used-ring entry closes, inline for the sync engine and from the io_uring
// completion drain for the async engine. `op` is the virtio-blk request type
// (0 in, 1 out, 4 flush, 8 get-id, 11 discard) and `len` the data length.
const BLK_SPAN: &str = "fc.blk.request";
/// One in-process virtio-blk kick: the eventfd read and the avail-ring drain.
pub static BLK_KICK: Event = Event::new("fc.blk.kick", &[arg("queue", U64)]);
/// A descriptor chain accepted for processing; opens the `fc.blk.request`
/// span. `offset` is in bytes for every request type.
pub static BLK_POP: Event = Event::new(
    "fc.blk.pop",
    &[
        arg("req", Hex),
        arg("op", U64),
        arg("offset", Hex),
        arg("len", Bytes),
        arg("queue", U64),
    ],
)
.render(Render::SpanBegin {
    span: BLK_SPAN,
    key: 0,
});
/// The chain returned to the avail ring because the io engine is throttled;
/// closes the `fc.blk.request` span (the retry opens a new one).
pub static BLK_THROTTLED: Event =
    Event::new("fc.blk.throttled", &[arg("req", Hex)]).render(Render::SpanEnd {
        span: BLK_SPAN,
        key: 0,
    });

/// The chain added to the used ring; closes the `fc.blk.request` span.
pub static BLK_USED: Event = Event::new("fc.blk.used", &[arg("req", Hex), arg("len", Bytes)])
    .render(Render::SpanEnd {
        span: BLK_SPAN,
        key: 0,
    });
/// One io_uring completion drain of the async file engine.
pub static BLK_COMPLETION: Event = Event::new("fc.blk.completion", &[arg("queue", U64)]);

// virtio-net: one span per rx / tx queue drain, one instant per transmitted
// frame (`head` is the descriptor head, `len` the frame length).
/// One rx drain: frames from the tap or MMDS written into guest buffers.
pub static NET_RX: Event = Event::new("fc.net.rx", &[arg("queue", U64)]);
/// One tx drain of the guest's transmit ring.
pub static NET_TX: Event = Event::new("fc.net.tx", &[arg("queue", U64)]);
/// One frame handed to the tap or MMDS from the tx ring.
pub static NET_TX_FRAME: Event =
    Event::new("fc.net.tx_frame", &[arg("head", U64), arg("len", Bytes)]);

// Interrupt delivery. `dev` is the device's irq eventfd number, shared by the
// three points so a `fc.irq` can be matched to the guest's ISR read and ack
// (`fc.mmio.isr_*` fire inside the vCPU's `fc.vcpu.mmio` span). A vhost-user
// backend signals its irqfd directly, so for it only the ISR points exist.
/// An interrupt raised by an in-process device; `status` is the ISR after the raise.
pub static IRQ: Event = Event::new(
    "fc.irq",
    &[arg("dev", U64), arg("vring", Bool), arg("status", Hex)],
);
/// The guest reading the interrupt status register.
pub static ISR_READ: Event = Event::new("fc.mmio.isr_read", &[arg("dev", U64), arg("status", Hex)]);
/// The guest acknowledging interrupt status bits.
pub static ISR_ACK: Event = Event::new("fc.mmio.isr_ack", &[arg("dev", U64), arg("mask", Hex)]);

// vCPU exits that reach device emulation. Port IO to the serial console is
// skipped: it is one exit per byte of guest output and carries no IO-path
// information.
/// Emulation of one MMIO exit on the vCPU thread.
pub static VCPU_MMIO: Event = Event::new(
    "fc.vcpu.mmio",
    &[
        arg("vcpu", U64),
        arg("write", Bool),
        arg("addr", Hex),
        arg("len", Bytes),
    ],
);
/// Emulation of one port IO exit on the vCPU thread.
pub static VCPU_PIO: Event = Event::new(
    "fc.vcpu.pio",
    &[
        arg("vcpu", U64),
        arg("write", Bool),
        arg("port", Hex),
        arg("len", Bytes),
    ],
);

const SERIAL_PORTS: std::ops::RangeInclusive<u16> = 0x3f8..=0x3ff;

/// Opens the span covering the emulation of a device-touching exit; `None`
/// for exits that are not traced.
#[inline]
pub fn vcpu_exit_span(vcpu: u8, exit: &Result<VcpuExit<'_>, errno::Error>) -> Option<Span> {
    match exit {
        Ok(VcpuExit::MmioRead(addr, data)) => {
            Some(VCPU_MMIO.span([u64::from(vcpu), 0, *addr, data.len() as u64]))
        }
        Ok(VcpuExit::MmioWrite(addr, data)) => {
            Some(VCPU_MMIO.span([u64::from(vcpu), 1, *addr, data.len() as u64]))
        }
        Ok(VcpuExit::IoIn(port, data)) if !SERIAL_PORTS.contains(port) => {
            Some(VCPU_PIO.span([u64::from(vcpu), 0, u64::from(*port), data.len() as u64]))
        }
        Ok(VcpuExit::IoOut(port, data)) if !SERIAL_PORTS.contains(port) => {
            Some(VCPU_PIO.span([u64::from(vcpu), 1, u64::from(*port), data.len() as u64]))
        }
        _ => None,
    }
}

/// Calibrates the trace clock and sizes the rings from the environment. Must
/// run before the first emit; a no-op without the feature.
pub fn init() {
    #[cfg(feature = "fasttrace")]
    fasttrace::init(fasttrace::Config::from_env());
}

/// Writes every thread's ring to the path named by `FIRECRACKER_FASTTRACE_DUMP`
/// as a raw `.ftrc` file. Does nothing when the variable is unset or without
/// the feature.
pub fn dump_if_requested() {
    #[cfg(feature = "fasttrace")]
    {
        use crate::logger::{info, warn};
        let Ok(path) = std::env::var("FIRECRACKER_FASTTRACE_DUMP") else {
            return;
        };
        let snap = fasttrace::snapshot();
        let res =
            std::fs::File::create(&path).and_then(|f| snap.write_raw(std::io::BufWriter::new(f)));
        match res {
            Ok(()) => info!(
                "fasttrace: wrote {} records to {path}",
                snap.total_entries()
            ),
            Err(err) => warn!("fasttrace: trace dump to {path} failed: {err}"),
        }
    }
}
