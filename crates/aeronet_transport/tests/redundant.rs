//! [`LaneKind::Redundant`] over a simulated lossy link: every message is
//! delivered once, in time, or not at all once expired, and the copies it
//! costs stay within the per-packet budget.
#![cfg(test)]
#![allow(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    reason = "a simulation over small known numbers"
)]

use {
    aeronet_io::Session,
    aeronet_transport::{
        Transport, TransportConfig,
        lane::{LaneIndex, LaneKind},
        packet::{Fragment, PacketHeader},
        recv, send,
    },
    bevy_platform::time::Instant,
    core::time::Duration,
    octs::{Bytes, EncodeLen, Read},
    std::collections::HashMap,
};

/// `aeronet_moq`'s and the web transports' smallest MTU.
const MTU: usize = 1004;
/// The server's flush rate: one flush per 20 Hz tick.
const TICK_MS: u64 = 50;
const EXPIRY_MS: u64 = 1000;
const REDUNDANT: LaneKind = LaneKind::Redundant {
    expiry: Duration::from_millis(EXPIRY_MS),
};
const LANES: [LaneKind; 2] = [LaneKind::UnreliableUnordered, REDUNDANT];
const STATE: LaneIndex = LaneIndex::new(0);
const EVENTS: LaneIndex = LaneIndex::new(1);
/// An event's size on the wire, about an `AttackHit`'s.
const EVENT_LEN: usize = 48;

/// xorshift64*: deterministic runs without a dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn chance(&mut self, p: f64) -> bool {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// Which packets the link loses, and how late it delivers the rest.
#[derive(Clone, Copy)]
enum Loss {
    /// Each packet independently.
    Random(f64),
    /// Gilbert-Elliott: good -> bad with `enter`, bad -> good with `leave`;
    /// everything is lost while bad.
    Bursts { enter: f64, leave: f64 },
    /// Nothing gets through in `[from, to)` ms, both ways.
    Blackout { from: u64, to: u64 },
}

#[derive(Clone, Copy)]
struct Link {
    loss: Loss,
    /// One-way delay, plus up to `jitter_ms` more: jitter past the tick
    /// spacing reorders packets.
    delay_ms: u64,
    jitter_ms: u64,
    bad: bool,
}

impl Link {
    /// The ms at which a packet sent at `t` arrives, or `None` if lost.
    fn carry(&mut self, rng: &mut Rng, t: u64) -> Option<u64> {
        let lost = match self.loss {
            Loss::Random(p) => rng.chance(p),
            Loss::Bursts { enter, leave } => {
                self.bad = if self.bad {
                    !rng.chance(leave)
                } else {
                    rng.chance(enter)
                };
                self.bad
            }
            Loss::Blackout { from, to } => (from..to).contains(&t),
        };
        (!lost).then(|| t + self.delay_ms + rng.below(self.jitter_ms + 1))
    }
}

#[derive(Default)]
struct Report {
    pushed: usize,
    /// Event id -> delivery delay (ms).
    delivered: HashMap<u32, u64>,
    expired: usize,
    /// Event ids pushed, with the ms they were pushed at.
    pushed_at: HashMap<u32, u64>,
    /// Most redundant-lane bytes seen in one packet.
    max_copy_bytes: usize,
    copy_bytes: usize,
    packets: usize,
}

impl Report {
    fn delays(&self) -> Vec<u64> {
        let mut delays = self.delivered.values().copied().collect::<Vec<_>>();
        delays.sort_unstable();
        delays
    }

    fn quantile(&self, q: f64) -> u64 {
        let delays = self.delays();
        delays
            .get(((delays.len() as f64 - 1.0) * q).round() as usize)
            .copied()
            .unwrap_or(0)
    }

    fn print(&self, name: &str) {
        println!(
            "{name}: pushed {} delivered {} expired {} | delay p50 {} p99 {} max {} ms | copies \
             {} B over {} packets ({:.1} B/packet, max {} B)",
            self.pushed,
            self.delivered.len(),
            self.expired,
            self.quantile(0.5),
            self.quantile(0.99),
            self.quantile(1.0),
            self.copy_bytes,
            self.packets,
            self.copy_bytes as f64 / self.packets.max(1) as f64,
            self.max_copy_bytes,
        );
    }
}

/// Redundant-lane fragment bytes in one packet.
fn copy_bytes(mut packet: &[u8]) -> usize {
    packet.read::<PacketHeader>().unwrap();
    let mut bytes = 0;
    while !packet.is_empty() {
        let frag = packet.read::<Fragment>().unwrap();
        if frag.header.lane == EVENTS {
            bytes += frag.encode_len();
        }
    }
    bytes
}

/// Runs `seconds` of a 20 Hz exchange: the sender pushes an event every
/// `event_every` ticks and some state every tick, both sides flush every
/// tick, `link` carries each way. The last second is clean, so everything
/// still live gets through or expires.
fn run(seed: u64, link: Link, seconds: u64, event_every: u64) -> Report {
    run_on(REDUNDANT, seed, link, seconds, event_every)
}

/// [`run`], with the events on a lane of `kind`.
fn run_on(kind: LaneKind, seed: u64, link: Link, seconds: u64, event_every: u64) -> Report {
    let lanes = [LaneKind::UnreliableUnordered, kind];
    let config = TransportConfig::default();
    let start = Instant::now();
    let session = Session::new(start, MTU);
    let mut tx = Transport::new(&session, lanes, lanes, start).unwrap();
    let mut rx = Transport::new(&session, lanes, lanes, start).unwrap();
    let mut rng = Rng(seed | 1);
    let (mut up, mut down) = (link, Link { bad: false, ..link });
    let mut to_rx: Vec<(u64, Bytes)> = Vec::new();
    let mut to_tx: Vec<(u64, Bytes)> = Vec::new();
    let mut report = Report::default();
    let end = seconds * 1000;
    let mut next_id = 0u32;

    let mut t = 0;
    while t <= end + EXPIRY_MS {
        let now = start + Duration::from_millis(t);
        let clean = t >= end;

        // arrivals, in arrival order (reordered by jitter)
        to_rx.sort_by_key(|(at, _)| *at);
        while to_rx.first().is_some_and(|(at, _)| *at <= t) {
            let (_, packet) = to_rx.remove(0);
            recv::recv_on(&mut rx, &config, now, &packet).unwrap();
        }
        for msg in rx.recv.msgs.drain() {
            if msg.lane != EVENTS {
                continue;
            }
            let id = u32::from_le_bytes(msg.payload[..4].try_into().unwrap());
            let delay = t - report.pushed_at[&id];
            assert!(
                report.delivered.insert(id, delay).is_none(),
                "event {id} delivered twice"
            );
        }
        rx.recv.acks.drain().for_each(drop);
        to_tx.sort_by_key(|(at, _)| *at);
        while to_tx.first().is_some_and(|(at, _)| *at <= t) {
            let (_, packet) = to_tx.remove(0);
            recv::recv_on(&mut tx, &config, now, &packet).unwrap();
        }
        tx.recv.msgs.drain().for_each(drop);
        tx.recv.acks.drain().for_each(drop);

        // this tick's traffic
        if t < end {
            tx.send.push(STATE, Bytes::from(vec![0; 200]), now).unwrap();
            if (t / TICK_MS).is_multiple_of(event_every) {
                let mut payload = next_id.to_le_bytes().to_vec();
                payload.resize(EVENT_LEN, 0);
                tx.send.push(EVENTS, Bytes::from(payload), now).unwrap();
                report.pushed_at.insert(next_id, t);
                report.pushed += 1;
                next_id += 1;
            }
        }
        for packet in send::flush_on(&mut tx, now, MTU) {
            let bytes = copy_bytes(&packet);
            report.copy_bytes += bytes;
            report.max_copy_bytes = report.max_copy_bytes.max(bytes);
            report.packets += 1;
            let arrives = if clean {
                Some(t + up.delay_ms)
            } else {
                up.carry(&mut rng, t)
            };
            if let Some(at) = arrives {
                to_rx.push((at, packet));
            }
        }
        for packet in send::flush_on(&mut rx, now, MTU) {
            let arrives = if clean {
                Some(t + down.delay_ms)
            } else {
                down.carry(&mut rng, t)
            };
            if let Some(at) = arrives {
                to_tx.push((at, packet));
            }
        }
        t += TICK_MS;
    }

    report.expired = tx.send.lanes()[1].num_expired();
    // the receiver keeps nothing per message
    assert_eq!(rx.recv.lanes()[1].num_reassembling_msgs(), 0);
    assert_eq!(rx.recv.lanes()[1].num_unordered_msgs(), 0);
    report
}

/// What every run must hold: delivered at most once (checked as it runs),
/// never later than its expiry plus the trip, every undelivered message
/// counted as expired, copies within the budget.
fn check(report: &Report, link: &Link) {
    let latest = EXPIRY_MS + link.delay_ms + link.jitter_ms + TICK_MS;
    for (id, delay) in &report.delivered {
        assert!(
            *delay <= latest,
            "event {id} delivered {delay} ms after its push"
        );
    }
    let undelivered = report.pushed - report.delivered.len();
    assert!(
        undelivered <= report.expired,
        "{undelivered} undelivered but only {} expired",
        report.expired
    );
    assert!(
        report.max_copy_bytes <= TransportConfig::default().redundant_bytes_per_packet,
        "{} B of copies in one packet",
        report.max_copy_bytes
    );
}

/// The one-way trip as the test sees it: arrivals are read once a tick.
const fn trip(link: &Link) -> u64 {
    (link.delay_ms + link.jitter_ms).div_ceil(TICK_MS) * TICK_MS
}

const fn link(loss: Loss) -> Link {
    Link {
        loss,
        delay_ms: 40,
        jitter_ms: 0,
        bad: false,
    }
}

#[test]
fn random_loss_delivers_everything_once() {
    let link = link(Loss::Random(0.15));
    let report = run(1, link, 30, 2);
    report.print("random 15%");
    check(&report, &link);
    assert_eq!(report.delivered.len(), report.pushed);
    // a lost copy is replaced by the next tick's: 2 in a row lost 2% of the
    // time (15% each way), 3 in a row 0.3%
    assert!(report.quantile(0.99) <= trip(&link) + 2 * TICK_MS);
}

#[test]
fn bursts_recover_on_the_first_packet_through() {
    // mean burst 1 / 0.25 = 4 packets (200 ms), ~6% of the time
    let link = link(Loss::Bursts {
        enter: 0.015,
        leave: 0.25,
    });
    let report = run(2, link, 60, 1);
    report.print("bursts");
    check(&report, &link);
    assert_eq!(report.delivered.len(), report.pushed);
}

#[test]
fn blackout_of_400_ms_costs_no_more_than_the_blackout() {
    let link = link(Loss::Blackout {
        from: 2000,
        to: 2400,
    });
    let report = run(3, link, 6, 1);
    report.print("400 ms blackout");
    check(&report, &link);
    assert_eq!(report.delivered.len(), report.pushed);
    assert_eq!(report.expired, 0);
    // pushed as the link died, arrives with the first packet after it: the
    // blackout, plus the trip, plus at most a tick of alignment
    assert!(
        report.quantile(1.0) <= 400 + trip(&link) + TICK_MS,
        "max delay {}",
        report.quantile(1.0)
    );
    // the same events on a reliable lane wait for its resend timer
    let reliable = run_on(LaneKind::ReliableUnordered, 3, link, 6, 1);
    reliable.print("400 ms blackout, reliable unordered");
    assert!(reliable.quantile(1.0) > report.quantile(1.0));
}

#[test]
fn reordering_and_loss_deliver_once() {
    // 120 ms of jitter against 50 ms ticks: packets overtake each other
    let link = Link {
        jitter_ms: 120,
        ..link(Loss::Random(0.05))
    };
    let report = run(4, link, 30, 1);
    report.print("reordered");
    check(&report, &link);
    assert_eq!(report.delivered.len(), report.pushed);
}

#[test]
fn blackout_past_the_expiry_drops_and_never_delivers() {
    let link = link(Loss::Blackout {
        from: 1000,
        to: 3500,
    });
    let report = run(5, link, 6, 1);
    report.print("2.5 s blackout");
    check(&report, &link);
    // pushed in the first 1.5 s of the blackout: expired before it lifted
    let lost = report
        .pushed_at
        .iter()
        .filter(|(id, _)| !report.delivered.contains_key(id))
        .map(|(_, at)| *at)
        .collect::<Vec<_>>();
    assert_ne!(lost.len(), 0);
    assert!(lost.iter().all(|at| (1000..=3500 - EXPIRY_MS).contains(at)));
    // and counted; a few more were delivered, but their acks died in the
    // blackout, so the sender gave up on them too
    assert!(lost.len() <= report.expired && report.expired <= lost.len() + 3);
}

#[test]
fn copies_stay_within_the_budget() {
    // 4 events a tick, never acked: 80 live, far over the budget
    let config = TransportConfig::default();
    let start = Instant::now();
    let session = Session::new(start, MTU);
    let mut tx = Transport::new(&session, LANES, LANES, start).unwrap();
    let mut max = 0;
    for tick in 0..40u64 {
        let now = start + Duration::from_millis(tick * TICK_MS);
        for _ in 0..4 {
            tx.send
                .push(EVENTS, Bytes::from(vec![0; EVENT_LEN]), now)
                .unwrap();
        }
        for packet in send::flush_on(&mut tx, now, MTU) {
            max = max.max(copy_bytes(&packet));
        }
    }
    assert!(max <= config.redundant_bytes_per_packet, "{max} B");
    assert!(max > config.redundant_bytes_per_packet - EVENT_LEN - 8);
}

#[test]
fn push_until_expires_on_its_own_deadline() {
    let start = Instant::now();
    let session = Session::new(start, MTU);
    let mut tx = Transport::new(&session, LANES, LANES, start).unwrap();
    let short = start + Duration::from_millis(100);
    tx.send
        .push_until(EVENTS, Bytes::from_static(b"short"), start, short)
        .unwrap();
    tx.send
        .push(EVENTS, Bytes::from_static(b"lane"), start)
        .unwrap();
    let count = |tx: &mut Transport, at| {
        send::flush_on(tx, at, MTU)
            .map(|packet| copy_bytes(&packet))
            .sum::<usize>()
    };
    assert!(count(&mut tx, start) > 0);
    let after_short = count(&mut tx, short);
    assert_eq!(tx.send.lanes()[1].num_expired(), 1);
    assert!(after_short > 0);
    let after_lane = count(&mut tx, start + Duration::from_millis(EXPIRY_MS));
    assert_eq!(tx.send.lanes()[1].num_expired(), 2);
    assert_eq!(after_lane, 0);
}

#[test]
#[should_panic = "only a redundant lane"]
fn push_until_refuses_other_lanes() {
    let start = Instant::now();
    let session = Session::new(start, MTU);
    let mut tx = Transport::new(&session, LANES, LANES, start).unwrap();
    _ = tx.send.push_until(STATE, Bytes::new(), start, start);
}

#[test]
fn sequence_wraparound_delivers_every_message_once() {
    let config = TransportConfig::default();
    let start = Instant::now();
    let session = Session::new(start, MTU);
    let mut tx = Transport::new(&session, LANES, LANES, start).unwrap();
    let mut rx = Transport::new(&session, LANES, LANES, start).unwrap();
    let mut delivered = vec![false; 3500 * 20];
    // past u16::MAX, 20 a tick, half the packets dropped
    // (and one tick more without pushing, to deliver the last copies)
    for tick in 0..=3500u32 {
        let now = start + Duration::from_millis(u64::from(tick) * TICK_MS);
        for i in (0..20u32).filter(|_| tick < 3500) {
            let id = tick * 20 + i;
            tx.send
                .push(EVENTS, Bytes::from(id.to_le_bytes().to_vec()), now)
                .unwrap();
        }
        let packets = send::flush_on(&mut tx, now, MTU).collect::<Vec<_>>();
        if tick % 2 == 0 {
            for packet in &packets {
                recv::recv_on(&mut rx, &config, now, packet).unwrap();
            }
        }
        for msg in rx.recv.msgs.drain() {
            let id = u32::from_le_bytes(msg.payload[..4].try_into().unwrap());
            let slot = &mut delivered[id as usize];
            assert!(!*slot, "event {id} delivered twice");
            *slot = true;
        }
        for packet in send::flush_on(&mut rx, now, MTU) {
            recv::recv_on(&mut tx, &config, now, &packet).unwrap();
        }
        tx.recv.acks.drain().for_each(drop);
    }
    assert!(delivered.iter().all(|&delivered| delivered));
    assert_eq!(tx.send.lanes()[1].num_expired(), 0);
}

#[test]
fn the_window_drops_what_the_receiver_would_refuse() {
    let start = Instant::now();
    let session = Session::new(start, MTU);
    let mut tx = Transport::new(&session, LANES, LANES, start).unwrap();
    // never acked: the window is all that ends them
    for _ in 0..1100 {
        tx.send
            .push(EVENTS, Bytes::from_static(b"x"), start)
            .unwrap();
    }
    let _ = send::flush_on(&mut tx, start, MTU).count();
    let lane = &tx.send.lanes()[1];
    assert_eq!(lane.num_expired(), 1100 - 1024);
    assert_eq!(lane.num_queued_msgs(), 1024);
}
