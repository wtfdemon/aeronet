//! A message which is never fully reassembled must not survive until its
//! 16-bit message sequence is reused by a later message on the same lane.
#![cfg(test)]

use {
    aeronet_io::{Session, packet::IP_MTU},
    aeronet_transport::{
        Transport, TransportConfig,
        lane::{LaneIndex, LaneKind},
        packet::{Fragment, PacketHeader},
        recv, send,
    },
    bevy_platform::time::Instant,
    core::time::Duration,
    octs::{Bytes, Read},
};

/// Same as `aeronet_websocket::MTU`.
const MTU: usize = IP_MTU - 60 - 40 - 14;
const LANE: LaneIndex = LaneIndex::new(0);
/// Splits into 3 fragments at this MTU.
const BIG: usize = 2500;
const SMALL: usize = 8;

struct Link {
    tx: Transport,
    rx: Transport,
    config: TransportConfig,
    now: Instant,
}

impl Link {
    fn new(kind: LaneKind) -> Self {
        let now = Instant::now();
        let session = Session::new(now, MTU);
        Self {
            tx: Transport::new(&session, [kind], [kind], now).unwrap(),
            rx: Transport::new(&session, [kind], [kind], now).unwrap(),
            config: TransportConfig::default(),
            now,
        }
    }

    fn tick(&mut self, by: Duration) {
        self.now = self.now.checked_add(by).unwrap();
    }

    fn flush_tx(&mut self) -> Vec<Bytes> {
        send::flush_on(&mut self.tx, self.now, MTU).collect()
    }

    /// Delivers `packets` to the receiver, returning delivered message
    /// payloads, or the first receive error (which disconnects the session).
    fn deliver(&mut self, packets: &[Bytes]) -> Result<Vec<Vec<u8>>, String> {
        for packet in packets {
            recv::recv_on(&mut self.rx, &self.config, self.now, packet)
                .map_err(|err| format!("{err:?}"))?;
        }
        Ok(self.rx.recv.msgs.drain().map(|msg| msg.payload).collect())
    }

    /// Sends the receiver's acks back to the sender.
    fn ack(&mut self) {
        let packets = send::flush_on(&mut self.rx, self.now, MTU).collect::<Vec<_>>();
        for packet in packets {
            recv::recv_on(&mut self.tx, &self.config, self.now, &packet).unwrap();
        }
        self.tx.recv.acks.drain().for_each(drop);
    }

    fn push(&mut self, msg: Vec<u8>) {
        self.tx.send.push(LANE, Bytes::from(msg), self.now).unwrap();
    }

    fn reassembling(&self) -> usize {
        self.rx
            .recv
            .lanes()
            .first()
            .unwrap()
            .num_reassembling_msgs()
    }

    /// Sends `count` small messages without loss, acking as we go.
    fn send_filler(&mut self, count: usize) {
        let mut sent = 0;
        while sent < count {
            let batch = count.saturating_sub(sent).min(256);
            for _ in 0..batch {
                self.push(vec![0; SMALL]);
            }
            sent = sent.saturating_add(batch);
            let packets = self.flush_tx();
            self.deliver(&packets).unwrap();
            self.ack();
            self.tick(Duration::from_millis(10));
        }
    }
}

/// Fragment indices carried by a packet.
fn frag_indices(mut packet: &[u8]) -> Vec<usize> {
    packet.read::<PacketHeader>().unwrap();
    let mut indices = Vec::new();
    while !packet.is_empty() {
        let frag = packet.read::<Fragment>().unwrap();
        indices.push(usize::from(frag.header.position.index()));
    }
    indices
}

fn without_frag(packets: &[Bytes], index: usize) -> Vec<Bytes> {
    packets
        .iter()
        .filter(|packet| !frag_indices(packet).contains(&index))
        .cloned()
        .collect()
}

fn only_frag(packets: &[Bytes], index: usize) -> Vec<Bytes> {
    packets
        .iter()
        .filter(|packet| frag_indices(packet) == [index])
        .cloned()
        .collect()
}

/// Run-length summary of a payload, e.g. `"y*892 X*1608"`.
fn summary(payload: &[u8]) -> String {
    payload
        .chunk_by(|a, b| a == b)
        .filter_map(|run| Some(format!("{}*{}", char::from(*run.first()?), run.len())))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Message 0 (`X`, 3 fragments) loses its first fragment in flight.
fn lose_first_frag(link: &mut Link) {
    link.push(vec![b'X'; BIG]);
    let packets = link.flush_tx();
    assert_eq!(packets.len(), 3, "one fragment per packet at this MTU");
    assert_eq!(link.deliver(&without_frag(&packets, 0)), Ok(Vec::new()));
    link.ack();
}

/// Message 0 (`X`, 3 fragments) is delivered whole, but the acks are late, so
/// the sender retransmits, and one retransmitted fragment arrives.
fn duplicate_after_delivery(link: &mut Link) {
    link.push(vec![b'X'; BIG]);
    let packets = link.flush_tx();
    assert_eq!(packets.len(), 3, "one fragment per packet at this MTU");
    assert_eq!(link.deliver(&packets).unwrap().len(), 1);
    // ack is still on its way when the sender's PTO fires
    link.tick(Duration::from_secs(5));
    let resent = link.flush_tx();
    assert_eq!(link.deliver(&only_frag(&resent, 1)), Ok(Vec::new()));
    link.ack();
}

/// Sets up a message 0 which the receiver may hold a partial for, wraps the
/// lane's 16-bit message sequence, then sends a new message 0 (`y`, `len`
/// bytes) which is delivered without loss.
fn wrap(kind: LaneKind, setup: fn(&mut Link), len: usize) -> (usize, Result<Vec<String>, String>) {
    let mut link = Link::new(kind);
    setup(&mut link);
    link.send_filler(usize::from(u16::MAX));
    let reassembling = link.reassembling();

    link.push(vec![b'y'; len]);
    let packets = link.flush_tx();
    let result = link
        .deliver(&packets)
        .map(|msgs| msgs.iter().map(|msg| summary(msg)).collect());
    (reassembling, result)
}

fn assert_delivered(kind: LaneKind, setup: fn(&mut Link), len: usize) {
    let (reassembling, result) = wrap(kind, setup, len);
    let expected = summary(&vec![b'y'; len]);
    assert_eq!(
        result,
        Ok(vec![expected]),
        "{kind:?}: new message 0 after wraparound ({reassembling} message(s) still being \
         reassembled before it was sent)"
    );
}

// unreliable unordered (`bevy_replicon` `Channel::Unreliable`)

#[test]
fn unreliable_unordered_same_size() {
    assert_delivered(LaneKind::UnreliableUnordered, lose_first_frag, BIG);
}

#[test]
fn unreliable_unordered_smaller() {
    assert_delivered(LaneKind::UnreliableUnordered, lose_first_frag, SMALL);
}

// unreliable sequenced

#[test]
fn unreliable_sequenced_same_size() {
    assert_delivered(LaneKind::UnreliableSequenced, lose_first_frag, BIG);
}

// reliable lanes (`Channel::Unordered`, `Channel::Ordered`)

#[test]
fn reliable_ordered_lost_frag() {
    assert_delivered(LaneKind::ReliableOrdered, lose_first_frag, BIG);
}

#[test]
fn reliable_unordered_late_duplicate_smaller() {
    assert_delivered(LaneKind::ReliableUnordered, duplicate_after_delivery, SMALL);
}

#[test]
fn reliable_ordered_late_duplicate_same_size() {
    assert_delivered(LaneKind::ReliableOrdered, duplicate_after_delivery, BIG);
}

#[test]
fn reliable_late_duplicate_leaks_partial() {
    for kind in [LaneKind::ReliableUnordered, LaneKind::ReliableOrdered] {
        let mut link = Link::new(kind);
        duplicate_after_delivery(&mut link);
        assert_eq!(
            link.reassembling(),
            0,
            "{kind:?}: duplicate fragment of a delivered message must not start a reassembly"
        );
    }
}
