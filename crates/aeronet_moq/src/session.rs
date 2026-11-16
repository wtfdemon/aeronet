//! Frontend half shared by clients and server-side clients: moves packets
//! between [`Session`] and the backend task every frame.

use {
    crate::conn::{MoqConn, OutTrack},
    aeronet_io::{
        AeronetIoPlugin, IoSystems, Session,
        connection::{DROP_DISCONNECT_REASON, Disconnect},
        packet::{IP_MTU, RecvPacket},
    },
    aeronet_webtransport::WebTransportRuntime,
    alloc::sync::Arc,
    bevy_app::prelude::*,
    bevy_ecs::prelude::*,
    bytes::Bytes,
    core::num::Saturating,
    futures::{
        StreamExt,
        channel::{mpsc, oneshot},
    },
    tracing::trace,
};

/// Smallest packet MTU a MoQ session must carry: aeronet's base IP MTU minus
/// the MoQ datagram header. Chrome reports a 1024-byte max datagram size, so
/// demanding the full [`IP_MTU`] would fail every browser client.
pub const MIN_MTU: usize = IP_MTU - crate::wire::DATAGRAM_OVERHEAD;

pub(crate) struct MoqSessionPlugin;

impl Plugin for MoqSessionPlugin {
    fn build(&self, app: &mut App) {
        if !app.is_plugin_added::<AeronetIoPlugin>() {
            app.add_plugins(AeronetIoPlugin);
        }
        #[cfg(not(target_family = "wasm"))]
        {
            _ = aeronet_webtransport::wtransport::tls::rustls::crypto::ring::default_provider()
                .install_default();
        }
        app.init_resource::<WebTransportRuntime>()
            .add_systems(PreUpdate, poll.in_set(IoSystems::Poll))
            .add_systems(PostUpdate, flush.in_set(IoSystems::Flush))
            .add_observer(on_disconnect);
    }
}

/// Packet channels of one MoQ-backed session. Managed by the client/server
/// implementations; don't add or remove it yourself.
#[derive(Debug, Component)]
pub struct MoqIo {
    pub(crate) rx_packet_b2f: mpsc::UnboundedReceiver<RecvPacket>,
    pub(crate) tx_packet_f2b: mpsc::UnboundedSender<Bytes>,
    pub(crate) tx_user_dc: Option<oneshot::Sender<String>>,
}

impl Drop for MoqIo {
    fn drop(&mut self) {
        if let Some(tx) = self.tx_user_dc.take() {
            _ = tx.send(DROP_DISCONNECT_REASON.to_owned());
        }
    }
}

fn on_disconnect(trigger: On<Disconnect>, mut sessions: Query<&mut MoqIo>) {
    if let Ok(mut io) = sessions.get_mut(trigger.event_target()) {
        if let Some(tx) = io.tx_user_dc.take() {
            _ = tx.send(trigger.reason.clone());
        }
    }
}

pub(crate) fn poll(mut sessions: Query<(Entity, &mut Session, &mut MoqIo)>) {
    for (entity, mut session, mut io) in &mut sessions {
        let mut n = Saturating(0usize);
        while let Ok(packet) = io.rx_packet_b2f.try_recv() {
            n += 1;
            session.stats.packets_recv += 1;
            session.stats.bytes_recv += packet.payload.len();
            session.recv.push(packet);
        }
        if n.0 > 0 {
            trace!(%entity, packets = n.0, "Received packets");
        }
    }
}

fn flush(mut sessions: Query<(&mut Session, &MoqIo)>) {
    for (mut session, io) in &mut sessions {
        let session = &mut *session;
        for packet in session.send.drain(..) {
            session.stats.packets_sent += 1;
            session.stats.bytes_sent += packet.len();
            _ = io.tx_packet_f2b.unbounded_send(packet);
        }
    }
}

/// Tracks per direction. Cloudflare's relay forwards only the newest datagram
/// of a track that it has more than one of at once (measured 2026-11-13: a
/// burst of 8 back to back arrives as 1, of 4 as 1, any burst spaced 1 ms
/// apart arrives whole), and a game flush is exactly such a burst. So a
/// session's packets are spread over lanes, each its own track, and a lane is
/// used again only [`LANE_GAP`] after its last datagram.
///
/// Measured again on draft-18 (2026-11-16): the same. Server -> client
/// carries the bursts (snapshots, a join's world state).
pub const DOWN_LANES: usize = 8;
/// Client -> server is mostly one usercmd packet a flush; each lane is a
/// SUBSCRIBE the server holds open (see `server.rs` on open requests).
pub const UP_LANES: usize = 4;
/// Tracks a server publishes once for every client: `broadcast.<k>` under
/// `<prefix>/server`. The relay subscribes to each once, however many clients
/// watch it, and sends each object once per subscriber (fan-out, measured
/// 2026-11-15: 60 subscribers cost the publisher one SUBSCRIBE and one copy).
/// A game puts at most one datagram on a lane per tick (several at once on a
/// track arrive as the newest one, see [`DOWN_LANES`]), so this many lanes is
/// the most it can broadcast in one burst.
pub const BROADCAST_LANES: usize = 8;
/// Largest broadcast payload: Cloudflare's relay passed 1140 B and dropped
/// 1160 B and up (2026-11-15), silently. Kept under [`MIN_MTU`] so a game can
/// write the same bytes into a non-relay session's packets too.
pub const BROADCAST_MAX_PAYLOAD: usize = 960;

/// How long a lane rests after a datagram (the gap measured to be enough).
/// `MOQ_LANE_GAP_US` overrides it natively.
pub const LANE_GAP: core::time::Duration = core::time::Duration::from_millis(1);

#[cfg(not(target_family = "wasm"))]
pub(crate) fn lane_gap() -> core::time::Duration {
    static GAP: std::sync::OnceLock<core::time::Duration> = std::sync::OnceLock::new();
    *GAP.get_or_init(|| {
        std::env::var("MOQ_LANE_GAP_US")
            .ok()
            .and_then(|v| v.parse().ok())
            .map_or(LANE_GAP, core::time::Duration::from_micros)
    })
}

/// Track name of lane `lane` of `base`.
pub(crate) fn lane_track(base: &str, lane: usize) -> String {
    format!("{base}.{lane}")
}

/// Backend: sends this session's packets as datagrams over `lanes` until the
/// frontend goes away or the connection dies. Each packet takes the lane that
/// rested longest, waiting out the rest of its [`LANE_GAP`] only when every
/// lane was just used (a flush bigger than the lane count). A lane the relay
/// hasn't subscribed to is skipped; with none, the packet is dropped. Nothing is
/// dropped for age (quinn's bounded datagram buffer drops the oldest).
#[cfg(not(target_family = "wasm"))]
pub(crate) async fn send_loop(
    conn: Arc<MoqConn>,
    lanes: Vec<Arc<OutTrack>>,
    mut rx_packet_f2b: mpsc::UnboundedReceiver<Bytes>,
) {
    use {aeronet_webtransport::wtransport::error::SendDatagramError, std::time::Instant};
    let gap = lane_gap();
    let mut clock = LaneClock::new(lanes.len());
    let (mut sent, mut waited, mut bytes) = (0u64, 0u64, 0usize);
    let mut logged_at = Instant::now();
    while let Some(packet) = rx_packet_f2b.next().await {
        let Some((lane, rest)) = clock.pick(|i| lanes[i].is_subscribed(), Instant::now(), gap) else {
            continue;
        };
        if let Some(rest) = rest {
            waited += 1;
            WebTransportRuntime::sleep(rest).await;
        }
        let mut gone = false;
        let went = lanes[lane].datagrams(&packet, |dg| {
            bytes += dg.len();
            match conn.conn.0.send_datagram(dg) {
                Ok(()) => {}
                Err(SendDatagramError::TooLarge) => {
                    tracing::debug!(len = packet.len(), "MoQ datagram larger than the path allows");
                }
                Err(SendDatagramError::NotConnected | SendDatagramError::UnsupportedByPeer) => gone = true,
            }
        });
        if gone {
            return;
        }
        if !went {
            continue;
        }
        clock.used(lane, Instant::now());
        sent += 1;
        if logged_at.elapsed().as_secs() >= 5 {
            tracing::debug!("MoQ sent in 5 s: {sent} datagrams, {bytes} B, {waited} waited for a lane");
            (sent, waited, bytes) = (0, 0, 0);
            logged_at = Instant::now();
        }
    }
}

/// Backend of [`crate::MoqBroadcast`]: each payload as one datagram on its
/// lane, a lane used again only [`LANE_GAP`] after its last datagram (waiting
/// out the rest when a burst puts two on one lane). The relay forwards only
/// the newest datagram of a track it holds several of, so without the gap a
/// server catching up two ticks in one burst loses the first. Ends when the
/// queue's sender goes (the relay session ended) or the connection dies.
#[cfg(not(target_family = "wasm"))]
pub(crate) async fn broadcast_loop(
    conn: Arc<MoqConn>,
    lanes: Vec<Arc<OutTrack>>,
    mut rx: mpsc::UnboundedReceiver<(usize, Bytes)>,
) {
    use {aeronet_webtransport::wtransport::error::SendDatagramError, std::time::Instant};
    let gap = lane_gap();
    let mut clock = LaneClock::new(lanes.len());
    let mut waited = 0u64;
    let mut logged_at = Instant::now();
    while let Some((lane, payload)) = rx.next().await {
        let Some(track) = lanes.get(lane) else { continue };
        if let Some(rest) = clock.rest(lane, Instant::now(), gap) {
            waited += 1;
            WebTransportRuntime::sleep(rest).await;
        }
        let mut gone = false;
        let went = track.datagrams(&payload, |dg| match conn.conn.0.send_datagram(dg) {
            Ok(()) => {}
            Err(SendDatagramError::TooLarge) => {
                tracing::debug!(len = payload.len(), "MoQ broadcast datagram larger than the path allows");
            }
            Err(SendDatagramError::NotConnected | SendDatagramError::UnsupportedByPeer) => gone = true,
        });
        if gone {
            return;
        }
        if went {
            clock.used(lane, Instant::now());
        }
        if waited > 0 && logged_at.elapsed().as_secs() >= 30 {
            tracing::info!("MoQ broadcast: {waited} datagrams waited for their lane in 30 s");
            waited = 0;
            logged_at = Instant::now();
        }
    }
}

/// When each lane last carried a datagram: how long one must still rest.
/// Every sender (session packets native and in the browser, the broadcast)
/// keeps its lanes with one.
pub(crate) struct LaneClock(Vec<Option<bevy_platform::time::Instant>>);

impl LaneClock {
    pub(crate) fn new(lanes: usize) -> Self {
        Self(alloc::vec![None; lanes])
    }

    /// The rest `lane` still owes at `now`, if any.
    pub(crate) fn rest(
        &self,
        lane: usize,
        now: bevy_platform::time::Instant,
        gap: core::time::Duration,
    ) -> Option<core::time::Duration> {
        let last = (*self.0.get(lane)?)?;
        gap.checked_sub(now.saturating_duration_since(last)).filter(|rest| !rest.is_zero())
    }

    /// The next packet's lane: of the `usable` ones (the relay subscribed),
    /// the one that rested longest (never used first), with the rest it still
    /// owes at `now` (`None`: free now). `None` when no lane is usable.
    pub(crate) fn pick(
        &self,
        usable: impl Fn(usize) -> bool,
        now: bevy_platform::time::Instant,
        gap: core::time::Duration,
    ) -> Option<(usize, Option<core::time::Duration>)> {
        let lane = (0..self.0.len()).filter(|&i| usable(i)).min_by_key(|&i| self.0[i])?;
        Some((lane, self.rest(lane, now, gap)))
    }

    pub(crate) fn used(&mut self, lane: usize, at: bevy_platform::time::Instant) {
        if let Some(slot) = self.0.get_mut(lane) {
            *slot = Some(at);
        }
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use {super::*, core::time::Duration, std::time::Instant};

    #[test]
    fn a_lane_rests_its_gap_and_others_dont_wait() {
        let gap = Duration::from_millis(1);
        let t0 = Instant::now();
        let mut clock = LaneClock::new(4);
        assert_eq!(clock.rest(0, t0, gap), None, "never used");
        clock.used(0, t0);
        assert_eq!(clock.rest(0, t0, gap), Some(gap), "right after: the whole gap");
        assert_eq!(clock.rest(0, t0 + Duration::from_micros(400), gap), Some(Duration::from_micros(600)));
        assert_eq!(clock.rest(0, t0 + gap, gap), None, "rested");
        assert_eq!(clock.rest(1, t0, gap), None, "another lane is free");
        assert_eq!(clock.rest(9, t0, gap), None, "out of range: nothing to wait for");
    }

    /// A flush bigger than the lanes (a browser's paint stroke, a resend
    /// round): the first goes on each lane at once, the next one waits out
    /// the gap of the lane that rested longest, never two on one lane inside
    /// a gap (the relay would keep only the newest).
    #[test]
    fn a_flush_bigger_than_the_lanes_waits_for_a_rested_one() {
        let gap = Duration::from_millis(1);
        let t0 = Instant::now();
        let mut clock = LaneClock::new(4);
        let all = |_: usize| true;
        for lane in 0..4 {
            let (picked, rest) = clock.pick(all, t0, gap).unwrap();
            assert_eq!((picked, rest), (lane, None), "a fresh lane each, at once");
            clock.used(picked, t0 + Duration::from_micros(10 * lane as u64));
        }
        assert_eq!(clock.pick(all, t0 + Duration::from_micros(30), gap), Some((0, Some(Duration::from_micros(970)))));
        assert_eq!(clock.pick(all, t0 + gap, gap), Some((0, None)), "lane 0 rested");
        assert_eq!(clock.pick(|i| i == 2, t0 + gap, gap), Some((2, Some(Duration::from_micros(20)))), "only subscribed lanes");
        assert_eq!(clock.pick(|_| false, t0, gap), None, "no lane: nowhere to send");
    }
}

/// Browser send loop: the newest flush wins (same rule and reasons as
/// `aeronet_webtransport`'s wasm send loop), each packet on the lane that
/// rested longest, a lane used again only [`LANE_GAP`] after its last
/// datagram, as the native loop does.
///
/// Chrome resolves a datagram `write()` only once the datagram has left its
/// QUIC send queue, so awaiting each write lets one held datagram stall every
/// packet behind it. Writes are never awaited: while the writer has room
/// (`desiredSize > 0`) queued packets are written at once, otherwise they
/// wait and a newer flush replaces them, written when the writer is `ready`.
/// Safe for aeronet packets: every header repeats the latest acks, reliable
/// lanes resend every PTO, and unreliable state is superseded.
///
/// The lane rest matters for any flush bigger than [`UP_LANES`] (a paint
/// stroke, a cosmetics change, a resend round): the relay keeps only the
/// newest datagram of a track it holds several of, so writing them all at
/// once lost all but the last on each lane. Packets waiting only for a lane
/// are never replaced: they go a round of lanes per gap (a browser timer is
/// 1 ms, 4 ms once nested, so a 20-packet flush takes up to ~16 ms).
#[cfg(target_family = "wasm")]
pub(crate) async fn send_loop(
    conn: Arc<MoqConn>,
    lanes: Vec<Arc<OutTrack>>,
    mut rx_packet_f2b: mpsc::UnboundedReceiver<Bytes>,
) {
    use {
        alloc::collections::VecDeque, bevy_platform::time::Instant, core::time::Duration,
        futures::FutureExt, js_sys::Uint8Array, wasm_bindgen_futures::JsFuture,
    };

    const LOG_INTERVAL: Duration = Duration::from_secs(5);

    let writer = &conn.conn.datagrams.writable_stream_writer;
    let has_room = || {
        writer
            .desired_size()
            .ok()
            .flatten()
            .is_some_and(|size| size > 0.0)
    };

    let mut clock = LaneClock::new(lanes.len());
    let mut queue = VecDeque::<Bytes>::new();
    let mut ready = None::<futures::future::Fuse<JsFuture>>;
    // since when the queue waits for the writer (not for a lane)
    let mut parked_at = None::<Instant>;
    let mut replaced = Saturating(0usize);
    let mut replaced_total = Saturating(0usize);
    let mut lane_waits = Saturating(0usize);
    let mut longest_wait = Duration::ZERO;
    let mut logged_at = Instant::now();

    loop {
        // Write what may go now: the writer has room, and per packet a lane
        // that rested its gap.
        let mut rest = None;
        if !queue.is_empty() && has_room() {
            ready = None;
            if let Some(at) = parked_at.take() {
                longest_wait = longest_wait.max(at.elapsed());
            }
            while let Some(packet) = queue.front() {
                let now = Instant::now();
                let Some((lane, owed)) = clock.pick(|i| lanes[i].is_subscribed(), now, LANE_GAP) else {
                    queue.clear(); // no lane subscribed: nowhere to send (as natively)
                    break;
                };
                if owed.is_some() {
                    rest = owed;
                    lane_waits += 1;
                    break;
                }
                lanes[lane].datagrams(packet, |dg| {
                    // not awaited; JsFuture handles a rejection (closing session)
                    drop(JsFuture::from(
                        writer.write_with_chunk(&Uint8Array::from(dg.as_ref())),
                    ));
                });
                clock.used(lane, now);
                queue.pop_front();
            }
        } else if !queue.is_empty() {
            parked_at.get_or_insert_with(Instant::now);
        }

        let first = if queue.is_empty() {
            match rx_packet_f2b.next().await {
                Some(p) => p,
                None => return,
            }
        } else if let Some(rest) = rest {
            futures::select! {
                x = rx_packet_f2b.next() => match x {
                    Some(p) => p,
                    None => return,
                },
                () = WebTransportRuntime::sleep(rest).fuse() => continue,
            }
        } else {
            let mut ready_fut = ready.get_or_insert_with(|| JsFuture::from(writer.ready()).fuse());
            futures::select! {
                x = rx_packet_f2b.next() => match x {
                    Some(p) => p,
                    None => return,
                },
                x = ready_fut => {
                    if x.is_err() {
                        return;
                    }
                    ready = None;
                    continue;
                }
            }
        };

        // A flush pushes all its packets at once. While the browser holds a
        // datagram (no room), it replaces what still waits for the writer.
        if parked_at.is_some() && !has_room() {
            replaced += queue.len();
            replaced_total += queue.len();
            queue.clear();
        }
        queue.push_back(first);
        while let Ok(packet) = rx_packet_f2b.try_recv() {
            queue.push_back(packet);
        }

        if (replaced.0 > 0 || lane_waits.0 > 0) && logged_at.elapsed() >= LOG_INTERVAL {
            tracing::info!(
                "MoQ send in {:.1} s: {} packets replaced by newer flushes while the browser held a datagram (total {}, longest wait {:?}), {} waits for a rested lane",
                logged_at.elapsed().as_secs_f32(),
                replaced,
                replaced_total,
                longest_wait,
                lane_waits,
            );
            replaced = Saturating(0);
            lane_waits = Saturating(0);
            longest_wait = Duration::ZERO;
            logged_at = Instant::now();
        }
    }
}
