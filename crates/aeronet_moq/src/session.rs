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
/// Server -> client carries the bursts (snapshots, a join's world state), and
/// its lanes cost the client's request ids, of which it has plenty.
pub const DOWN_LANES: usize = 8;
/// Client -> server is mostly one usercmd packet a flush; each lane costs the
/// server a SUBSCRIBE (see `server.rs` on the request id budget).
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
    let mut last = alloc::vec![None::<Instant>; lanes.len()];
    let (mut sent, mut waited, mut bytes) = (0u64, 0u64, 0usize);
    let mut logged_at = Instant::now();
    while let Some(packet) = rx_packet_f2b.next().await {
        let Some(lane) = (0..lanes.len())
            .filter(|&i| lanes[i].is_subscribed())
            .min_by_key(|&i| last[i])
        else {
            continue;
        };
        if let Some(rest) = last[lane].and_then(|t| gap.checked_sub(t.elapsed())) {
            waited += 1;
            WebTransportRuntime::sleep(rest).await;
        }
        let Some(dg) = lanes[lane].datagram(&packet) else { continue };
        last[lane] = Some(Instant::now());
        sent += 1;
        bytes += dg.len();
        match conn.conn.0.send_datagram(dg) {
            Ok(()) => {}
            Err(SendDatagramError::TooLarge) => {
                tracing::debug!(len = packet.len(), "MoQ datagram larger than the path allows");
            }
            Err(SendDatagramError::NotConnected | SendDatagramError::UnsupportedByPeer) => return,
        }
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
        let Some(dg) = track.datagram(&payload) else { continue };
        clock.used(lane, Instant::now());
        match conn.conn.0.send_datagram(dg) {
            Ok(()) => {}
            Err(SendDatagramError::TooLarge) => {
                tracing::debug!(len = payload.len(), "MoQ broadcast datagram larger than the path allows");
            }
            Err(SendDatagramError::NotConnected | SendDatagramError::UnsupportedByPeer) => return,
        }
        if waited > 0 && logged_at.elapsed().as_secs() >= 30 {
            tracing::info!("MoQ broadcast: {waited} datagrams waited for their lane in 30 s");
            waited = 0;
            logged_at = Instant::now();
        }
    }
}

/// When each lane last carried a datagram: how long one must still rest.
#[cfg(not(target_family = "wasm"))]
pub(crate) struct LaneClock(Vec<Option<std::time::Instant>>);

#[cfg(not(target_family = "wasm"))]
impl LaneClock {
    pub(crate) fn new(lanes: usize) -> Self {
        Self(alloc::vec![None; lanes])
    }

    /// The rest `lane` still owes at `now`, if any.
    pub(crate) fn rest(
        &self,
        lane: usize,
        now: std::time::Instant,
        gap: core::time::Duration,
    ) -> Option<core::time::Duration> {
        let last = (*self.0.get(lane)?)?;
        gap.checked_sub(now.saturating_duration_since(last)).filter(|rest| !rest.is_zero())
    }

    pub(crate) fn used(&mut self, lane: usize, at: std::time::Instant) {
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
}

/// Browser send loop: the newest flush wins (same rule and reasons as
/// `aeronet_webtransport`'s wasm send loop), each packet on the next lane.
///
/// Chrome resolves a datagram `write()` only once the datagram has left its
/// QUIC send queue, so awaiting each write lets one held datagram stall every
/// packet behind it. Writes are never awaited: while the writer has room
/// (`desiredSize > 0`) a flush is written at once, otherwise it waits in a
/// one-flush slot that a newer flush replaces, written when the writer is
/// `ready`. Safe for aeronet packets: every header repeats the latest acks,
/// reliable lanes resend every PTO, and unreliable state is superseded.
/// Lanes go round robin without a rest: an upstream flush is rarely bigger
/// than [`UP_LANES`].
#[cfg(target_family = "wasm")]
pub(crate) async fn send_loop(
    conn: Arc<MoqConn>,
    lanes: Vec<Arc<OutTrack>>,
    mut rx_packet_f2b: mpsc::UnboundedReceiver<Bytes>,
) {
    use {
        bevy_platform::time::Instant, core::time::Duration, futures::FutureExt,
        js_sys::Uint8Array, wasm_bindgen_futures::JsFuture,
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
    let next_lane = core::cell::Cell::new(0usize);
    let write = |packet: &Bytes| {
        for _ in 0..lanes.len() {
            let lane = next_lane.get();
            next_lane.set((lane + 1) % lanes.len());
            if let Some(dg) = lanes[lane].datagram(packet) {
                // not awaited; JsFuture handles a rejection (closing session)
                drop(JsFuture::from(
                    writer.write_with_chunk(&Uint8Array::from(dg.as_ref())),
                ));
                return;
            }
        }
    };

    let mut slot = Vec::<Bytes>::new();
    let mut ready = None::<futures::future::Fuse<JsFuture>>;
    let mut parked_at = Instant::now();
    let mut replaced = Saturating(0usize);
    let mut replaced_total = Saturating(0usize);
    let mut longest_wait = Duration::ZERO;
    let mut logged_at = Instant::now();

    loop {
        let first = if slot.is_empty() {
            match rx_packet_f2b.next().await {
                Some(p) => p,
                None => return,
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
                    longest_wait = longest_wait.max(parked_at.elapsed());
                    core::mem::take(&mut slot).iter().for_each(write);
                    continue;
                }
            }
        };

        // a flush pushes all its packets at once
        let mut batch = alloc::vec![first];
        while let Ok(packet) = rx_packet_f2b.try_recv() {
            batch.push(packet);
        }

        let parked = !slot.is_empty();
        replaced += slot.len();
        replaced_total += slot.len();
        if has_room() {
            ready = None;
            slot.clear();
            batch.iter().for_each(write);
        } else {
            if !parked {
                parked_at = Instant::now();
            }
            slot = batch;
        }

        if replaced.0 > 0 && logged_at.elapsed() >= LOG_INTERVAL {
            tracing::info!(
                "MoQ send: {} packets replaced by newer flushes in {:.1} s while the browser held a datagram (total {}, longest wait {:?})",
                replaced,
                logged_at.elapsed().as_secs_f32(),
                replaced_total,
                longest_wait,
            );
            replaced = Saturating(0);
            longest_wait = Duration::ZERO;
            logged_at = Instant::now();
        }
    }
}
