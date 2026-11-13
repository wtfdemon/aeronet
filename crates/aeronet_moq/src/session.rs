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

/// Backend: sends this session's packets as datagrams on `track` until the
/// frontend goes away or the connection dies. Nothing is dropped for age here
/// (the IO layer never expires packets; quinn's bounded datagram buffer drops
/// the oldest natively, and the browser's `outgoingMaxAge` does in Chrome).
#[cfg(not(target_family = "wasm"))]
pub(crate) async fn send_loop(
    conn: Arc<MoqConn>,
    track: Arc<OutTrack>,
    mut rx_packet_f2b: mpsc::UnboundedReceiver<Bytes>,
) {
    use aeronet_webtransport::wtransport::error::SendDatagramError;
    while let Some(packet) = rx_packet_f2b.next().await {
        // a packet before the relay subscribed has nowhere to go
        let Some(dg) = track.datagram(&packet) else { continue };
        match conn.conn.0.send_datagram(dg) {
            Ok(()) => {}
            Err(SendDatagramError::TooLarge) => {
                tracing::debug!(len = packet.len(), "MoQ datagram larger than the path allows");
            }
            Err(SendDatagramError::NotConnected | SendDatagramError::UnsupportedByPeer) => return,
        }
    }
}

/// Browser send loop: the newest flush wins (same rule and reasons as
/// `aeronet_webtransport`'s wasm send loop).
///
/// Chrome resolves a datagram `write()` only once the datagram has left its
/// QUIC send queue, so awaiting each write lets one held datagram stall every
/// packet behind it. Writes are never awaited: while the writer has room
/// (`desiredSize > 0`) a flush is written at once, otherwise it waits in a
/// one-flush slot that a newer flush replaces, written when the writer is
/// `ready`. Safe for aeronet packets: every header repeats the latest acks,
/// reliable lanes resend every PTO, and unreliable state is superseded.
#[cfg(target_family = "wasm")]
pub(crate) async fn send_loop(
    conn: Arc<MoqConn>,
    track: Arc<OutTrack>,
    mut rx_packet_f2b: mpsc::UnboundedReceiver<Bytes>,
) {
    use {
        core::time::Duration, futures::FutureExt, js_sys::Uint8Array,
        wasm_bindgen_futures::JsFuture,
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
    let write = |packet: &Bytes| {
        if let Some(dg) = track.datagram(packet) {
            // not awaited; JsFuture handles a rejection (closing session)
            drop(JsFuture::from(
                writer.write_with_chunk(&Uint8Array::from(dg.as_ref())),
            ));
        }
    };

    let mut slot = Vec::<Bytes>::new();
    let mut ready = None::<futures::future::Fuse<JsFuture>>;
    let mut parked_at = bevy_platform::time::Instant::now();
    let mut replaced = Saturating(0usize);
    let mut replaced_total = Saturating(0usize);
    let mut longest_wait = Duration::ZERO;
    let mut logged_at = bevy_platform::time::Instant::now();

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
                parked_at = bevy_platform::time::Instant::now();
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
            logged_at = bevy_platform::time::Instant::now();
        }
    }
}
