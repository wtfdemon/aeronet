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
    bevy_platform::time::Instant,
    bytes::Bytes,
    core::{num::Saturating, time::Duration},
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

/// How long a packet may wait behind a stalled datagram writer before it is
/// dropped instead of sent (same rule as `aeronet_webtransport`).
pub(crate) const MAX_PACKET_AGE: Duration = Duration::from_millis(50);

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
    pub(crate) tx_packet_f2b: mpsc::UnboundedSender<(Instant, Bytes)>,
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
        while let Ok(Some(packet)) = io.rx_packet_b2f.try_next() {
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
            _ = io.tx_packet_f2b.unbounded_send((Instant::now(), packet));
        }
    }
}

/// Backend: sends this session's packets as datagrams on `track`.
pub(crate) async fn send_loop(
    conn: Arc<MoqConn>,
    track: Arc<OutTrack>,
    mut rx_packet_f2b: mpsc::UnboundedReceiver<(Instant, Bytes)>,
) {
    let mut expired = 0usize;
    while let Some((queued_at, packet)) = rx_packet_f2b.next().await {
        if queued_at.elapsed() > MAX_PACKET_AGE {
            expired += 1;
            continue;
        }
        if expired > 0 {
            trace!("Datagram writer congested, dropped {expired} expired packets");
            expired = 0;
        }
        if conn.send(&track, &packet).await.is_err() {
            return;
        }
    }
}
