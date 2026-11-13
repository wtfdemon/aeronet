//! A game server whose clients reach it through a MoQ relay.
//!
//! It dials out to the relay (no listening socket), announces
//! `<prefix>/server` (one track per client id, server -> client packets) and
//! sends SUBSCRIBE_NAMESPACE for `<prefix>/client`. Each client namespace the
//! relay reports becomes a child session entity once our SUBSCRIBE to its
//! `up` track succeeds; NAMESPACE_DONE disconnects it.

use {
    crate::{
        MoqTarget,
        client::{ClientConfig, dial, try_disconnect},
        conn::{MoqConn, MoqError, NamespaceEvent},
        session::{MIN_MTU, MoqIo, MoqSessionPlugin, send_loop},
    },
    aeronet_io::{
        IoSystems, Session, SessionEndpoint,
        connection::DisconnectReason,
        packet::RecvPacket,
        server::{CloseReason, Closed, Server, ServerEndpoint},
    },
    aeronet_webtransport::WebTransportRuntime,
    bevy_app::prelude::*,
    bevy_ecs::{prelude::*, system::EntityCommand},
    bevy_platform::time::Instant,
    bytes::Bytes,
    core::mem,
    futures::{
        FutureExt, StreamExt,
        channel::{mpsc, oneshot},
    },
    std::collections::HashMap,
    tracing::{Instrument, debug, debug_span, info},
};

pub struct MoqServerPlugin;

impl Plugin for MoqServerPlugin {
    fn build(&self, app: &mut App) {
        if !app.is_plugin_added::<MoqSessionPlugin>() {
            app.add_plugins(MoqSessionPlugin);
        }
        app.add_systems(
            PreUpdate,
            (poll_opening, poll_opened, poll_clients)
                .in_set(IoSystems::Poll)
                .before(crate::session::poll),
        );
    }
}

/// Marker for a server reached through a MoQ relay.
#[derive(Debug, Component)]
#[require(ServerEndpoint)]
pub struct MoqServer(());

/// Marker for a client session of a [`MoqServer`] (a child entity of it).
#[derive(Debug, Component)]
#[require(SessionEndpoint)]
pub struct MoqServerClient {
    /// The client's id from its namespace.
    pub id: String,
}

impl MoqServer {
    #[must_use]
    pub fn open(config: ClientConfig, target: MoqTarget) -> impl EntityCommand {
        move |entity: EntityWorldMut| open(entity, config, target)
    }
}

fn open(mut entity: EntityWorldMut, config: ClientConfig, target: MoqTarget) {
    let runtime = entity.world().resource::<WebTransportRuntime>().clone();
    let (tx_close, rx_close) = oneshot::channel::<CloseReason>();
    let (tx_clients, rx_clients) = mpsc::unbounded::<NewClient>();
    let (tx_opened, rx_opened) = oneshot::channel::<()>();
    runtime.spawn_on_self(
        async move {
            let reason = run(config, target, tx_opened, tx_clients).await;
            debug!("MoQ server closed: {reason:?}");
            _ = tx_close.send(reason);
        }
        .instrument(debug_span!("moq_server", entity = %entity.id())),
    );
    entity.insert((
        MoqServer(()),
        Opening {
            rx_close,
            rx_opened,
            rx_clients: Some(rx_clients),
        },
    ));
}

#[derive(Debug, Component)]
struct Opening {
    rx_close: oneshot::Receiver<CloseReason>,
    rx_opened: oneshot::Receiver<()>,
    rx_clients: Option<mpsc::UnboundedReceiver<NewClient>>,
}

#[derive(Debug, Component)]
struct Opened {
    rx_close: oneshot::Receiver<CloseReason>,
    rx_clients: mpsc::UnboundedReceiver<NewClient>,
}

#[derive(Debug, Component)]
struct ClientConnected {
    rx_dc_reason: oneshot::Receiver<DisconnectReason>,
}

#[derive(Debug)]
struct NewClient {
    id: String,
    mtu: usize,
    rx_packet_b2f: mpsc::UnboundedReceiver<RecvPacket>,
    tx_packet_f2b: mpsc::UnboundedSender<(Instant, Bytes)>,
    tx_user_dc: oneshot::Sender<String>,
    rx_dc_reason: oneshot::Receiver<DisconnectReason>,
}

async fn run(
    config: ClientConfig,
    target: MoqTarget,
    tx_opened: oneshot::Sender<()>,
    tx_clients: mpsc::UnboundedSender<NewClient>,
) -> CloseReason {
    match run_inner(config, target, tx_opened, tx_clients).await {
        Ok(never) => match never {},
        Err(e) => CloseReason::by_error(e),
    }
}

async fn run_inner(
    config: ClientConfig,
    target: MoqTarget,
    tx_opened: oneshot::Sender<()>,
    tx_clients: mpsc::UnboundedSender<NewClient>,
) -> Result<core::convert::Infallible, MoqError> {
    let conn = dial(config, &target.url).await?;
    let (moq, closed) = MoqConn::setup(conn).await?;
    let mtu = moq
        .max_payload()
        .ok_or_else(|| MoqError::new("datagrams not supported"))?;
    let server_ns = target.ns(&["server"]);
    moq.announce(server_ns.clone()).await?;
    let client_prefix = target.ns(&["client"]);
    let mut events = moq.subscribe_namespace(client_prefix.clone()).await?;
    info!(
        server = %server_ns.join("/"),
        clients = %client_prefix.join("/"),
        "MoQ server open"
    );
    _ = tx_opened.send(());

    // client id -> sender that disconnects it
    let mut kick: HashMap<String, oneshot::Sender<DisconnectReason>> = HashMap::new();
    let closed = closed.fuse();
    futures::pin_mut!(closed);
    loop {
        let event = futures::select! {
            e = events.next() => e.ok_or_else(|| MoqError::new("SUBSCRIBE_NAMESPACE ended"))?,
            reason = closed => {
                return Err(reason.unwrap_or_else(|_| MoqError::new("session closed")));
            }
        };
        match event {
            NamespaceEvent::Added(ns) => {
                let Some(id) = ns.last().cloned() else { continue };
                let (tx_dc_reason, rx_dc_reason) = oneshot::channel();
                if let Some(old) = kick.insert(id.clone(), tx_dc_reason) {
                    _ = old.send(DisconnectReason::by_peer("replaced by a new session"));
                }
                WebTransportRuntime::spawn(serve_client(
                    moq.clone(),
                    ns,
                    id,
                    server_ns.clone(),
                    mtu,
                    rx_dc_reason,
                    tx_clients.clone(),
                ));
            }
            NamespaceEvent::Removed(ns) => {
                if let Some(tx) = ns.last().and_then(|id| kick.remove(id)) {
                    _ = tx.send(DisconnectReason::by_peer("client left the relay"));
                }
            }
        }
    }
}

/// Subscribes to one client's `up` track and, once that works, hands the
/// session to the frontend; then pumps its outgoing packets until it ends.
async fn serve_client(
    moq: alloc::sync::Arc<MoqConn>,
    ns: Vec<String>,
    id: String,
    server_ns: Vec<String>,
    mtu: usize,
    rx_kick: oneshot::Receiver<DisconnectReason>,
    tx_clients: mpsc::UnboundedSender<NewClient>,
) {
    let (tx_packet_b2f, rx_packet_b2f) = mpsc::unbounded();
    if let Err(e) = moq.subscribe(&ns, "up", tx_packet_b2f).await {
        debug!(%id, "subscribe to client: {e}");
        return;
    }
    let down = moq.out_track(&server_ns, &id);
    let (tx_packet_f2b, rx_packet_f2b) = mpsc::unbounded();
    let (tx_user_dc, rx_user_dc) = oneshot::channel::<String>();
    let (tx_dc_reason, rx_dc_reason) = oneshot::channel::<DisconnectReason>();
    if tx_clients
        .unbounded_send(NewClient {
            id: id.clone(),
            mtu,
            rx_packet_b2f,
            tx_packet_f2b,
            tx_user_dc,
            rx_dc_reason,
        })
        .is_err()
    {
        return;
    }
    info!(%id, "client joined through relay");
    let sending = send_loop(moq.clone(), down, rx_packet_f2b).fuse();
    let rx_kick = rx_kick.fuse();
    let rx_user_dc = rx_user_dc.fuse();
    futures::pin_mut!(sending, rx_kick, rx_user_dc);
    let reason = futures::select! {
        r = rx_kick => r.unwrap_or_else(|_| DisconnectReason::by_error(MoqError::new("server closed"))),
        r = rx_user_dc => match r {
            Ok(r) => DisconnectReason::by_user(r),
            Err(_) => DisconnectReason::by_error(MoqError::new("frontend closed")),
        },
        () = sending => DisconnectReason::by_error(MoqError::new("send loop ended")),
    };
    _ = tx_dc_reason.send(reason);
}

fn poll_opening(mut commands: Commands, mut servers: Query<(Entity, &mut Opening), With<MoqServer>>) {
    for (entity, mut server) in &mut servers {
        if try_close(&mut commands, entity, &mut server.rx_close) {
            continue;
        }
        let Ok(Some(())) = server.rx_opened.try_recv() else {
            continue;
        };
        let (_, dummy) = oneshot::channel();
        let rx_close = mem::replace(&mut server.rx_close, dummy);
        let rx_clients = server.rx_clients.take().expect("taken once");
        commands
            .entity(entity)
            .remove::<Opening>()
            .insert((Server::new(Instant::now()), Opened { rx_close, rx_clients }));
    }
}

fn poll_opened(mut commands: Commands, mut servers: Query<(Entity, &mut Opened), With<MoqServer>>) {
    for (entity, mut server) in &mut servers {
        if try_close(&mut commands, entity, &mut server.rx_close) {
            continue;
        }
        while let Ok(client) = server.rx_clients.try_recv() {
            let mut session = Session::new(Instant::now(), MIN_MTU);
            if let Err(e) = session.set_mtu(client.mtu) {
                tracing::warn!("client {}: {e}", client.id);
                continue;
            }
            commands.spawn((
                ChildOf(entity),
                MoqServerClient { id: client.id },
                MoqIo {
                    rx_packet_b2f: client.rx_packet_b2f,
                    tx_packet_f2b: client.tx_packet_f2b,
                    tx_user_dc: Some(client.tx_user_dc),
                },
                ClientConnected {
                    rx_dc_reason: client.rx_dc_reason,
                },
                session,
            ));
        }
    }
}

fn poll_clients(
    mut commands: Commands,
    mut clients: Query<(Entity, &mut ClientConnected), With<MoqServerClient>>,
) {
    for (entity, mut client) in &mut clients {
        try_disconnect(&mut commands, entity, &mut client.rx_dc_reason);
    }
}

fn try_close(commands: &mut Commands, entity: Entity, rx: &mut oneshot::Receiver<CloseReason>) -> bool {
    let reason = match rx.try_recv() {
        Ok(None) => None,
        Ok(Some(r)) => Some(r),
        Err(_) => Some(CloseReason::by_error(MoqError::new("backend closed"))),
    };
    reason.is_some_and(|reason| {
        commands.trigger(Closed { entity, reason });
        true
    })
}
