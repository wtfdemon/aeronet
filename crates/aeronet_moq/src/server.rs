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
        client::{ClientConfig, Dialer, try_disconnect},
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
    alloc::sync::Arc,
    core::{mem, time::Duration},
    futures::{
        FutureExt, StreamExt,
        channel::{mpsc, oneshot},
    },
    std::{collections::HashMap, sync::Mutex},
    tracing::{Instrument, debug, debug_span, info, warn},
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
    rx_packet_b2f: mpsc::UnboundedReceiver<RecvPacket>,
    tx_packet_f2b: mpsc::UnboundedSender<Bytes>,
    tx_user_dc: oneshot::Sender<String>,
    rx_dc_reason: oneshot::Receiver<DisconnectReason>,
}

/// Live clients by id: the sender that kicks each one.
type Kick = Arc<Mutex<HashMap<String, oneshot::Sender<DisconnectReason>>>>;

/// Waits before redialling a relay that dropped us, doubling up to the cap.
const REDIAL_MIN: Duration = Duration::from_millis(500);
const REDIAL_MAX: Duration = Duration::from_secs(8);
/// A session that lasted this long resets the backoff.
const REDIAL_RESET_AFTER: Duration = Duration::from_secs(30);

/// Keeps a session with the relay up for as long as the server lives: a
/// dropped connection (relay restart, network blip, idle timeout) disconnects
/// the clients that rode it and dials again. The server entity stays open.
async fn run(
    config: ClientConfig,
    target: MoqTarget,
    tx_opened: oneshot::Sender<()>,
    tx_clients: mpsc::UnboundedSender<NewClient>,
) -> CloseReason {
    let dialer = match Dialer::new(config) {
        Ok(d) => d,
        Err(e) => return CloseReason::by_error(e),
    };
    let mut tx_opened = Some(tx_opened);
    let mut backoff = REDIAL_MIN;
    loop {
        let started = Instant::now();
        let kick = Kick::default();
        let e = match session(&dialer, &target, &mut tx_opened, &tx_clients, &kick).await {
            Ok(never) => match never {},
            Err(e) => e,
        };
        for (_, tx) in kick.lock().unwrap().drain() {
            _ = tx.send(DisconnectReason::by_error(MoqError(format!("relay session lost: {e}"))));
        }
        if tx_clients.is_closed() {
            return CloseReason::by_error(e);
        }
        if started.elapsed() > REDIAL_RESET_AFTER {
            backoff = REDIAL_MIN;
        }
        warn!("MoQ relay session ended ({e}); redialling in {backoff:?}");
        WebTransportRuntime::sleep(backoff).await;
        backoff = (backoff * 2).min(REDIAL_MAX);
    }
}

async fn session(
    dialer: &Dialer,
    target: &MoqTarget,
    tx_opened: &mut Option<oneshot::Sender<()>>,
    tx_clients: &mpsc::UnboundedSender<NewClient>,
    kick: &Kick,
) -> Result<core::convert::Infallible, MoqError> {
    let conn = dialer.dial(&target.url).await?;
    let (moq, closed) = MoqConn::setup(conn).await?;
    if moq.max_payload().is_none() {
        return Err(MoqError::new("datagrams not supported"));
    }
    let server_ns = target.ns(&["server"]);
    moq.announce(server_ns.clone()).await?;
    let client_prefix = target.ns(&["client"]);
    let mut events = moq.subscribe_namespace(client_prefix.clone()).await?;
    info!(
        server = %server_ns.join("/"),
        clients = %client_prefix.join("/"),
        "MoQ server open"
    );
    if let Some(tx) = tx_opened.take() {
        _ = tx.send(());
    }

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
                let (tx_kick, rx_kick) = oneshot::channel();
                {
                    let mut kick = kick.lock().unwrap();
                    if kick.contains_key(&id) {
                        // never let a second announce of a live id take its
                        // session over: ids are the client's to make up
                        warn!(%id, "client id already joined; ignoring the second announce");
                        continue;
                    }
                    kick.insert(id.clone(), tx_kick);
                }
                WebTransportRuntime::spawn(serve_client(
                    moq.clone(),
                    ns,
                    id,
                    server_ns.clone(),
                    rx_kick,
                    tx_clients.clone(),
                    kick.clone(),
                ));
            }
            NamespaceEvent::Removed(ns) => {
                let tx = ns.last().and_then(|id| kick.lock().unwrap().remove(id));
                if let Some(tx) = tx {
                    _ = tx.send(DisconnectReason::by_peer("client left the relay"));
                }
            }
        }
    }
}

/// Subscribes to one client's `up` track and, once that works, hands the
/// session to the frontend; then pumps its outgoing packets until it ends.
async fn serve_client(
    moq: Arc<MoqConn>,
    ns: Vec<String>,
    id: String,
    server_ns: Vec<String>,
    rx_kick: oneshot::Receiver<DisconnectReason>,
    tx_clients: mpsc::UnboundedSender<NewClient>,
    kick: Kick,
) {
    let (tx_packet_b2f, rx_packet_b2f) = mpsc::unbounded();
    let up = match moq.subscribe(&ns, "up", tx_packet_b2f).await {
        Ok(sub) => sub,
        Err(e) => {
            debug!(%id, "subscribe to client: {e}");
            kick.lock().unwrap().remove(&id);
            return;
        }
    };
    let down = moq.out_track(&server_ns, &id);
    let (tx_packet_f2b, rx_packet_f2b) = mpsc::unbounded();
    let (tx_user_dc, rx_user_dc) = oneshot::channel::<String>();
    let (tx_dc_reason, rx_dc_reason) = oneshot::channel::<DisconnectReason>();
    if tx_clients
        .unbounded_send(NewClient {
            id: id.clone(),
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
    let up_id = up.id;
    let sending = send_loop(moq.clone(), down, rx_packet_f2b).fuse();
    let rx_kick = rx_kick.fuse();
    let rx_user_dc = rx_user_dc.fuse();
    let up_done = up.ended.fuse();
    futures::pin_mut!(sending, rx_kick, rx_user_dc, up_done);
    let reason = futures::select! {
        r = rx_kick => r.unwrap_or_else(|_| DisconnectReason::by_error(MoqError::new("server closed"))),
        r = rx_user_dc => match r {
            Ok(r) => DisconnectReason::by_user(r),
            Err(_) => DisconnectReason::by_error(MoqError::new("frontend closed")),
        },
        _ = up_done => DisconnectReason::by_peer("client's track ended"),
        () = sending => DisconnectReason::by_error(MoqError::new("send loop ended")),
    };
    info!(%id, "client left: {reason:?}");
    kick.lock().unwrap().remove(&id);
    moq.unsubscribe(up_id).await;
    moq.drop_out_track(&server_ns, &id);
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
            // Packets to a client are capped at what a browser can take,
            // whatever our own link to the relay allows: the relay forwards a
            // datagram as is, and one too big for its hop to the client is
            // dropped there, so a big snapshot would never arrive.
            let session = Session::new(Instant::now(), MIN_MTU);
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
