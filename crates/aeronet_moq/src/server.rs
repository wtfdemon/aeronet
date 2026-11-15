//! A game server whose clients reach it through a MoQ relay.
//!
//! It dials out to the relay (no listening socket), announces
//! `<prefix>/server` (one track per client id, server -> client packets) and
//! sends SUBSCRIBE_NAMESPACE for `<prefix>/client`. Each client namespace the
//! relay reports becomes a child session entity once our SUBSCRIBEs to its
//! `up.<lane>` tracks succeed; NAMESPACE_DONE disconnects it.
//!
//! Request ids: Cloudflare's relay lets a session make 50 requests and never
//! raises that, while each joining client costs [`UP_LANES`] SUBSCRIBEs. So
//! the announcing session makes no SUBSCRIBEs to clients; those go out on
//! subscriber sessions from a pool, a new one dialled whenever the others are
//! spent, a spent one closed once its last client has left.

use {
    crate::{
        MoqTarget,
        client::{ClientConfig, Dialer, try_disconnect},
        conn::{MoqConn, MoqError, NamespaceEvent, OutTrack, ShutdownOnDrop},
        session::{BROADCAST_LANES, BROADCAST_MAX_PAYLOAD, DOWN_LANES, MIN_MTU, MoqIo, MoqSessionPlugin, UP_LANES, lane_track, send_loop},
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
        lock::Mutex as AsyncMutex,
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

/// The server's broadcast lanes ([`BROADCAST_LANES`] tracks
/// `<prefix>/server/broadcast.<k>`), on the server entity from
/// [`MoqServer::open`] on. Every client that asked for them
/// ([`crate::MoqTarget::with_broadcast`]) gets every payload sent here; the
/// relay subscribes to each lane once and makes the copies.
#[cfg(not(target_family = "wasm"))]
#[derive(Debug, Component, Clone, Default)]
pub struct MoqBroadcast(Arc<Mutex<Option<BroadcastLanes>>>);

/// No server runs in a browser: nothing to send on.
#[cfg(target_family = "wasm")]
#[derive(Debug, Component, Clone, Default)]
pub struct MoqBroadcast(());

/// One relay session's broadcast lanes, and the queue into the task that
/// sends on them ([`crate::session::broadcast_loop`]).
#[derive(Debug)]
#[cfg_attr(target_family = "wasm", expect(dead_code, reason = "no server on wasm"))]
struct BroadcastLanes {
    lanes: Vec<Arc<OutTrack>>,
    tx: mpsc::UnboundedSender<(usize, Bytes)>,
}

#[cfg(not(target_family = "wasm"))]
impl MoqBroadcast {
    /// Sends `payload` (at most [`BROADCAST_MAX_PAYLOAD`] bytes) as one
    /// datagram on `lane`, its own group. `false` if it can't go: the relay
    /// session is down or redialling, nobody watches the lane yet, or the
    /// lane or the payload is out of range.
    ///
    /// Any number may be sent on one lane at once: the relay keeps only the
    /// newest datagram of a track it holds several of (3 back to back arrive
    /// as 1), so the send task spaces a lane's datagrams [`LANE_GAP`] apart,
    /// as the per-player lanes are (a server catching up several ticks in a
    /// burst sends them all, each lane's 1 ms later than the one before).
    /// Spreading a burst over lanes avoids the wait.
    ///
    /// [`LANE_GAP`]: crate::session::LANE_GAP
    #[must_use]
    pub fn send(&self, lane: usize, payload: Bytes) -> bool {
        if payload.len() > BROADCAST_MAX_PAYLOAD {
            return false;
        }
        let guard = self.0.lock().unwrap();
        let Some(lanes) = guard.as_ref() else { return false };
        if !lanes.lanes.get(lane).is_some_and(|track| track.is_subscribed()) {
            return false;
        }
        lanes.tx.unbounded_send((lane, payload)).is_ok()
    }

    /// Lanes the relay has subscribed to (it does once someone watches them).
    #[must_use]
    pub fn watched_lanes(&self) -> usize {
        self.0.lock().unwrap().as_ref().map_or(0, |lanes| lanes.lanes.iter().filter(|l| l.is_subscribed()).count())
    }

    fn attach(&self, lanes: Option<BroadcastLanes>) {
        *self.0.lock().unwrap() = lanes;
    }
}

#[cfg(target_family = "wasm")]
impl MoqBroadcast {
    /// Never sends: no server runs in a browser.
    #[must_use]
    pub fn send(&self, _lane: usize, _payload: Bytes) -> bool {
        false
    }

    /// Always 0 in a browser.
    #[must_use]
    pub fn watched_lanes(&self) -> usize {
        0
    }

    fn attach(&self, _lanes: Option<BroadcastLanes>) {}
}

/// Decides from a client's id alone, before the server spends anything on
/// it, whether it may join and which group it counts against (`None`
/// refuses). Anyone holding the relay's token can announce client namespaces,
/// so a game binds ids to something only a real player has (skatepark: its
/// join ticket) and groups them by who that is.
pub type Admit = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// Who may join through the relay, and how many at once.
#[derive(Clone)]
pub struct Admission {
    /// `None`: any well-formed id, each its own group.
    pub admit: Option<Admit>,
    /// Sessions one group may hold, joining or joined.
    pub max_per_group: usize,
    /// Clients joining at once (announced, not yet handed to the game).
    pub max_joining: usize,
}

impl Default for Admission {
    fn default() -> Self {
        Self {
            admit: None,
            max_per_group: 16,
            max_joining: 64,
        }
    }
}

impl core::fmt::Debug for Admission {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Admission")
            .field("admit", &self.admit.is_some())
            .field("max_per_group", &self.max_per_group)
            .field("max_joining", &self.max_joining)
            .finish()
    }
}

impl MoqServer {
    #[must_use]
    pub fn open(config: ClientConfig, target: MoqTarget) -> impl EntityCommand {
        Self::open_with(config, target, Admission::default())
    }

    /// [`Self::open`] with who may join and how many.
    #[must_use]
    pub fn open_with(config: ClientConfig, target: MoqTarget, admission: Admission) -> impl EntityCommand {
        move |entity: EntityWorldMut| open(entity, config, target, admission)
    }
}

fn open(mut entity: EntityWorldMut, config: ClientConfig, target: MoqTarget, admission: Admission) {
    let runtime = entity.world().resource::<WebTransportRuntime>().clone();
    let (tx_close, rx_close) = oneshot::channel::<CloseReason>();
    let (tx_clients, rx_clients) = mpsc::unbounded::<NewClient>();
    let (tx_opened, rx_opened) = oneshot::channel::<()>();
    let broadcast = MoqBroadcast::default();
    let lanes = broadcast.clone();
    runtime.spawn_on_self(
        async move {
            let reason = run(config, target, admission, tx_opened, tx_clients, lanes).await;
            debug!("MoQ server closed: {reason:?}");
            _ = tx_close.send(reason);
        }
        .instrument(debug_span!("moq_server", entity = %entity.id())),
    );
    entity.insert((
        MoqServer(()),
        broadcast,
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

/// Clients joining or joined, by id, with the sender that kicks each one and
/// the counts [`Admission`] limits.
#[derive(Default)]
struct Clients {
    by_id: HashMap<String, ClientEntry>,
    per_group: HashMap<String, usize>,
    joining: usize,
}

struct ClientEntry {
    kick: oneshot::Sender<DisconnectReason>,
    group: String,
    joining: bool,
}

impl Clients {
    /// Takes `id` in, or says why not.
    fn admit(
        &mut self,
        id: &str,
        group: String,
        admission: &Admission,
    ) -> Result<oneshot::Receiver<DisconnectReason>, &'static str> {
        if self.by_id.contains_key(id) {
            // never let a second announce of a live id take its session
            // over: ids are the client's to make up
            return Err("id already joined");
        }
        if self.joining >= admission.max_joining {
            return Err("too many joining at once");
        }
        let count = self.per_group.entry(group.clone()).or_default();
        if *count >= admission.max_per_group {
            return Err("its group holds too many sessions");
        }
        *count += 1;
        self.joining += 1;
        let (kick, rx_kick) = oneshot::channel();
        self.by_id.insert(id.to_owned(), ClientEntry { kick, group, joining: true });
        Ok(rx_kick)
    }

    /// `id` is handed to the game: no longer joining.
    fn joined(&mut self, id: &str) {
        if let Some(entry) = self.by_id.get_mut(id)
            && mem::take(&mut entry.joining)
        {
            self.joining -= 1;
        }
    }

    /// Forgets `id`; its kick sender, if it was still here.
    fn remove(&mut self, id: &str) -> Option<oneshot::Sender<DisconnectReason>> {
        let entry = self.by_id.remove(id)?;
        if entry.joining {
            self.joining -= 1;
        }
        if let Some(count) = self.per_group.get_mut(&entry.group) {
            *count -= 1;
            if *count == 0 {
                self.per_group.remove(&entry.group);
            }
        }
        Some(entry.kick)
    }

    fn take_all(&mut self) -> Vec<oneshot::Sender<DisconnectReason>> {
        self.per_group.clear();
        self.joining = 0;
        self.by_id.drain().map(|(_, entry)| entry.kick).collect()
    }
}

type Kick = Arc<Mutex<Clients>>;

/// A dial (QUIC + WebTransport + MoQ setup) to the relay must finish by then.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

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
    admission: Admission,
    tx_opened: oneshot::Sender<()>,
    tx_clients: mpsc::UnboundedSender<NewClient>,
    broadcast: MoqBroadcast,
) -> CloseReason {
    let dialer = match Dialer::new(config) {
        Ok(d) => Arc::new(d),
        Err(e) => return CloseReason::by_error(e),
    };
    let mut tx_opened = Some(tx_opened);
    let mut backoff = REDIAL_MIN;
    loop {
        let started = Instant::now();
        let kick = Kick::default();
        let e = match session(&dialer, &target, &admission, &mut tx_opened, &tx_clients, &kick, &broadcast).await {
            Ok(never) => match never {},
            Err(e) => e,
        };
        broadcast.attach(None);
        for tx in kick.lock().unwrap().take_all() {
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
    dialer: &Arc<Dialer>,
    target: &MoqTarget,
    admission: &Admission,
    tx_opened: &mut Option<oneshot::Sender<()>>,
    tx_clients: &mpsc::UnboundedSender<NewClient>,
    kick: &Kick,
    broadcast: &MoqBroadcast,
) -> Result<core::convert::Infallible, MoqError> {
    let (moq, closed) = crate::conn::timeout(DIAL_TIMEOUT, "dial the relay", async {
        MoqConn::setup(dialer.dial(&target.url).await?).await
    })
    .await?;
    // Any way out (announce refused, SUBSCRIBE_NAMESPACE failed, the session
    // lost) closes it: a half-set-up session must not stay at the relay
    // announcing `<prefix>/server` next to the one we redial.
    let _shutdown = ShutdownOnDrop(moq.clone());
    if moq.max_payload().is_none() {
        return Err(MoqError::new("datagrams not supported"));
    }
    let server_ns = target.ns(&["server"]);
    // named before the announce, so the relay's first SUBSCRIBE finds them
    // ours rather than provisional
    let lanes: Vec<_> = (0..BROADCAST_LANES)
        .map(|lane| moq.out_track(&server_ns, &lane_track("broadcast", lane)))
        .collect();
    // the send task ends with the queue: when the session ends and detaches
    let (tx, rx) = mpsc::unbounded();
    #[cfg(not(target_family = "wasm"))]
    WebTransportRuntime::spawn(crate::session::broadcast_loop(moq.clone(), lanes.clone(), rx));
    #[cfg(target_family = "wasm")]
    drop(rx);
    broadcast.attach(Some(BroadcastLanes { lanes, tx }));
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

    let pool = Arc::new(SubscriberPool {
        dialer: dialer.clone(),
        url: target.url.clone(),
        conns: AsyncMutex::new(Vec::new()),
    });
    let result = serve_namespace(
        &moq,
        closed,
        &mut events,
        &server_ns,
        client_prefix.len(),
        admission,
        tx_clients,
        kick,
        &pool,
    )
    .await;
    // the subscriber sessions go with the session they served
    for conn in pool.conns.lock().await.drain(..) {
        conn.shutdown();
    }
    moq.shutdown();
    result
}

async fn serve_namespace(
    moq: &Arc<MoqConn>,
    closed: oneshot::Receiver<MoqError>,
    events: &mut mpsc::UnboundedReceiver<NamespaceEvent>,
    server_ns: &[String],
    client_prefix_len: usize,
    admission: &Admission,
    tx_clients: &mpsc::UnboundedSender<NewClient>,
    kick: &Kick,
    pool: &Arc<SubscriberPool>,
) -> Result<core::convert::Infallible, MoqError> {
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
                let Some(id) = client_id(&ns, client_prefix_len) else {
                    debug!(ns = %ns.join("/"), "not a client namespace; ignored");
                    continue;
                };
                let id = id.to_owned();
                let group = match &admission.admit {
                    Some(admit) => admit(&id),
                    None => Some(id.clone()),
                };
                let Some(group) = group else {
                    debug!(%id, "client id not admitted; ignored");
                    continue;
                };
                let admitted = kick.lock().unwrap().admit(&id, group, admission);
                let rx_kick = match admitted {
                    Ok(rx_kick) => rx_kick,
                    Err(why) => {
                        warn!(%id, "client not taken: {why}");
                        continue;
                    }
                };
                WebTransportRuntime::spawn(serve_client(
                    moq.clone(),
                    pool.clone(),
                    ns,
                    id,
                    server_ns.to_vec(),
                    rx_kick,
                    tx_clients.clone(),
                    kick.clone(),
                ));
            }
            NamespaceEvent::Removed(ns) => {
                let tx = client_id(&ns, client_prefix_len).and_then(|id| kick.lock().unwrap().remove(id));
                if let Some(tx) = tx {
                    _ = tx.send(DisconnectReason::by_peer("client left the relay"));
                }
            }
        }
    }
}

/// The client id of `ns` if it is exactly `<prefix>/client/<id>` with an id a
/// client makes: anyone holding the relay's token can announce anything under
/// the prefix, and `<prefix>/client/x/<victim>` (announced, then withdrawn)
/// must not reach the victim's session.
fn client_id(ns: &[String], client_prefix_len: usize) -> Option<&str> {
    let id = match ns {
        [.., id] if ns.len() == client_prefix_len + 1 => id.as_str(),
        _ => return None,
    };
    let valid = (1..=MAX_CLIENT_ID_LEN).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    valid.then_some(id)
}

/// Longest client id taken (skatepark's ticket-bound ones are 77 bytes).
const MAX_CLIENT_ID_LEN: usize = 128;

/// Relay sessions that subscribe to clients' `up` lanes (see the module docs).
struct SubscriberPool {
    dialer: Arc<Dialer>,
    url: String,
    conns: AsyncMutex<Vec<Arc<MoqConn>>>,
}

impl SubscriberPool {
    /// A session with `needed` requests left, dialling one if none has.
    async fn conn(&self, needed: u64) -> Result<Arc<MoqConn>, MoqError> {
        let mut conns = self.conns.lock().await;
        conns.retain(|conn| {
            let spent = conn.requests_left() < needed && conn.live_subscriptions() == 0;
            if spent {
                conn.shutdown();
            }
            !spent && !conn.is_closed()
        });
        if let Some(conn) = conns.iter().find(|conn| conn.requests_left() >= needed) {
            return Ok(conn.clone());
        }
        let (conn, _closed) = crate::conn::timeout(DIAL_TIMEOUT, "dial a subscriber session", async {
            MoqConn::setup(self.dialer.dial(&self.url).await?).await
        })
        .await?;
        info!(sessions = conns.len() + 1, "MoQ: new subscriber session to the relay");
        conns.push(conn.clone());
        Ok(conn)
    }
}

/// Subscribes to one client's `up` lanes and, once that works, hands the
/// session to the frontend; then pumps its outgoing packets until it ends.
async fn serve_client(
    moq: Arc<MoqConn>,
    pool: Arc<SubscriberPool>,
    ns: Vec<String>,
    id: String,
    server_ns: Vec<String>,
    rx_kick: oneshot::Receiver<DisconnectReason>,
    tx_clients: mpsc::UnboundedSender<NewClient>,
    kick: Kick,
) {
    // Our down lanes for this client are named before anything else, so the
    // relay's SUBSCRIBEs to them (the client asked as it announced) find them
    // claimed rather than provisional.
    let down_tracks: Vec<String> = (0..DOWN_LANES).map(|lane| lane_track(&id, lane)).collect();
    let down: Vec<_> = down_tracks
        .iter()
        .map(|track| moq.out_track(&server_ns, track))
        .collect();
    let (tx_packet_b2f, rx_packet_b2f) = mpsc::unbounded();
    let subscribed = async {
        let sub = pool.conn(UP_LANES as u64).await?;
        let subs = futures::future::join_all((0..UP_LANES).map(|lane| {
            let (sub, ns, route) = (&sub, &ns, tx_packet_b2f.clone());
            async move { sub.subscribe(ns, &lane_track("up", lane), route).await }
        }))
        .await;
        let mut up = Vec::new();
        let mut failed = None;
        for s in subs {
            match s {
                Ok(s) => up.push(s),
                Err(e) => failed = Some(e),
            }
        }
        if let Some(e) = failed {
            for s in &up {
                sub.unsubscribe(s.id).await;
            }
            return Err(e);
        }
        Ok((sub, up))
    }
    .await;
    drop(tx_packet_b2f);
    let (sub, up) = match subscribed {
        Ok(x) => x,
        Err(e) => {
            debug!(%id, "subscribe to client: {e}");
            kick.lock().unwrap().remove(&id);
            for track in &down_tracks {
                moq.end_track(&server_ns, track, "join failed").await;
            }
            return;
        }
    };
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
        kick.lock().unwrap().remove(&id);
        return;
    }
    kick.lock().unwrap().joined(&id);
    info!(%id, "client joined through relay");
    let up_ids: Vec<u64> = up.iter().map(|s| s.id).collect();
    let sending = send_loop(moq.clone(), down, rx_packet_f2b).fuse();
    let rx_kick = rx_kick.fuse();
    let rx_user_dc = rx_user_dc.fuse();
    // any up lane ending means the client is gone
    let up_done = futures::future::select_all(up.into_iter().map(|s| s.ended)).fuse();
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
    for up_id in up_ids {
        sub.unsubscribe(up_id).await;
    }
    // The client hears it at once (its down lanes end: "server left"), not
    // after its silence timeout: a disconnect message of ours has no way to
    // it once the session is gone.
    for track in &down_tracks {
        moq.end_track(&server_ns, track, "session ended").await;
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ns(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn only_a_direct_child_of_client_is_a_client() {
        // prefix skatepark/room1/abcd + "client" = 4 fields
        assert_eq!(client_id(&ns(&["skatepark", "room1", "abcd", "client", "0123abcd"]), 4), Some("0123abcd"));
        // a nested announce must not name (and so kick or take over) a victim
        assert_eq!(client_id(&ns(&["skatepark", "room1", "abcd", "client", "x", "0123abcd"]), 4), None);
        assert_eq!(client_id(&ns(&["skatepark", "room1", "abcd", "client"]), 4), None);
    }

    #[test]
    fn odd_ids_are_refused() {
        let p = |id: &str| ns(&["p", "client", id]);
        assert_eq!(client_id(&p(""), 2), None);
        assert_eq!(client_id(&p("a b"), 2), None);
        assert_eq!(client_id(&p(&"a".repeat(129)), 2), None);
        assert_eq!(client_id(&p(&"a".repeat(128)), 2).map(str::len), Some(128));
        assert_eq!(client_id(&p("Ab-_9"), 2), Some("Ab-_9"));
    }
}
