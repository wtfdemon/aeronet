//! A game client whose traffic goes through a MoQ relay.
//!
//! It announces `<prefix>/client/<id>` with track `up` (client -> server
//! packets) and subscribes to `<prefix>/server` track `<id>` (server ->
//! client packets). It counts as connected once both directions are live:
//! our SUBSCRIBE was accepted and the relay subscribed to our `up` track
//! (which happens when the server subscribes to us).

use {
    crate::{
        MoqTarget,
        conn::{Connection, MoqConn, MoqError},
        session::{DOWN_LANES, MIN_MTU, MoqIo, MoqSessionPlugin, UP_LANES, lane_track, send_loop},
        wire,
    },
    aeronet_io::{
        IoSystems, Session, SessionEndpoint,
        connection::{DisconnectReason, Disconnected},
        packet::RecvPacket,
    },
    aeronet_webtransport::WebTransportRuntime,
    bevy_app::prelude::*,
    bevy_ecs::{prelude::*, system::EntityCommand},
    bevy_platform::time::Instant,
    bytes::Bytes,
    core::{mem, time::Duration},
    futures::{
        FutureExt,
        channel::{mpsc, oneshot},
    },
    tracing::{Instrument, debug, debug_span},
};

cfg_if::cfg_if! {
    if #[cfg(target_family = "wasm")] {
        /// Browser WebTransport options (the `moqt-16` protocol is added for you).
        pub type ClientConfig = aeronet_webtransport::xwt_web::WebTransportOptions;
    } else {
        /// wtransport client config; use `with_native_certs()` for a public relay.
        pub type ClientConfig = aeronet_webtransport::wtransport::ClientConfig;
    }
}

pub struct MoqClientPlugin;

impl Plugin for MoqClientPlugin {
    fn build(&self, app: &mut App) {
        if !app.is_plugin_added::<MoqSessionPlugin>() {
            app.add_plugins(MoqSessionPlugin);
        }
        app.add_systems(
            PreUpdate,
            (poll_connecting, poll_connected)
                .in_set(IoSystems::Poll)
                .before(crate::session::poll),
        );
    }
}

/// Marker for a session connecting to a server through a MoQ relay.
#[derive(Debug, Component)]
#[require(SessionEndpoint)]
pub struct MoqClient(());

/// A WebTransport session to the relay that the page opened ahead of the game
/// (browser only): `connect_warm` adopts it instead of dialling, saving the
/// handshake. Opened with the `moqt-16` protocol and nothing sent on it yet.
#[cfg(target_family = "wasm")]
pub type WarmSession = xwt_web::web_wt_sys::WebTransport;
/// No pre-opened sessions natively.
#[cfg(not(target_family = "wasm"))]
pub type WarmSession = core::convert::Infallible;

impl MoqClient {
    /// `id` must be unique among the clients of this server, and a valid
    /// namespace field (no `/`).
    #[must_use]
    pub fn connect(config: ClientConfig, target: MoqTarget, id: impl Into<String>) -> impl EntityCommand {
        Self::connect_warm(config, target, id, None)
    }

    /// [`Self::connect`], adopting `warm` if it is (or becomes) open, else
    /// dialling with `config` as usual.
    #[must_use]
    pub fn connect_warm(
        config: ClientConfig,
        target: MoqTarget,
        id: impl Into<String>,
        warm: Option<WarmSession>,
    ) -> impl EntityCommand {
        let id = id.into();
        move |entity: EntityWorldMut| connect(entity, config, target, id, warm)
    }
}

/// The pre-opened session, once it's open; `None` if it failed.
async fn adopt(warm: Option<WarmSession>) -> Option<Connection> {
    #[cfg(target_family = "wasm")]
    {
        let transport = warm?;
        wasm_bindgen_futures::JsFuture::from(transport.ready()).await.ok()?;
        Some(xwt_web::Session::new(transport))
    }
    #[cfg(not(target_family = "wasm"))]
    {
        warm.map(|never| match never {})
    }
}

fn connect(mut entity: EntityWorldMut, config: ClientConfig, target: MoqTarget, id: String, warm: Option<WarmSession>) {
    let runtime = entity.world().resource::<WebTransportRuntime>().clone();
    let (tx_dc_reason, rx_dc_reason) = oneshot::channel::<DisconnectReason>();
    let (tx_next, rx_next) = oneshot::channel::<ToConnected>();
    runtime.spawn_on_self(
        async move {
            let reason = run(config, target, id, warm, tx_next).await;
            debug!("MoQ client disconnected: {reason:?}");
            _ = tx_dc_reason.send(reason);
        }
        .instrument(debug_span!("moq_client", entity = %entity.id())),
    );
    entity.insert((MoqClient(()), Connecting { rx_dc_reason, rx_next }));
}

#[derive(Debug, Component)]
struct Connecting {
    rx_dc_reason: oneshot::Receiver<DisconnectReason>,
    rx_next: oneshot::Receiver<ToConnected>,
}

#[derive(Debug, Component)]
struct Connected {
    rx_dc_reason: oneshot::Receiver<DisconnectReason>,
}

#[derive(Debug)]
struct ToConnected {
    mtu: usize,
    rx_packet_b2f: mpsc::UnboundedReceiver<RecvPacket>,
    tx_packet_f2b: mpsc::UnboundedSender<Bytes>,
    tx_user_dc: oneshot::Sender<String>,
}

/// How long the server gets to notice us and subscribe back.
const JOIN_TIMEOUT: Duration = Duration::from_secs(15);
const RETRY: Duration = Duration::from_millis(500);

async fn run(
    config: ClientConfig,
    target: MoqTarget,
    id: String,
    warm: Option<WarmSession>,
    tx_next: oneshot::Sender<ToConnected>,
) -> DisconnectReason {
    match run_inner(config, target, id, warm, tx_next).await {
        Ok(reason) | Err(reason) => reason,
    }
}

async fn run_inner(
    config: ClientConfig,
    target: MoqTarget,
    id: String,
    warm: Option<WarmSession>,
    tx_next: oneshot::Sender<ToConnected>,
) -> Result<DisconnectReason, DisconnectReason> {
    let t0 = Instant::now();
    let had_warm = warm.is_some();
    let conn = match adopt(warm).await {
        Some(conn) => {
            tracing::info!("moq join: adopted the pre-opened relay session at {:?}", t0.elapsed());
            conn
        }
        None => {
            if had_warm {
                tracing::info!("moq join: the pre-opened relay session failed, dialling");
            }
            let conn = Dialer::new(config)
                .map_err(DisconnectReason::by_error)?
                .dial(&target.url)
                .await
                .map_err(DisconnectReason::by_error)?;
            tracing::info!("moq join: connected to relay at {:?}", t0.elapsed());
            conn
        }
    };
    let (moq, closed) = MoqConn::setup(conn).await.map_err(DisconnectReason::by_error)?;
    tracing::info!("moq join: setup done at {:?}", t0.elapsed());
    let mtu = moq
        .max_payload()
        .ok_or_else(|| DisconnectReason::by_error(MoqError::new("datagrams not supported")))?;

    let my_ns = target.ns(&["client", &id]);
    let up: Vec<_> = (0..UP_LANES)
        .map(|lane| moq.out_track(&my_ns, &lane_track("up", lane)))
        .collect();

    // The announce and our SUBSCRIBEs go out together: neither waits on the
    // other, so the join costs one round trip here instead of two. The server
    // may not be up yet, so the SUBSCRIBEs retry.
    let (tx_packet_b2f, rx_packet_b2f) = mpsc::unbounded();
    let server_ns = target.ns(&["server"]);
    let started = Instant::now();
    let subscribe_all = || {
        futures::future::join_all((0..DOWN_LANES).map(|lane| {
            let track = lane_track(&id, lane);
            let (moq, server_ns, route) = (&moq, &server_ns, tx_packet_b2f.clone());
            async move { moq.subscribe(server_ns, &track, route).await }
        }))
    };
    let (announced, mut subs) = futures::join!(moq.announce(my_ns.clone()), subscribe_all());
    announced.map_err(DisconnectReason::by_error)?;
    tracing::info!("moq join: announced and subscribed at {:?}", t0.elapsed());
    let down = loop {
        let mut live = Vec::new();
        let mut failed = None;
        for sub in subs {
            match sub {
                Ok(sub) => live.push(sub),
                Err(e) => failed = Some(e),
            }
        }
        match failed {
            None => break live,
            Some(e) => {
                for sub in &live {
                    moq.unsubscribe(sub.id).await;
                }
                if started.elapsed() >= JOIN_TIMEOUT {
                    return Err(DisconnectReason::by_error(e));
                }
                debug!("subscribe to server: {e}; retrying");
                WebTransportRuntime::sleep(RETRY).await;
                subs = subscribe_all().await;
            }
        }
    };
    // wait for the server to subscribe to every `up` lane
    while !up.iter().all(|lane| lane.is_subscribed()) {
        if started.elapsed() > JOIN_TIMEOUT {
            return Err(DisconnectReason::by_error(MoqError::new(
                "server never subscribed to us",
            )));
        }
        WebTransportRuntime::sleep(Duration::from_millis(20)).await;
    }
    tracing::info!("moq join: server subscribed to us at {:?}", t0.elapsed());

    let (tx_packet_f2b, rx_packet_f2b) = mpsc::unbounded();
    let (tx_user_dc, rx_user_dc) = oneshot::channel::<String>();
    tx_next
        .send(ToConnected {
            mtu,
            rx_packet_b2f,
            tx_packet_f2b,
            tx_user_dc,
        })
        .map_err(|_| DisconnectReason::by_error(MoqError::new("frontend closed")))?;
    drop(tx_packet_b2f);

    let sending = send_loop(moq.clone(), up, rx_packet_f2b).fuse();
    futures::pin_mut!(sending);
    let closed = closed.fuse();
    let rx_user_dc = rx_user_dc.fuse();
    // any down lane ending means the server is gone
    let server_done = futures::future::select_all(down.into_iter().map(|sub| sub.ended)).fuse();
    futures::pin_mut!(closed, rx_user_dc, server_done);
    Ok(futures::select! {
        _ = server_done => {
            moq.shutdown();
            DisconnectReason::by_peer("server left the relay")
        }
        reason = closed => DisconnectReason::by_error(
            reason.unwrap_or_else(|_| MoqError::new("session closed"))),
        reason = rx_user_dc => {
            moq.shutdown();
            match reason {
                Ok(r) => DisconnectReason::by_user(r),
                Err(_) => DisconnectReason::by_error(MoqError::new("frontend closed")),
            }
        }
        () = sending => DisconnectReason::by_error(MoqError::new("send loop ended")),
    })
}

/// Dials the relay, offering the `moqt-16` protocol; reusable for redials.
pub struct Dialer {
    #[cfg(target_family = "wasm")]
    options: ClientConfig,
    #[cfg(not(target_family = "wasm"))]
    endpoint: aeronet_webtransport::wtransport::Endpoint<
        aeronet_webtransport::wtransport::endpoint::endpoint_side::Client,
    >,
}

impl Dialer {
    pub fn new(config: ClientConfig) -> Result<Self, MoqError> {
        #[cfg(target_family = "wasm")]
        {
            Ok(Self { options: config })
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let endpoint = aeronet_webtransport::wtransport::Endpoint::client(config)
                .map_err(|e| MoqError(format!("endpoint: {e}")))?;
            Ok(Self { endpoint })
        }
    }

    pub async fn dial(&self, url: &str) -> Result<Connection, MoqError> {
        #[cfg(target_family = "wasm")]
        {
            use xwt_core::prelude::*;
            let options = self.options.to_js();
            let protocols = js_sys::Array::of1(&wire::PROTOCOL.into());
            js_sys::Reflect::set(&options, &"protocols".into(), &protocols)
                .map_err(|_| MoqError::new("set WebTransport protocols"))?;
            let endpoint = aeronet_webtransport::xwt_web::Endpoint { options };
            endpoint
                .connect(url)
                .await
                .map_err(|e| MoqError(format!("connect: {e:?}")))?
                .wait_connect()
                .await
                .map_err(|e| MoqError(format!("connect: {e:?}")))
        }
        #[cfg(not(target_family = "wasm"))]
        {
            use aeronet_webtransport::wtransport::endpoint::ConnectOptions;
            // structured-field list of strings
            let options = ConnectOptions::builder(url)
                .add_header("wt-available-protocols", format!("\"{}\"", wire::PROTOCOL))
                .build();
            self.endpoint
                .connect(options)
                .await
                .map(xwt_wtransport::Connection)
                .map_err(|e| MoqError(format!("connect: {e}")))
        }
    }
}

fn poll_connecting(mut commands: Commands, mut clients: Query<(Entity, &mut Connecting), With<MoqClient>>) {
    for (entity, mut client) in &mut clients {
        if try_disconnect(&mut commands, entity, &mut client.rx_dc_reason) {
            continue;
        }
        let Ok(Some(next)) = client.rx_next.try_recv() else {
            continue;
        };
        let mut session = Session::new(Instant::now(), MIN_MTU);
        if let Err(e) = session.set_mtu(next.mtu) {
            commands.trigger(Disconnected {
                entity,
                reason: DisconnectReason::by_error(MoqError(format!("{e}"))),
            });
            continue;
        }
        let (_, dummy) = oneshot::channel();
        let rx_dc_reason = mem::replace(&mut client.rx_dc_reason, dummy);
        commands.entity(entity).remove::<Connecting>().insert((
            MoqIo {
                rx_packet_b2f: next.rx_packet_b2f,
                tx_packet_f2b: next.tx_packet_f2b,
                tx_user_dc: Some(next.tx_user_dc),
            },
            Connected { rx_dc_reason },
            session,
        ));
    }
}

fn poll_connected(mut commands: Commands, mut clients: Query<(Entity, &mut Connected), With<MoqClient>>) {
    for (entity, mut client) in &mut clients {
        try_disconnect(&mut commands, entity, &mut client.rx_dc_reason);
    }
}

pub(crate) fn try_disconnect(
    commands: &mut Commands,
    entity: Entity,
    rx: &mut oneshot::Receiver<DisconnectReason>,
) -> bool {
    let reason = match rx.try_recv() {
        Ok(None) => None,
        Ok(Some(r)) => Some(r),
        Err(_) => Some(DisconnectReason::by_error(MoqError::new("backend closed"))),
    };
    reason.is_some_and(|reason| {
        commands.trigger(Disconnected { entity, reason });
        true
    })
}
