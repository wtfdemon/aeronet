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
        session::{MIN_MTU, MoqIo, MoqSessionPlugin, send_loop},
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

impl MoqClient {
    /// `id` must be unique among the clients of this server, and a valid
    /// namespace field (no `/`).
    #[must_use]
    pub fn connect(config: ClientConfig, target: MoqTarget, id: impl Into<String>) -> impl EntityCommand {
        let id = id.into();
        move |entity: EntityWorldMut| connect(entity, config, target, id)
    }
}

fn connect(mut entity: EntityWorldMut, config: ClientConfig, target: MoqTarget, id: String) {
    let runtime = entity.world().resource::<WebTransportRuntime>().clone();
    let (tx_dc_reason, rx_dc_reason) = oneshot::channel::<DisconnectReason>();
    let (tx_next, rx_next) = oneshot::channel::<ToConnected>();
    runtime.spawn_on_self(
        async move {
            let reason = run(config, target, id, tx_next).await;
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
    tx_next: oneshot::Sender<ToConnected>,
) -> DisconnectReason {
    match run_inner(config, target, id, tx_next).await {
        Ok(reason) | Err(reason) => reason,
    }
}

async fn run_inner(
    config: ClientConfig,
    target: MoqTarget,
    id: String,
    tx_next: oneshot::Sender<ToConnected>,
) -> Result<DisconnectReason, DisconnectReason> {
    let conn = Dialer::new(config)
        .map_err(DisconnectReason::by_error)?
        .dial(&target.url)
        .await
        .map_err(DisconnectReason::by_error)?;
    let (moq, closed) = MoqConn::setup(conn).await.map_err(DisconnectReason::by_error)?;
    let mtu = moq
        .max_payload()
        .ok_or_else(|| DisconnectReason::by_error(MoqError::new("datagrams not supported")))?;

    let my_ns = target.ns(&["client", &id]);
    moq.announce(my_ns.clone()).await.map_err(DisconnectReason::by_error)?;
    let up = moq.out_track(&my_ns, "up");
    debug!(ns = %my_ns.join("/"), "announced");

    // server -> us; the server may not be up yet, so retry
    let (tx_packet_b2f, rx_packet_b2f) = mpsc::unbounded();
    let server_ns = target.ns(&["server"]);
    let started = Instant::now();
    let down = loop {
        match moq.subscribe(&server_ns, &id, tx_packet_b2f.clone()).await {
            Ok(sub) => break sub,
            Err(e) if started.elapsed() < JOIN_TIMEOUT => {
                debug!("subscribe to server: {e}; retrying");
                WebTransportRuntime::sleep(RETRY).await;
            }
            Err(e) => return Err(DisconnectReason::by_error(e)),
        }
    };
    // wait for the server to subscribe to our `up` track
    while !up.is_subscribed() {
        if started.elapsed() > JOIN_TIMEOUT {
            return Err(DisconnectReason::by_error(MoqError::new(
                "server never subscribed to us",
            )));
        }
        WebTransportRuntime::sleep(Duration::from_millis(20)).await;
    }
    debug!(mtu, "joined through relay");

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
    let server_done = down.ended.fuse();
    futures::pin_mut!(closed, rx_user_dc, server_done);
    Ok(futures::select! {
        _ = server_done => {
            close(&moq).await;
            DisconnectReason::by_peer("server left the relay")
        }
        reason = closed => DisconnectReason::by_error(
            reason.unwrap_or_else(|_| MoqError::new("session closed"))),
        reason = rx_user_dc => {
            close(&moq).await;
            match reason {
                Ok(r) => DisconnectReason::by_user(r),
                Err(_) => DisconnectReason::by_error(MoqError::new("frontend closed")),
            }
        }
        () = sending => DisconnectReason::by_error(MoqError::new("send loop ended")),
    })
}

pub(crate) async fn close(moq: &MoqConn) {
    #[cfg(target_family = "wasm")]
    {
        moq.conn.transport_ref().close();
    }
    #[cfg(not(target_family = "wasm"))]
    {
        moq.conn.0.close(0u32.into(), b"bye");
    }
}

/// Dials the relay, offering the `moqt-16` protocol; reusable for redials.
pub(crate) struct Dialer {
    #[cfg(target_family = "wasm")]
    options: ClientConfig,
    #[cfg(not(target_family = "wasm"))]
    endpoint: aeronet_webtransport::wtransport::Endpoint<
        aeronet_webtransport::wtransport::endpoint::endpoint_side::Client,
    >,
}

impl Dialer {
    pub(crate) fn new(config: ClientConfig) -> Result<Self, MoqError> {
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

    pub(crate) async fn dial(&self, url: &str) -> Result<Connection, MoqError> {
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
