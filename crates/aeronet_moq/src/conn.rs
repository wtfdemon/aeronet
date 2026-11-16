//! One MoQ draft-18 session over one WebTransport connection to a relay.
//!
//! - Each side opens a unidirectional control stream that starts with SETUP.
//!   Ours goes out with the session's first requests in the same flight: we
//!   offer no extensions, so nothing has to wait for the relay's SETUP
//!   (draft-18 10.3). The relay's SETUP must arrive within
//!   [`SETUP_TIMEOUT`]; a GOAWAY on its control stream ends the session.
//! - Every request is its own bidirectional stream, request first, answers
//!   on the same stream. Our SUBSCRIBEs, PUBLISH_NAMESPACEs and
//!   SUBSCRIBE_NAMESPACEs live as long as their stream; the relay's
//!   SUBSCRIBEs to tracks we publish arrive as streams it opens, each one
//!   subscription with its own alias, ended by PUBLISH_DONE + FIN (us) or a
//!   reset / STOP_SENDING (the relay). No request id budget to spend.
//!
//! Only datagrams carry data: aeronet's transport layer already does
//! reliability and ordering on top of unreliable packets.

use {
    crate::wire::{self, Control, Namespace},
    aeronet_io::packet::RecvPacket,
    aeronet_webtransport::WebTransportRuntime,
    alloc::sync::Arc,
    bevy_platform::time::Instant,
    bytes::Bytes,
    core::{
        sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        time::Duration,
    },
    futures::{
        FutureExt,
        channel::{mpsc, oneshot},
    },
    std::{collections::HashMap, sync::Mutex},
    tracing::{debug, trace, warn},
    xwt_core::prelude::*,
};

cfg_if::cfg_if! {
    if #[cfg(target_family = "wasm")] {
        pub type Connection = xwt_web::Session;
    } else {
        pub type Connection = xwt_wtransport::Connection;
    }
}

pub(crate) type SendStream = <Connection as xwt_core::session::stream::SendSpec>::SendStream;
pub(crate) type RecvStream = <Connection as xwt_core::session::stream::RecvSpec>::RecvStream;

/// Something went wrong on the MoQ session; the connection is unusable.
#[derive(Debug, Clone, derive_more::Display, derive_more::Error)]
#[display("{_0}")]
pub struct MoqError(#[error(not(source))] pub String);

impl MoqError {
    pub(crate) fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }

    /// The relay told us it is going away (GOAWAY): dial again right away.
    #[must_use]
    pub fn is_goaway(&self) -> bool {
        self.0.starts_with(GOAWAY)
    }
}

const GOAWAY: &str = "relay sent GOAWAY";

fn err(context: &str, e: impl core::fmt::Display) -> MoqError {
    MoqError(format!("{context}: {e}"))
}

/// One relay subscription to a track of ours: its alias, and the way to end
/// it from our side (PUBLISH_DONE with the reason).
#[derive(Debug)]
struct RelaySub {
    key: u64,
    alias: u64,
    end: Option<oneshot::Sender<&'static str>>,
}

/// A track we publish. Packets are dropped until the relay subscribes to it.
#[derive(Debug)]
pub struct OutTrack {
    /// The relay's live subscriptions. One normally; a relay that subscribes
    /// twice (draft-18 says it mustn't) gets every object on both, so no
    /// group of its subscribers ever goes dark.
    subs: Mutex<Vec<RelaySub>>,
    group: AtomicU64,
    /// SUBSCRIBEs the relay has sent us for this track (fan-out probe).
    subscribes: AtomicU64,
    /// Asked for by the relay before we named it ([`MoqConn::out_track`]): a
    /// join's SUBSCRIBE can beat the server seeing the client, but so can a
    /// SUBSCRIBE for a made-up name from anyone holding the relay's token.
    /// Held for [`PROVISIONAL_LIFE`] at most, [`MAX_PROVISIONAL`] at once.
    provisional_since: Mutex<Option<Instant>>,
    /// Woken when the relay subscribes ([`Self::subscribed`]).
    waiters: Mutex<Vec<oneshot::Sender<()>>>,
}

impl OutTrack {
    fn new(provisional: bool) -> Self {
        Self {
            subs: Mutex::new(Vec::new()),
            group: AtomicU64::new(0),
            subscribes: AtomicU64::new(0),
            provisional_since: Mutex::new(provisional.then(Instant::now)),
            waiters: Mutex::new(Vec::new()),
        }
    }

    fn provisional_since(&self) -> Option<Instant> {
        *self.provisional_since.lock().unwrap()
    }

    /// `payload` as one datagram per relay subscription (one, normally), all
    /// in one group, each handed to `send`. `false` while the relay hasn't
    /// subscribed (nothing sent).
    pub fn datagrams(&self, payload: &[u8], mut send: impl FnMut(Bytes)) -> bool {
        let subs = self.subs.lock().unwrap();
        if subs.is_empty() {
            return false;
        }
        let group = self.group.fetch_add(1, Ordering::Relaxed);
        for sub in subs.iter() {
            send(wire::datagram(sub.alias, group, payload));
        }
        true
    }

    /// The datagram for `payload` on the first relay subscription, or `None`
    /// while there is none (probes and tests; senders use [`Self::datagrams`]).
    pub fn datagram(&self, payload: &[u8]) -> Option<Bytes> {
        let mut out = None;
        self.datagrams(payload, |dg| {
            out.get_or_insert(dg);
        });
        out
    }

    /// Like [`Self::datagram`] but the caller picks group and object (probe).
    pub fn datagram_object(&self, group: u64, object: u64, payload: &[u8]) -> Option<Bytes> {
        let subs = self.subs.lock().unwrap();
        subs.first()
            .map(|sub| wire::datagram_object(sub.alias, group, object, payload))
    }

    pub fn is_subscribed(&self) -> bool {
        !self.subs.lock().unwrap().is_empty()
    }

    /// Resolves once the relay has subscribed (at once if it has).
    pub async fn subscribed(&self) {
        let rx = {
            let mut waiters = self.waiters.lock().unwrap();
            if self.is_subscribed() {
                return;
            }
            let (tx, rx) = oneshot::channel();
            waiters.push(tx);
            rx
        };
        _ = rx.await;
    }

    /// How many SUBSCRIBEs the relay has sent us for this track: 1 however
    /// many players watch it means the relay fans out.
    pub fn relay_subscribes(&self) -> u64 {
        self.subscribes.load(Ordering::Relaxed)
    }

    fn add_sub(&self, key: u64, alias: u64, end: oneshot::Sender<&'static str>) {
        self.subscribes.fetch_add(1, Ordering::Relaxed);
        let mut subs = self.subs.lock().unwrap();
        if !subs.is_empty() {
            warn!(alias, "relay subscribed a second time to one track; serving both");
        }
        subs.push(RelaySub {
            key,
            alias,
            end: Some(end),
        });
        drop(subs);
        for w in self.waiters.lock().unwrap().drain(..) {
            _ = w.send(());
        }
    }

    fn remove_sub(&self, key: u64) {
        self.subs.lock().unwrap().retain(|sub| sub.key != key);
    }

    /// Ends every relay subscription (PUBLISH_DONE `reason`) and sends nothing more.
    fn end_all(&self, reason: &'static str) {
        for mut sub in self.subs.lock().unwrap().drain(..) {
            if let Some(end) = sub.end.take() {
                _ = end.send(reason);
            }
        }
    }
}

/// A live SUBSCRIBE of ours.
#[derive(Debug)]
pub struct Subscription {
    /// Request id, for [`MoqConn::unsubscribe`].
    pub id: u64,
    /// Resolves (or is cancelled) when the publisher ends the track, the
    /// subscription is cancelled or the session dies.
    pub ended: oneshot::Receiver<()>,
}

/// Namespace appearing or going away under a SUBSCRIBE_NAMESPACE prefix.
#[derive(Debug, Clone)]
pub enum NamespaceEvent {
    Added(Namespace),
    Removed(Namespace),
}

#[derive(Default)]
struct State {
    /// Namespaces we announced: the relay may SUBSCRIBE to any track in them.
    announced: Vec<Namespace>,
    /// Keyed by `ns/track`.
    out_tracks: HashMap<String, Arc<OutTrack>>,
    next_alias: u64,
    next_sub_key: u64,
    /// Incoming datagrams by track alias.
    in_routes: HashMap<u64, mpsc::UnboundedSender<RecvPacket>>,
    /// Our live SUBSCRIBEs by request id: dropping the sender cancels one.
    cancels: HashMap<u64, oneshot::Sender<()>>,
}

impl State {
    fn provisional_count(&self) -> usize {
        self.out_tracks
            .values()
            .filter(|track| track.provisional_since().is_some())
            .count()
    }

    /// Forgets relay-asked tracks nobody named within [`PROVISIONAL_LIFE`],
    /// ending their relay subscriptions.
    fn drop_stale_provisional(&mut self) {
        self.out_tracks.retain(|_, track| {
            let stale = track
                .provisional_since()
                .is_some_and(|since| since.elapsed() >= PROVISIONAL_LIFE);
            if stale {
                track.end_all("never claimed");
            }
            !stale
        });
    }
}

fn track_key(ns: &[String], track: &str) -> String {
    let mut k = ns.join("/");
    k.push('/');
    k.push_str(track);
    k
}

pub struct MoqConn {
    pub conn: Arc<Connection>,
    /// Our control stream (SETUP sent): never written again, but held open
    /// for the session's life, as draft-18 requires.
    #[expect(dead_code, reason = "held, not read")]
    control: Mutex<SendStream>,
    state: Mutex<State>,
    next_request_id: AtomicU64,
    /// Request streams we opened that are still open (the relay caps how
    /// many at once: [`MAX_OPEN_REQUESTS`]).
    open_requests: AtomicUsize,
    /// Room promised to callers about to open requests ([`Self::reserve`]).
    reserved: AtomicUsize,
    /// Fires once when the session dies.
    closed_tx: Mutex<Option<oneshot::Sender<MoqError>>>,
    /// The connection has been closed (once, whoever ends the session).
    shut: AtomicBool,
    /// Relay SUBSCRIBEs for tracks we haven't named are refused, not held
    /// ([`Self::name_tracks_first`]).
    named_only: AtomicBool,
}

/// Closes the session when dropped: every way out of a task that owns one
/// (an error, a timeout, the frontend going away) ends it, so no relay
/// session lives on with our announce in it. The background tasks hold their
/// own `Arc`s, so dropping the last handle alone would not.
pub struct ShutdownOnDrop(pub Arc<MoqConn>);

impl Drop for ShutdownOnDrop {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

impl core::fmt::Debug for MoqConn {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MoqConn").finish_non_exhaustive()
    }
}

/// Relay-asked tracks we hold before naming them (see [`OutTrack`]).
pub const MAX_PROVISIONAL: usize = 256;
/// And for how long: a real join names its tracks within a round trip or two.
pub const PROVISIONAL_LIFE: Duration = Duration::from_secs(20);
/// A request (announce, SUBSCRIBE, ...) the relay hasn't answered by then
/// fails, so no task waits forever on a relay that went quiet.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// The relay's SETUP must arrive by then, or the session is closed.
pub const SETUP_TIMEOUT: Duration = Duration::from_secs(10);
/// Request streams one session keeps open at once. Cloudflare's draft-18
/// relay lets a session hold 99 (measured 2026-11-16; closed ones free their
/// room at once), so a few are kept spare.
pub const MAX_OPEN_REQUESTS: usize = 96;
/// Request streams the relay may hold open towards us at once: every relay
/// SUBSCRIBE to a track of ours is one, for as long as it lives (a server's
/// room: [`crate::DOWN_LANES`] per client and the broadcast lanes, plus up to
/// [`MAX_PROVISIONAL`] the relay was asked for by anyone). quinn's default
/// is 100: junk SUBSCRIBEs alone filled it on the real relay and the next
/// real one never got through. Dialled sessions need [`quic_limits`].
pub const MAX_INCOMING_REQUESTS: u32 = 4096;

/// Raises `config`'s stream limits to what a MoQ draft-18 session needs
/// ([`MAX_INCOMING_REQUESTS`]); use it for the transport config of every
/// [`crate::ClientConfig`] a [`crate::MoqServer`] dials with.
#[cfg(not(target_family = "wasm"))]
pub fn quic_limits(config: &mut aeronet_webtransport::wtransport::config::QuicTransportConfig) {
    config.max_concurrent_bidi_streams(MAX_INCOMING_REQUESTS.into());
}

/// quinn's default transport with [`quic_limits`]: for
/// `ClientConfig::builder().with_bind_default().with_custom_transport(..)`.
#[cfg(not(target_family = "wasm"))]
#[must_use]
pub fn moq_transport() -> aeronet_webtransport::wtransport::config::QuicTransportConfig {
    let mut config = aeronet_webtransport::wtransport::config::QuicTransportConfig::default();
    quic_limits(&mut config);
    config
}

/// The relay's first message on a stream it opens must come by then.
const INBOUND_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// `fut`, or `Err(what timed out)` after `after`.
pub async fn timeout<T>(
    after: Duration,
    what: &str,
    fut: impl Future<Output = Result<T, MoqError>>,
) -> Result<T, MoqError> {
    let fut = fut.fuse();
    let sleep = WebTransportRuntime::sleep(after).fuse();
    futures::pin_mut!(fut, sleep);
    futures::select! {
        r = fut => r,
        () = sleep => Err(MoqError(format!("{what}: no answer in {after:?}"))),
    }
}

/// Closes a connection that never became a [`MoqConn`].
fn close_connection(conn: &Connection) {
    #[cfg(target_family = "wasm")]
    {
        conn.transport_ref().close();
    }
    #[cfg(not(target_family = "wasm"))]
    {
        conn.0.close(0u32.into(), b"bye");
    }
}

/// Counts a request stream of ours as open until dropped.
struct OpenRequest(Arc<MoqConn>);

impl OpenRequest {
    fn new(conn: &Arc<MoqConn>) -> Self {
        conn.open_requests.fetch_add(1, Ordering::AcqRel);
        Self(conn.clone())
    }
}

impl Drop for OpenRequest {
    fn drop(&mut self) {
        self.0.open_requests.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Cancels a request stream (draft-18 3.3.2): resets our half and asks the
/// peer to stop sending its half. Best effort, in the background.
fn cancel_streams(send: SendStream, recv: RecvStream) {
    WebTransportRuntime::spawn(async move {
        let code = wire::RESET_CANCELLED.into();
        _ = xwt_core::stream::WriteAbort::abort(send, code).await;
        _ = xwt_core::stream::ReadAbort::abort(recv, code).await;
    });
}

/// Ends a request stream we answered for the last time (REQUEST_ERROR,
/// PUBLISH_DONE): FIN on ours, and STOP_SENDING on the peer's so the stream
/// closes now and frees its room even if the peer never FINs.
fn finish_streams(send: SendStream, recv: RecvStream) {
    WebTransportRuntime::spawn(async move {
        _ = xwt_core::stream::Finish::finish(send).await;
        _ = xwt_core::stream::ReadAbort::abort(recv, wire::RESET_CANCELLED.into()).await;
    });
}

/// Resolves when the peer sends STOP_SENDING for (or resets the session
/// under) `send`, without giving it up.
async fn stopped(send: &mut SendStream) {
    #[cfg(not(target_family = "wasm"))]
    {
        send.0.stopped().await;
    }
    #[cfg(target_family = "wasm")]
    {
        _ = wasm_bindgen_futures::JsFuture::from(send.writer.inner.closed()).await;
    }
}

impl MoqConn {
    /// Starts MoQ on `conn`: opens our control stream with SETUP and starts
    /// the background tasks. Doesn't wait for the relay's SETUP, so the
    /// caller's first requests share the flight with ours. The receiver
    /// fires with the reason when the session ends.
    pub async fn setup(conn: Connection) -> Result<(Arc<Self>, oneshot::Receiver<MoqError>), MoqError> {
        let conn = Arc::new(conn);
        let result = timeout(SETUP_TIMEOUT, "MoQ setup", Self::setup_inner(conn.clone())).await;
        if result.is_err() {
            close_connection(&conn);
        }
        result
    }

    async fn setup_inner(conn: Arc<Connection>) -> Result<(Arc<Self>, oneshot::Receiver<MoqError>), MoqError> {
        let mut control = conn
            .open_uni()
            .await
            .map_err(|e| err("open control stream", e))?
            .wait_uni()
            .await
            .map_err(|e| err("open control stream", e))?;
        write_all(&mut control, &wire::setup()).await?;

        let (closed_tx, closed_rx) = oneshot::channel();
        let this = Arc::new(Self {
            conn,
            control: Mutex::new(control),
            state: Mutex::new(State {
                next_alias: 1,
                ..Default::default()
            }),
            next_request_id: AtomicU64::new(0),
            open_requests: AtomicUsize::new(0),
            reserved: AtomicUsize::new(0),
            closed_tx: Mutex::new(Some(closed_tx)),
            shut: AtomicBool::new(false),
            named_only: AtomicBool::new(false),
        });

        WebTransportRuntime::spawn({
            let this = this.clone();
            async move {
                let e = this.clone().control_loop().await;
                this.close(e);
            }
        });
        WebTransportRuntime::spawn({
            let this = this.clone();
            async move {
                let e = this.clone().accept_loop().await;
                this.close(e);
            }
        });
        WebTransportRuntime::spawn({
            let this = this.clone();
            async move {
                let e = this.clone().datagram_loop().await;
                this.close(e);
            }
        });
        Ok((this, closed_rx))
    }

    /// Ends the session, whatever ended it (our shutdown, a dead link, a
    /// protocol error, GOAWAY): the connection is closed too, so a session
    /// can't live on at the relay with our announces.
    fn close(&self, reason: MoqError) {
        if !self.shut.swap(true, Ordering::AcqRel) {
            close_connection(&self.conn);
        }
        if let Some(tx) = self.closed_tx.lock().unwrap().take() {
            debug!("MoQ session closed: {reason}");
            _ = tx.send(reason);
        }
        // fail everything still waiting
        let mut st = self.state.lock().unwrap();
        st.in_routes.clear();
        st.cancels.clear();
        for track in st.out_tracks.values() {
            track.end_all("session closed");
        }
    }

    fn request_id(&self) -> u64 {
        // we dialled: even ids, from 0
        self.next_request_id.fetch_add(2, Ordering::Relaxed)
    }

    /// Opens a request stream, writes `msg` and reads the first answer.
    async fn request(
        self: &Arc<Self>,
        what: &str,
        msg: Bytes,
    ) -> Result<(Control, SendStream, FrameReader, OpenRequest), MoqError> {
        let open = OpenRequest::new(self);
        timeout(REQUEST_TIMEOUT, what, async {
            let (mut send, recv) = self
                .conn
                .open_bi()
                .await
                .map_err(|e| err(&format!("open {what} stream"), e))?
                .wait_bi()
                .await
                .map_err(|e| err(&format!("open {what} stream"), e))?;
            write_all(&mut send, &msg).await?;
            let mut reader = FrameReader::new(recv);
            let (kind, body) = reader.frame().await?;
            let answer = wire::decode_control(kind, &body).map_err(|e| err(&format!("{what} answer"), e))?;
            Ok((answer, send, reader, open))
        })
        .await
    }

    /// Announces `ns`. The relay will then SUBSCRIBE to tracks in it as
    /// subscribers ask for them; get their handles with [`Self::out_track`].
    /// Held until the session ends; the relay withdrawing it ends the session
    /// (nobody could reach us under it any more).
    pub async fn announce(self: &Arc<Self>, ns: Namespace) -> Result<(), MoqError> {
        self.state.lock().unwrap().announced.push(ns.clone());
        let id = self.request_id();
        let answer = self.request("PUBLISH_NAMESPACE", wire::publish_namespace(id, &ns)).await;
        let (send, mut reader, open) = match answer {
            Ok((Control::RequestOk, send, reader, open)) => (send, reader, open),
            other => {
                self.state.lock().unwrap().announced.retain(|a| a != &ns);
                return Err(match other {
                    Ok((Control::RequestError { code, reason }, ..)) => {
                        MoqError(format!("announce refused: {code} {reason}"))
                    }
                    Ok((other, ..)) => MoqError(format!("announce answered with {other:?}")),
                    Err(e) => e,
                });
            }
        };
        let this = self.clone();
        WebTransportRuntime::spawn(async move {
            let _open = open;
            let _send = send;
            // nothing more is sent on it; it ends with the namespace
            while reader.frame().await.is_ok() {}
            if !this.is_closed() {
                this.close(MoqError(format!("relay withdrew our namespace {}", ns.join("/"))));
            }
        });
        Ok(())
    }

    /// Probe: `PUBLISH` `track` under `ns` as `alias` (no announce); `Ok`
    /// when the relay answers REQUEST_OK. Objects then go out as
    /// [`wire::datagram`]`(alias, ..)`. The game never does this.
    pub async fn publish_probe(self: &Arc<Self>, ns: &[String], track: &str, alias: u64) -> Result<(), MoqError> {
        let id = self.request_id();
        match self.request("PUBLISH", wire::publish(id, ns, track, alias)).await? {
            (Control::RequestOk, send, reader, open) => {
                WebTransportRuntime::spawn(async move {
                    let (_open, _send, mut reader) = (open, send, reader);
                    while reader.frame().await.is_ok() {}
                });
                Ok(())
            }
            (Control::RequestError { code, reason }, ..) => Err(MoqError(format!("request refused: {code} {reason}"))),
            (other, ..) => Err(MoqError(format!("PUBLISH answered with {other:?}"))),
        }
    }

    /// The handle for a track we publish, created on first use or taken over
    /// from the relay's early SUBSCRIBE (no longer provisional then).
    pub fn out_track(&self, ns: &[String], track: &str) -> Arc<OutTrack> {
        let track = self
            .state
            .lock()
            .unwrap()
            .out_tracks
            .entry(track_key(ns, track))
            .or_insert_with(|| Arc::new(OutTrack::new(false)))
            .clone();
        *track.provisional_since.lock().unwrap() = None;
        track
    }

    /// From now on, a relay SUBSCRIBE for a track not named with
    /// [`Self::out_track`] is refused at once instead of held provisionally.
    /// For a session that names every track before announcing (a client's
    /// up lanes): nothing it publishes can be asked for early, so a held
    /// one is only someone's junk keeping a stream open.
    pub fn name_tracks_first(&self) {
        self.named_only.store(true, Ordering::Relaxed);
    }

    /// Subscribes to `ns`/`track` from the newest object on; its datagrams go to `route`.
    pub async fn subscribe(
        self: &Arc<Self>,
        ns: &[String],
        track: &str,
        route: mpsc::UnboundedSender<RecvPacket>,
    ) -> Result<Subscription, MoqError> {
        let id = self.request_id();
        let (send, mut reader, open, alias) = match self.request("SUBSCRIBE", wire::subscribe(id, ns, track)).await? {
            (Control::SubscribeOk { alias }, send, reader, open) => (send, reader, open, alias),
            (Control::RequestError { code, reason }, ..) => {
                return Err(MoqError(format!("request refused: {code} {reason}")));
            }
            (other, ..) => return Err(MoqError(format!("SUBSCRIBE answered with {other:?}"))),
        };
        let (tx_cancel, mut rx_cancel) = oneshot::channel();
        let (tx_ended, ended) = oneshot::channel();
        {
            let mut st = self.state.lock().unwrap();
            if st.in_routes.contains_key(&alias) {
                drop(st);
                cancel_streams(send, reader.into_inner());
                return Err(MoqError(format!("relay reused live track alias {alias}")));
            }
            // datagrams may follow SUBSCRIBE_OK at once: routed from here on
            st.in_routes.insert(alias, route);
            st.cancels.insert(id, tx_cancel);
        }
        let this = self.clone();
        WebTransportRuntime::spawn(async move {
            let _open = open;
            // until PUBLISH_DONE, the stream ending, or our cancel
            let cancelled = loop {
                let next = {
                    let frame = reader.frame().fuse();
                    futures::pin_mut!(frame);
                    futures::select! {
                        f = frame => Some(f),
                        _ = rx_cancel => None,
                    }
                };
                match next {
                    None => break true,
                    Some(Ok((kind, body))) => match wire::decode_control(kind, &body) {
                        Ok(Control::PublishDone { code, reason }) => {
                            debug!(alias, code, %reason, "publisher ended our subscription");
                            break false;
                        }
                        other => trace!(?other, "subscription stream"),
                    },
                    Some(Err(_)) => break false,
                }
            };
            {
                let mut st = this.state.lock().unwrap();
                st.in_routes.remove(&alias);
                st.cancels.remove(&id);
            }
            if cancelled {
                cancel_streams(send, reader.into_inner());
            }
            _ = tx_ended.send(());
        });
        Ok(Subscription { id, ended })
    }

    /// Ends our subscription `id`: its route closes and the relay is told to
    /// stop (its stream cancelled). Nothing to do on a dead session.
    pub fn unsubscribe(&self, id: u64) {
        let cancel = self.state.lock().unwrap().cancels.remove(&id);
        if let Some(cancel) = cancel {
            _ = cancel.send(());
        }
    }

    /// Ends a track we publish: PUBLISH_DONE on each relay subscription to
    /// it, which the relay passes on to the subscribers, and forgets it.
    pub fn end_track(&self, ns: &[String], track: &str, reason: &'static str) {
        let removed = self.state.lock().unwrap().out_tracks.remove(&track_key(ns, track));
        if let Some(track) = removed {
            track.end_all(reason);
        }
    }

    /// SUBSCRIBE_NAMESPACE on its own stream: events for every namespace
    /// under `prefix`, as full namespaces. The channel closes when the relay
    /// ends the stream (every namespace is then gone).
    pub async fn subscribe_namespace(
        self: &Arc<Self>,
        prefix: Namespace,
    ) -> Result<mpsc::UnboundedReceiver<NamespaceEvent>, MoqError> {
        let id = self.request_id();
        let (send, mut reader, open) =
            match self.request("SUBSCRIBE_NAMESPACE", wire::subscribe_namespace(id, &prefix)).await? {
                (Control::RequestOk, send, reader, open) => (send, reader, open),
                (Control::RequestError { code, reason }, ..) => {
                    return Err(MoqError(format!("SUBSCRIBE_NAMESPACE refused: {code} {reason}")));
                }
                (other, ..) => return Err(MoqError(format!("SUBSCRIBE_NAMESPACE answered with {other:?}"))),
            };
        let (tx, rx) = mpsc::unbounded();
        WebTransportRuntime::spawn(async move {
            // keep our half open: finishing it would end the subscription
            let (_open, _send) = (open, send);
            loop {
                let (kind, body) = match reader.frame().await {
                    Ok(f) => f,
                    Err(e) => {
                        debug!("SUBSCRIBE_NAMESPACE stream ended: {e}");
                        return;
                    }
                };
                let event = match wire::decode_control(kind, &body) {
                    Ok(Control::Namespace(suffix)) => {
                        NamespaceEvent::Added(prefix.iter().cloned().chain(suffix).collect())
                    }
                    Ok(Control::NamespaceDone(suffix)) => {
                        NamespaceEvent::Removed(prefix.iter().cloned().chain(suffix).collect())
                    }
                    other => {
                        debug!("SUBSCRIBE_NAMESPACE stream: ignoring {other:?}");
                        continue;
                    }
                };
                if tx.unbounded_send(event).is_err() {
                    return;
                }
            }
        });
        Ok(rx)
    }

    /// The relay's control stream: SETUP first (within [`SETUP_TIMEOUT`]),
    /// then only GOAWAY matters.
    async fn control_loop(self: Arc<Self>) -> MoqError {
        let first = timeout(SETUP_TIMEOUT, "the relay's SETUP", async {
            #[cfg(not(target_family = "wasm"))]
            let recv = self.conn.accept_uni().await.map_err(|e| err("accept control stream", e))?;
            #[cfg(target_family = "wasm")]
            let recv = crate::web_streams::accept_uni(&self.conn)
                .await
                .map_err(|e| err("accept control stream", e))?;
            let mut reader = FrameReader::new(recv);
            let (kind, _options) = reader.frame().await?;
            if kind != wire::msg::SETUP {
                return Err(MoqError(format!("expected SETUP, got {kind:#x}")));
            }
            Ok(reader)
        })
        .await;
        let mut reader = match first {
            Ok(r) => r,
            Err(e) => return e,
        };
        debug!("MoQ setup done");
        loop {
            let (kind, body) = match reader.frame().await {
                Ok(f) => f,
                Err(e) => return err("relay control stream", e),
            };
            match wire::decode_control(kind, &body) {
                Ok(Control::GoAway { uri, timeout_ms }) => {
                    warn!(%uri, timeout_ms, "relay sent GOAWAY; ending the session");
                    return MoqError::new(GOAWAY);
                }
                Ok(other) => debug!(?other, "relay control message ignored"),
                Err(e) => return err(&format!("control message {kind:#x}"), e),
            }
        }
    }

    /// Requests the relay opens towards us: SUBSCRIBEs to our tracks.
    async fn accept_loop(self: Arc<Self>) -> MoqError {
        #[cfg(target_family = "wasm")]
        let acceptor = crate::web_streams::BiAcceptor::new(&self.conn);
        loop {
            #[cfg(not(target_family = "wasm"))]
            let accepted = self.conn.accept_bi().await.map_err(|e| err("accept request stream", e));
            #[cfg(target_family = "wasm")]
            let accepted = acceptor.accept().await.map_err(|e| err("accept request stream", e));
            let (send, recv) = match accepted {
                Ok(s) => s,
                Err(e) => return e,
            };
            WebTransportRuntime::spawn(self.clone().on_request(send, recv));
        }
    }

    async fn on_request(self: Arc<Self>, mut send: SendStream, recv: RecvStream) {
        let mut reader = FrameReader::new(recv);
        let first = timeout(INBOUND_REQUEST_TIMEOUT, "relay request", reader.frame()).await;
        let (kind, body) = match first {
            Ok(f) => f,
            Err(e) => {
                debug!("relay request stream: {e}");
                return;
            }
        };
        let (code, reason) = match wire::decode_control(kind, &body) {
            Ok(Control::Subscribe { id: _, ns, track }) => {
                self.on_subscribe(send, reader, &ns, &track).await;
                return;
            }
            Ok(Control::Publish { .. }) => (wire::error::UNINTERESTED, "not subscribed"),
            Ok(other) => {
                debug!(?other, "relay request not supported");
                (wire::error::NOT_SUPPORTED, "not supported")
            }
            Err(e) => {
                self.close(err(&format!("relay request {kind:#x}"), e));
                return;
            }
        };
        if write_all(&mut send, &wire::request_error(code, reason)).await.is_ok() {
            finish_streams(send, reader.into_inner());
        }
    }

    /// One relay subscription to `ns`/`track`: refused unless the namespace
    /// is ours; held until we end the track (PUBLISH_DONE + FIN) or the relay
    /// cancels it (its half ends, or it stops ours).
    async fn on_subscribe(&self, mut send: SendStream, mut reader: FrameReader, ns: &[String], track: &str) {
        let taken = {
            let mut st = self.state.lock().unwrap();
            st.drop_stale_provisional();
            let ours = st.announced.iter().any(|a| a.as_slice() == ns);
            let key = track_key(ns, track);
            let found = match st.out_tracks.get(&key) {
                _ if !ours => None,
                Some(track) => Some(track.clone()),
                None if !self.named_only.load(Ordering::Relaxed) && st.provisional_count() < MAX_PROVISIONAL => {
                    let track = Arc::new(OutTrack::new(true));
                    st.out_tracks.insert(key, track.clone());
                    Some(track)
                }
                None => None,
            };
            found.map(|track| {
                let alias = st.next_alias;
                st.next_alias += 1;
                let key = st.next_sub_key;
                st.next_sub_key += 1;
                (track, alias, key)
            })
        };
        let Some((out, alias, key)) = taken else {
            if write_all(&mut send, &wire::request_error(wire::error::DOES_NOT_EXIST, "not published"))
                .await
                .is_ok()
            {
                finish_streams(send, reader.into_inner());
            }
            return;
        };
        // SUBSCRIBE_OK first, then the track goes live: no datagram may
        // carry an alias the relay hasn't been given
        if write_all(&mut send, &wire::subscribe_ok(alias)).await.is_err() {
            return;
        }
        let (tx_end, rx_end) = oneshot::channel();
        out.add_sub(key, alias, tx_end);
        debug!(track = %track_key(ns, track), alias, "relay subscribed");
        let ended_by_us = {
            let relay_done = async {
                // REQUEST_UPDATE and the like change nothing for datagrams
                while reader.frame().await.is_ok() {}
            }
            .fuse();
            let stopped = stopped(&mut send).fuse();
            let rx_end = rx_end.fuse();
            futures::pin_mut!(relay_done, stopped, rx_end);
            futures::select! {
                () = relay_done => None,
                () = stopped => None,
                r = rx_end => r.ok(),
            }
        };
        out.remove_sub(key);
        match ended_by_us {
            Some(reason) => {
                if write_all(&mut send, &wire::publish_done(wire::PUBLISH_DONE_TRACK_ENDED, reason))
                    .await
                    .is_ok()
                {
                    finish_streams(send, reader.into_inner());
                }
            }
            None => {
                debug!(alias, "relay ended its subscription");
                cancel_streams(send, reader.into_inner());
            }
        }
    }

    async fn datagram_loop(self: Arc<Self>) -> MoqError {
        let (mut routed, mut unrouted) = (0u64, 0u64);
        let mut logged_at = Instant::now();
        loop {
            if logged_at.elapsed().as_secs() >= 5 {
                debug!("MoQ datagrams in 5 s: {routed} routed, {unrouted} for no live subscription");
                (routed, unrouted) = (0, 0);
                logged_at = Instant::now();
            }
            let dg = match self.conn.receive_datagram().await {
                Ok(d) => d,
                Err(e) => return err("receive datagram", e),
            };
            let now = Instant::now();
            let buf: &[u8] = dg.as_ref();
            let (alias, payload) = match wire::parse_datagram(buf) {
                Ok(Some(d)) => d,
                Ok(None) => continue,
                Err(e) => return err("datagram", e),
            };
            let st = self.state.lock().unwrap();
            if let Some(route) = st.in_routes.get(&alias) {
                routed += 1;
                _ = route.unbounded_send(RecvPacket {
                    recv_at: now,
                    payload: Bytes::copy_from_slice(payload),
                });
            } else {
                unrouted += 1;
            }
        }
    }

    /// Request streams of ours still open (SUBSCRIBEs, announces, ...).
    pub fn open_requests(&self) -> usize {
        self.open_requests.load(Ordering::Acquire)
    }

    /// Room for `n` more open requests (under [`MAX_OPEN_REQUESTS`],
    /// counting other reservations), held until the [`Reservation`] drops:
    /// by then the caller's requests are open and count for themselves.
    /// `None` when the session is full or closed.
    pub fn reserve(self: &Arc<Self>, n: usize) -> Option<Reservation> {
        if self.is_closed() {
            return None;
        }
        let mut reserved = self.reserved.load(Ordering::Acquire);
        loop {
            if self.open_requests() + reserved + n > MAX_OPEN_REQUESTS {
                return None;
            }
            match self
                .reserved
                .compare_exchange_weak(reserved, reserved + n, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(now) => reserved = now,
            }
        }
        Some(Reservation { conn: self.clone(), n })
    }

    /// Room reserved and not yet taken up by open requests.
    pub fn reserved(&self) -> usize {
        self.reserved.load(Ordering::Acquire)
    }

    /// Our subscriptions on this session.
    pub fn live_subscriptions(&self) -> usize {
        self.state.lock().unwrap().cancels.len()
    }

    /// Relay-asked tracks held before anyone named them (probes and tests).
    pub fn provisional_tracks(&self) -> usize {
        self.state.lock().unwrap().provisional_count()
    }

    /// Names (`<namespace>/<track>` keys) of the tracks we publish or hold for
    /// the relay, provisional ones included: what the relay has asked this
    /// session for (probes and tests).
    pub fn out_track_names(&self) -> Vec<String> {
        self.state.lock().unwrap().out_tracks.keys().cloned().collect()
    }

    /// Tracks we publish or hold for the relay (probes and tests).
    pub fn out_track_count(&self) -> usize {
        self.state.lock().unwrap().out_tracks.len()
    }

    /// Whether the session has ended.
    pub fn is_closed(&self) -> bool {
        self.closed_tx.lock().unwrap().is_none()
    }

    /// Closes the underlying connection.
    pub fn shutdown(&self) {
        self.close(MoqError::new("closed by us"));
    }

    /// Largest aeronet packet that fits in a datagram after the MoQ header.
    pub fn max_payload(&self) -> Option<usize> {
        self.conn
            .max_datagram_size()
            .map(|n| n.saturating_sub(wire::DATAGRAM_OVERHEAD))
    }
}

/// Room on a session for requests about to open ([`MoqConn::reserve`]).
#[derive(Debug)]
pub struct Reservation {
    conn: Arc<MoqConn>,
    n: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.conn.reserved.fetch_sub(self.n, Ordering::AcqRel);
    }
}

async fn write_all(stream: &mut SendStream, mut buf: &[u8]) -> Result<(), MoqError> {
    while !buf.is_empty() {
        let n = stream.write(buf).await.map_err(|e| err("write stream", e))?;
        buf = &buf[n.get()..];
    }
    Ok(())
}

/// Reads `type | u16 length | body` messages off a stream.
pub struct FrameReader {
    stream: RecvStream,
    buf: Vec<u8>,
}

impl FrameReader {
    fn new(stream: RecvStream) -> Self {
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    fn into_inner(self) -> RecvStream {
        self.stream
    }

    async fn fill(&mut self, n: usize) -> Result<(), MoqError> {
        let mut chunk = [0u8; 4096];
        while self.buf.len() < n {
            let got = self
                .stream
                .read(&mut chunk)
                .await
                .map_err(|e| err("read stream", e))?;
            self.buf.extend_from_slice(&chunk[..got.get()]);
        }
        Ok(())
    }

    async fn frame(&mut self) -> Result<(u64, Vec<u8>), MoqError> {
        // type varint: its first byte gives its length
        self.fill(1).await?;
        let vlen = wire::varint_len(self.buf[0]);
        self.fill(vlen + 2).await?;
        let kind = wire::Cursor::new(&self.buf[..vlen])
            .varint()
            .map_err(|e| err("message type", e))?;
        let len = usize::from(u16::from_be_bytes([self.buf[vlen], self.buf[vlen + 1]]));
        self.fill(vlen + 2 + len).await?;
        let body = self.buf[vlen + 2..vlen + 2 + len].to_vec();
        self.buf.drain(..vlen + 2 + len);
        Ok((kind, body))
    }
}
