//! One MoQ session over one WebTransport connection to a relay: the control
//! stream, request ids, the tracks we publish (answering the relay's
//! SUBSCRIBEs), the tracks we subscribe to (routing their datagrams), and
//! SUBSCRIBE_NAMESPACE event streams.
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
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
        time::Duration,
    },
    futures::{
        FutureExt,
        channel::{mpsc, oneshot},
        lock::Mutex as AsyncMutex,
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

type SendStream = <Connection as xwt_core::session::stream::SendSpec>::SendStream;
type RecvStream = <Connection as xwt_core::session::stream::RecvSpec>::RecvStream;

/// Something went wrong on the MoQ session; the connection is unusable.
#[derive(Debug, Clone, derive_more::Display, derive_more::Error)]
#[display("{_0}")]
pub struct MoqError(#[error(not(source))] pub String);

impl MoqError {
    pub(crate) fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

fn err(context: &str, e: impl core::fmt::Display) -> MoqError {
    MoqError(format!("{context}: {e}"))
}

/// A track we publish. Packets are dropped until the relay subscribes to it.
#[derive(Debug)]
pub struct OutTrack {
    /// `u64::MAX` = nobody subscribed.
    alias: AtomicU64,
    request_id: AtomicU64,
    group: AtomicU64,
    /// Asked for by the relay before we named it ([`MoqConn::out_track`]): a
    /// join's SUBSCRIBE can beat the server seeing the client, but so can a
    /// SUBSCRIBE for a made-up name from anyone holding the relay's token.
    /// Held for [`PROVISIONAL_LIFE`] at most, [`MAX_PROVISIONAL`] at once.
    provisional_since: Mutex<Option<Instant>>,
}

impl OutTrack {
    fn new(provisional: bool) -> Self {
        Self {
            alias: AtomicU64::new(u64::MAX),
            request_id: AtomicU64::new(u64::MAX),
            group: AtomicU64::new(0),
            provisional_since: Mutex::new(provisional.then(Instant::now)),
        }
    }

    fn provisional_since(&self) -> Option<Instant> {
        *self.provisional_since.lock().unwrap()
    }

    /// The datagram for `payload`, or `None` while the relay hasn't subscribed.
    pub fn datagram(&self, payload: &[u8]) -> Option<Bytes> {
        let alias = self.alias.load(Ordering::Acquire);
        if alias == u64::MAX {
            return None;
        }
        let group = self.group.fetch_add(1, Ordering::Relaxed);
        Some(wire::datagram(alias, group, payload))
    }

    pub fn is_subscribed(&self) -> bool {
        self.alias.load(Ordering::Acquire) != u64::MAX
    }
}

/// A live SUBSCRIBE of ours.
#[derive(Debug)]
pub struct Subscription {
    /// Request id, for [`MoqConn::unsubscribe`].
    pub id: u64,
    /// Resolves (or is cancelled) when the publisher ends the track or the
    /// session dies.
    pub ended: oneshot::Receiver<()>,
}

/// Namespace appearing or going away under a SUBSCRIBE_NAMESPACE prefix.
#[derive(Debug, Clone)]
pub enum NamespaceEvent {
    Added(Namespace),
    Removed(Namespace),
}

enum Reply {
    Ok,
    SubscribeOk(u64),
}

/// Where a SUBSCRIBE of ours sends its datagrams and its end, registered by
/// the control loop the moment SUBSCRIBE_OK arrives: a PUBLISH_DONE or a
/// datagram right behind it must not find nothing there.
struct SubRoute {
    route: mpsc::UnboundedSender<RecvPacket>,
    ended: oneshot::Sender<()>,
}

#[derive(Default)]
struct State {
    next_request_id: u64,
    peer_max_request_id: u64,
    request_id_waiters: Vec<oneshot::Sender<()>>,
    pending: HashMap<u64, oneshot::Sender<Result<Reply, MoqError>>>,
    /// SUBSCRIBEs of ours waiting for SUBSCRIBE_OK, by request id.
    sub_routes: HashMap<u64, SubRoute>,
    /// The relay may make requests with ids below this (we raise it).
    our_max_request_id: u64,
    /// Namespaces we announced: the relay may SUBSCRIBE to any track in them.
    announced: Vec<Namespace>,
    /// Keyed by `ns/track`.
    out_tracks: HashMap<String, Arc<OutTrack>>,
    next_alias: u64,
    /// Incoming datagrams by track alias.
    in_routes: HashMap<u64, mpsc::UnboundedSender<RecvPacket>>,
    /// Our SUBSCRIBE request id -> alias, to drop routes on PUBLISH_DONE.
    in_by_request: HashMap<u64, u64>,
    /// Our SUBSCRIBE request id -> fires (or drops) when the publisher is done.
    ended: HashMap<u64, oneshot::Sender<()>>,
}

impl State {
    fn provisional_count(&self) -> usize {
        self.out_tracks
            .values()
            .filter(|track| track.provisional_since().is_some())
            .count()
    }

    /// Forgets relay-asked tracks nobody named within [`PROVISIONAL_LIFE`];
    /// returns the relay subscriptions to end (PUBLISH_DONE).
    fn drop_stale_provisional(&mut self) -> Vec<u64> {
        let mut ended = Vec::new();
        self.out_tracks.retain(|_, track| {
            let stale = track
                .provisional_since()
                .is_some_and(|since| since.elapsed() >= PROVISIONAL_LIFE);
            if stale {
                let id = track.request_id.swap(u64::MAX, Ordering::AcqRel);
                track.alias.store(u64::MAX, Ordering::Release);
                if id != u64::MAX {
                    ended.push(id);
                }
            }
            !stale
        });
        ended
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
    control: AsyncMutex<SendStream>,
    state: Mutex<State>,
    /// Fires once when the session dies.
    closed_tx: Mutex<Option<oneshot::Sender<MoqError>>>,
    /// The connection has been closed (once, whoever ends the session).
    shut: AtomicBool,
}

/// Closes the session when dropped: every way out of a task that owns one
/// (an error, a timeout, the frontend going away) ends it, so no relay
/// session lives on with our announce in it. The background loops hold their
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

/// How many requests we let the relay make towards us at first, and how far
/// we raise it each time it has used half (MAX_REQUEST_ID): every relay
/// SUBSCRIBE spends one, and a spent budget would stop new joins on a session
/// that otherwise looks healthy.
const OUR_MAX_REQUEST_ID: u64 = 1 << 20;
/// Relay-asked tracks we hold before naming them (see [`OutTrack`]).
pub const MAX_PROVISIONAL: usize = 256;
/// And for how long: a real join names its tracks within a round trip or two.
pub const PROVISIONAL_LIFE: Duration = Duration::from_secs(20);
/// A request (announce, SUBSCRIBE, ...) the relay hasn't answered by then
/// fails, so no task waits forever on a relay that went quiet.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// How long to wait for the relay to raise our request budget once it is
/// spent. Cloudflare's never does, so this only has to cover a relay that
/// raises it promptly; then the request fails with "out of request ids".
const REQUEST_ID_WAIT: Duration = Duration::from_secs(1);
/// MoQ setup (control stream + CLIENT_SETUP/SERVER_SETUP) must finish by then.
const SETUP_TIMEOUT: Duration = Duration::from_secs(10);

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

impl MoqConn {
    /// Runs MoQ setup on `conn` and starts the background loops. The receiver
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
        let (send, recv) = conn
            .open_bi()
            .await
            .map_err(|e| err("open control stream", e))?
            .wait_bi()
            .await
            .map_err(|e| err("open control stream", e))?;
        let mut send = send;
        write_all(&mut send, &wire::client_setup(OUR_MAX_REQUEST_ID)).await?;
        let mut reader = FrameReader::new(recv);
        let (kind, body) = reader.frame().await?;
        if kind != wire::msg::SERVER_SETUP {
            return Err(MoqError(format!("expected SERVER_SETUP, got {kind:#x}")));
        }
        let peer_max = wire::server_setup(&body).map_err(|e| err("SERVER_SETUP", e))?;
        debug!(peer_max_request_id = peer_max, "MoQ setup done");

        let (closed_tx, closed_rx) = oneshot::channel();
        let this = Arc::new(Self {
            conn,
            control: AsyncMutex::new(send),
            state: Mutex::new(State {
                peer_max_request_id: peer_max,
                our_max_request_id: OUR_MAX_REQUEST_ID,
                next_alias: 1,
                ..Default::default()
            }),
            closed_tx: Mutex::new(Some(closed_tx)),
            shut: AtomicBool::new(false),
        });

        WebTransportRuntime::spawn({
            let this = this.clone();
            async move {
                let e = this.clone().control_loop(reader).await;
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
    /// control loop error): the connection is closed too, so a session whose
    /// control stream broke can't live on at the relay with our announces.
    fn close(&self, reason: MoqError) {
        if !self.shut.swap(true, Ordering::AcqRel) {
            close_connection(&self.conn);
        }
        debug!("MoQ session closed: {reason}");
        if let Some(tx) = self.closed_tx.lock().unwrap().take() {
            _ = tx.send(reason);
        }
        // fail everything still waiting
        let mut st = self.state.lock().unwrap();
        st.pending.clear();
        st.sub_routes.clear();
        st.request_id_waiters.clear();
        st.in_routes.clear();
        st.ended.clear();
    }

    async fn write_control(&self, bytes: &[u8]) -> Result<(), MoqError> {
        write_all(&mut *self.control.lock().await, bytes).await
    }

    async fn request_id(&self) -> Result<u64, MoqError> {
        timeout(REQUEST_ID_WAIT, "out of request ids", self.request_id_inner()).await
    }

    async fn request_id_inner(&self) -> Result<u64, MoqError> {
        loop {
            let rx = {
                let mut st = self.state.lock().unwrap();
                let id = st.next_request_id;
                if id < st.peer_max_request_id {
                    st.next_request_id += 2; // we dialled: even ids
                    return Ok(id);
                }
                let (tx, rx) = oneshot::channel();
                st.request_id_waiters.push(tx);
                rx
            };
            trace!("waiting for MAX_REQUEST_ID");
            rx.await.map_err(|_| MoqError::new("session closed"))?;
        }
    }

    async fn request(&self, id: u64, bytes: Bytes) -> Result<Reply, MoqError> {
        let (tx, rx) = oneshot::channel();
        self.state.lock().unwrap().pending.insert(id, tx);
        let result = timeout(REQUEST_TIMEOUT, "request", async {
            self.write_control(&bytes).await?;
            rx.await.map_err(|_| MoqError::new("session closed"))?
        })
        .await;
        self.state.lock().unwrap().pending.remove(&id);
        result
    }

    /// Announces `ns`. The relay will then SUBSCRIBE to tracks in it as
    /// subscribers ask for them; get their handles with [`Self::out_track`].
    pub async fn announce(&self, ns: Namespace) -> Result<(), MoqError> {
        self.state.lock().unwrap().announced.push(ns.clone());
        let id = self.request_id().await?;
        match self.request(id, wire::publish_namespace(id, &ns)).await? {
            Reply::Ok => Ok(()),
            Reply::SubscribeOk(_) => Err(MoqError::new("unexpected SUBSCRIBE_OK")),
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

    /// Subscribes to `ns`/`track` from the newest object on; its datagrams go to `route`.
    pub async fn subscribe(
        &self,
        ns: &[String],
        track: &str,
        route: mpsc::UnboundedSender<RecvPacket>,
    ) -> Result<Subscription, MoqError> {
        let id = self.request_id().await?;
        let (tx_ended, ended) = oneshot::channel();
        self.state
            .lock()
            .unwrap()
            .sub_routes
            .insert(id, SubRoute { route, ended: tx_ended });
        let reply = self.request(id, wire::subscribe(id, ns, track)).await;
        let mut st = self.state.lock().unwrap();
        st.sub_routes.remove(&id);
        match reply {
            // the control loop routed it already
            Ok(Reply::SubscribeOk(_)) => Ok(Subscription { id, ended }),
            Ok(Reply::Ok) => Err(MoqError::new("unexpected REQUEST_OK to SUBSCRIBE")),
            Err(e) => {
                // answered after we gave up: no route for a subscription
                // nobody holds (the relay keeps it until the session ends)
                if let Some(alias) = st.in_by_request.remove(&id) {
                    st.in_routes.remove(&alias);
                }
                st.ended.remove(&id);
                Err(e)
            }
        }
    }

    /// Ends our subscription `id`: its route closes and the relay is told to
    /// stop forwarding. Best effort: a dead session has nothing to stop.
    pub async fn unsubscribe(&self, id: u64) {
        {
            let mut st = self.state.lock().unwrap();
            if let Some(alias) = st.in_by_request.remove(&id) {
                st.in_routes.remove(&alias);
            }
            st.ended.remove(&id);
        }
        _ = self.write_control(&wire::unsubscribe(id)).await;
    }

    /// Ends a track we publish: tells the relay (PUBLISH_DONE on its
    /// subscription, if it has one), which passes it on to the subscribers,
    /// and forgets the track. Best effort.
    pub async fn end_track(&self, ns: &[String], track: &str, reason: &str) {
        let removed = self.state.lock().unwrap().out_tracks.remove(&track_key(ns, track));
        let Some(track) = removed else { return };
        let request_id = track.request_id.load(Ordering::Relaxed);
        track.alias.store(u64::MAX, Ordering::Release);
        if request_id != u64::MAX {
            _ = self
                .write_control(&wire::publish_done(request_id, wire::PUBLISH_DONE_TRACK_ENDED, reason))
                .await;
        }
    }

    /// SUBSCRIBE_NAMESPACE on its own stream (draft-16): events for every
    /// namespace under `prefix`, as full namespaces.
    pub async fn subscribe_namespace(
        &self,
        prefix: Namespace,
    ) -> Result<mpsc::UnboundedReceiver<NamespaceEvent>, MoqError> {
        let id = self.request_id().await?;
        let (mut send, recv) = self
            .conn
            .open_bi()
            .await
            .map_err(|e| err("open SUBSCRIBE_NAMESPACE stream", e))?
            .wait_bi()
            .await
            .map_err(|e| err("open SUBSCRIBE_NAMESPACE stream", e))?;
        write_all(&mut send, &wire::subscribe_namespace(id, &prefix)).await?;
        let mut reader = FrameReader::new(recv);
        let (kind, body) = reader.frame().await?;
        match wire::decode_control(kind, &body).map_err(|e| err("SUBSCRIBE_NAMESPACE reply", e))? {
            Control::RequestOk { .. } => {}
            Control::RequestError { code, reason, .. } => {
                return Err(MoqError(format!("SUBSCRIBE_NAMESPACE refused: {code} {reason}")));
            }
            other => return Err(MoqError(format!("SUBSCRIBE_NAMESPACE reply: {other:?}"))),
        }
        let (tx, rx) = mpsc::unbounded();
        WebTransportRuntime::spawn(async move {
            // keep the send half open: finishing it would end the subscription
            let _send = send;
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

    async fn control_loop(self: Arc<Self>, mut reader: FrameReader) -> MoqError {
        loop {
            let (kind, body) = match reader.frame().await {
                Ok(f) => f,
                Err(e) => return e,
            };
            let msg = match wire::decode_control(kind, &body) {
                Ok(m) => m,
                Err(e) => return err(&format!("control message {kind:#x}"), e),
            };
            trace!(?msg, "control");
            match msg {
                Control::SubscribeOk { id, alias } => {
                    {
                        let mut st = self.state.lock().unwrap();
                        if let Some(sub) = st.sub_routes.remove(&id) {
                            st.in_routes.insert(alias, sub.route);
                            st.in_by_request.insert(id, alias);
                            st.ended.insert(id, sub.ended);
                        }
                    }
                    self.reply(id, Ok(Reply::SubscribeOk(alias)));
                }
                Control::RequestOk { id } => self.reply(id, Ok(Reply::Ok)),
                Control::RequestError { id, code, reason } => {
                    self.reply(id, Err(MoqError(format!("request refused: {code} {reason}"))));
                }
                Control::Subscribe { id, ns, track } => {
                    if let Err(e) = self.raise_request_budget(id).await {
                        return e;
                    }
                    if let Err(e) = self.on_subscribe(id, &ns, &track).await {
                        return e;
                    }
                }
                Control::Unsubscribe { id } => {
                    let st = self.state.lock().unwrap();
                    for t in st.out_tracks.values() {
                        // only the subscription it names: no PUBLISH_DONE
                        // later for one the relay already ended
                        if t
                            .request_id
                            .compare_exchange(id, u64::MAX, Ordering::AcqRel, Ordering::Relaxed)
                            .is_ok()
                        {
                            t.alias.store(u64::MAX, Ordering::Release);
                        }
                    }
                }
                Control::PublishDone { id } => {
                    let mut st = self.state.lock().unwrap();
                    if let Some(alias) = st.in_by_request.remove(&id) {
                        st.in_routes.remove(&alias);
                    }
                    if let Some(tx) = st.ended.remove(&id) {
                        _ = tx.send(());
                    }
                }
                Control::MaxRequestId(max) => {
                    let mut st = self.state.lock().unwrap();
                    st.peer_max_request_id = st.peer_max_request_id.max(max);
                    for w in st.request_id_waiters.drain(..) {
                        _ = w.send(());
                    }
                }
                Control::GoAway => warn!("relay sent GOAWAY"),
                Control::Namespace(_) | Control::NamespaceDone(_) | Control::Other(_) => {}
            }
        }
    }

    fn reply(&self, id: u64, reply: Result<Reply, MoqError>) {
        if let Some(tx) = self.state.lock().unwrap().pending.remove(&id) {
            _ = tx.send(reply);
        }
    }

    /// Raises the relay's request budget towards us once it has used half.
    async fn raise_request_budget(&self, used: u64) -> Result<(), MoqError> {
        let raised = {
            let mut st = self.state.lock().unwrap();
            (used + OUR_MAX_REQUEST_ID / 2 >= st.our_max_request_id).then(|| {
                st.our_max_request_id += OUR_MAX_REQUEST_ID;
                st.our_max_request_id
            })
        };
        match raised {
            Some(max) => {
                debug!(max, "raising the relay's MAX_REQUEST_ID");
                self.write_control(&wire::max_request_id(max)).await
            }
            None => Ok(()),
        }
    }

    async fn on_subscribe(&self, id: u64, ns: &[String], track: &str) -> Result<(), MoqError> {
        let (alias, expired) = {
            let mut st = self.state.lock().unwrap();
            let expired = st.drop_stale_provisional();
            let alias = st.announced.iter().any(|a| a.as_slice() == ns).then_some(()).and_then(|()| {
                let key = track_key(ns, track);
                let track = match st.out_tracks.get(&key) {
                    Some(track) => track.clone(),
                    None if st.provisional_count() < MAX_PROVISIONAL => {
                        let track = Arc::new(OutTrack::new(true));
                        st.out_tracks.insert(key, track.clone());
                        track
                    }
                    None => return None,
                };
                let alias = st.next_alias;
                st.next_alias += 1;
                track.request_id.store(id, Ordering::Relaxed);
                track.alias.store(alias, Ordering::Release);
                Some(alias)
            });
            (alias, expired)
        };
        for request_id in expired {
            self.write_control(&wire::publish_done(request_id, wire::PUBLISH_DONE_TRACK_ENDED, "never claimed"))
                .await?;
        }
        match alias {
            Some(alias) => {
                debug!(track = %track_key(ns, track), alias, "relay subscribed");
                self.write_control(&wire::subscribe_ok(id, alias)).await
            }
            None => {
                self.write_control(&wire::request_error(id, wire::ERROR_DOES_NOT_EXIST, "not published"))
                    .await
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
            let Ok(Some((alias, payload))) = wire::parse_datagram(buf) else {
                continue;
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

    /// How many more requests (SUBSCRIBE, announce, ...) this session may make.
    /// Cloudflare's relay grants 100 request ids (50 of ours) at setup and
    /// never raises it, so a long-lived session runs out.
    pub fn requests_left(&self) -> u64 {
        let st = self.state.lock().unwrap();
        st.peer_max_request_id.saturating_sub(st.next_request_id).div_ceil(2)
    }

    /// Our subscriptions on this session, live or still being asked for.
    pub fn live_subscriptions(&self) -> usize {
        let st = self.state.lock().unwrap();
        st.in_by_request.len() + st.sub_routes.len()
    }

    /// Relay-asked tracks held before anyone named them (probes and tests).
    pub fn provisional_tracks(&self) -> usize {
        self.state.lock().unwrap().provisional_count()
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

async fn write_all(stream: &mut SendStream, mut buf: &[u8]) -> Result<(), MoqError> {
    while !buf.is_empty() {
        let n = stream.write(buf).await.map_err(|e| err("write control stream", e))?;
        buf = &buf[n.get()..];
    }
    Ok(())
}

/// Reads `type | u16 length | body` frames off a stream.
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

    async fn fill(&mut self, n: usize) -> Result<(), MoqError> {
        let mut chunk = [0u8; 4096];
        while self.buf.len() < n {
            let got = self
                .stream
                .read(&mut chunk)
                .await
                .map_err(|e| err("read control stream", e))?;
            self.buf.extend_from_slice(&chunk[..got.get()]);
        }
        Ok(())
    }

    async fn frame(&mut self) -> Result<(u64, Vec<u8>), MoqError> {
        // type varint: first byte gives its length
        self.fill(1).await?;
        let vlen = 1usize << (self.buf[0] >> 6);
        self.fill(vlen + 2).await?;
        let kind = wire::Cursor::new(&self.buf[..vlen])
            .varint()
            .map_err(|e| err("frame type", e))?;
        let len = usize::from(u16::from_be_bytes([self.buf[vlen], self.buf[vlen + 1]]));
        self.fill(vlen + 2 + len).await?;
        let body = self.buf[vlen + 2..vlen + 2 + len].to_vec();
        self.buf.drain(..vlen + 2 + len);
        Ok((kind, body))
    }
}
