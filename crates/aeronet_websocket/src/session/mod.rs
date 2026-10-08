//! Implementation for WebSocket sessions, shared between clients and servers.

pub(crate) mod backend;

use {
    crate::WebSocketRuntime,
    aeronet_io::{
        AeronetIoPlugin, IoSystems, Session,
        connection::{DROP_DISCONNECT_REASON, Disconnect},
        packet::{RecvPacket, SendBacklog},
    },
    alloc::sync::Arc,
    bevy_app::prelude::*,
    bevy_ecs::prelude::*,
    bevy_platform::time::Instant,
    bytes::Bytes,
    core::{
        num::Saturating,
        sync::atomic::{AtomicUsize, Ordering},
    },
    derive_more::{Display, Error},
    futures::channel::{mpsc, oneshot},
    std::io,
    tracing::{trace, trace_span},
};

cfg_if::cfg_if! {
    if #[cfg(target_family = "wasm")] {
        type ConnectionError = crate::JsError;
        type SendError = crate::JsError;
    } else {
        use futures::never::Never;

        type ConnectionError = crate::tungstenite::Error;
        type SendError = Never;
    }
}

pub(crate) struct WebSocketSessionPlugin;

impl Plugin for WebSocketSessionPlugin {
    fn build(&self, app: &mut App) {
        if !app.is_plugin_added::<AeronetIoPlugin>() {
            app.add_plugins(AeronetIoPlugin);
        }

        app.init_resource::<WebSocketRuntime>()
            .add_systems(PreUpdate, poll.in_set(IoSystems::Poll))
            .add_systems(PostUpdate, flush.in_set(IoSystems::Flush))
            .add_observer(on_disconnect);
    }
}

/// Manages a WebSocket session's connection.
///
/// This may represent either an outgoing client connection (this session is
/// connecting to a server), or an incoming client connection (this session is
/// a child of a server that the user has spawned).
///
/// You should not add or remove this component directly - it is managed
/// entirely by the client and server implementations.
///
/// Packets flushed from [`Session::send`] wait in a queue until the socket
/// accepts them. The socket only accepts more once its own unsent data is
/// below the configured send buffer limit (see the client and server
/// configurations), and [`SendBacklog`] reports both.
#[derive(Debug, Component)]
#[require(Session::new(Instant::now(), MTU), SendBacklog)]
pub struct WebSocketIo {
    pub(crate) rx_packet_b2f: mpsc::UnboundedReceiver<RecvPacket>,
    pub(crate) tx_packet_f2b: mpsc::UnboundedSender<Bytes>,
    pub(crate) tx_user_dc: Option<oneshot::Sender<String>>,
    pub(crate) send_queue: Arc<SendQueue>,
}

impl WebSocketIo {
    pub(crate) fn new(frontend: SessionFrontend) -> Self {
        Self {
            rx_packet_b2f: frontend.rx_packet_b2f,
            tx_packet_f2b: frontend.tx_packet_f2b,
            tx_user_dc: Some(frontend.tx_user_dc),
            send_queue: frontend.send_queue,
        }
    }
}

/// Packets the frontend has flushed which the backend has not yet handed to
/// the socket, shared between the two.
#[derive(Debug, Default)]
pub(crate) struct SendQueue {
    packets: AtomicUsize,
    bytes: AtomicUsize,
    /// The browser's `bufferedAmount`, as the backend last saw it.
    #[cfg(target_family = "wasm")]
    buffered: AtomicUsize,
    /// The socket, while it is open, to read its unsent bytes from.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    socket: std::sync::Mutex<Option<std::os::fd::RawFd>>,
}

impl SendQueue {
    fn push(&self, len: usize) {
        self.packets.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(len, Ordering::Relaxed);
    }

    /// The backend handed a packet of `len` bytes to the socket.
    pub(crate) fn sent(&self, len: usize) {
        self.packets.fetch_sub(1, Ordering::Relaxed);
        self.bytes.fetch_sub(len, Ordering::Relaxed);
    }

    #[cfg(target_family = "wasm")]
    pub(crate) fn set_buffered(&self, bytes: usize) {
        self.buffered.store(bytes, Ordering::Relaxed);
    }

    /// Lets the frontend read the unsent bytes of `socket` until the returned
    /// guard drops. The guard must drop before the socket closes, so the
    /// frontend never reads a closed (or reused) descriptor.
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn attach_socket(&self, socket: backend::native::SocketInfo) -> AttachedSocket<'_> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            *self
                .socket
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = socket.fd;
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            _ = socket;
        }
        AttachedSocket(self)
    }

    fn socket_bytes(&self) -> Option<usize> {
        cfg_if::cfg_if! {
            if #[cfg(target_family = "wasm")] {
                Some(self.buffered.load(Ordering::Relaxed))
            } else if #[cfg(any(target_os = "linux", target_os = "android"))] {
                let socket = self
                    .socket
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (*socket).and_then(backend::native::unsent_bytes)
            } else {
                None
            }
        }
    }

    fn backlog(&self) -> SendBacklog {
        SendBacklog {
            packets: self.packets.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            socket_bytes: self.socket_bytes(),
        }
    }
}

/// See [`SendQueue::attach_socket`].
#[cfg(not(target_family = "wasm"))]
pub(crate) struct AttachedSocket<'a>(&'a SendQueue);

#[cfg(not(target_family = "wasm"))]
impl Drop for AttachedSocket<'_> {
    fn drop(&mut self) {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            *self
                .0
                .socket
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }
    }
}

/// Packet MTU of [`WebSocketIo`] sessions.
///
/// A WebSocket runs over TCP, and TCP is a byte stream: the kernel cuts it
/// into segments that fit the path MTU on its own. So this is not sized to an
/// IP packet, as it once was: [`IP_MTU`](aeronet_io::packet::IP_MTU) minus
/// the TCP, IPv6 and WebSocket frame headers, 910 bytes. Sized that way, the
/// transport fragmented every message to fit and flushed each packet as its
/// own WebSocket message, which with Nagle off is its own TCP segment: a
/// 20 Hz tick sent ~1.4 segments per tick per peer, each with its own
/// headers, ACK and chance to be lost and head-of-line block the stream.
///
/// At 64 KiB, everything flushed in one tick goes out as one packet, one
/// WebSocket message, and the kernel segments it. The limit only bounds a
/// single packet: buffers are sized by what is written, not by the MTU, and
/// it stays far below tungstenite's default frame (16 MiB) and message
/// (64 MiB) limits and any browser's.
///
/// Both peers must agree on it: a receiver checks the length of each
/// non-last fragment against its own MTU.
///
/// For a WebSocket, the minimum MTU is always the same as the current MTU.
pub const MTU: usize = 64 * 1024;

/// Error that occurs when polling a session using the [`WebSocketIo`] IO
/// layer.
#[derive(Debug, Display, Error)]
#[non_exhaustive]
pub enum SessionError {
    /// Frontend ([`WebSocketIo`]) was dropped.
    #[display("frontend closed")]
    FrontendClosed,
    /// Backend async task was unexpectedly cancelled and dropped.
    #[display("backend closed")]
    BackendClosed,
    /// Failed to read the local socket address of the endpoint.
    #[display("failed to get local socket address")]
    GetLocalAddr(io::Error),
    /// Failed to read the peer socket address of the endpoint.
    #[display("failed to get peer socket address")]
    GetPeerAddr(io::Error),
    /// Receiver stream was unexpectedly closed.
    #[display("receiver stream closed")]
    RecvStreamClosed,
    /// Unexpectedly lost connection from the peer.
    #[display("connection lost")]
    Connection(ConnectionError),
    /// Connection closed with an error code which wasn't `1000`.
    #[display("connection closed with code {_0}")]
    Closed(#[error(not(source))] u16),
    /// The peer sent us a close frame, but it did not include a reason.
    ///
    /// [`WebSocketIo`] will always send a reason when closing a connection.
    #[display("peer disconnected without reason")]
    DisconnectedWithoutReason,
    /// Failed to send data across the socket.
    #[display("failed to send data")]
    Send(SendError),
}

impl Drop for WebSocketIo {
    fn drop(&mut self) {
        if let Some(tx_dc) = self.tx_user_dc.take() {
            _ = tx_dc.send(DROP_DISCONNECT_REASON.to_owned());
        }
    }
}

#[derive(Debug)]
pub(crate) struct SessionFrontend {
    pub rx_packet_b2f: mpsc::UnboundedReceiver<RecvPacket>,
    pub tx_packet_f2b: mpsc::UnboundedSender<Bytes>,
    pub tx_user_dc: oneshot::Sender<String>,
    pub send_queue: Arc<SendQueue>,
}

fn on_disconnect(trigger: On<Disconnect>, mut sessions: Query<&mut WebSocketIo>) {
    let entity = trigger.event_target();
    let Ok(mut io) = sessions.get_mut(entity) else {
        return;
    };

    if let Some(tx_dc) = io.tx_user_dc.take() {
        _ = tx_dc.send(trigger.reason.clone());
    }
}

pub(crate) fn poll(
    mut sessions: Query<(Entity, &mut Session, &mut WebSocketIo, &mut SendBacklog)>,
) {
    for (entity, mut session, mut io, mut backlog) in &mut sessions {
        let span = trace_span!("poll", %entity);
        let _span = span.enter();

        let mut num_packets = Saturating(0);
        let mut num_bytes = Saturating(0);
        while let Ok(packet) = io.rx_packet_b2f.try_recv() {
            num_packets += 1;
            session.stats.packets_recv += 1;

            num_bytes += packet.payload.len();
            session.stats.bytes_recv += packet.payload.len();

            session.recv.push(packet);
        }

        if num_packets.0 > 0 {
            trace!(%num_packets, %num_bytes, "Received packets");
        }

        backlog.set_if_neq(io.send_queue.backlog());
    }
}

fn flush(mut sessions: Query<(Entity, &mut Session, &WebSocketIo)>) {
    for (entity, mut session, io) in &mut sessions {
        let span = trace_span!("flush", %entity);
        let _span = span.enter();

        // explicit deref so we can access disjoint fields
        let session = &mut *session;
        let mut num_packets = Saturating(0);
        let mut num_bytes = Saturating(0);
        for packet in session.send.drain(..) {
            num_packets += 1;
            session.stats.packets_sent += 1;

            num_bytes += packet.len();
            session.stats.bytes_sent += packet.len();

            // handle connection errors in `poll`
            // count it before the backend can see it
            let len = packet.len();
            io.send_queue.push(len);
            if io.tx_packet_f2b.unbounded_send(packet).is_err() {
                io.send_queue.sent(len);
            }
        }

        if num_packets.0 > 0 {
            trace!(%num_packets, %num_bytes, "Flushed packets");
        }
    }
}
