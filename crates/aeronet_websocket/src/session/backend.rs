#[cfg(target_family = "wasm")]
pub mod wasm {
    use {
        crate::{
            JsError,
            session::{SendQueue, SessionError, SessionFrontend},
        },
        aeronet_io::{connection::DisconnectReason, packet::RecvPacket},
        alloc::sync::Arc,
        bevy_platform::time::Instant,
        bytes::Bytes,
        futures::{
            SinkExt, StreamExt,
            channel::{mpsc, oneshot},
            never::Never,
        },
        js_sys::Uint8Array,
        wasm_bindgen::{JsCast, prelude::Closure},
        web_sys::{BinaryType, CloseEvent, Event, MessageEvent, WebSocket},
    };

    #[derive(Debug)]
    pub struct SessionBackend {
        socket: WebSocket,
        rx_user_dc: oneshot::Receiver<String>,
        rx_dc_reason: mpsc::Receiver<DisconnectReason>,
    }

    // https://www.rfc-editor.org/rfc/rfc6455.html#section-7.4.1
    const NORMAL_CLOSE_CODE: u16 = 1000;

    /// How often a packet held back by the send buffer limit checks whether
    /// the browser has drained enough: browsers have no event for it.
    const BUFFER_POLL: core::time::Duration = core::time::Duration::from_millis(5);

    /// `send_buffer_limit`: see the client config.
    pub fn split(
        socket: WebSocket,
        send_buffer_limit: Option<usize>,
    ) -> (SessionFrontend, SessionBackend) {
        socket.set_binary_type(BinaryType::Arraybuffer);
        let send_queue = Arc::new(SendQueue::default());

        let (tx_packet_b2f, rx_packet_b2f) = mpsc::unbounded::<RecvPacket>();
        let (tx_packet_f2b, rx_packet_f2b) = mpsc::unbounded::<Bytes>();
        let (tx_user_dc, rx_user_dc) = oneshot::channel::<String>();

        let (mut tx_dc_reason, rx_dc_reason) = mpsc::channel::<DisconnectReason>(1);

        let (_tx_dropped, rx_dropped) = oneshot::channel::<()>();
        let on_open = Closure::once({
            let socket = socket.clone();
            let mut tx_dc_reason = tx_dc_reason.clone();
            let send_queue = send_queue.clone();
            move || {
                wasm_bindgen_futures::spawn_local(async move {
                    let Err(err) = send_loop(
                        socket,
                        rx_packet_f2b,
                        rx_dropped,
                        &send_queue,
                        send_buffer_limit,
                    )
                    .await;
                    _ = tx_dc_reason.send(err.into());
                });
            }
        });

        let on_message = Closure::<dyn FnMut(_)>::new(move |event: MessageEvent| {
            let data = event.data();
            let packet = data
                .as_string()
                .map_or_else(|| Uint8Array::new(&data).to_vec(), String::into_bytes);
            let packet = Bytes::from(packet);
            let now = Instant::now();

            let mut tx_packet_b2f = tx_packet_b2f.clone();
            wasm_bindgen_futures::spawn_local(async move {
                _ = tx_packet_b2f
                    .send(RecvPacket {
                        recv_at: now,
                        payload: packet,
                    })
                    .await;
            });
        });

        let on_close = {
            let mut tx_dc_reason = tx_dc_reason.clone();
            Closure::<dyn FnMut(_)>::new(move |event: CloseEvent| {
                let dc_reason = if event.code() == NORMAL_CLOSE_CODE {
                    DisconnectReason::by_peer(event.reason())
                } else {
                    // TODO friendly error messages
                    // https://www.rfc-editor.org/rfc/rfc6455.html#section-7.4.1
                    DisconnectReason::by_error(SessionError::Closed(event.code()))
                };
                _ = tx_dc_reason.try_send(dc_reason);
            })
        };

        let on_error = Closure::<dyn FnMut(_)>::new(move |event: Event| {
            let err = SessionError::Connection(JsError(event.to_string().into()));
            _ = tx_dc_reason.try_send(DisconnectReason::by_error(err));
        });

        socket.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        on_open.forget();

        socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        on_message.forget();

        socket.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        on_close.forget();

        socket.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        on_error.forget();

        (
            SessionFrontend {
                rx_packet_b2f,
                tx_packet_f2b,
                tx_user_dc,
                send_queue,
            },
            SessionBackend {
                socket,
                rx_user_dc,
                rx_dc_reason,
            },
        )
    }

    async fn send_loop(
        socket: WebSocket,
        mut rx_packet_f2b: mpsc::UnboundedReceiver<Bytes>,
        mut rx_dropped: oneshot::Receiver<()>,
        send_queue: &SendQueue,
        send_buffer_limit: Option<usize>,
    ) -> Result<Never, SessionError> {
        loop {
            let packet = futures::select! {
                x = rx_packet_f2b.next() => x,
                _ = rx_dropped => None,
            }
            .ok_or(SessionError::FrontendClosed)?;

            // `send` never blocks: the browser queues without bound. Hold the
            // packet in our queue instead, where the app can see it, until the
            // browser's own queue has drained below the limit.
            if let Some(limit) = send_buffer_limit {
                loop {
                    let buffered = buffered_amount(&socket);
                    send_queue.set_buffered(buffered);
                    if buffered < limit {
                        break;
                    }
                    crate::WebSocketRuntime::sleep(BUFFER_POLL).await;
                }
            }

            socket
                .send_with_u8_array(&packet)
                .map_err(JsError::from)
                .map_err(SessionError::Send)?;
            send_queue.sent(packet.len());
            send_queue.set_buffered(buffered_amount(&socket));
        }
    }

    fn buffered_amount(socket: &WebSocket) -> usize {
        usize::try_from(socket.buffered_amount()).unwrap_or(usize::MAX)
    }


    impl SessionBackend {
        pub async fn start(self) -> Result<Never, DisconnectReason> {
            let Self {
                socket,
                mut rx_user_dc,
                mut rx_dc_reason,
            } = self;

            futures::select! {
                dc_reason = rx_dc_reason.next() => {
                    let dc_reason = dc_reason.ok_or(SessionError::BackendClosed)?;
                    Err(dc_reason)
                }
                reason = rx_user_dc => {
                    let reason = reason.map_err(|_| SessionError::FrontendClosed)?;
                    _ = socket.close_with_code_and_reason(NORMAL_CLOSE_CODE, &reason);
                    Err(DisconnectReason::by_user(reason))
                }
            }
        }
    }
}

#[cfg(not(target_family = "wasm"))]
pub mod native {
    use {
        crate::session::{SendQueue, SessionError, SessionFrontend},
        aeronet_io::{connection::DisconnectReason, packet::RecvPacket},
        alloc::sync::Arc,
        bevy_platform::time::Instant,
        bytes::Bytes,
        futures::{
            FutureExt, SinkExt, StreamExt,
            channel::{mpsc, oneshot},
            never::Never,
            pin_mut,
            stream::{SplitSink, SplitStream},
        },
        tokio::io::{AsyncRead, AsyncWrite},
        tokio_tungstenite::{
            WebSocketStream,
            tungstenite::{
                Message, Utf8Bytes,
                protocol::{CloseFrame, frame::coding::CloseCode},
            },
        },
    };

    /// What the backend knows about its TCP socket, from [`configure_socket`].
    #[derive(Debug, Clone, Copy, Default)]
    pub struct SocketInfo {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        pub(crate) fd: Option<std::os::fd::RawFd>,
    }

    /// Applies the send buffer limit to a connected TCP socket.
    ///
    /// The limit is `TCP_NOTSENT_LOWAT`: the kernel takes new data only while
    /// less than this many bytes of what it already holds are unsent, so the
    /// rest of a backlog waits in the session's queue where the app can see
    /// it. Unlike a small `SO_SNDBUF`, this doesn't limit the data in flight.
    /// Linux only; elsewhere the limit is ignored.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn configure_socket(
        socket: &impl std::os::fd::AsRawFd,
        send_buffer_limit: Option<usize>,
    ) -> SocketInfo {
        let fd = socket.as_raw_fd();
        if let Some(limit) = send_buffer_limit {
            let value = libc::c_int::try_from(limit).unwrap_or(libc::c_int::MAX);
            #[expect(
                clippy::cast_possible_truncation,
                reason = "size of a `c_int` fits in a `socklen_t`"
            )]
            let len = size_of::<libc::c_int>() as libc::socklen_t;
            // SAFETY: `fd` is an open socket for the duration of this call
            // and `value` outlives it.
            let result = unsafe {
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NOTSENT_LOWAT,
                    (&raw const value).cast(),
                    len,
                )
            };
            if result != 0 {
                tracing::debug!(
                    "Failed to set TCP_NOTSENT_LOWAT: {}",
                    std::io::Error::last_os_error()
                );
            }
        }
        SocketInfo { fd: Some(fd) }
    }

    /// Applies the send buffer limit to a connected TCP socket.
    ///
    /// Only supported on Linux; elsewhere this does nothing.
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub fn configure_socket<T>(_socket: &T, _send_buffer_limit: Option<usize>) -> SocketInfo {
        SocketInfo::default()
    }

    /// Bytes the kernel holds for `fd` that it has not sent yet.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn unsent_bytes(fd: std::os::fd::RawFd) -> Option<usize> {
        let mut value: libc::c_int = 0;
        // SAFETY: the caller guarantees `fd` is open (see
        // `SendQueue::attach_socket`); `SIOCOUTQNSD` writes one `c_int`.
        #[allow(clippy::useless_conversion, reason = "the request type differs per libc")]
        let result = unsafe {
            libc::ioctl(fd, libc::SIOCOUTQNSD.try_into().ok()?, &raw mut value)
        };
        if result == 0 {
            usize::try_from(value).ok()
        } else {
            None
        }
    }

    #[derive(Debug)]
    pub struct SessionBackend<S> {
        stream: WebSocketStream<S>,
        socket: SocketInfo,
        tx_packet_b2f: mpsc::UnboundedSender<RecvPacket>,
        rx_packet_f2b: mpsc::UnboundedReceiver<Bytes>,
        rx_user_dc: oneshot::Receiver<String>,
        send_queue: Arc<SendQueue>,
    }

    pub fn split<S: AsyncRead + AsyncWrite + Unpin>(
        stream: WebSocketStream<S>,
        socket: SocketInfo,
    ) -> (SessionFrontend, SessionBackend<S>) {
        let (tx_packet_b2f, rx_packet_b2f) = mpsc::unbounded::<RecvPacket>();
        let (tx_packet_f2b, rx_packet_f2b) = mpsc::unbounded::<Bytes>();
        let (tx_user_dc, rx_user_dc) = oneshot::channel::<String>();
        let send_queue = Arc::new(SendQueue::default());

        (
            SessionFrontend {
                rx_packet_b2f,
                tx_packet_f2b,
                tx_user_dc,
                send_queue: send_queue.clone(),
            },
            SessionBackend {
                stream,
                socket,
                tx_packet_b2f,
                rx_packet_f2b,
                rx_user_dc,
                send_queue,
            },
        )
    }

    impl<S: Send + AsyncRead + AsyncWrite + Unpin> SessionBackend<S> {
        pub async fn start(self) -> Result<Never, DisconnectReason> {
            let Self {
                stream,
                socket,
                tx_packet_b2f,
                mut rx_packet_f2b,
                rx_user_dc,
                send_queue,
            } = self;

            // Reading and writing run side by side: a write waiting for the
            // socket to drain must not stop us from receiving.
            let (mut sink, mut stream) = stream.split();
            // declared after the stream halves, so it drops before them
            let _attached = send_queue.attach_socket(socket);

            let reason = {
                let recv = Self::recv_loop(&mut stream, &tx_packet_b2f).fuse();
                let send = Self::send_loop(&mut sink, &mut rx_packet_f2b, &send_queue).fuse();
                pin_mut!(recv, send, rx_user_dc);
                futures::select! {
                    result = recv => return result,
                    result = send => return result,
                    reason = rx_user_dc => reason.map_err(|_| SessionError::FrontendClosed)?,
                }
            };

            let mut stream = stream
                .reunite(sink)
                .expect("both halves come from the same stream");
            Self::close(&mut stream, reason.clone()).await?;
            Err(DisconnectReason::by_user(reason))
        }

        async fn recv_loop(
            stream: &mut SplitStream<WebSocketStream<S>>,
            tx_packet_b2f: &mpsc::UnboundedSender<RecvPacket>,
        ) -> Result<Never, DisconnectReason> {
            loop {
                let msg = stream
                    .next()
                    .await
                    .ok_or(SessionError::RecvStreamClosed)?
                    .map_err(SessionError::Connection)?;
                Self::recv(tx_packet_b2f, msg)?;
            }
        }

        async fn send_loop(
            sink: &mut SplitSink<WebSocketStream<S>, Message>,
            rx_packet_f2b: &mut mpsc::UnboundedReceiver<Bytes>,
            send_queue: &SendQueue,
        ) -> Result<Never, DisconnectReason> {
            loop {
                let packet = rx_packet_f2b
                    .next()
                    .await
                    .ok_or(SessionError::FrontendClosed)?;
                let len = packet.len();
                // this waits until the socket takes the whole message
                sink.send(Message::binary(packet))
                    .await
                    .map_err(SessionError::Connection)?;
                send_queue.sent(len);
            }
        }

        fn recv(
            tx_packet_b2f: &mpsc::UnboundedSender<RecvPacket>,
            msg: Message,
        ) -> Result<(), DisconnectReason> {
            let packet = match msg {
                Message::Close(None) => {
                    return Err(SessionError::DisconnectedWithoutReason.into());
                }
                Message::Close(Some(frame)) => {
                    return Err(DisconnectReason::by_peer(frame.reason.to_string()));
                }
                Message::Ping(_) | Message::Pong(_) => {
                    // explicitly ignore ping/pong messages
                    return Ok(());
                }
                Message::Binary(msg) => msg,
                msg @ Message::Text(_) => msg.into_data(),
                Message::Frame(_) => {
                    unreachable!("should not receive `Message::Frame`s from reading message");
                }
            };
            let now = Instant::now();

            tx_packet_b2f
                .unbounded_send(RecvPacket {
                    recv_at: now,
                    payload: packet,
                })
                .map_err(|_| SessionError::BackendClosed)?;
            Ok(())
        }

        async fn close(
            stream: &mut WebSocketStream<S>,
            reason: String,
        ) -> Result<(), DisconnectReason> {
            let close_frame = CloseFrame {
                code: CloseCode::Normal,
                reason: Utf8Bytes::from(reason),
            };
            stream
                .close(Some(close_frame))
                .await
                .map_err(SessionError::Connection)?;
            Ok(())
        }
    }
}
