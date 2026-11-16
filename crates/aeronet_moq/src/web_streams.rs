//! Streams the relay opens towards a browser session.
//!
//! `xwt_web::Session::accept_bi` locks the incoming-streams `ReadableStream`
//! with a fresh reader on every call and never releases it, so a second call
//! fails: fine for one stream, not for draft-18, where every SUBSCRIBE the
//! relay sends us (one per up lane) is a stream of its own. Here one reader
//! is taken for the session's life and each stream it yields is wrapped the
//! way xwt-web wraps its own.

use {
    alloc::rc::Rc,
    wasm_bindgen::{JsCast, JsValue},
    xwt_web::{RecvStream, SendStream, Session, web_wt_sys},
};

#[derive(Debug)]
pub struct StreamError(String);

impl core::fmt::Display for StreamError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

fn js_err(what: &str, e: &JsValue) -> StreamError {
    StreamError(format!("{what}: {e:?}"))
}

/// Accepts the relay's bidirectional streams, one reader for all.
pub struct BiAcceptor {
    transport: Rc<web_wt_sys::WebTransport>,
    reader: web_sys::ReadableStreamDefaultReader,
}

impl BiAcceptor {
    pub fn new(session: &Session) -> Self {
        let transport = session.transport_ref().clone();
        let incoming: web_sys::ReadableStream = transport.incoming_bidirectional_streams();
        let reader: JsValue = incoming.get_reader().into();
        Self {
            transport,
            reader: reader.unchecked_into(),
        }
    }

    pub async fn accept(&self) -> Result<(SendStream, RecvStream), StreamError> {
        let read = wasm_bindgen_futures::JsFuture::from(self.reader.read())
            .await
            .map_err(|e| js_err("accept bi", &e))?;
        let read: web_wt_sys::ReadableStreamReadResult<web_wt_sys::WebTransportBidirectionalStream> =
            read.unchecked_into();
        if read.is_done() {
            return Err(StreamError("no more incoming streams".to_owned()));
        }
        let stream = read
            .get_value()
            .ok_or_else(|| StreamError("incoming stream without a value".to_owned()))?;
        Ok((
            wrap_send(&self.transport, stream.writable()),
            wrap_recv(&self.transport, stream.readable()),
        ))
    }
}

/// The relay's control stream: the first (and only) unidirectional stream
/// it opens. xwt-web's one-shot accept is fine for that.
pub async fn accept_uni(session: &Session) -> Result<RecvStream, StreamError> {
    use xwt_core::session::stream::AcceptUni;
    session
        .accept_uni()
        .await
        .map_err(|e| StreamError(format!("accept uni: {e}")))
}

fn wrap_recv(transport: &Rc<web_wt_sys::WebTransport>, stream: web_wt_sys::WebTransportReceiveStream) -> RecvStream {
    // BYOB where the browser has it (Chrome, Firefox), else a default reader
    let mode = match web_sys_stream_utils::try_get_reader_byob(stream.clone()) {
        Ok(reader) => web_sys_async_io::reader::Mode::Byob {
            reader,
            internal_buf: None,
        },
        Err(_) => web_sys_async_io::reader::Mode::Default {
            reader: web_sys_stream_utils::get_reader(stream.clone()),
        },
    };
    RecvStream {
        transport: Rc::clone(transport),
        stream,
        reader: web_sys_async_io::Reader::new(mode),
        unlock_reader_on_drop: true,
    }
}

fn wrap_send(transport: &Rc<web_wt_sys::WebTransport>, stream: web_wt_sys::WebTransportSendStream) -> SendStream {
    let writer = stream.get_writer().unwrap();
    SendStream {
        transport: Rc::clone(transport),
        stream,
        writer: web_sys_async_io::Writer::new(writer.into()),
        unlock_writer_on_drop: true,
    }
}
