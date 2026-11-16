//! The subset of MoQ Transport draft-16 that a datagram pipe needs.
//!
//! Wire format taken from `cloudflare/moq-rs`, branch `draft-ietf-moq-transport-16`
//! (`moq-transport/src`), and checked against Cloudflare's relay.
//!
//! - Varints are QUIC varints (2-bit length prefix).
//! - A control message is `type (varint) | length (u16, big endian) | body`.
//! - Parameters are `count`, then keys in ascending order as *deltas*; an even
//!   key carries a varint value, an odd key a length-prefixed byte string.
//! - A namespace is a tuple: `count`, then length-prefixed fields.

use bytes::{BufMut, Bytes, BytesMut};

/// WebTransport application protocol that selects draft-16 (`WT-Available-Protocols`).
pub const PROTOCOL: &str = "moqt-16";

pub mod msg {
    pub const REQUEST_UPDATE: u64 = 0x02;
    pub const SUBSCRIBE: u64 = 0x03;
    pub const SUBSCRIBE_OK: u64 = 0x04;
    pub const REQUEST_ERROR: u64 = 0x05;
    pub const PUBLISH_NAMESPACE: u64 = 0x06;
    pub const REQUEST_OK: u64 = 0x07;
    pub const NAMESPACE: u64 = 0x08;
    pub const PUBLISH_NAMESPACE_DONE: u64 = 0x09;
    pub const UNSUBSCRIBE: u64 = 0x0a;
    pub const PUBLISH_DONE: u64 = 0x0b;
    pub const PUBLISH_NAMESPACE_CANCEL: u64 = 0x0c;
    pub const NAMESPACE_DONE: u64 = 0x0e;
    pub const GOAWAY: u64 = 0x10;
    pub const SUBSCRIBE_NAMESPACE: u64 = 0x11;
    pub const PUBLISH: u64 = 0x1d;
    pub const PUBLISH_OK: u64 = 0x1e;
    pub const MAX_REQUEST_ID: u64 = 0x15;
    pub const REQUESTS_BLOCKED: u64 = 0x1a;
    pub const CLIENT_SETUP: u64 = 0x20;
    pub const SERVER_SETUP: u64 = 0x21;
}

/// Setup parameter: the highest request id + 1 the peer may use towards us.
pub const SETUP_MAX_REQUEST_ID: u64 = 0x02;
/// SUBSCRIBE parameter: subscription filter (bytes).
pub const PARAM_SUBSCRIPTION_FILTER: u64 = 0x21;
/// Filter type: start at the largest (newest) object.
pub const FILTER_LARGEST_OBJECT: u64 = 0x02;
/// SUBSCRIBE_NAMESPACE option: namespace events only (no PUBLISH).
pub const SUBSCRIBE_OPTIONS_NAMESPACE: u64 = 0x01;
/// REQUEST_ERROR code for "track does not exist".
pub const ERROR_DOES_NOT_EXIST: u64 = 0x10;

/// OBJECT_DATAGRAM type without object id, extensions or end-of-group: header
/// is `type | alias | group | priority`, the payload runs to the end.
pub const DATAGRAM_PAYLOAD: u64 = 0x04;

/// Upper bound on the MoQ header in front of every datagram we send
/// (type 1 + alias ≤ 8 + group ≤ 8 + priority 1), rounded up.
pub const DATAGRAM_OVERHEAD: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError;

impl core::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("malformed MoQ message")
    }
}

impl core::error::Error for DecodeError {}

pub type Namespace = Vec<String>;

/// Writes the pieces a message body is made of.
#[derive(Debug, Default)]
pub struct Writer(pub BytesMut);

impl Writer {
    pub fn varint(&mut self, v: u64) -> &mut Self {
        if v < 1 << 6 {
            self.0.put_u8(v as u8);
        } else if v < 1 << 14 {
            self.0.put_u16(v as u16 | 0x4000);
        } else if v < 1 << 30 {
            self.0.put_u32(v as u32 | 0x8000_0000);
        } else {
            assert!(v < 1 << 62, "varint out of range");
            self.0.put_u64(v | 0xc000_0000_0000_0000);
        }
        self
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.0.put_u8(v);
        self
    }

    pub fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.varint(b.len() as u64);
        self.0.put_slice(b);
        self
    }

    pub fn string(&mut self, s: &str) -> &mut Self {
        self.bytes(s.as_bytes())
    }

    pub fn namespace(&mut self, ns: &[String]) -> &mut Self {
        self.varint(ns.len() as u64);
        for field in ns {
            self.string(field);
        }
        self
    }

    /// `pairs` may be in any order; they are written sorted, as deltas.
    pub fn params(&mut self, pairs: &[(u64, Param<'_>)]) -> &mut Self {
        let mut sorted: Vec<_> = pairs.iter().collect();
        sorted.sort_by_key(|(k, _)| *k);
        self.varint(sorted.len() as u64);
        let mut prev = 0;
        for (key, value) in sorted {
            self.varint(key - prev);
            prev = *key;
            match value {
                Param::Int(v) => {
                    debug_assert_eq!(key % 2, 0);
                    self.varint(*v);
                }
                Param::Bytes(b) => {
                    debug_assert_eq!(key % 2, 1);
                    self.bytes(b);
                }
            }
        }
        self
    }

    pub fn finish(self) -> Bytes {
        self.0.freeze()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Param<'a> {
    Int(u64),
    Bytes(&'a [u8]),
}

/// A complete control message on the wire.
#[must_use]
pub fn frame(kind: u64, body: &Writer) -> Bytes {
    let mut out = Writer::default();
    out.varint(kind);
    let len = u16::try_from(body.0.len()).expect("control message too large");
    out.0.put_u16(len);
    out.0.put_slice(&body.0);
    out.finish()
}

/// Reads from one complete buffer (a message body or a datagram).
#[derive(Debug)]
pub struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    #[must_use]
    pub const fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        let b = *self.buf.get(self.pos).ok_or(DecodeError)?;
        self.pos += 1;
        Ok(b)
    }

    pub fn varint(&mut self) -> Result<u64, DecodeError> {
        let first = self.u8()?;
        let len = 1usize << (first >> 6);
        let mut v = u64::from(first & 0x3f);
        for _ in 1..len {
            v = (v << 8) | u64::from(self.u8()?);
        }
        Ok(v)
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(n).ok_or(DecodeError)?;
        let s = self.buf.get(self.pos..end).ok_or(DecodeError)?;
        self.pos = end;
        Ok(s)
    }

    pub fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let n = usize::try_from(self.varint()?).map_err(|_| DecodeError)?;
        self.take(n)
    }

    pub fn string(&mut self) -> Result<String, DecodeError> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| DecodeError)
    }

    pub fn namespace(&mut self) -> Result<Namespace, DecodeError> {
        let n = self.varint()?;
        if n > 32 {
            return Err(DecodeError);
        }
        (0..n).map(|_| self.string()).collect()
    }

    /// Skips a parameter list, returning the integer value of `want` if present.
    pub fn params_int(&mut self, want: u64) -> Result<Option<u64>, DecodeError> {
        let n = self.varint()?;
        let mut key = 0;
        let mut found = None;
        for _ in 0..n {
            key += self.varint()?;
            if key % 2 == 0 {
                let v = self.varint()?;
                if key == want {
                    found = Some(v);
                }
            } else {
                self.bytes()?;
            }
        }
        Ok(found)
    }

    #[must_use]
    pub fn rest(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }
}

// ---------- messages ----------

#[must_use]
pub fn client_setup(max_request_id: u64) -> Bytes {
    let mut b = Writer::default();
    b.params(&[(SETUP_MAX_REQUEST_ID, Param::Int(max_request_id))]);
    frame(msg::CLIENT_SETUP, &b)
}

/// Returns the peer's MAX_REQUEST_ID (draft default 0).
pub fn server_setup(body: &[u8]) -> Result<u64, DecodeError> {
    Ok(Cursor::new(body)
        .params_int(SETUP_MAX_REQUEST_ID)?
        .unwrap_or(0))
}

#[must_use]
pub fn subscribe(id: u64, ns: &[String], track: &str) -> Bytes {
    let mut filter = Writer::default();
    filter.varint(FILTER_LARGEST_OBJECT);
    let filter = filter.finish();
    let mut b = Writer::default();
    b.varint(id)
        .namespace(ns)
        .string(track)
        .params(&[(PARAM_SUBSCRIPTION_FILTER, Param::Bytes(&filter))]);
    frame(msg::SUBSCRIBE, &b)
}

/// Status code for [`publish_done`]: the publisher ended the track.
pub const PUBLISH_DONE_TRACK_ENDED: u64 = 0x2;

/// We, the publisher, end the subscription `id` the relay made to one of our
/// tracks: no more objects come, and the relay tells its own subscribers.
/// Body: request id, status code, stream count (0: datagrams only), reason.
#[must_use]
pub fn publish_done(id: u64, code: u64, reason: &str) -> Bytes {
    let mut b = Writer::default();
    b.varint(id).varint(code).varint(0).string(reason);
    frame(msg::PUBLISH_DONE, &b)
}

/// Lets the peer make requests with ids below `max`.
#[must_use]
pub fn max_request_id(max: u64) -> Bytes {
    let mut b = Writer::default();
    b.varint(max);
    frame(msg::MAX_REQUEST_ID, &b)
}

/// Ends our subscription `id`; the relay stops forwarding its objects.
#[must_use]
pub fn unsubscribe(id: u64) -> Bytes {
    let mut b = Writer::default();
    b.varint(id);
    frame(msg::UNSUBSCRIBE, &b)
}

#[must_use]
pub fn subscribe_ok(id: u64, alias: u64) -> Bytes {
    // id, alias, no params, no track extensions (they run to the end of the message)
    let mut b = Writer::default();
    b.varint(id).varint(alias).params(&[]);
    frame(msg::SUBSCRIBE_OK, &b)
}

#[must_use]
pub fn request_error(id: u64, code: u64, reason: &str) -> Bytes {
    let mut b = Writer::default();
    b.varint(id).varint(code).varint(0).string(reason);
    frame(msg::REQUEST_ERROR, &b)
}

#[must_use]
pub fn publish_namespace(id: u64, ns: &[String]) -> Bytes {
    let mut b = Writer::default();
    b.varint(id).namespace(ns).params(&[]);
    frame(msg::PUBLISH_NAMESPACE, &b)
}

#[must_use]
pub fn subscribe_namespace(id: u64, prefix: &[String]) -> Bytes {
    let mut b = Writer::default();
    b.varint(id)
        .namespace(prefix)
        .varint(SUBSCRIBE_OPTIONS_NAMESPACE)
        .params(&[]);
    frame(msg::SUBSCRIBE_NAMESPACE, &b)
}

/// `PUBLISH` (draft-16 9.13): offer `track` under `ns` as `alias` without a
/// namespace announce. Not used by the game; the relay probes check what a
/// stranger can push with it.
pub fn publish(id: u64, ns: &[String], track: &str, alias: u64) -> Bytes {
    let mut b = Writer::default();
    // no track extensions: they run to the end of the message (moq-rs d16
    // TrackExtensions), so nothing at all, not a zero length
    b.varint(id).namespace(ns).string(track).varint(alias).params(&[]);
    frame(msg::PUBLISH, &b)
}

/// An inbound control message we act on; everything else is `Other`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    SubscribeOk { id: u64, alias: u64 },
    RequestOk { id: u64 },
    PublishOk { id: u64 },
    RequestError { id: u64, code: u64, reason: String },
    Subscribe { id: u64, ns: Namespace, track: String },
    Unsubscribe { id: u64 },
    PublishDone { id: u64 },
    MaxRequestId(u64),
    Namespace(Namespace),
    NamespaceDone(Namespace),
    GoAway,
    Other(u64),
}

pub fn decode_control(kind: u64, body: &[u8]) -> Result<Control, DecodeError> {
    let mut c = Cursor::new(body);
    Ok(match kind {
        msg::SUBSCRIBE_OK => Control::SubscribeOk {
            id: c.varint()?,
            alias: c.varint()?,
        },
        msg::REQUEST_OK => Control::RequestOk { id: c.varint()? },
        msg::PUBLISH_OK => Control::PublishOk { id: c.varint()? },
        msg::REQUEST_ERROR => {
            let id = c.varint()?;
            let code = c.varint()?;
            let _retry = c.varint()?;
            Control::RequestError {
                id,
                code,
                reason: c.string()?,
            }
        }
        msg::SUBSCRIBE => Control::Subscribe {
            id: c.varint()?,
            ns: c.namespace()?,
            track: c.string()?,
        },
        msg::UNSUBSCRIBE => Control::Unsubscribe { id: c.varint()? },
        msg::PUBLISH_DONE => Control::PublishDone { id: c.varint()? },
        msg::MAX_REQUEST_ID => Control::MaxRequestId(c.varint()?),
        msg::NAMESPACE => Control::Namespace(c.namespace()?),
        msg::NAMESPACE_DONE => Control::NamespaceDone(c.namespace()?),
        msg::GOAWAY => Control::GoAway,
        other => Control::Other(other),
    })
}

// ---------- datagrams ----------

/// `OBJECT_DATAGRAM` carrying `payload` as object 0 of `group`.
#[must_use]
pub fn datagram(alias: u64, group: u64, payload: &[u8]) -> Bytes {
    let mut b = Writer::default();
    b.varint(DATAGRAM_PAYLOAD).varint(alias).varint(group).u8(0);
    b.0.put_slice(payload);
    b.finish()
}

/// An `OBJECT_DATAGRAM` (type 0x00) naming its object id, for several
/// objects in one group.
pub fn datagram_object(alias: u64, group: u64, object: u64, payload: &[u8]) -> Bytes {
    let mut b = Writer::default();
    b.varint(0x00).varint(alias).varint(group).varint(object).u8(0);
    b.0.put_slice(payload);
    b.finish()
}

/// Splits an `OBJECT_DATAGRAM` into `(alias, payload)`; `None` for status-only datagrams.
pub fn parse_datagram(buf: &[u8]) -> Result<Option<(u64, &[u8])>, DecodeError> {
    let mut c = Cursor::new(buf);
    let kind = c.varint()?;
    let alias = c.varint()?;
    let _group = c.varint()?;
    let has_object_id = kind <= 0x03 || kind == 0x20 || kind == 0x21;
    if has_object_id {
        c.varint()?;
    }
    c.u8()?; // publisher priority
    if matches!(kind, 0x01 | 0x03 | 0x05 | 0x07 | 0x21) {
        c.bytes()?; // extension headers
    }
    if kind >= 0x20 {
        return Ok(None);
    }
    if kind > 0x07 {
        return Err(DecodeError);
    }
    Ok(Some((alias, c.rest())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints() {
        for v in [0, 1, 63, 64, 16383, 16384, (1 << 30) - 1, 1 << 30, (1 << 62) - 1] {
            let mut w = Writer::default();
            w.varint(v);
            let b = w.finish();
            assert_eq!(Cursor::new(&b).varint().unwrap(), v, "{v}");
        }
    }

    #[test]
    fn client_setup_bytes() {
        // type 0x20, len 4, 1 param, key delta 2, value 1000 (2-byte varint)
        assert_eq!(&client_setup(1000)[..], &[0x20, 0x00, 0x04, 0x01, 0x02, 0x43, 0xe8]);
    }

    #[test]
    fn params_are_delta_coded() {
        let mut w = Writer::default();
        w.params(&[(0x21, Param::Bytes(&[2])), (0x02, Param::Int(5))]);
        // count 2, key 2 value 5, key delta 0x1f bytes [2]
        assert_eq!(&w.finish()[..], &[2, 0x02, 5, 0x1f, 1, 2]);
        let mut w = Writer::default();
        w.params(&[(0x21, Param::Bytes(&[2])), (0x02, Param::Int(5))]);
        let b = w.finish();
        assert_eq!(Cursor::new(&b).params_int(0x02).unwrap(), Some(5));
    }

    #[test]
    fn subscribe_roundtrip() {
        let ns = vec!["game".to_owned(), "room".to_owned(), "server".to_owned()];
        let m = subscribe(4, &ns, "abc");
        let mut c = Cursor::new(&m);
        assert_eq!(c.varint().unwrap(), msg::SUBSCRIBE);
        let len = u16::from_be_bytes(c.take(2).unwrap().try_into().unwrap()) as usize;
        let body = c.take(len).unwrap();
        assert_eq!(
            decode_control(msg::SUBSCRIBE, body).unwrap(),
            Control::Subscribe {
                id: 4,
                ns,
                track: "abc".to_owned()
            }
        );
    }

    #[test]
    fn datagram_roundtrip() {
        let d = datagram(7, 300, b"hello");
        assert_eq!(parse_datagram(&d).unwrap(), Some((7, &b"hello"[..])));
        // object-id form (type 0x00), as Cloudflare's own publisher sends
        let mut w = Writer::default();
        w.varint(0x00).varint(3).varint(9).varint(0).u8(0);
        w.0.put_slice(b"x");
        assert_eq!(parse_datagram(&w.finish()).unwrap(), Some((3, &b"x"[..])));
    }
}
