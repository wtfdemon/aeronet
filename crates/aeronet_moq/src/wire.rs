//! The subset of MoQ Transport draft-18 that a datagram pipe needs.
//!
//! Wire format from `draft-ietf-moq-transport-18` (and Cloudflare's
//! `moq-rs`, which their relay runs), checked against Cloudflare's relay:
//!
//! - Varints have their length in the leading 1 bits of the first byte
//!   (1 to 9 bytes, 7 bits in one byte), not QUIC's 2-bit prefix.
//! - A message is `type (varint) | length (u16, big endian) | body`.
//! - Each endpoint opens one unidirectional control stream, `SETUP` first;
//!   every request (SUBSCRIBE, PUBLISH_NAMESPACE, SUBSCRIBE_NAMESPACE, ...)
//!   is the first message of its own bidirectional stream, and its answers
//!   come back on that stream without a request id. Closing the stream ends
//!   the request: no UNSUBSCRIBE, no request id budget.
//! - Message parameters are a count, then types as ascending deltas, each
//!   value encoded as its type says ([`param`]).
//! - A namespace is a tuple: `count`, then length-prefixed fields.

use bytes::{BufMut, Bytes, BytesMut};

/// WebTransport application protocol that selects draft-18 (`WT-Available-Protocols`).
pub const PROTOCOL: &str = "moqt-18";

pub mod msg {
    pub const REQUEST_UPDATE: u64 = 0x02;
    pub const SUBSCRIBE: u64 = 0x03;
    pub const SUBSCRIBE_OK: u64 = 0x04;
    pub const REQUEST_ERROR: u64 = 0x05;
    pub const PUBLISH_NAMESPACE: u64 = 0x06;
    pub const REQUEST_OK: u64 = 0x07;
    pub const NAMESPACE: u64 = 0x08;
    pub const PUBLISH_DONE: u64 = 0x0b;
    pub const TRACK_STATUS: u64 = 0x0d;
    pub const NAMESPACE_DONE: u64 = 0x0e;
    pub const PUBLISH_BLOCKED: u64 = 0x0f;
    pub const GOAWAY: u64 = 0x10;
    pub const FETCH: u64 = 0x16;
    pub const PUBLISH: u64 = 0x1d;
    pub const SUBSCRIBE_NAMESPACE: u64 = 0x50;
    pub const SUBSCRIBE_TRACKS: u64 = 0x51;
    /// Also the type of the control stream it opens.
    pub const SETUP: u64 = 0x2f00;
}

/// Message parameter types we send or must be able to skip.
pub mod param {
    pub const OBJECT_DELIVERY_TIMEOUT: u64 = 0x02;
    pub const AUTHORIZATION_TOKEN: u64 = 0x03;
    pub const RENDEZVOUS_TIMEOUT: u64 = 0x04;
    pub const SUBGROUP_DELIVERY_TIMEOUT: u64 = 0x06;
    pub const EXPIRES: u64 = 0x08;
    pub const LARGEST_OBJECT: u64 = 0x09;
    pub const FILL_TIMEOUT: u64 = 0x0a;
    pub const FORWARD: u64 = 0x10;
    pub const SUBSCRIBER_PRIORITY: u64 = 0x20;
    pub const SUBSCRIPTION_FILTER: u64 = 0x21;
    pub const GROUP_ORDER: u64 = 0x22;
    pub const NEW_GROUP_REQUEST: u64 = 0x32;
    pub const TRACK_NAMESPACE_PREFIX: u64 = 0x34;
}

/// Setup option: our implementation's name (draft-18 10.3.1.5).
pub const SETUP_MOQT_IMPLEMENTATION: u64 = 0x07;
/// Filter type: start after the largest (newest) object.
pub const FILTER_LARGEST_OBJECT: u64 = 0x02;

/// REQUEST_ERROR codes (draft-18 15.10.2).
pub mod error {
    pub const INTERNAL_ERROR: u64 = 0x00;
    pub const NOT_SUPPORTED: u64 = 0x03;
    pub const DOES_NOT_EXIST: u64 = 0x10;
    pub const UNINTERESTED: u64 = 0x20;
}

/// PUBLISH_DONE status: the publisher ended the track.
pub const PUBLISH_DONE_TRACK_ENDED: u64 = 0x2;

/// Stream reset / STOP_SENDING code for a request we give up (draft-18 3.3.3).
pub const RESET_CANCELLED: u32 = 0x1;

/// OBJECT_DATAGRAM type we send: ZERO_OBJECT_ID (0x04) | DEFAULT_PRIORITY
/// (0x08), no properties, a payload. Header `type | alias | group`, the
/// payload runs to the end; priority is the track's default (128). One byte
/// less than draft-16's type 0x04, which had to carry a priority byte.
pub const DATAGRAM_PAYLOAD: u64 = 0x0c;

/// Upper bound on the MoQ header in front of every datagram we send
/// (type 1 + alias ≤ 9 + group ≤ 9), rounded up. Kept at draft-16's 20 so
/// the packet MTU (and every packet the game already sizes) is unchanged.
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

/// Bytes a varint whose first byte is `first` takes, that byte included.
#[must_use]
pub const fn varint_len(first: u8) -> usize {
    1 + first.leading_ones() as usize
}

/// Writes the pieces a message body is made of.
#[derive(Debug, Default)]
pub struct Writer(pub BytesMut);

impl Writer {
    pub fn varint(&mut self, v: u64) -> &mut Self {
        // bytes needed: 7 usable bits in one byte, 7 more per extra byte up
        // to 8 bytes (56 bits), then 0xff + all 64
        let len = match v {
            0..=0x7f => 1,
            _ if v < 1 << 56 => (64 - v.leading_zeros() as usize).div_ceil(7),
            _ => 9,
        };
        if len == 9 {
            self.0.put_u8(0xff);
            self.0.put_u64(v);
            return self;
        }
        // len - 1 leading ones, a zero, then the value's top bits
        let tag = !(0xffu8 >> (len - 1));
        let bytes = v.to_be_bytes();
        let first = bytes[8 - len] | tag;
        self.0.put_u8(first);
        self.0.put_slice(&bytes[9 - len..]);
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

    /// Message parameters, `pairs` in any order (written sorted, as deltas).
    pub fn params(&mut self, pairs: &[(u64, Param<'_>)]) -> &mut Self {
        let mut sorted: Vec<_> = pairs.iter().collect();
        sorted.sort_by_key(|(k, _)| *k);
        self.varint(sorted.len() as u64);
        let mut prev = 0;
        for (key, value) in sorted {
            self.varint(key - prev);
            prev = *key;
            match value {
                Param::U8(v) => {
                    self.u8(*v);
                }
                Param::Int(v) => {
                    self.varint(*v);
                }
                Param::Bytes(b) => {
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
    U8(u8),
    Int(u64),
    Bytes(&'a [u8]),
}

/// A complete message on the wire.
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
        let len = varint_len(first);
        if len == 9 {
            let b = self.take(8)?;
            return Ok(u64::from_be_bytes(b.try_into().map_err(|_| DecodeError)?));
        }
        // the bits after the leading ones and their terminating zero
        let mut v = u64::from(first & 0xffu8.checked_shr(len as u32).unwrap_or(0));
        for &b in self.take(len - 1)? {
            v = (v << 8) | u64::from(b);
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

    /// Reads a message parameter block, returning the varint value of `want`
    /// if present. Values are read as each type says (uint8, Location,
    /// varint for other even types, length-prefixed for odd ones).
    pub fn params_int(&mut self, want: u64) -> Result<Option<u64>, DecodeError> {
        let n = self.varint()?;
        let mut key = 0u64;
        let mut found = None;
        for _ in 0..n {
            key = key.checked_add(self.varint()?).ok_or(DecodeError)?;
            match key {
                param::FORWARD | param::SUBSCRIBER_PRIORITY | param::GROUP_ORDER => {
                    let v = u64::from(self.u8()?);
                    if key == want {
                        found = Some(v);
                    }
                }
                param::LARGEST_OBJECT => {
                    self.varint()?;
                    self.varint()?;
                }
                k if k % 2 == 0 => {
                    let v = self.varint()?;
                    if key == want {
                        found = Some(v);
                    }
                }
                _ => {
                    self.bytes()?;
                }
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

/// Our SETUP: the first (and type) of our control stream.
#[must_use]
pub fn setup() -> Bytes {
    // Setup Options span the message (no count): one option, delta 7, bytes
    let mut b = Writer::default();
    b.varint(SETUP_MOQT_IMPLEMENTATION)
        .string(concat!("aeronet_moq/", env!("CARGO_PKG_VERSION")));
    frame(msg::SETUP, &b)
}

/// SUBSCRIBE (first message of its request stream), from the newest object on.
#[must_use]
pub fn subscribe(id: u64, ns: &[String], track: &str) -> Bytes {
    let mut filter = Writer::default();
    filter.varint(FILTER_LARGEST_OBJECT);
    let filter = filter.finish();
    let mut b = Writer::default();
    b.varint(id)
        .namespace(ns)
        .string(track)
        .params(&[(param::SUBSCRIPTION_FILTER, Param::Bytes(&filter))]);
    frame(msg::SUBSCRIBE, &b)
}

/// SUBSCRIBE_OK on the relay's SUBSCRIBE stream: the alias our datagrams
/// for it carry, no parameters, no track properties.
#[must_use]
pub fn subscribe_ok(alias: u64) -> Bytes {
    let mut b = Writer::default();
    b.varint(alias).params(&[]);
    frame(msg::SUBSCRIBE_OK, &b)
}

/// REQUEST_OK (e.g. to a PUBLISH we take): no parameters, no properties.
#[must_use]
pub fn request_ok() -> Bytes {
    let mut b = Writer::default();
    b.params(&[]);
    frame(msg::REQUEST_OK, &b)
}

/// REQUEST_ERROR, not to be retried.
#[must_use]
pub fn request_error(code: u64, reason: &str) -> Bytes {
    let mut b = Writer::default();
    b.varint(code).varint(0).string(reason);
    frame(msg::REQUEST_ERROR, &b)
}

/// We, the publisher, end a subscription the relay made to one of our
/// tracks: the last message on its stream before FIN. Stream count 0: we
/// only send datagrams.
#[must_use]
pub fn publish_done(code: u64, reason: &str) -> Bytes {
    let mut b = Writer::default();
    b.varint(code).varint(0).string(reason);
    frame(msg::PUBLISH_DONE, &b)
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
    b.varint(id).namespace(prefix).params(&[]);
    frame(msg::SUBSCRIBE_NAMESPACE, &b)
}

/// `PUBLISH` (draft-18 10.10): offer `track` under `ns` as `alias` without a
/// namespace announce. Not used by the game; the relay probes check what a
/// stranger can push with it.
#[must_use]
pub fn publish(id: u64, ns: &[String], track: &str, alias: u64) -> Bytes {
    let mut b = Writer::default();
    b.varint(id).namespace(ns).string(track).varint(alias).params(&[]);
    frame(msg::PUBLISH, &b)
}

/// A message on a request stream or the control stream that we act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    /// First answer to our SUBSCRIBE.
    SubscribeOk { alias: u64 },
    /// First answer to our PUBLISH_NAMESPACE, SUBSCRIBE_NAMESPACE, PUBLISH.
    RequestOk,
    RequestError { code: u64, reason: String },
    /// The relay subscribes to a track of ours (first on its stream).
    Subscribe { id: u64, ns: Namespace, track: String },
    /// The relay offers a track (first on its stream); we never ask for any.
    Publish { id: u64 },
    /// The publisher ends our subscription (last on its stream).
    PublishDone { code: u64, reason: String },
    /// On a SUBSCRIBE_NAMESPACE stream: a namespace suffix appeared or went.
    Namespace(Namespace),
    NamespaceDone(Namespace),
    GoAway { uri: String, timeout_ms: u64 },
    Other(u64),
}

pub fn decode_control(kind: u64, body: &[u8]) -> Result<Control, DecodeError> {
    let mut c = Cursor::new(body);
    Ok(match kind {
        msg::SUBSCRIBE_OK => Control::SubscribeOk { alias: c.varint()? },
        msg::REQUEST_OK => Control::RequestOk,
        msg::REQUEST_ERROR => {
            let code = c.varint()?;
            let _retry = c.varint()?;
            Control::RequestError {
                code,
                reason: c.string()?,
            }
        }
        msg::SUBSCRIBE => {
            let id = c.varint()?;
            let ns = c.namespace()?;
            let track = c.string()?;
            // every parameter must parse (an unknown one is a protocol error)
            c.params_int(u64::MAX)?;
            Control::Subscribe { id, ns, track }
        }
        msg::PUBLISH => Control::Publish { id: c.varint()? },
        msg::PUBLISH_DONE => {
            let code = c.varint()?;
            let _streams = c.varint()?;
            Control::PublishDone {
                code,
                reason: c.string()?,
            }
        }
        msg::NAMESPACE => Control::Namespace(c.namespace()?),
        msg::NAMESPACE_DONE => Control::NamespaceDone(c.namespace()?),
        msg::GOAWAY => {
            let uri = c.string()?;
            Control::GoAway {
                uri,
                timeout_ms: c.varint()?,
            }
        }
        other => Control::Other(other),
    })
}

// ---------- datagrams ----------

/// `OBJECT_DATAGRAM` carrying `payload` as object 0 of `group`
/// ([`DATAGRAM_PAYLOAD`]).
#[must_use]
pub fn datagram(alias: u64, group: u64, payload: &[u8]) -> Bytes {
    let mut b = Writer::default();
    b.varint(DATAGRAM_PAYLOAD).varint(alias).varint(group);
    b.0.put_slice(payload);
    b.finish()
}

/// An `OBJECT_DATAGRAM` (type 0x08: default priority) naming its object id,
/// for several objects in one group (probes).
#[must_use]
pub fn datagram_object(alias: u64, group: u64, object: u64, payload: &[u8]) -> Bytes {
    let mut b = Writer::default();
    b.varint(0x08).varint(alias).varint(group).varint(object);
    b.0.put_slice(payload);
    b.finish()
}

/// Splits an `OBJECT_DATAGRAM` into `(alias, payload)`; `None` for
/// status-only datagrams (and padding). Fields by the type's bits
/// (draft-18 11.3.1): 0x01 properties, 0x02 end of group, 0x04 no object id,
/// 0x08 no priority, 0x20 status instead of payload.
pub fn parse_datagram(buf: &[u8]) -> Result<Option<(u64, &[u8])>, DecodeError> {
    const PADDING: u64 = 0x132b_3e29;
    let mut c = Cursor::new(buf);
    let kind = c.varint()?;
    if kind == PADDING {
        return Ok(None);
    }
    // 0b00X0XXXX, and status can't signal end of group
    if kind & !0x2f != 0 || kind & 0x22 == 0x22 {
        return Err(DecodeError);
    }
    let alias = c.varint()?;
    let _group = c.varint()?;
    if kind & 0x04 == 0 {
        c.varint()?; // object id
    }
    if kind & 0x08 == 0 {
        c.u8()?; // publisher priority
    }
    if kind & 0x01 != 0 {
        c.bytes()?; // properties
    }
    if kind & 0x20 != 0 {
        return Ok(None);
    }
    Ok(Some((alias, c.rest())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: u64) -> Vec<u8> {
        let mut w = Writer::default();
        w.varint(v);
        w.finish().to_vec()
    }

    #[test]
    fn varints_round_trip_at_every_length() {
        for v in [
            0,
            1,
            127,
            128,
            16383,
            16384,
            (1 << 21) - 1,
            1 << 21,
            (1 << 28) - 1,
            1 << 28,
            (1 << 35) - 1,
            1 << 42,
            (1 << 49) - 1,
            1 << 49,
            (1 << 56) - 1,
            1 << 56,
            u64::MAX,
        ] {
            let b = enc(v);
            assert_eq!(varint_len(b[0]), b.len(), "{v}");
            let mut c = Cursor::new(&b);
            assert_eq!(c.varint().unwrap(), v, "{v}");
            assert!(c.rest().is_empty());
        }
    }

    /// The spec's own examples (draft-18 Table 2), decoded and encoded.
    #[test]
    fn varints_match_the_spec_examples() {
        let examples: &[(&[u8], u64)] = &[
            (&[0x25], 37),
            (&[0x80, 0x25], 37),
            (&[0xbb, 0xbd], 15_293),
            (&[0xed, 0x7f, 0x3e, 0x7d], 226_442_877),
            (&[0xfa, 0xa1, 0xa0, 0xe4, 0x03, 0xd8], 2_893_212_287_960),
            (&[0xfc, 0x89, 0x98, 0xab, 0xc6, 0x6b, 0xc0], 151_288_809_941_952),
            (&[0xfe, 0xfa, 0x31, 0x8f, 0xa8, 0xe3, 0xca, 0x11], 70_423_237_261_249_041),
            (&[0xff; 9], u64::MAX),
        ];
        for (bytes, v) in examples {
            assert_eq!(Cursor::new(bytes).varint().unwrap(), *v, "{bytes:x?}");
        }
        // minimal encodings
        assert_eq!(enc(37), [0x25]);
        assert_eq!(enc(15_293), [0xbb, 0xbd]);
        assert_eq!(enc(226_442_877), [0xed, 0x7f, 0x3e, 0x7d]);
        assert_eq!(enc(u64::MAX), [0xff; 9]);
    }

    #[test]
    fn setup_is_the_control_stream_type() {
        let s = setup();
        // 0x2f00 takes 2 bytes: 10 + 14 bits
        assert_eq!(&s[..2], &[0xaf, 0x00]);
        let len = u16::from_be_bytes([s[2], s[3]]) as usize;
        assert_eq!(s.len(), 4 + len);
        let mut c = Cursor::new(&s[4..]);
        assert_eq!(c.varint().unwrap(), SETUP_MOQT_IMPLEMENTATION);
        assert!(c.string().unwrap().starts_with("aeronet_moq/"));
    }

    #[test]
    fn params_are_typed_and_delta_coded() {
        let mut w = Writer::default();
        w.params(&[
            (param::SUBSCRIPTION_FILTER, Param::Bytes(&[2])),
            (param::OBJECT_DELIVERY_TIMEOUT, Param::Int(500)),
            (param::FORWARD, Param::U8(1)),
        ]);
        // count 3; 0x02 varint 500; delta 0x0e uint8 1; delta 0x11 bytes [2]
        assert_eq!(&w.finish()[..], &[3, 0x02, 0x81, 0xf4, 0x0e, 1, 0x11, 1, 2]);
        // LARGEST_OBJECT is a Location: two varints, not a length
        let mut raw = Writer::default();
        raw.varint(3).varint(param::EXPIRES).varint(9);
        raw.varint(1).varint(5).varint(7); // 0x09: group 5 object 7
        raw.varint(param::GROUP_ORDER - param::LARGEST_OBJECT).u8(1);
        let raw = raw.finish();
        assert_eq!(Cursor::new(&raw).params_int(param::EXPIRES).unwrap(), Some(9));
        assert_eq!(Cursor::new(&raw).params_int(param::GROUP_ORDER).unwrap(), Some(1));
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
    fn answers_carry_no_request_id() {
        let ok = subscribe_ok(300);
        let body = &ok[3..];
        assert_eq!(decode_control(msg::SUBSCRIBE_OK, body).unwrap(), Control::SubscribeOk { alias: 300 });
        let err = request_error(error::DOES_NOT_EXIST, "no");
        assert_eq!(
            decode_control(msg::REQUEST_ERROR, &err[3..]).unwrap(),
            Control::RequestError { code: 0x10, reason: "no".to_owned() }
        );
        let done = publish_done(PUBLISH_DONE_TRACK_ENDED, "bye");
        assert_eq!(
            decode_control(msg::PUBLISH_DONE, &done[3..]).unwrap(),
            Control::PublishDone { code: 2, reason: "bye".to_owned() }
        );
    }

    #[test]
    fn datagram_roundtrip() {
        let d = datagram(7, 300, b"hello");
        // type 0x0c, alias 7, group 300 (2 bytes), no priority byte
        assert_eq!(&d[..4], &[0x0c, 7, 0x81, 0x2c]);
        assert_eq!(parse_datagram(&d).unwrap(), Some((7, &b"hello"[..])));
        // object id and priority present (type 0x00), as another publisher may send
        let mut w = Writer::default();
        w.varint(0x00).varint(3).varint(9).varint(0).u8(128);
        w.0.put_slice(b"x");
        assert_eq!(parse_datagram(&w.finish()).unwrap(), Some((3, &b"x"[..])));
        // properties (0x01 | 0x04): skipped
        let mut w = Writer::default();
        w.varint(0x05).varint(3).varint(9).u8(1).bytes(&[0x02, 0x05]);
        w.0.put_slice(b"y");
        assert_eq!(parse_datagram(&w.finish()).unwrap(), Some((3, &b"y"[..])));
        // status (0x20 | 0x04): no payload
        let mut w = Writer::default();
        w.varint(0x24).varint(3).varint(9).u8(1).varint(3);
        assert_eq!(parse_datagram(&w.finish()).unwrap(), None);
        // status and end of group together is invalid
        let mut w = Writer::default();
        w.varint(0x26).varint(3).varint(9);
        assert!(parse_datagram(&w.finish()).is_err());
        assert_eq!(parse_datagram(&datagram_object(1, 2, 3, b"z")).unwrap(), Some((1, &b"z"[..])));
    }
}
