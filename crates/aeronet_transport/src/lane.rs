//! Provides guarantees on message delivery and reception.
//!
//! Packets are not guaranteed to have any guarantees on delivery or ordering -
//! that is, if you send out a packet, there is no guarantee that:
//! - the packet will be received by the peer
//! - the packet will only be received once
//! - packets are received in the same order that they are sent
//!
//! (Note that receiving a packet is guaranteed to contain the exact same
//! content that was sent, without any corruption, truncation, or extension -
//! this is guaranteed by the IO layer.)
//!
//! Instead, these guarantees are provided when sending out *messages* out over
//! a *lane*. There may be multiple lanes on a single session, and they provide
//! guarantees on:
//! - reliability - the message is guaranteed to be received by the peer once,
//!   and only once
//! - ordering - messages sent on a specific lane are guaranteed to be received
//!   in the same order that they are sent
//!   - note that ordering *between* lanes is *never* guaranteed
//!
//! The name "lane" was chosen specifically to avoid ambiguity with:
//! - TCP, QUIC, or WebTransport *streams*
//! - MPSC *channels*
//!
//! Note that lanes provide a *minimum* guarantee of reliability and ordering.
//! If you are using an IO layer which is already reliable-ordered, then even
//! unreliable-unordered messages will be reliable-ordered. However, in this
//! situation we still need lanes as they are a part of the protocol - we can't
//! just ignore them for certain IO layers. Lanes send messages, and messages
//! include extra metadata like their sequence number, which we can't ignore.

use {
    crate::size::MinSize,
    bevy_reflect::prelude::*,
    core::time::Duration,
    derive_more::Display,
    octs::{BufTooShortOr, Decode, Encode, EncodeLen, FixedEncodeLenHint, Read, Write},
    typesize::derive::TypeSize,
};

/// What guarantees a kind of [lane] provides about message delivery.
///
/// [lane]: crate::lane
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, TypeSize, Reflect)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum LaneKind {
    /// No guarantees given on *reliability* or *ordering*.
    ///
    /// This is useful for messages which should be sent in a fire-and-forget
    /// manner: that is, you don't expect to get a response for this message,
    /// and it is OK if a few messages are lost in transit.
    ///
    /// This lane kind typically has the best performance, as it does not
    /// require any sort of handshaking to ensure that messages have arrived
    /// from one side to the other.
    ///
    /// For example, spawning particle effects in a game could be sent as an
    /// unreliable unordered message, as we'll probably be OK with losing a few
    /// of the messages, or if they arrive out of order. They can also be
    /// discarded if they arrive too late.
    UnreliableUnordered,
    /// Messages are *unreliable* but only messages newer than the last
    /// message will be received.
    ///
    /// Similar to [`LaneKind::UnreliableUnordered`], but any messages which
    /// are received and are older than an already-received message will be
    /// instantly dropped.
    ///
    /// This lane kind has the same performance as
    /// [`LaneKind::UnreliableUnordered`].
    ///
    /// An example of a message using this lane kind is a player positional
    /// update, sent to the server whenever a client moves in a game world.
    /// Since the game client will constantly be sending positional update
    /// messages at a high rate, it's OK if a few are lost in transit, as the
    /// server will hopefully catch the next messages. However, positional
    /// updates should not make the player go back in time - so any messages
    /// older than the most recent ones are dropped.
    UnreliableSequenced,
    /// Messages are sent *reliably* but the *ordering* is not guaranteed.
    ///
    /// This is useful for important one-off events where you need a guarantee
    /// that the message will be delivered, but the order in which it is
    /// delivered is not important.
    ///
    /// This lane kind is typically slower to send and receive than an
    /// unreliable message, but is still faster than an ordered lane because
    /// the implementation will avoid head-of-line blocking.
    ///
    /// An example of a message using this lane kind is sending level data
    /// from a server to a client. It is not important what order the different
    /// parts of the level are received in, but it is important that they are
    /// all received.
    ReliableUnordered,
    /// Messages are sent *reliably* and *ordered*.
    ///
    /// This is useful for important one-off events where you need a guarantee
    /// that the message will be delivered, and the order in which it's
    /// delivered is important.
    ///
    /// This lane kind offers the most guarantees, but is typically slower to
    /// send and receive than other lane kinds. Most notably, implementations
    /// may suffer from head-of-line blocking.
    ///
    /// Implementations may suffer from head-of-line blocking if new messages
    /// cannot be received because our peer is stuck waiting to receive the
    /// final parts of a previous message.
    ///
    /// An example of a message using this lane kind is sending chat messages
    /// from the server to the client. Since the server aggregates chat messages
    /// from different sources (system, other players, etc.) in a specific
    /// order, it must then tell its clients about the chat messages in that
    /// specific order as well.
    ReliableOrdered,
    /// Messages are sent in *every* packet until a packet carrying them is
    /// acknowledged or they expire, and are received at most once, in no
    /// particular order.
    ///
    /// Nothing is resent on a timer. A message rides each packet the
    /// transport flushes (in the room the other lanes leave, up to
    /// [`TransportConfig::redundant_bytes_per_packet`], oldest first) until
    /// the peer's packet acks cover one of those packets, so after a burst of
    /// loss it arrives with the first packet that gets through. A reliable
    /// lane's retransmit timer waits a PTO first, and backs off.
    ///
    /// A message that is still unacked `expiry` after it was pushed is
    /// dropped without being delivered ([`SendLane::num_expired`]). Messages
    /// should be small: they are copied into every packet, and one that never
    /// fits next to the other lanes' traffic waits for a packet with room.
    ///
    /// Suits events that matter for a short while and then not at all: an
    /// explosion, a hit, a kill notice. Wire-compatible with the other kinds:
    /// only the sender's schedule and the receiver's duplicate window differ.
    ///
    /// [`TransportConfig::redundant_bytes_per_packet`]: crate::TransportConfig::redundant_bytes_per_packet
    /// [`SendLane::num_expired`]: crate::send::SendLane::num_expired
    Redundant {
        /// How long after it was pushed a message stops being sent.
        expiry: Duration,
    },
}

/// How many of the newest message sequences a [`LaneKind::Redundant`] lane
/// remembers delivering.
///
/// The receiver drops a message this many sequences or more behind the newest
/// it has seen (it can't tell a duplicate from a fresh message there), so the
/// sender drops a message once this many newer ones have been pushed after it
/// (counted as expired). That bounds both ends with fixed storage: pushing more
/// than this many messages within one expiry would expire the oldest early.
pub const REDUNDANT_WINDOW: u16 = 1024;

impl LaneKind {
    /// Gets whether this lane kind guarantees message reliability or not.
    #[must_use]
    pub const fn reliability(&self) -> LaneReliability {
        match self {
            Self::UnreliableUnordered | Self::UnreliableSequenced | Self::Redundant { .. } => {
                LaneReliability::Unreliable
            }
            Self::ReliableUnordered | Self::ReliableOrdered => LaneReliability::Reliable,
        }
    }
}

/// Guarantees that a [lane] provides with relation to if a message is
/// received by the peer.
///
/// [lane]: crate::lane
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, TypeSize, Reflect)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum LaneReliability {
    /// Messages may not be received by the peer, or may be received multiple
    /// times.
    Unreliable,
    /// Messages will always be received once and only once by the peer.
    Reliable,
}

/// Index of a [lane] on either the sender or receiver side.
///
/// [lane]: crate::lane
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash, TypeSize, Reflect)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LaneIndex(pub MinSize);

impl LaneIndex {
    /// Creates a new lane index from a raw integer.
    #[must_use]
    pub const fn new(n: u32) -> Self {
        Self(MinSize(n))
    }
}

impl<T: Into<MinSize>> From<T> for LaneIndex {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl FixedEncodeLenHint for LaneIndex {
    const MIN_ENCODE_LEN: usize = <MinSize as FixedEncodeLenHint>::MIN_ENCODE_LEN;

    const MAX_ENCODE_LEN: usize = <MinSize as FixedEncodeLenHint>::MAX_ENCODE_LEN;
}

impl EncodeLen for LaneIndex {
    fn encode_len(&self) -> usize {
        self.0.encode_len()
    }
}

impl Encode for LaneIndex {
    type Error = <MinSize as Encode>::Error;

    fn encode(&self, dst: impl Write) -> Result<(), BufTooShortOr<Self::Error>> {
        self.0.encode(dst)
    }
}

impl Decode for LaneIndex {
    type Error = <MinSize as Decode>::Error;

    fn decode(src: impl Read) -> Result<Self, BufTooShortOr<Self::Error>> {
        MinSize::decode(src).map(Self)
    }
}
