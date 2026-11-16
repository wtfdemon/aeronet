#![doc = include_str!("../README.md")]
#![cfg_attr(
    target_family = "wasm",
    expect(
        clippy::future_not_send,
        clippy::arc_with_non_send_sync,
        reason = "`Send`, `Sync` are not used on WASM"
    )
)]

extern crate alloc;

pub mod client;
pub mod conn;
pub mod server;
pub mod session;
pub mod wire;

pub use {
    client::{ClientConfig, MoqBroadcastRx, MoqClient, MoqClientPlugin, WarmSession},
    server::{Admission, Admit, Admitted, MoqBroadcast, MoqServer, MoqServerClient, MoqServerPlugin},
    session::{BROADCAST_LANES, BROADCAST_MAX_PAYLOAD, MoqIo},
};

/// Where a game lives on a relay.
#[derive(Debug, Clone)]
pub struct MoqTarget {
    /// Relay URL including any token path, e.g.
    /// `https://draft-16.cloudflare.mediaoverquic.com/<token>`.
    pub url: String,
    /// Namespace prefix for this game, e.g. `["mygame", "room1"]`. The server
    /// publishes `<prefix>/server`, clients publish `<prefix>/client/<id>`.
    pub prefix: Vec<String>,
    /// Client: also subscribe to the server's [`BROADCAST_LANES`] broadcast
    /// tracks (`<prefix>/server/broadcast.<k>`); their payloads arrive in
    /// [`MoqBroadcastRx`]. Off by [`MoqTarget::new`].
    pub broadcast: bool,
    /// Client: base name of this client's tracks (see
    /// [`Admitted::tracks`]); `None`: its id, as a server without an
    /// [`Admit`] expects.
    pub tracks: Option<String>,
}

impl MoqTarget {
    #[must_use]
    pub fn new(url: impl Into<String>, prefix: &str) -> Self {
        Self {
            url: url.into(),
            prefix: prefix.split('/').filter(|s| !s.is_empty()).map(str::to_owned).collect(),
            broadcast: false,
            tracks: None,
        }
    }

    /// Subscribe to the server's broadcast lanes too (client side).
    #[must_use]
    pub fn with_broadcast(mut self) -> Self {
        self.broadcast = true;
        self
    }

    /// Name this client's tracks `tracks.<k>` (client side): what the
    /// server's [`Admit`] says for its id, from a secret the two share.
    #[must_use]
    pub fn with_tracks(mut self, tracks: impl Into<String>) -> Self {
        self.tracks = Some(tracks.into());
        self
    }

    pub(crate) fn ns(&self, rest: &[&str]) -> Vec<String> {
        self.prefix
            .iter()
            .cloned()
            .chain(rest.iter().map(|s| (*s).to_owned()))
            .collect()
    }
}
