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
#[cfg(target_family = "wasm")]
mod web_streams;
pub mod wire;

#[cfg(not(target_family = "wasm"))]
pub use conn::{moq_transport, quic_limits};
pub use {
    client::{ClientConfig, MoqBroadcastRx, MoqClient, MoqClientPlugin, WarmSession},
    server::{Admission, Admit, Admitted, MoqBroadcast, MoqServer, MoqServerClient, MoqServerPlugin},
    session::{BROADCAST_LANES, BROADCAST_MAX_PAYLOAD, DOWN_LANES, MoqIo, UP_LANES},
};

/// Where a game lives on a relay.
#[derive(Debug, Clone)]
pub struct MoqTarget {
    /// Relay URL including any token path, e.g.
    /// `https://draft-18.cloudflare.mediaoverquic.com/<token>`.
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
    /// The input relay: clients announce `<prefix>/client/<id>` and publish
    /// their up lanes there, and the server reads them there, while the
    /// server's own tracks stay on [`Self::url`]. `None`: one relay for both.
    ///
    /// Why two: Cloudflare's relay keys can't be scoped to a namespace, only
    /// to publish and/or subscribe. A client that publishes its inputs needs a
    /// publishing key, which can also announce any namespace nobody holds,
    /// the server's own the moment the server's relay session ends (measured:
    /// a stranger took it 309 ms after the server left). With the server's
    /// tracks on a relay where clients hold a subscribe-only key, nobody but
    /// the server can ever announce there (measured: refused, a free
    /// namespace and the server's after it left alike).
    pub up_url: Option<String>,
}

impl MoqTarget {
    #[must_use]
    pub fn new(url: impl Into<String>, prefix: &str) -> Self {
        Self {
            url: url.into(),
            prefix: prefix.split('/').filter(|s| !s.is_empty()).map(str::to_owned).collect(),
            broadcast: false,
            tracks: None,
            up_url: None,
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

    /// Clients' namespaces and up lanes on the input relay `up_url` (see
    /// [`Self::up_url`]).
    #[must_use]
    pub fn with_up(mut self, up_url: impl Into<String>) -> Self {
        self.up_url = Some(up_url.into());
        self
    }

    /// The relay clients publish on: [`Self::up_url`], else [`Self::url`].
    pub(crate) fn input_url(&self) -> &str {
        self.up_url.as_deref().unwrap_or(&self.url)
    }

    pub(crate) fn ns(&self, rest: &[&str]) -> Vec<String> {
        self.prefix
            .iter()
            .cloned()
            .chain(rest.iter().map(|s| (*s).to_owned()))
            .collect()
    }
}
