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
    client::{ClientConfig, MoqClient, MoqClientPlugin},
    server::{MoqServer, MoqServerClient, MoqServerPlugin},
    session::MoqIo,
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
}

impl MoqTarget {
    #[must_use]
    pub fn new(url: impl Into<String>, prefix: &str) -> Self {
        Self {
            url: url.into(),
            prefix: prefix.split('/').filter(|s| !s.is_empty()).map(str::to_owned).collect(),
        }
    }

    pub(crate) fn ns(&self, rest: &[&str]) -> Vec<String> {
        self.prefix
            .iter()
            .cloned()
            .chain(rest.iter().map(|s| (*s).to_owned()))
            .collect()
    }
}
