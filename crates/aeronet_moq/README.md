# `aeronet_moq`

[`aeronet`] IO layer that carries a game's packets through a [Media over QUIC]
relay (MoQ Transport draft-18, as run by Cloudflare) instead of a direct
WebTransport connection. Both the server and the clients dial *out* to the
relay; the server needs no open port.

Only datagrams carry data; `aeronet_transport` keeps doing reliability and
ordering on top. The MoQ subset used: SETUP, PUBLISH_NAMESPACE, SUBSCRIBE (and
answering the relay's SUBSCRIBE), SUBSCRIBE_NAMESPACE events, PUBLISH_DONE,
GOAWAY, OBJECT_DATAGRAM. Each request is its own bidirectional stream, and a
session's SETUP goes out with its first requests in one flight.

- server: announces `<prefix>/server` (each client's down lanes, plus the
  shared broadcast lanes) and watches `<prefix>/client/*`;
- client `<id>`: announces `<prefix>/client/<id>` (its up lanes) and
  subscribes to its down lanes under `<prefix>/server`. Lanes are named from
  a secret the game gives both ends ([`Admitted::tracks`]).

[`aeronet`]: https://github.com/aecsocket/aeronet
[Media over QUIC]: https://datatracker.ietf.org/wg/moq/about/
[`Admitted::tracks`]: crate::Admitted
