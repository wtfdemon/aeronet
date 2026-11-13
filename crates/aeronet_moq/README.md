# `aeronet_moq`

[`aeronet`] IO layer that carries a game's packets through a [Media over QUIC]
relay (MoQ Transport draft-16, as run by Cloudflare) instead of a direct
WebTransport connection. Both the server and the clients dial *out* to the
relay; the server needs no open port.

Only datagrams carry data; `aeronet_transport` keeps doing reliability and
ordering on top. The MoQ subset used: setup, PUBLISH_NAMESPACE, SUBSCRIBE (and
answering the relay's SUBSCRIBE), SUBSCRIBE_NAMESPACE events, OBJECT_DATAGRAM.

- server: announces `<prefix>/server` (one track per client id) and watches
  `<prefix>/client/*`;
- client `<id>`: announces `<prefix>/client/<id>` (track `up`) and subscribes to
  `<prefix>/server` track `<id>`.

[`aeronet`]: https://github.com/aecsocket/aeronet
[Media over QUIC]: https://datatracker.ietf.org/wg/moq/about/
