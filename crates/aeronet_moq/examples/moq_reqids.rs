//! Probe: how many requests the relay lets one session make (MAX_REQUEST_ID),
//! and whether it raises the limit as requests finish. Each SUBSCRIBE is to a
//! track nobody publishes, so it fails fast.
#[cfg(not(target_family = "wasm"))]
#[tokio::main]
async fn main() {
    use aeronet_moq::{client::Dialer, conn::MoqConn, MoqTarget};
    use aeronet_webtransport::wtransport::ClientConfig;
    let url = std::env::var("MOQ_URL").expect("MOQ_URL");
    let n: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(70);
    _ = aeronet_webtransport::wtransport::tls::rustls::crypto::ring::default_provider().install_default();
    let dialer = Dialer::new(ClientConfig::builder().with_bind_default().with_native_certs().build()).unwrap();
    let conn = dialer.dial(&url).await.unwrap();
    let (moq, _closed) = MoqConn::setup(conn).await.unwrap();
    let target = MoqTarget::new(url, "test/reqids");
    let mut ns = target.prefix.clone(); ns.push("nobody".into());
    for i in 0..n {
        let (tx, _rx) = futures::channel::mpsc::unbounded();
        let r = tokio::time::timeout(std::time::Duration::from_secs(3), moq.subscribe(&ns, "t", tx)).await;
        match r {
            Ok(Ok(_)) => println!("{i}: ok"),
            Ok(Err(e)) => println!("{i}: {e}"),
            Err(_) => { println!("{i}: TIMEOUT (blocked on request ids?)"); break; }
        }
    }
}
#[cfg(target_family = "wasm")]
fn main() {}
