//! Probe: what the relay does when a second session announces a namespace
//! another session already holds, and which NAMESPACE / NAMESPACE_DONE events
//! a SUBSCRIBE_NAMESPACE watcher sees as each of them goes away. Decides
//! whether a token holder can kick a player by announcing their exact
//! namespace and withdrawing it.
#[cfg(not(target_family = "wasm"))]
#[tokio::main]
async fn main() {
    use {
        aeronet_moq::{client::Dialer, conn::MoqConn, conn::NamespaceEvent},
        aeronet_webtransport::wtransport::ClientConfig,
        futures::StreamExt,
        std::time::Duration,
    };
    let url = std::env::var("MOQ_URL").expect("MOQ_URL");
    _ = aeronet_webtransport::wtransport::tls::rustls::crypto::ring::default_provider().install_default();
    let dialer = Dialer::new(ClientConfig::builder().with_bind_default().with_custom_transport(aeronet_moq::moq_transport()).build()).unwrap();
    let nonce = format!("{:x}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u32);
    let prefix: Vec<String> = ["probe", "dup", &nonce, "client"].iter().map(|s| (*s).to_owned()).collect();
    let mut ns = prefix.clone();
    ns.push("victim".into());
    let open = || async { MoqConn::setup(dialer.dial(&url).await.unwrap()).await.unwrap().0 };

    let watcher = open().await;
    let mut events = watcher.subscribe_namespace(prefix.clone()).await.unwrap();
    let drain = |events: &mut futures::channel::mpsc::UnboundedReceiver<NamespaceEvent>, label: &'static str| {
        let mut out = Vec::new();
        while let Ok(Some(e)) = events.try_recv().map(Some) {
            out.push(format!("{e:?}"));
        }
        println!("{label}: watcher saw {out:?}");
    };

    let a = open().await;
    println!("A announce: {:?}", a.announce(ns.clone()).await);
    tokio::time::sleep(Duration::from_secs(1)).await;
    drain(&mut events, "after A");

    let b = open().await;
    println!("B announce same ns: {:?}", b.announce(ns.clone()).await);
    tokio::time::sleep(Duration::from_secs(1)).await;
    drain(&mut events, "after B");

    b.shutdown();
    tokio::time::sleep(Duration::from_secs(2)).await;
    drain(&mut events, "after B closed");

    a.shutdown();
    tokio::time::sleep(Duration::from_secs(2)).await;
    drain(&mut events, "after A closed");
    let _ = events.next();
}
#[cfg(target_family = "wasm")]
fn main() {}
