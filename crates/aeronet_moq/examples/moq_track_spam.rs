//! Probe: does the relay pass a SUBSCRIBE for any made-up track name in an
//! announced namespace on to its publisher (which then holds state for it)?
//! Session A announces; session B (same token, as any player could) asks for
//! N random track names in it. Prints A's held track count and B's answers.
#[cfg(not(target_family = "wasm"))]
#[tokio::main]
async fn main() {
    use {
        aeronet_moq::{client::Dialer, conn::MoqConn},
        aeronet_webtransport::wtransport::ClientConfig,
        std::time::Duration,
    };
    let url = std::env::var("MOQ_URL").expect("MOQ_URL");
    let n: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(40);
    _ = aeronet_webtransport::wtransport::tls::rustls::crypto::ring::default_provider().install_default();
    let dialer = Dialer::new(ClientConfig::builder().with_bind_default().with_native_certs().build()).unwrap();
    let nonce = format!("{:x}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u32);
    let ns: Vec<String> = ["probe", "spam", &nonce, "server"].iter().map(|s| (*s).to_owned()).collect();
    let open = || async { MoqConn::setup(dialer.dial(&url).await.unwrap()).await.unwrap().0 };
    let a = open().await;
    a.announce(ns.clone()).await.unwrap();
    println!("A announced; holds {} tracks", a.out_track_count());
    let b = open().await;
    let (mut ok, mut refused) = (0, 0);
    for i in 0..n {
        let (tx, _rx) = futures::channel::mpsc::unbounded();
        match b.subscribe(&ns, &format!("junk{i}"), tx).await {
            Ok(_) => ok += 1,
            Err(e) => {
                refused += 1;
                if refused <= 3 {
                    println!("B junk{i}: {e}");
                }
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("B asked {n}: {ok} accepted, {refused} refused; A now holds {} tracks", a.out_track_count());
    a.shutdown();
    b.shutdown();
}
#[cfg(target_family = "wasm")]
fn main() {}
