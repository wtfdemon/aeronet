//! Probe: does the relay fan one track out to many subscribers, so the
//! publisher sends each object once?
//!
//! ```sh
//! MOQ_URL=... cargo run -p aeronet_moq --example moq_fanout -- shared 60 30
//! MOQ_URL=... cargo run -p aeronet_moq --example moq_fanout -- unicast 60 30
//! ```
//!
//! `shared`: one track, every subscriber session subscribes to it, the
//! publisher sends one datagram per packet. `unicast`: one track per
//! subscriber (what rooms do today), the publisher sends one copy per
//! subscriber. Env: FANOUT_SIZE (bytes per datagram, 1000), FANOUT_BURST
//! (datagrams per tick, 1), FANOUT_HZ (20).
//!
//! Prints the publisher's UDP bytes out, how many SUBSCRIBEs the relay sent
//! it, and per subscriber what arrived, lost and late.
#[cfg(not(target_family = "wasm"))]
#[tokio::main]
async fn main() {
    use {
        aeronet_moq::{client::Dialer, conn::MoqConn},
        aeronet_webtransport::wtransport::ClientConfig,
        futures::StreamExt,
        std::{
            sync::{Arc, Mutex},
            time::{Duration, Instant, SystemTime, UNIX_EPOCH},
        },
    };
    let url = std::env::var("MOQ_URL").expect("MOQ_URL");
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "shared".to_owned());
    let n: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(60);
    let secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(30);
    let env = |k: &str, d: usize| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
    let (size, burst, hz) = (env("FANOUT_SIZE", 1000).max(16), env("FANOUT_BURST", 1), env("FANOUT_HZ", 20));
    let shared = mode == "shared";
    _ = aeronet_webtransport::wtransport::tls::rustls::crypto::ring::default_provider().install_default();
    let dialer = Arc::new(Dialer::new(ClientConfig::builder().with_bind_default().with_native_certs().build()).unwrap());
    let now_us = || SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;
    let nonce = format!("{:x}", now_us() as u32);
    let ns: Vec<String> = ["probe", "fanout", &nonce, "server"].iter().map(|s| (*s).to_owned()).collect();
    let track_name = |i: usize| if shared { "world".to_owned() } else { format!("c{i}") };

    let open = |d: Arc<Dialer>, url: String| async move { MoqConn::setup(d.dial(&url).await.unwrap()).await.unwrap().0 };
    let publisher = open(dialer.clone(), url.clone()).await;
    publisher.announce(ns.clone()).await.unwrap();
    let tracks: Vec<_> = if shared {
        vec![publisher.out_track(&ns, "world")]
    } else {
        (0..n).map(|i| publisher.out_track(&ns, &format!("c{i}"))).collect()
    };
    println!("{mode}: {n} subscribers, {secs} s, {burst} x {size} B at {hz} Hz; ns {}", ns.join("/"));

    // per subscriber: (seqs seen, latencies ms)
    type Stat = (Vec<u32>, Vec<f64>);
    let stats: Vec<Arc<Mutex<Stat>>> = (0..n).map(|_| Arc::default()).collect();
    let mut subs = Vec::new();
    let joined_at = Instant::now();
    let joins = futures::future::join_all((0..n).map(|i| {
        let (d, url, ns, name, stat) = (dialer.clone(), url.clone(), ns.clone(), track_name(i), stats[i].clone());
        async move {
            let conn = open(d, url).await;
            let (tx, mut rx) = futures::channel::mpsc::unbounded();
            let mut tries = 0;
            let sub = loop {
                match conn.subscribe(&ns, &name, tx.clone()).await {
                    Ok(sub) => break sub,
                    Err(e) if tries < 20 => {
                        tries += 1;
                        if tries == 1 {
                            println!("sub {i}: {e}, retrying");
                        }
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                    Err(e) => panic!("sub {i}: {e}"),
                }
            };
            tokio::spawn(async move {
                while let Some(p) = rx.next().await {
                    let b = &p.payload;
                    if b.len() < 12 {
                        continue;
                    }
                    let seq = u32::from_le_bytes(b[0..4].try_into().unwrap());
                    let sent = u64::from_le_bytes(b[4..12].try_into().unwrap());
                    let lat = now_us().saturating_sub(sent) as f64 / 1000.0;
                    let mut s = stat.lock().unwrap();
                    s.0.push(seq);
                    s.1.push(lat);
                }
            });
            (conn, sub)
        }
    }));
    for (conn, sub) in joins.await {
        subs.push((conn, sub));
    }
    println!("{n} subscribed in {:.1} s", joined_at.elapsed().as_secs_f64());
    // let the relay's SUBSCRIBEs land
    tokio::time::sleep(Duration::from_millis(500)).await;
    let subscribed = tracks.iter().filter(|t| t.is_subscribed()).count();
    println!("publisher tracks subscribed by the relay: {subscribed}/{}", tracks.len());

    let quic = || publisher.conn.0.quic_connection().stats();
    let tx0 = quic().udp_tx;
    let started = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / hz as u64));
    let (mut seq, mut sent_dg, mut dropped) = (0u32, 0u64, 0u64);
    while started.elapsed() < Duration::from_secs(secs) {
        tick.tick().await;
        for _ in 0..burst {
            seq += 1;
            let mut p = Vec::with_capacity(size);
            p.extend_from_slice(&seq.to_le_bytes());
            p.extend_from_slice(&now_us().to_le_bytes());
            p.resize(size, 0xAB);
            for t in &tracks {
                match t.datagram(&p) {
                    Some(dg) => match publisher.conn.0.send_datagram(dg) {
                        Ok(()) => sent_dg += 1,
                        Err(_) => dropped += 1,
                    },
                    None => dropped += 1,
                }
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let tx1 = quic().udp_tx;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let out_bytes = tx1.bytes - tx0.bytes;
    let subscribes: Vec<u64> = tracks.iter().map(|t| t.relay_subscribes()).collect();
    println!(
        "publisher: {sent_dg} datagrams sent ({dropped} not sent), UDP out {:.1} kB/s = {:.3} Mbit/s ({} packets), relay SUBSCRIBEs per track {:?}",
        out_bytes as f64 / elapsed / 1000.0,
        out_bytes as f64 * 8.0 / elapsed / 1e6,
        tx1.datagrams - tx0.datagrams,
        if subscribes.len() > 8 { format!("min {} max {}", subscribes.iter().min().unwrap(), subscribes.iter().max().unwrap()) } else { format!("{subscribes:?}") },
    );

    let expected = seq as usize;
    let (mut lost_total, mut dup_total, mut all_lat, mut worst) = (0usize, 0usize, Vec::new(), (0usize, 0usize));
    let mut first_seen = Vec::new();
    for (i, s) in stats.iter().enumerate() {
        let s = s.lock().unwrap();
        let mut seqs = s.0.clone();
        seqs.sort_unstable();
        let before = seqs.len();
        seqs.dedup();
        dup_total += before - seqs.len();
        let got = seqs.iter().filter(|&&q| q >= 1 && q as usize <= expected).count();
        let lost = expected - got;
        lost_total += lost;
        if lost > worst.1 {
            worst = (i, lost);
        }
        first_seen.push(seqs.first().copied().unwrap_or(0));
        all_lat.extend_from_slice(&s.1);
    }
    all_lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |q: f64| all_lat.get(((all_lat.len().max(1) - 1) as f64 * q).round() as usize).copied().unwrap_or(f64::NAN);
    println!(
        "subscribers: {expected} sent each; lost {lost_total} of {} ({:.3}%), worst sub {} lost {}, dups {dup_total}, first seq min {} max {}",
        expected * n,
        100.0 * lost_total as f64 / (expected * n).max(1) as f64,
        worst.0,
        worst.1,
        first_seen.iter().min().unwrap(),
        first_seen.iter().max().unwrap(),
    );
    println!(
        "one-way publisher -> relay -> subscriber ms: p50 {:.1} p90 {:.1} p99 {:.1} max {:.1}",
        pct(0.5),
        pct(0.9),
        pct(0.99),
        pct(1.0)
    );
    for (conn, _) in &subs {
        conn.shutdown();
    }
    publisher.shutdown();
}
#[cfg(target_family = "wasm")]
fn main() {}
