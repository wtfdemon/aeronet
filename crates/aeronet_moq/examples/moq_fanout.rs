//! Probe: what Cloudflare's relay does with one track watched by many.
//!
//! ```sh
//! MOQ_URL=... cargo run -p aeronet_moq --example moq_fanout -- shared 60 30
//! MOQ_URL=... cargo run -p aeronet_moq --example moq_fanout -- unicast 60 30
//! MOQ_URL=... cargo run -p aeronet_moq --example moq_fanout -- sizes 3
//! ```
//!
//! `shared`: every subscriber session subscribes to the same world track(s),
//! the publisher sends each datagram once. `unicast`: one track per
//! subscriber (what rooms do today), one copy per subscriber. `sizes`: which
//! datagram sizes make it through the relay.
//!
//! Env:
//! - FANOUT_SIZE bytes per datagram (1000), FANOUT_BURST datagrams per tick
//!   (1), FANOUT_HZ ticks a second (20)
//! - FANOUT_LANES shared: world tracks a tick's datagrams rotate over (1)
//! - FANOUT_SPACING_US gap between datagrams within a tick (0 = back to back)
//! - FANOUT_PRIVATE shared: also one private track per subscriber carrying
//!   this many bytes a tick (0 = none), as a room's per-player data would
//! - FANOUT_LATE_JOINS shared: this many extra subscribers join one by one
//!   while it runs; prints how long each took to its first object
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
    let (lanes, spacing_us) = (env("FANOUT_LANES", 1).max(1), env("FANOUT_SPACING_US", 0) as u64);
    let (private, late_joins) = (env("FANOUT_PRIVATE", 0), env("FANOUT_LATE_JOINS", 0));
    // a tick's world datagrams as objects 0.. of one group (else one group each)
    let same_group = env("FANOUT_SAME_GROUP", 0) == 1;
    let shared = mode != "unicast";
    _ = aeronet_webtransport::wtransport::tls::rustls::crypto::ring::default_provider().install_default();
    let dialer = Arc::new(Dialer::new(ClientConfig::builder().with_bind_default().with_custom_transport(aeronet_moq::moq_transport()).build()).unwrap());
    let now_us = || SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;
    let nonce = format!("{:x}", now_us() as u32);
    let ns: Vec<String> = ["probe", "fanout", &nonce, "server"].iter().map(|s| (*s).to_owned()).collect();
    let world = |l: usize| format!("world{l}");
    // tracks subscriber i listens to
    let tracks_of = move |i: usize| -> Vec<String> {
        if !shared {
            return vec![format!("c{i}")];
        }
        let mut t: Vec<String> = (0..lanes).map(world).collect();
        if private > 0 {
            t.push(format!("p{i}"));
        }
        t
    };
    let spin = |until: Instant| {
        while Instant::now() < until {
            std::hint::spin_loop();
        }
    };

    let open = |d: Arc<Dialer>, url: String| async move { MoqConn::setup(d.dial(&url).await.unwrap()).await.unwrap().0 };
    let publisher = open(dialer.clone(), url.clone()).await;
    publisher.announce(ns.clone()).await.unwrap();
    let world_tracks: Vec<_> = (0..lanes).map(|l| publisher.out_track(&ns, &world(l))).collect();
    let total_subs = n + late_joins;
    let private_tracks: Vec<_> = if !shared {
        (0..total_subs).map(|i| publisher.out_track(&ns, &format!("c{i}"))).collect()
    } else if private > 0 {
        (0..total_subs).map(|i| publisher.out_track(&ns, &format!("p{i}"))).collect()
    } else {
        Vec::new()
    };
    println!(
        "{mode}: {n} subscribers (+{late_joins} late), {secs} s, {burst} x {size} B at {hz} Hz, {lanes} lane(s), spacing {spacing_us} us, private {private} B, same group {same_group}; ns {}",
        ns.join("/")
    );
    println!(
        "publisher max datagram {:?} B, aeronet payload {:?}",
        publisher.conn.0.max_datagram_size(),
        publisher.max_payload()
    );

    // per subscriber: (shared seqs, private seqs, latencies ms)
    #[derive(Default)]
    struct Stat {
        shared: Vec<u32>,
        private: Vec<u32>,
        lat: Vec<f64>,
        sizes: Vec<usize>,
        first_at: Option<Instant>,
    }
    let stats: Vec<Arc<Mutex<Stat>>> = (0..total_subs).map(|_| Arc::default()).collect();
    let subscribe = {
        let (dialer, url, ns) = (dialer.clone(), url.clone(), ns.clone());
        move |i: usize, stat: Arc<Mutex<Stat>>| {
            let (d, url, ns, names) = (dialer.clone(), url.clone(), ns.clone(), tracks_of(i));
            async move {
                let conn = open(d, url).await;
                let (tx, mut rx) = futures::channel::mpsc::unbounded::<aeronet_io::packet::RecvPacket>();
                for name in &names {
                    let mut tries = 0;
                    loop {
                        match conn.subscribe(&ns, name, tx.clone()).await {
                            Ok(_) => break,
                            Err(e) if tries < 20 => {
                                tries += 1;
                                if tries == 1 {
                                    println!("sub {i} {name}: {e}, retrying");
                                }
                                tokio::time::sleep(Duration::from_millis(250)).await;
                            }
                            Err(e) => panic!("sub {i} {name}: {e}"),
                        }
                    }
                }
                tokio::spawn(async move {
                    while let Some(p) = rx.next().await {
                        let b = &p.payload;
                        if b.len() < 13 {
                            continue;
                        }
                        let seq = u32::from_le_bytes(b[0..4].try_into().unwrap());
                        let sent = u64::from_le_bytes(b[4..12].try_into().unwrap());
                        let lat = now_us().saturating_sub(sent) as f64 / 1000.0;
                        let mut s = stat.lock().unwrap();
                        s.first_at.get_or_insert_with(Instant::now);
                        if b[12] == 1 {
                            s.private.push(seq);
                        } else {
                            s.shared.push(seq);
                            s.lat.push(lat);
                            s.sizes.push(b.len());
                        }
                    }
                });
                conn
            }
        }
    };
    let joined_at = Instant::now();
    let conns: Vec<_> = futures::future::join_all((0..n).map(|i| subscribe(i, stats[i].clone()))).await;
    println!("{n} subscribed in {:.1} s", joined_at.elapsed().as_secs_f64());
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!(
        "relay subscribed: world {}/{}, private {}/{}",
        world_tracks.iter().filter(|t| t.is_subscribed()).count(),
        world_tracks.len(),
        private_tracks.iter().filter(|t| t.is_subscribed()).count(),
        private_tracks.len()
    );

    let packet = |seq: u32, tag: u8, len: usize| {
        let mut p = Vec::with_capacity(len);
        p.extend_from_slice(&seq.to_le_bytes());
        p.extend_from_slice(&now_us().to_le_bytes());
        p.push(tag);
        p.resize(len.max(13), 0xAB);
        p
    };

    if mode == "sizes" {
        let mut sizes_sent = Vec::new();
        let mut seq = 0u32;
        for len in (1000..=1500).step_by(20) {
            for _ in 0..10 {
                seq += 1;
                let p = packet(seq, 0, len);
                let r = world_tracks[0].datagram(&p).map(|dg| publisher.conn.0.send_datagram(dg));
                sizes_sent.push((len, matches!(r, Some(Ok(())))));
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let got: Vec<usize> = stats.iter().flat_map(|s| s.lock().unwrap().sizes.clone()).collect();
        for len in (1000..=1500).step_by(20) {
            let sent = sizes_sent.iter().filter(|(l, ok)| *l == len && *ok).count();
            let refused = sizes_sent.iter().filter(|(l, ok)| *l == len && !*ok).count();
            let arrived = got.iter().filter(|&&l| l == len).count();
            println!("payload {len} B: sent {sent} refused locally {refused}, arrived {arrived} of {}", sent * n);
        }
        for c in &conns {
            c.shutdown();
        }
        publisher.shutdown();
        return;
    }

    // late joiners, one every 2 s from 2 s in
    let late = {
        let subscribe = subscribe.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            let mut out = Vec::new();
            for k in 0..late_joins {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let i = n + k;
                let t0 = Instant::now();
                let conn = subscribe(i, stats[i].clone()).await;
                let subscribed = t0.elapsed();
                let deadline = Instant::now() + Duration::from_secs(5);
                while stats[i].lock().unwrap().first_at.is_none() && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                let first = stats[i].lock().unwrap().first_at.map(|t| t.duration_since(t0));
                out.push((subscribed, first, conn));
            }
            out
        })
    };

    let quic = || publisher.conn.0.quic_connection().stats();
    let tx0 = quic().udp_tx;
    let started = Instant::now();
    let tick_len = Duration::from_micros(1_000_000 / hz as u64);
    let (mut seq, mut pseq, mut sent_dg, mut dropped) = (0u32, 0u32, 0u64, 0u64);
    let mut next_tick = Instant::now();
    while started.elapsed() < Duration::from_secs(secs) {
        tokio::time::sleep_until(next_tick.into()).await;
        next_tick += tick_len;
        let mut at = Instant::now();
        let tick_no = seq as u64 / burst as u64 + 1;
        let mut send = |track: &aeronet_moq::conn::OutTrack, p: &[u8], at: &mut Instant, object: Option<u64>| {
            if spacing_us > 0 {
                spin(*at);
                *at += Duration::from_micros(spacing_us);
            }
            let dg = match object {
                Some(o) => track.datagram_object(tick_no, o, p),
                None => track.datagram(p),
            };
            match dg {
                Some(dg) => match publisher.conn.0.send_datagram(dg) {
                    Ok(()) => sent_dg += 1,
                    Err(_) => dropped += 1,
                },
                None => dropped += 1,
            }
        };
        if shared {
            for b in 0..burst {
                seq += 1;
                let p = packet(seq, 0, size);
                send(&world_tracks[b % lanes], &p, &mut at, same_group.then_some(b as u64));
            }
            if private > 0 {
                pseq += 1;
                let p = packet(pseq, 1, private);
                for t in &private_tracks {
                    send(t, &p, &mut at, None);
                }
            }
        } else {
            for _ in 0..burst {
                seq += 1;
                let p = packet(seq, 0, size);
                for t in private_tracks.iter().take(n) {
                    send(t, &p, &mut at, None);
                }
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let tx1 = quic().udp_tx;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let out_bytes = tx1.bytes - tx0.bytes;
    let subs_per_track = |ts: &[Arc<aeronet_moq::conn::OutTrack>]| {
        let v: Vec<u64> = ts.iter().map(|t| t.relay_subscribes()).collect();
        if v.is_empty() { "-".to_owned() } else { format!("min {} max {}", v.iter().min().unwrap(), v.iter().max().unwrap()) }
    };
    println!(
        "publisher: {sent_dg} datagrams sent ({dropped} not sent), UDP out {:.1} kB/s = {:.3} Mbit/s ({} packets); relay SUBSCRIBEs per world track {}, per private track {}",
        out_bytes as f64 / elapsed / 1000.0,
        out_bytes as f64 * 8.0 / elapsed / 1e6,
        tx1.datagrams - tx0.datagrams,
        subs_per_track(&world_tracks),
        subs_per_track(&private_tracks),
    );

    let tally = |which: &dyn Fn(&Stat) -> Vec<u32>, expected: usize, subs: core::ops::Range<usize>| {
        let (mut lost, mut worst, mut dups) = (0usize, 0usize, 0usize);
        for i in subs.clone() {
            let s = stats[i].lock().unwrap();
            let mut q = which(&s);
            q.sort_unstable();
            let before = q.len();
            q.dedup();
            dups += before - q.len();
            let l = expected - q.iter().filter(|&&x| x >= 1 && x as usize <= expected).count();
            lost += l;
            worst = worst.max(l);
        }
        let total = expected * subs.len();
        format!(
            "lost {lost} of {total} ({:.3}%), worst sub lost {worst}, dups {dups}",
            100.0 * lost as f64 / total.max(1) as f64
        )
    };
    println!("shared/world: {}", tally(&|s| s.shared.clone(), seq as usize, 0..n));
    if shared && private > 0 {
        println!("private: {}", tally(&|s| s.private.clone(), pseq as usize, 0..n));
    }
    let mut lat: Vec<f64> = (0..n).flat_map(|i| stats[i].lock().unwrap().lat.clone()).collect();
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |q: f64| lat.get(((lat.len().max(1) - 1) as f64 * q).round() as usize).copied().unwrap_or(f64::NAN);
    println!(
        "one-way publisher -> relay -> subscriber ms: p50 {:.1} p90 {:.1} p99 {:.1} max {:.1}",
        pct(0.5),
        pct(0.9),
        pct(0.99),
        pct(1.0)
    );
    let late = late.await.unwrap();
    for (k, (subscribed, first, _)) in late.iter().enumerate() {
        println!(
            "late join {k}: dial+setup+subscribe {:.0} ms, first object {}",
            subscribed.as_secs_f64() * 1000.0,
            first.map_or("NONE in 5 s".to_owned(), |f| format!("{:.0} ms after start ({:.0} ms after subscribed)", f.as_secs_f64() * 1000.0, (f.saturating_sub(*subscribed)).as_secs_f64() * 1000.0)),
        );
    }
    for c in &conns {
        c.shutdown();
    }
    for (_, _, c) in &late {
        c.shutdown();
    }
    publisher.shutdown();
}
#[cfg(target_family = "wasm")]
fn main() {}
