//! Against a real MoQ relay (Cloudflare's): what the relay actually does is
//! the point, so no mock. Skipped unless `MOQ_URL` (relay URL with a token)
//! is set. Every test works under its own fresh namespace prefix.
//!
//! ```sh
//! MOQ_URL=https://draft-16.cloudflare.mediaoverquic.com/<token> cargo nextest run -p aeronet_moq --test relay
//! ```

#![cfg(not(target_family = "wasm"))]

use {
    aeronet_io::{Session, connection::Disconnected},
    aeronet_moq::{
        Admission, ClientConfig, MoqClient, MoqClientPlugin, MoqServer, MoqServerPlugin, MoqTarget,
        client::Dialer,
        conn::{MAX_PROVISIONAL, MoqConn},
    },
    bevy::{prelude::*, time::TimeUpdateStrategy},
    std::{
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
};

fn relay() -> Option<String> {
    let url = std::env::var("MOQ_URL").ok().filter(|url| !url.is_empty());
    if url.is_none() {
        eprintln!("MOQ_URL not set: skipped");
    }
    _ = aeronet_webtransport::wtransport::tls::rustls::crypto::ring::default_provider().install_default();
    url
}

fn fresh_prefix(what: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("test/{what}/{:x}", (nanos as u64) ^ u64::from(std::process::id()))
}

fn config() -> ClientConfig {
    ClientConfig::builder().with_bind_default().with_native_certs().build()
}

async fn open(url: &str) -> Arc<MoqConn> {
    let dialer = Dialer::new(config()).unwrap();
    MoqConn::setup(dialer.dial(url).await.unwrap()).await.unwrap().0
}

fn ns(prefix: &str, rest: &[&str]) -> Vec<String> {
    MoqTarget::new("", prefix)
        .prefix
        .into_iter()
        .chain(rest.iter().map(|s| (*s).to_owned()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn made_up_track_names_are_held_only_up_to_the_cap() {
    let Some(url) = relay() else { return };
    let prefix = fresh_prefix("spam");
    let server_ns = ns(&prefix, &["server"]);
    let publisher = open(&url).await;
    publisher.announce(server_ns.clone()).await.unwrap();
    // each relay session may make 50 requests: enough sessions to pass the cap
    let sessions = MAX_PROVISIONAL / 50 + 2;
    let mut accepted = 0;
    for s in 0..sessions {
        let spammer = open(&url).await;
        let results = futures::future::join_all((0..50).map(|i| {
            let (spammer, server_ns) = (&spammer, &server_ns);
            async move {
                let (tx, _rx) = futures::channel::mpsc::unbounded();
                spammer.subscribe(server_ns, &format!("junk{s}.{i}"), tx).await
            }
        }))
        .await;
        accepted += results.iter().filter(|r| r.is_ok()).count();
        spammer.shutdown();
    }
    let held = publisher.provisional_tracks();
    println!("asked {}, accepted {accepted}, publisher holds {held}", sessions * 50);
    assert!(held <= MAX_PROVISIONAL, "publisher holds {held}");
    assert!(accepted <= MAX_PROVISIONAL, "relay got {accepted} accepted");
    // and a named track still works past the cap
    let real = publisher.out_track(&server_ns, "real");
    let watcher = open(&url).await;
    let (tx, _rx) = futures::channel::mpsc::unbounded();
    watcher.subscribe(&server_ns, "real", tx).await.expect("a named track is still served");
    assert!(real.is_subscribed());
    publisher.shutdown();
    watcher.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spent_request_budget_fails_fast() {
    let Some(url) = relay() else { return };
    let prefix = fresh_prefix("budget");
    let conn = open(&url).await;
    let missing = ns(&prefix, &["nobody"]);
    let spend = futures::future::join_all((0..conn.requests_left()).map(|i| {
        let (conn, missing) = (&conn, &missing);
        async move {
            let (tx, _rx) = futures::channel::mpsc::unbounded();
            _ = conn.subscribe(missing, &format!("t{i}"), tx).await;
        }
    }));
    spend.await;
    assert_eq!(conn.requests_left(), 0);
    let started = Instant::now();
    let (tx, _rx) = futures::channel::mpsc::unbounded();
    let result = conn.subscribe(&missing, "one-more", tx).await;
    println!("after the budget: {result:?} in {:?}", started.elapsed());
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(3), "waited {:?}", started.elapsed());
    conn.shutdown();
}

/// Who got in and who was dropped, by client name.
#[derive(Resource, Default, Clone)]
struct Seen(Arc<Mutex<Vec<String>>>);

fn app(url: &str, prefix: &str, admission: Admission) -> App {
    let mut app = App::new();
    app.add_plugins((MinimalPlugins, MoqServerPlugin, MoqClientPlugin))
        .insert_resource(TimeUpdateStrategy::Automatic)
        .init_resource::<Seen>()
        .add_observer(|t: On<Disconnected>, names: Query<&Name>, seen: Res<Seen>| {
            if let Ok(name) = names.get(t.entity) {
                seen.0.lock().unwrap().push(format!("{name} dropped"));
            }
        });
    let target = MoqTarget::new(url, prefix);
    app.world_mut()
        .commands()
        .spawn_empty()
        .queue(MoqServer::open_with(config(), target, admission));
    app.world_mut().flush();
    app
}

fn connect(app: &mut App, url: &str, prefix: &str, name: &str, id: &str) -> Entity {
    let target = MoqTarget::new(url, prefix);
    let client = app
        .world_mut()
        .commands()
        .spawn(Name::new(name.to_owned()))
        .queue(MoqClient::connect(config(), target, id.to_owned()))
        .id();
    app.world_mut().flush();
    client
}

fn connected(app: &mut App, client: Entity) -> bool {
    app.world().get::<Session>(client).is_some()
}

fn run_for(app: &mut App, time: Duration) {
    let until = Instant::now() + time;
    while Instant::now() < until {
        app.update();
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn run_until(app: &mut App, time: Duration, done: impl Fn(&mut App) -> bool) -> bool {
    let until = Instant::now() + time;
    while Instant::now() < until {
        app.update();
        if done(app) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

// A plain test: the app owns aeronet's runtime, which can't be dropped
// inside another one; the attacker's session runs on a runtime of its own.
#[test]
fn only_admitted_ids_join_and_a_nested_announce_kicks_nobody() {
    let Some(url) = relay() else { return };
    let prefix = fresh_prefix("admit");
    let admission = Admission {
        // "good-<group>-<n>" counts against <group>; anything else is refused
        admit: Some(Arc::new(|id: &str| {
            let mut parts = id.split('-');
            (parts.next() == Some("good")).then(|| parts.next().unwrap_or("").to_owned())
        })),
        max_per_group: 2,
        max_joining: 64,
    };
    let mut app = app(&url, &prefix, admission);
    run_for(&mut app, Duration::from_secs(2));

    let victim = connect(&mut app, &url, &prefix, "victim", "good-a-1");
    let second = connect(&mut app, &url, &prefix, "second", "good-a-2");
    let other = connect(&mut app, &url, &prefix, "other", "good-b-1");
    assert!(
        run_until(&mut app, Duration::from_secs(10), |app| {
            connected(app, victim) && connected(app, second) && connected(app, other)
        }),
        "admitted clients join"
    );
    // group a is at its cap; a refused id never joins
    let third = connect(&mut app, &url, &prefix, "third", "good-a-3");
    let refused = connect(&mut app, &url, &prefix, "refused", "bad-1");
    run_for(&mut app, Duration::from_secs(5));
    assert!(!connected(&mut app, third), "a group past its cap gets no session");
    assert!(!connected(&mut app, refused), "an id the game refuses gets no session");

    // anyone with the token announces <prefix>/client/x/good-a-1, then leaves
    let rt = tokio::runtime::Runtime::new().unwrap();
    let attacker = rt.block_on(open(&url));
    rt.block_on(attacker.announce(ns(&prefix, &["client", "x", "good-a-1"])))
        .expect("the relay takes a nested namespace");
    run_for(&mut app, Duration::from_secs(1));
    attacker.shutdown();
    run_for(&mut app, Duration::from_secs(3));
    let seen = app.world().resource::<Seen>().0.lock().unwrap().clone();
    assert!(connected(&mut app, victim), "victim still on; seen {seen:?}");
    assert!(!seen.contains(&"victim dropped".to_owned()), "seen {seen:?}");

    // a client leaving frees its group's place
    app.world_mut().despawn(victim);
    run_for(&mut app, Duration::from_secs(2));
    let fourth = connect(&mut app, &url, &prefix, "fourth", "good-a-4");
    assert!(
        run_until(&mut app, Duration::from_secs(10), |app| connected(app, fourth)),
        "a freed place in the group is taken"
    );
}

/// A server catching up several ticks sends them in one burst: three
/// payloads back to back on ONE broadcast lane, which the relay would keep
/// only the newest of (moq_fanout: 3 arrive as 1). MoqBroadcast spaces a
/// lane's datagrams LANE_GAP apart, so every one arrives. 40 bursts of 3.
#[test]
fn a_burst_on_one_broadcast_lane_arrives_whole() {
    use aeronet_moq::{MoqBroadcast, MoqBroadcastRx};
    let Some(url) = relay() else { return };
    // the tester's own namespace, never a live room's
    let prefix = fresh_prefix("probe-roomcast-burst").replacen("test/", "probe/roomcast/", 1);
    let mut app = app(&url, &prefix, Admission::default());
    run_for(&mut app, Duration::from_secs(2));
    let target = MoqTarget::new(url.clone(), &prefix).with_broadcast();
    let client = app
        .world_mut()
        .commands()
        .spawn(Name::new("watcher"))
        .queue(MoqClient::connect(config(), target, "watcher".to_owned()))
        .id();
    app.world_mut().flush();
    assert!(run_until(&mut app, Duration::from_secs(15), |app| connected(app, client)), "the watcher joins");
    let lanes_watched = |app: &mut App| {
        let mut q = app.world_mut().query::<&MoqBroadcast>();
        q.iter(app.world()).map(MoqBroadcast::watched_lanes).max().unwrap_or(0)
    };
    assert!(run_until(&mut app, Duration::from_secs(10), |app| lanes_watched(app) > 0), "the relay subscribes the lanes");
    run_for(&mut app, Duration::from_millis(500));

    let broadcast = {
        let mut q = app.world_mut().query::<&MoqBroadcast>();
        q.single(app.world()).unwrap().clone()
    };
    let mut received = std::collections::BTreeSet::new();
    let drain = |app: &mut App, received: &mut std::collections::BTreeSet<u32>| {
        if let Some(mut rx) = app.world_mut().get_mut::<MoqBroadcastRx>(client) {
            while let Ok(packet) = rx.0.try_recv() {
                received.insert(u32::from_le_bytes(packet.payload[..4].try_into().unwrap()));
            }
        }
    };
    for burst in 0..40u32 {
        for i in 0..3u32 {
            let n = burst * 3 + i;
            let mut payload = n.to_le_bytes().to_vec();
            payload.resize(200, 0);
            assert!(broadcast.send(0, payload.into()), "lane 0 is watched");
        }
        // a tick later, the next burst
        run_for(&mut app, Duration::from_millis(50));
        drain(&mut app, &mut received);
    }
    run_for(&mut app, Duration::from_secs(1));
    drain(&mut app, &mut received);
    eprintln!("received {} of 120", received.len());
    assert_eq!(received.len(), 120, "every payload of every burst arrived; missing {:?}", (0..120).filter(|n| !received.contains(n)).collect::<Vec<_>>());
}
