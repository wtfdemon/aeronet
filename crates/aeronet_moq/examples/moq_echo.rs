//! Echo through a MoQ relay at the aeronet IO layer.
//!
//! ```sh
//! MOQ_URL=https://draft-16.cloudflare.mediaoverquic.com/<token> \
//!   cargo run -p aeronet_moq --example moq_echo -- server [prefix]
//! MOQ_URL=... cargo run -p aeronet_moq --example moq_echo -- client [prefix] [seconds]
//! ```
//!
//! The client sends a 60 Hz packet `seq (u32) | sent_at_us (u64) | padding`;
//! the server echoes every packet; the client prints round trip and loss.

use {
    aeronet_io::{Session, connection::Disconnected, server::Closed},
    aeronet_moq::{
        ClientConfig, MoqClient, MoqClientPlugin, MoqServer, MoqServerClient, MoqServerPlugin,
        MoqTarget,
    },
    bevy::{app::ScheduleRunnerPlugin, log::LogPlugin, prelude::*},
    bytes::Bytes,
    core::time::Duration,
    std::time::Instant,
};

#[derive(Resource)]
struct Echo {
    started: Instant,
    seconds: u64,
    seq: u32,
    sent: u64,
    rtts: Vec<f64>,
    got: u64,
    window_start: Instant,
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "client".to_owned());
    let prefix = args.next().unwrap_or_else(|| "aeronet-moq/echo".to_owned());
    let seconds: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let url = std::env::var("MOQ_URL").expect("set MOQ_URL to the relay URL incl. token");
    let target = MoqTarget::new(url, &prefix);

    let mut app = App::new();
    app.add_plugins((
        MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(Duration::from_secs_f64(
            1.0 / std::env::var("ECHO_LOOP_HZ").ok().and_then(|v| v.parse::<f64>().ok()).unwrap_or(120.0),
        ))),
        LogPlugin {
            filter: std::env::var("RUST_LOG").unwrap_or_else(|_| "warn,aeronet_moq=info,moq_echo=info".into()),
            ..default()
        },
    ))
    .add_observer(|t: On<Disconnected>| info!("{} disconnected: {:?}", t.entity, t.reason))
    .add_observer(|t: On<Closed>| {
        error!("server closed: {:?}", t.reason);
        std::process::exit(1);
    });

    let config = || {
        ClientConfig::builder()
            .with_bind_default()
            .with_custom_transport(aeronet_moq::moq_transport())
            .build()
    };

    if mode == "server" {
        app.add_plugins(MoqServerPlugin)
            .add_systems(Startup, move |mut commands: Commands| {
                commands.spawn_empty().queue(MoqServer::open(config(), target.clone()));
            })
            .add_systems(Update, echo_back);
    } else {
        let id = format!("{:08x}", rand_id());
        info!("client id {id}");
        app.add_plugins(MoqClientPlugin)
            .insert_resource(Echo {
                started: Instant::now(),
                seconds,
                seq: 0,
                sent: 0,
                rtts: Vec::new(),
                got: 0,
                window_start: Instant::now(),
            })
            .add_systems(Startup, move |mut commands: Commands| {
                commands
                    .spawn_empty()
                    .queue(MoqClient::connect(config(), target.clone(), id.clone()));
            })
            .add_systems(Update, client_tick);
    }
    app.run();
}

fn rand_id() -> u32 {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    (t.subsec_nanos() ^ std::process::id().rotate_left(16)).wrapping_mul(2_654_435_761)
}

fn echo_back(mut sessions: Query<(&MoqServerClient, Mut<Session>)>) {
    for (client, mut session) in &mut sessions {
        if session.is_added() {
            info!("client {} connected", client.id);
        }
        let session = &mut *session;
        for packet in session.recv.drain(..) {
            session.send.push(packet.payload);
        }
    }
}

fn client_tick(mut echo: ResMut<Echo>, mut sessions: Query<&mut Session, With<MoqClient>>, mut last_send: Local<Option<Instant>>) {
    let Ok(mut session) = sessions.single_mut() else {
        return;
    };
    let now = Instant::now();
    for packet in session.recv.drain(..) {
        if packet.payload.len() >= 12 {
            let sent_us = u64::from_le_bytes(packet.payload[4..12].try_into().unwrap());
            let now_us = now.duration_since(echo.started).as_micros() as u64;
            echo.rtts.push((now_us.saturating_sub(sent_us)) as f64 / 1000.0);
            echo.got += 1;
        }
    }
    // ECHO_SIZE bytes per packet, ECHO_BURST packets per send, ECHO_HZ sends a second
    let env = |k: &str, d: usize| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
    let (size, burst, hz) = (env("ECHO_SIZE", 200).max(12), env("ECHO_BURST", 1), env("ECHO_HZ", 60));
    if last_send.is_none_or(|t| now.duration_since(t) >= Duration::from_micros(1_000_000 / hz as u64)) {
        *last_send = Some(now);
        for _ in 0..burst {
            echo.seq += 1;
            let mut p = Vec::with_capacity(size);
            p.extend_from_slice(&echo.seq.to_le_bytes());
            p.extend_from_slice(&(now.duration_since(echo.started).as_micros() as u64).to_le_bytes());
            p.resize(size.min(session.mtu()), 0);
            session.send.push(Bytes::from(p));
            echo.sent += 1;
        }
    }
    if now.duration_since(echo.window_start) >= Duration::from_secs(5) {
        let mut r = core::mem::take(&mut echo.rtts);
        r.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pct = |q: f64| r.get(((r.len().max(1) - 1) as f64 * q).round() as usize).copied().unwrap_or(f64::NAN);
        info!(
            "5s: sent {} echoed {} ({:.1}% lost) | rtt ms p50 {:.1} p95 {:.1} max {:.1} | mtu {}",
            echo.sent,
            echo.got,
            100.0 * (echo.sent.saturating_sub(echo.got)) as f64 / echo.sent.max(1) as f64,
            pct(0.5),
            pct(0.95),
            pct(1.0),
            session.mtu(),
        );
        echo.sent = 0;
        echo.got = 0;
        echo.window_start = now;
    }
    if echo.seconds > 0 && echo.started.elapsed() > Duration::from_secs(echo.seconds) {
        std::process::exit(0);
    }
}
