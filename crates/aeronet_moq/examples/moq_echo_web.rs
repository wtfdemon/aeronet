//! Browser (and native) echo client for `moq_echo server`.
//!
//! On the web the relay URL comes from the page: `?url=<relay url incl. token>`
//! (or `?token=<token>` for Cloudflare's draft-16 relay) and `&prefix=<prefix>`.
//! Natively: `MOQ_URL=... cargo run --example moq_echo_web -- [prefix]`.

use {
    aeronet_io::{Session, connection::Disconnected},
    aeronet_moq::{ClientConfig, MoqClient, MoqClientPlugin, MoqTarget},
    bevy::{log::LogPlugin, prelude::*},
    bevy_platform::time::Instant,
    bytes::Bytes,
    core::time::Duration,
};

#[derive(Resource)]
struct Echo {
    started: Instant,
    seq: u32,
    sent: u64,
    got: u64,
    rtts: Vec<f64>,
    window_start: Instant,
    last_send: Option<Instant>,
}

#[cfg(target_family = "wasm")]
fn settings() -> (String, String, String) {
    let search = js_sys::Reflect::get(&js_sys::global(), &"location".into())
        .and_then(|l| js_sys::Reflect::get(&l, &"search".into()))
        .ok()
        .and_then(|s| s.as_string())
        .unwrap_or_default();
    let get = |key: &str| {
        search
            .trim_start_matches('?')
            .split('&')
            .find_map(|kv| kv.strip_prefix(&format!("{key}=")))
            .map(|v| js_sys::decode_uri_component(v).ok().and_then(|s| s.as_string()).unwrap_or_default())
    };
    let url = get("url").unwrap_or_else(|| {
        format!(
            "https://draft-16.cloudflare.mediaoverquic.com/{}",
            get("token").unwrap_or_default()
        )
    });
    let prefix = get("prefix").unwrap_or_else(|| "aeronet-moq/echo".to_owned());
    let id = format!("{:08x}", (js_sys::Math::random() * f64::from(u32::MAX)) as u32);
    (url, prefix, id)
}

#[cfg(not(target_family = "wasm"))]
fn settings() -> (String, String, String) {
    let url = std::env::var("MOQ_URL").expect("set MOQ_URL");
    let prefix = std::env::args().nth(1).unwrap_or_else(|| "aeronet-moq/echo".to_owned());
    let id = format!("{:08x}", std::process::id().wrapping_mul(2_654_435_761));
    (url, prefix, id)
}

fn main() {
    let (url, prefix, id) = settings();
    let target = MoqTarget::new(url, &prefix);
    App::new()
        .add_plugins((
            MinimalPlugins,
            LogPlugin {
                filter: "warn,aeronet_moq=debug,moq_echo_web=info".into(),
                ..default()
            },
            MoqClientPlugin,
        ))
        .insert_resource(Echo {
            started: Instant::now(),
            seq: 0,
            sent: 0,
            got: 0,
            rtts: Vec::new(),
            window_start: Instant::now(),
            last_send: None,
        })
        .add_observer(|t: On<Disconnected>| warn!("disconnected: {:?}", t.reason))
        .add_systems(Startup, move |mut commands: Commands| {
            info!("client {id} -> prefix {}", target.prefix.join("/"));
            commands
                .spawn_empty()
                .queue(MoqClient::connect(config(), target.clone(), id.clone()));
        })
        .add_systems(Update, tick)
        .run();
}

fn config() -> ClientConfig {
    #[cfg(target_family = "wasm")]
    {
        ClientConfig::default()
    }
    #[cfg(not(target_family = "wasm"))]
    {
        ClientConfig::builder().with_bind_default().with_native_certs().build()
    }
}

fn tick(mut echo: ResMut<Echo>, mut sessions: Query<Mut<Session>, With<MoqClient>>) {
    let Ok(mut session) = sessions.single_mut() else {
        return;
    };
    if session.is_added() {
        info!("connected through the relay, mtu {}", session.mtu());
    }
    let now = Instant::now();
    let now_us = now.duration_since(echo.started).as_micros() as u64;
    for packet in session.recv.drain(..) {
        if packet.payload.len() >= 12 {
            let sent = u64::from_le_bytes(packet.payload[4..12].try_into().unwrap());
            echo.rtts.push(now_us.saturating_sub(sent) as f64 / 1000.0);
            echo.got += 1;
        }
    }
    if echo.last_send.is_none_or(|t| now.duration_since(t) >= Duration::from_micros(16_667)) {
        echo.last_send = Some(now);
        echo.seq += 1;
        let mut p = Vec::with_capacity(200);
        p.extend_from_slice(&echo.seq.to_le_bytes());
        p.extend_from_slice(&now_us.to_le_bytes());
        p.resize(200, 0);
        session.send.push(Bytes::from(p));
        echo.sent += 1;
    }
    if now.duration_since(echo.window_start) >= Duration::from_secs(5) {
        let mut r = core::mem::take(&mut echo.rtts);
        r.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pct = |q: f64| r.get(((r.len().max(1) - 1) as f64 * q).round() as usize).copied().unwrap_or(f64::NAN);
        info!(
            "5s: sent {} echoed {} | rtt ms p50 {:.1} p95 {:.1} max {:.1}",
            echo.sent, echo.got, pct(0.5), pct(0.95), pct(1.0)
        );
        echo.sent = 0;
        echo.got = 0;
        echo.window_start = now;
    }
}
