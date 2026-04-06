use perspective::server::Server;
use perspective::client::{TableInitOptions, UpdateData, UpdateOptions};
use axum::Router;
use std::fmt::Write;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{MissedTickBehavior, interval};

struct OtrTreasury {
    instrument: &'static str,
    tenor: &'static str,
    coupon: f64,
    base_mid: f64,
    spread: f64,
    amplitude: f64,
    phase: f64,
}

const OTR_TREASURIES: [OtrTreasury; 7] = [
    OtrTreasury {
        instrument: "UST-2Y",
        tenor: "2Y",
        coupon: 4.625,
        base_mid: 99.8125,
        spread: 0.0150,
        amplitude: 0.0450,
        phase: 0.20,
    },
    OtrTreasury {
        instrument: "UST-3Y",
        tenor: "3Y",
        coupon: 4.500,
        base_mid: 99.5313,
        spread: 0.0180,
        amplitude: 0.0500,
        phase: 0.70,
    },
    OtrTreasury {
        instrument: "UST-5Y",
        tenor: "5Y",
        coupon: 4.250,
        base_mid: 99.0469,
        spread: 0.0225,
        amplitude: 0.0600,
        phase: 1.10,
    },
    OtrTreasury {
        instrument: "UST-7Y",
        tenor: "7Y",
        coupon: 4.125,
        base_mid: 98.7813,
        spread: 0.0275,
        amplitude: 0.0700,
        phase: 1.60,
    },
    OtrTreasury {
        instrument: "UST-10Y",
        tenor: "10Y",
        coupon: 4.000,
        base_mid: 98.4375,
        spread: 0.0310,
        amplitude: 0.0800,
        phase: 2.10,
    },
    OtrTreasury {
        instrument: "UST-20Y",
        tenor: "20Y",
        coupon: 4.375,
        base_mid: 101.1250,
        spread: 0.0380,
        amplitude: 0.0950,
        phase: 2.70,
    },
    OtrTreasury {
        instrument: "UST-30Y",
        tenor: "30Y",
        coupon: 4.250,
        base_mid: 100.6875,
        spread: 0.0450,
        amplitude: 0.1100,
        phase: 3.20,
    },
];

fn round_price(value: f64) -> f64 {
    (value * 100_000.0).round() / 100_000.0
}

fn now_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn market_rows_json(tick: u64) -> String {
    let timestamp = now_epoch_ms();
    let mut json = String::from("[");

    for (idx, security) in OTR_TREASURIES.iter().enumerate() {
        if idx > 0 {
            json.push(',');
        }

        let drift = ((tick as f64 / 6.0) + security.phase).sin() * security.amplitude
            + ((tick as f64 / 13.0) + security.phase * 0.5).cos() * security.amplitude * 0.35;
        let mid = round_price(security.base_mid + drift);
        let bid = round_price(mid - security.spread / 2.0);
        let ask = round_price(mid + security.spread / 2.0);

        let _ = write!(
            json,
            "{{\"instrument\":\"{}\",\"tenor\":\"{}\",\"coupon\":{:.3},\"bid\":{:.5},\"mid\":{:.5},\"ask\":{:.5},\"last_update_epoch_ms\":{},\"tick\":{}}}",
            security.instrument,
            security.tenor,
            security.coupon,
            bid,
            mid,
            ask,
            timestamp,
            tick
        );
    }

    json.push(']');
    json
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let server = Server::new(None);

    let client = server.new_local_client();
    let mut opts = TableInitOptions::default();
    opts.set_name("my_table");
    opts.index = Some("instrument".to_string());

    let table = client
        .table(UpdateData::JsonRows(market_rows_json(0)).into(), opts)
        .await?;

    tokio::spawn(async move {
        let mut tick: u64 = 1;
        let mut updates = interval(Duration::from_secs(1));
        updates.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            updates.tick().await;

            if let Err(error) = table
                .update(
                    UpdateData::JsonRows(market_rows_json(tick)),
                    UpdateOptions::default(),
                )
                .await
            {
                eprintln!("failed to publish OTR Treasury update: {error}");
                break;
            }

            tick = tick.saturating_add(1);
        }
    });

    let app = Router::new()
        .route("/ws", perspective::axum::websocket_handler())
        .with_state(server);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:4000").await?;
    tracing::info!("VortexServer listening on http://localhost:4000");
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}
