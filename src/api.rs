use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use clickhouse::Client;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

use crate::db;

#[derive(Clone)]
pub struct AppState {
    pub clickhouse: Option<Arc<Client>>,
    #[allow(dead_code)]
    pub health_state: Arc<crate::health::HealthState>,
}

#[derive(Debug, Deserialize)]
pub struct UdfHistoryParams {
    pub symbol: String,
    pub resolution: String,
    pub from: u64,
    pub to: u64,
    #[allow(dead_code)]
    pub countback: Option<u64>,
}

#[derive(serde::Serialize)]
struct UdfHistoryResponse {
    s: String,
    t: Vec<u64>,
    o: Vec<f64>,
    h: Vec<f64>,
    l: Vec<f64>,
    c: Vec<f64>,
    v: Vec<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "nextTime")]
    next_time: Option<u64>,
}

#[derive(serde::Serialize)]
struct UdfConfigResponse {
    supported_resolutions: Vec<String>,
    supports_group_request: bool,
    supports_marks: bool,
    supports_search: bool,
    supports_timescale_marks: bool,
}

#[derive(serde::Serialize)]
struct UdfSymbolInfo {
    name: String,
    full_name: String,
    description: String,
    type_: String,
    session: String,
    exchange: String,
    listed_exchange: String,
    timezone: String,
    format: String,
    pricescale: u64,
    minmov: u64,
    has_intraday: bool,
    supported_resolutions: Vec<String>,
    volume_precision: u64,
    data_status: String,
}

async fn get_config() -> impl IntoResponse {
    let config = UdfConfigResponse {
        supported_resolutions: vec!["1".to_string(), "5".to_string(), "60".to_string(), "D".to_string()],
        supports_group_request: false,
        supports_marks: false,
        supports_search: true,
        supports_timescale_marks: false,
    };
    Json(config)
}

async fn get_symbols(Query(params): Query<std::collections::HashMap<String, String>>) -> impl IntoResponse {
    let symbol = params.get("symbol").cloned().unwrap_or_default();
    let info = UdfSymbolInfo {
        name: symbol.clone(),
        full_name: symbol.clone(),
        description: format!("Token {}", symbol),
        type_: "crypto".to_string(),
        session: "24x7".to_string(),
        exchange: "DEX".to_string(),
        listed_exchange: "DEX".to_string(),
        timezone: "Etc/UTC".to_string(),
        format: "price".to_string(),
        pricescale: 1000000000,
        minmov: 1,
        has_intraday: true,
        supported_resolutions: vec!["1".to_string(), "5".to_string(), "60".to_string(), "D".to_string()],
        volume_precision: 2,
        data_status: "streaming".to_string(),
    };
    
    Json(info)
}

async fn get_history(
    State(state): State<AppState>,
    Query(params): Query<UdfHistoryParams>,
) -> impl IntoResponse {
    if let Some(ref client) = state.clickhouse {
        let interval = match params.resolution.as_str() {
            "1" => 1,
            "5" => 5,
            "15" => 15,
            "30" => 30,
            "60" | "1H" => 60,
            "240" | "4H" => 240,
            "D" | "1D" => 1440,
            _ => 1,
        };

        let result = db::fetch_aggregated_ohlcv(
            client,
            &params.symbol,
            interval,
            params.from * 1_000_000,
            params.to * 1_000_000
        ).await;

        match result {
            Ok(rows) => {
                if rows.is_empty() {
                    return Json(UdfHistoryResponse {
                        s: "no_data".to_string(),
                        t: vec![], o: vec![], h: vec![], l: vec![], c: vec![], v: vec![],
                        next_time: None,
                    }).into_response();
                }

                let mut t = Vec::with_capacity(rows.len());
                let mut o = Vec::with_capacity(rows.len());
                let mut h = Vec::with_capacity(rows.len());
                let mut l = Vec::with_capacity(rows.len());
                let mut c = Vec::with_capacity(rows.len());
                let mut v = Vec::with_capacity(rows.len());

                for row in rows {
                    t.push(row.start_time / 1_000_000);
                    o.push(row.open);
                    h.push(row.high);
                    l.push(row.low);
                    c.push(row.close);
                    v.push(row.volume);
                }

                return Json(UdfHistoryResponse {
                    s: "ok".to_string(),
                    t, o, h, l, c, v,
                    next_time: None
                }).into_response();
            },
            Err(e) => {
                tracing::error!("Failed to fetch history: {}", e);
                return (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response();
            }
        }
    }
    (StatusCode::SERVICE_UNAVAILABLE, "ClickHouse not configured").into_response()
}

async fn get_health(State(_state): State<AppState>) -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

pub struct ApiServer;

impl ApiServer {
    pub async fn start(port: u16, clickhouse: Option<Arc<Client>>, health_state: Arc<crate::health::HealthState>) {
        let state = AppState { clickhouse, health_state };

        let app = Router::new()
            .route("/config", get(get_config))
            .route("/symbols", get(get_symbols))
            .route("/history", get(get_history))
            .route("/health", get(get_health))
            .route("/", get(get_health)) // Root health check
            .layer(CorsLayer::permissive())
            .with_state(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        tracing::info!("API/Health/Metrics server listening on http://{}", addr);

        if let Err(e) = axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app).await {
            tracing::error!("API server error: {}", e);
        }
    }
}
