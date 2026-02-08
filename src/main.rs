mod client;
mod config;
mod db;
mod events;
mod handler;
mod health;
mod messages;
mod protocols;
mod multi_parser;
mod storage;
mod ws_server;

use anyhow::Context;
use backoff::{future, ExponentialBackoff, Error as BackoffError};
use client::GrpcClient;
use config::Config;
use handler::handle_stream_updates;
use futures_util::TryFutureExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use storage::SnapshotState;
use ws_server::WsServer;
use yellowstone_grpc_proto::prelude::{
    SubscribeRequest, SubscribeRequestFilterTransactions,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    let config = Config::default();
    config.validate()?;
    let config_arc = Arc::new(config);

    let ws_port: u16 = std::env::var("WS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5566);

    let ws_config = ws_server::WsConfig::default();
    let snapshot_state = Arc::new(RwLock::new(SnapshotState::default()));
    let redis_client: Option<Arc<redis::Client>> = config_arc
        .redis_url
        .as_ref()
        .and_then(|url| redis::Client::open(url.as_str()).ok())
        .map(Arc::new);
    let ws_server = Arc::new(WsServer::new(ws_config, Some(snapshot_state.clone()), redis_client));
    let ws_handle = ws_server.start(ws_port).await;

    let client = Arc::new(GrpcClient::new((*config_arc).clone()));

    let mut metrics_client: Option<Arc<clickhouse::Client>> = if config_arc.skip_clickhouse {
        info!("ClickHouse disabled (SKIP_CLICKHOUSE=1)");
        None
    } else {
        config_arc
            .clickhouse_url
            .as_ref()
            .map(|url| {
                let c = db::make_client(
                    url,
                    config_arc.clickhouse_user.as_deref(),
                    config_arc.clickhouse_password.as_deref(),
                );
                Arc::new(c)
            })
    };
    if let Some(ref c) = metrics_client {
        db::ensure_table(c.as_ref()).await.map_err(|e| anyhow::anyhow!("ClickHouse ensure_table: {}", e))?;
        db::ensure_token_launches_table(c.as_ref()).await.map_err(|e| anyhow::anyhow!("ClickHouse token_launches: {}", e))?;
        db::ensure_trades_table(c.as_ref()).await.map_err(|e| anyhow::anyhow!("ClickHouse trades: {}", e))?;
        db::ensure_migrations_table(c.as_ref()).await.map_err(|e| anyhow::anyhow!("ClickHouse migrations: {}", e))?;
        if config_arc.clear_clickhouse || config_arc.clear_clickhouse_only {
            db::drop_tables(c.as_ref()).await.map_err(|e| anyhow::anyhow!("ClickHouse drop_tables: {}", e))?;
            db::ensure_table(c.as_ref()).await.map_err(|e| anyhow::anyhow!("ClickHouse ensure_table: {}", e))?;
            db::ensure_token_launches_table(c.as_ref()).await.map_err(|e| anyhow::anyhow!("ClickHouse token_launches: {}", e))?;
            db::ensure_trades_table(c.as_ref()).await.map_err(|e| anyhow::anyhow!("ClickHouse trades: {}", e))?;
            db::ensure_migrations_table(c.as_ref()).await.map_err(|e| anyhow::anyhow!("ClickHouse migrations: {}", e))?;
            info!("ClickHouse tables dropped and recreated");
        }
        if config_arc.clear_clickhouse_only {
            info!("ClickHouse cleared; not storing further data (CLEAR_CLICKHOUSE_ONLY=1)");
            metrics_client = None;
        }
    }
    let metrics_client_for_handler = metrics_client.clone();

    let (storage_tx, storage_rx) = tokio::sync::mpsc::channel(50_000);
    storage::spawn_storage_worker(
        storage_rx,
        metrics_client.clone(),
        config_arc.redis_url.clone(),
        Some(snapshot_state),
        config_arc.snapshot_launch_limit,
        config_arc.snapshot_trade_limit,
    );

    let backoff = ExponentialBackoff {
        initial_interval: Duration::from_millis(config_arc.reconnect_initial_interval_ms),
        max_interval: Duration::from_millis(config_arc.reconnect_max_interval_ms),
        max_elapsed_time: config_arc
            .reconnect_max_elapsed_time_ms
            .map(Duration::from_millis),
        multiplier: 1.5,
        ..Default::default()
    };

    info!("gRPC endpoint: {}", config_arc.endpoint);
    info!("WebSocket ws://0.0.0.0:{}", ws_port);

    let health_state = Arc::new(health::HealthState::new());
    health_state.record_activity();
    health::spawn_health_server(
        config_arc.health_port,
        config_arc.health_grpc_max_age_secs,
        Arc::clone(&health_state),
    );

    let shutdown_rx = {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let _tx = tx.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down");
            let _ = _tx.send(true);
        });
        rx
    };

    let storage_tx_for_retry = storage_tx.clone();
    let health_state_for_retry = Arc::clone(&health_state);
    let run = future::retry(backoff, move || {
        let client = Arc::clone(&client);
        let config = Arc::clone(&config_arc);
        let ws_server = Arc::clone(&ws_server);
        let metrics_client = metrics_client_for_handler.clone();
        let storage_tx = storage_tx_for_retry.clone();
        let health_state = Arc::clone(&health_state_for_retry);
        let mut shutdown_rx = shutdown_rx.clone();

        async move {
            if *shutdown_rx.borrow() {
                return Err(BackoffError::Permanent(anyhow::anyhow!("Shutdown")));
            }

            let mut grpc = client.connect().await?;
            info!("Connected to blockchain gRPC");
            health_state.record_activity();

            if *shutdown_rx.borrow() {
                return Err(BackoffError::Permanent(anyhow::anyhow!("Shutdown")));
            }

            let parser_map = Arc::new(multi_parser::build_parser_map());
            let tx_account_include: Vec<String> = parser_map
                .keys()
                .map(|k| bs58::encode(k).into_string())
                .collect();
            let mut transactions = HashMap::new();
            transactions.insert(
                "transactions".to_string(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: None,
                    signature: None,
                    account_include: tx_account_include,
                    account_exclude: vec![],
                    account_required: vec![],
                },
            );

            let subscribe_request = SubscribeRequest {
                slots: HashMap::new(),
                accounts: HashMap::new(),
                transactions,
                transactions_status: HashMap::new(),
                entry: HashMap::new(),
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                commitment: Some(config.commitment as i32),
                accounts_data_slice: vec![],
                ping: None,
                from_slot: None,
            };

            let (subscribe_tx, stream) = grpc
                .subscribe_with_request(Some(subscribe_request))
                .await
                .context("subscribe")?;

            tokio::select! {
                result = handle_stream_updates(stream, subscribe_tx, (*config).clone(), ws_server, parser_map, metrics_client, storage_tx, Some(health_state)) => {
                    result.map_err(|e| {
                        error!("Stream: {}", e);
                        BackoffError::transient(e)
                    })
                }
                _ = shutdown_rx.changed() => {
                    Err(BackoffError::Permanent(anyhow::anyhow!("Shutdown")))
                }
            }
        }
        .inspect_err(|e| {
            if !e.to_string().contains("Shutdown") {
                warn!("Connection failed, retrying: {}", e);
            }
        })
    });

    tokio::select! {
        r = run => {
            if let Err(e) = r {
                if !e.to_string().contains("Shutdown") {
                    error!("{}", e);
                }
            }
        }
        _ = ws_handle => {}
    }

    Ok(())
}
