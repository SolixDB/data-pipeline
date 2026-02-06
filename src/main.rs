mod client;
mod config;
mod db;
mod events;
mod handler;
mod messages;
mod protocols;
mod multi_parser;
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
use tracing::{error, info, warn};
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

    let ws_port: u16 = std::env::var("WS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5566);

    let ws_config = ws_server::WsConfig::default();
    let ws_server = Arc::new(WsServer::new(ws_config));
    let ws_handle = ws_server.start(ws_port).await;

    let config_arc = Arc::new(config);
    let client = Arc::new(GrpcClient::new((*config_arc).clone()));

    let metrics_client: Option<Arc<clickhouse::Client>> = config_arc
        .clickhouse_url
        .as_ref()
        .map(|url| {
            let c = db::make_client(
                url,
                config_arc.clickhouse_user.as_deref(),
                config_arc.clickhouse_password.as_deref(),
            );
            Arc::new(c)
        });
    if let Some(ref c) = metrics_client {
        db::ensure_table(c.as_ref()).await.map_err(|e| anyhow::anyhow!("ClickHouse ensure_table: {}", e))?;
    }
    let metrics_client_for_handler = metrics_client.clone();

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

    let run = future::retry(backoff, move || {
        let client = Arc::clone(&client);
        let config = Arc::clone(&config_arc);
        let ws_server = Arc::clone(&ws_server);
        let metrics_client = metrics_client_for_handler.clone();
        let mut shutdown_rx = shutdown_rx.clone();

        async move {
            if *shutdown_rx.borrow() {
                return Err(BackoffError::Permanent(anyhow::anyhow!("Shutdown")));
            }

            let mut grpc = client.connect().await?;
            info!("Connected to blockchain gRPC");

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
                result = handle_stream_updates(stream, subscribe_tx, (*config).clone(), ws_server, parser_map, metrics_client) => {
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
