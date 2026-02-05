use crate::config::Config;
use crate::db;
use crate::messages::{ServerMessage, TokenLaunchPayload};
use crate::multi_parser;
use crate::ws_server::WsServer;
use futures::{sink::SinkExt, stream::StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{error, info, warn};
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestPing,
};
use yellowstone_vixen_core::instruction::InstructionUpdate;

pub async fn handle_stream_updates(
    mut stream: impl futures::Stream<Item = Result<yellowstone_grpc_proto::prelude::SubscribeUpdate, tonic::Status>>
        + Unpin,
    mut subscribe_tx: impl futures::Sink<SubscribeRequest> + Unpin,
    config: Config,
    ws_server: Arc<WsServer>,
    parser_map: Arc<HashMap<Vec<u8>, &'static str>>,
    metrics_client: Option<Arc<clickhouse::Client>>,
) -> anyhow::Result<()> {
    let mut last_ping = std::time::Instant::now();
    let ping_interval = std::time::Duration::from_secs(30);
    let launch_count = Arc::new(AtomicU64::new(0));

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(UpdateOneof::Transaction(tx_update)) => {
                        if let Err(e) = handle_transaction(
                            &tx_update,
                            &*parser_map,
                            &*launch_count,
                            ws_server.as_ref(),
                            metrics_client.as_ref().map(Arc::as_ref),
                        )
                        .await
                        {
                            warn!("Transaction error: {:?}", e);
                        }
                    }
                    Some(UpdateOneof::Ping(_)) => send_ping(&mut subscribe_tx, &config).await,
                    _ => {}
                }
                if last_ping.elapsed() >= ping_interval {
                    send_ping(&mut subscribe_tx, &config).await;
                    last_ping = std::time::Instant::now();
                }
            }
            Err(e) => {
                error!("Stream error: {:?}", e);
                return Err(anyhow::anyhow!("Stream error: {}", e));
            }
        }
    }

    warn!("Stream ended");
    Ok(())
}

async fn handle_transaction(
    tx_update: &yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction,
    parser_map: &HashMap<Vec<u8>, &'static str>,
    launch_count: &AtomicU64,
    ws_server: &WsServer,
    metrics_client: Option<&clickhouse::Client>,
) -> anyhow::Result<()> {
    let instructions = InstructionUpdate::parse_from_txn(tx_update)
        .map_err(|e| anyhow::anyhow!("parse_from_txn: {:?}", e))?;
    let received_at = unix_timestamp_micros();

    let launches = multi_parser::parse_token_launches(&instructions, parser_map, received_at);
    for mut launch in launches {
        let count = launch_count.fetch_add(1, Ordering::Relaxed) + 1;
        let log_msg = format!(
            "#{} LAUNCH | {} ({}) | mint={}",
            count,
            launch.name,
            launch.symbol,
            &launch.mint
        );
        launch.sent_at = unix_timestamp_micros();

        // Metrics: grpc latency (received_at in us -> ms, txn_timestamp in ms)
        let grpc_ms = (received_at as f64 / 1000.0) - (launch.txn_timestamp as f64);
        let server_us = (launch.sent_at - received_at) as f64;
        db::update_metrics_aggregates_optional(metrics_client, grpc_ms, server_us).await;

        let payload = TokenLaunchPayload {
            signature: launch.signature,
            slot: launch.slot,
            mint: launch.mint,
            name: launch.name,
            symbol: launch.symbol,
            uri: launch.uri,
            creator: launch.creator,
        };
        if let Ok(bytes) = rmp_serde::to_vec(&ServerMessage::TokenLaunch(payload)) {
            ws_server.broadcast(bytes);
        }
        info!("{} | clients={}", log_msg, ws_server.client_count());
    }

    Ok(())
}

/// Current time as Unix timestamp (microseconds since 1970-01-01 00:00:00 UTC).
#[inline]
fn unix_timestamp_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

async fn send_ping(
    subscribe_tx: &mut (impl futures::Sink<SubscribeRequest> + Unpin),
    config: &Config,
) {
    let ping_request = SubscribeRequest {
        slots: HashMap::new(),
        accounts: HashMap::new(),
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        entry: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        commitment: Some(config.commitment as i32),
        accounts_data_slice: vec![],
        ping: Some(SubscribeRequestPing { id: 1 }),
        from_slot: None,
    };
    let _ = subscribe_tx.send(ping_request).await;
}
