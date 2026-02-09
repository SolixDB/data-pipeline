use crate::config::Config;
use crate::db;
use crate::events::TradeDirection;
use crate::health::HealthState;
use crate::messages::{ServerMessage, TokenLaunchPayload};
use crate::multi_parser;
use crate::storage::{StorageMessage, SnapshotState, TokenState};
use crate::ws_server::WsServer;
use futures::{sink::SinkExt, stream::StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock}; // Added RwLock
use tracing::{debug, error, warn};
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
    storage_tx: mpsc::Sender<StorageMessage>,
    health_state: Option<Arc<HealthState>>,
    snapshot_state: Option<Arc<RwLock<SnapshotState>>>,
) -> anyhow::Result<()> {
    let mut last_ping = std::time::Instant::now();
    let ping_interval = std::time::Duration::from_secs(30);
    let launch_count = Arc::new(AtomicU64::new(0));
    let trade_count = Arc::new(AtomicU64::new(0));
    let migration_count = Arc::new(AtomicU64::new(0));

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(UpdateOneof::Transaction(tx_update)) => {
                        if let Err(e) = handle_transaction(
                            &tx_update,
                            &*parser_map,
                            &*launch_count,
                            &*trade_count,
                            &*migration_count,
                            ws_server.as_ref(),
                            metrics_client.as_ref().map(Arc::as_ref),
                            &storage_tx,
                            health_state.as_deref(),
                            snapshot_state.as_ref().map(|s| s.as_ref()),
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
    trade_count: &AtomicU64,
    migration_count: &AtomicU64,
    ws_server: &WsServer,
    metrics_client: Option<&clickhouse::Client>,
    storage_tx: &mpsc::Sender<StorageMessage>,
    health_state: Option<&HealthState>,
    snapshot_state: Option<&RwLock<SnapshotState>>,
) -> anyhow::Result<()> {
    if let Some(h) = health_state {
        h.record_activity();
    }
    let instructions = InstructionUpdate::parse_from_txn(tx_update)
        .map_err(|e| anyhow::anyhow!("parse_from_txn: {:?}", e))?;
    let received_at = unix_timestamp_micros();

    let mut launches = multi_parser::parse_token_launches(&instructions, parser_map, received_at);
    let trades = multi_parser::parse_trade_events(&instructions, parser_map, received_at);
    multi_parser::enrich_token_launches(&instructions, parser_map, &mut launches, &trades);

    for mut launch in launches {
        let has_display_data = !launch.mint.is_empty()
            && (!launch.name.is_empty() || !launch.symbol.is_empty() || !launch.uri.is_empty());
        if !has_display_data {
            debug!(
                "Skipping launch with no display data: mint={} name={:?} symbol={:?}",
                &launch.mint,
                launch.name.is_empty(),
                launch.symbol.is_empty()
            );
            continue;
        }

        let count = launch_count.fetch_add(1, Ordering::Relaxed) + 1;
        let log_msg = format!(
            "#{} LAUNCH | {} ({}) | mint={}",
            count,
            launch.name,
            launch.symbol,
            &launch.mint
        );
        launch.sent_at = unix_timestamp_micros();

        let grpc_ms = (received_at as f64 / 1000.0) - (launch.txn_timestamp as f64);
        let server_us = (launch.sent_at - received_at) as f64;
        db::update_metrics_aggregates_optional(metrics_client, grpc_ms, server_us).await;

        storage_tx.send(StorageMessage::TokenLaunch(launch.clone())).await
            .map_err(|_| anyhow::anyhow!("Storage channel closed"))?;

        let payload = TokenLaunchPayload {
            signature: launch.signature,
            slot: launch.slot,
            mint: launch.mint,
            name: launch.name,
            symbol: launch.symbol,
            uri: launch.uri,
            creator: launch.creator,
            pump_accounts: launch.pump_accounts,
            token_decimals: launch.token_decimals,
            initial_bonding_curve_state: launch.initial_bonding_curve_state,
            pump_constants: launch.pump_constants,
        };
        if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::TokenLaunch(payload)) {
            ws_server.broadcast(bytes);
        }
        debug!("{} | clients={}", log_msg, ws_server.client_count());
    }

    for mut trade in trades {
        // Update ticker state
        if let Some(state_lock) = snapshot_state {
            let mut w = state_lock.write().await;
            let token_state = w.tickers.entry(trade.mint.clone())
                .or_insert_with(|| TokenState::new(trade.mint.clone()));

            token_state.price = trade.price;
            token_state.market_cap = trade.market_cap;
            token_state.liquidity_sol = trade.liquidity_sol;
            token_state.last_slot = trade.slot;
            // sol_amount is in lamports, convert to SOL
            token_state.volume_sol += trade.sol_amount as f64 / 1_000_000_000.0;
            token_state.tx_count += 1;

            trade.volume_sol = token_state.volume_sol;
            trade.tx_count = token_state.tx_count;
        }
        let count = trade_count.fetch_add(1, Ordering::Relaxed) + 1;
        let dir = match trade.direction {
            TradeDirection::Buy => "BUY",
            TradeDirection::Sell => "SELL",
        };
        let create_marker = if trade.is_create { " (create)" } else { "" };
        let log_msg = format!(
            "#{} {} | {} | mint={} | tokens={} | sol={}{}",
            count,
            dir,
            &trade.trader[..trade.trader.len().min(8)],
            &trade.mint[..trade.mint.len().min(8)],
            trade.token_amount,
            trade.sol_amount,
            create_marker
        );
        trade.sent_at = unix_timestamp_micros();
        if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::Trade(trade.clone())) {
            ws_server.broadcast(bytes);
        }
        storage_tx.send(StorageMessage::Trade(trade)).await
            .map_err(|_| anyhow::anyhow!("Storage channel closed"))?;
        debug!("{} | clients={}", log_msg, ws_server.client_count());
    }

    let migrations = multi_parser::parse_migration_events(&instructions, parser_map, received_at);
    for mut migration in migrations {
        let count = migration_count.fetch_add(1, Ordering::Relaxed) + 1;
        let log_msg = format!(
            "#{} MIGRATE | mint={} -> pool={}",
            count,
            &migration.mint[..migration.mint.len().min(8)],
            &migration.pool[..migration.pool.len().min(8)]
        );
        migration.sent_at = unix_timestamp_micros();
        if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::Migration(migration.clone())) {
            ws_server.broadcast(bytes);
        }
        storage_tx.send(StorageMessage::Migration(migration)).await
            .map_err(|_| anyhow::anyhow!("Storage channel closed"))?;
        debug!("{} | clients={}", log_msg, ws_server.client_count());
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
