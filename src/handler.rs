use crate::config::Config;
use crate::db;
use crate::events::TradeDirection;
use crate::health::HealthState;
use crate::messages::{ServerMessage, TokenLaunchPayload};
use crate::multi_parser;
use crate::storage::{StorageMessage, SnapshotState, TokenState};
use crate::ws_server::WsServer;
use futures::{sink::SinkExt, stream::StreamExt};
use redis::AsyncCommands;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};
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
    redis_client: Option<Arc<redis::Client>>,
) -> anyhow::Result<()> {
    let mut last_ping = std::time::Instant::now();
    let ping_interval = std::time::Duration::from_secs(30);
    let launch_count = Arc::new(AtomicU64::new(0));
    let trade_count = Arc::new(AtomicU64::new(0));
    let migration_count = Arc::new(AtomicU64::new(0));
    
    // Track known mints from launched tokens to filter trades (no bonding curve data in RAM)
    let known_mints = Arc::new(RwLock::new(HashSet::<String>::new()));
    
    // Initialize with existing launches from snapshot
    if let Some(state_lock) = &snapshot_state {
        let state = state_lock.read().await;
        let mut mints = known_mints.write().await;
        for launch in &state.launches {
            mints.insert(launch.mint.clone());
        }
        info!("Initialized known_mints with {} tokens from snapshot", mints.len());
    }

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
                            known_mints.clone(),
                            redis_client.clone(),
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
    known_mints: Arc<RwLock<HashSet<String>>>,
    redis_client: Option<Arc<redis::Client>>,
) -> anyhow::Result<()> {
    if let Some(h) = health_state {
        h.record_activity();
    }

    if let Some(meta) = &tx_update.transaction.as_ref().and_then(|t| t.meta.as_ref()) {
        if meta.err.is_some() {
            return Ok(());
        }
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

        // Clone mint and bonding curve before moving launch into payload
        let mint_for_tracking = launch.mint.clone();
        let bonding_curve_for_tracking = launch.pump_accounts.bonding_curve.clone().unwrap_or_default();

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
        
        // Write bonding curve to Redis for future lookups (enrichment)
        if !bonding_curve_for_tracking.is_empty() {
             if let Some(ref r) = redis_client {
                if let Ok(mut conn) = r.get_multiplexed_async_connection().await {
                     let _: redis::RedisResult<()> = conn.hset("bonding_curves", &mint_for_tracking, &bonding_curve_for_tracking).await;
                }
            }
        }
        
        // Add mint to known_mints set
        known_mints.write().await.insert(mint_for_tracking);
    }

    for mut trade in trades {
        // Filter: only process trades for tokens we've seen launched
        if !known_mints.read().await.contains(&trade.mint) {
            continue;
        }

        // Fix missing pool for bonding curve trades (if extraction failed due to CPI)
        if trade.is_bonding_curve && trade.pool.is_none() {
             if let Some(ref r) = redis_client {
                if let Ok(mut conn) = r.get_multiplexed_async_connection().await {
                     if let Ok(bc) = conn.hget::<_, _, String>("bonding_curves", &trade.mint).await {
                         if !bc.is_empty() {
                             trade.pool = Some(bc);
                         }
                     }
                }
            }
        }
        // Update ticker state
        if let Some(state_lock) = snapshot_state {
            let mut w = state_lock.write().await;
            let token_state = w.tickers.entry(trade.mint.clone())
                .or_insert_with(|| TokenState::new(trade.mint.clone()));

            token_state.price = trade.price;
            token_state.market_cap = trade.market_cap;
            token_state.liquidity_sol = trade.liquidity_sol;
            token_state.last_slot = trade.slot;
            
            // Add trade to time-windowed volume tracking
            // sol_amount is in lamports, convert to SOL
            let sol_amount = trade.sol_amount as f64 / 1_000_000_000.0;
            token_state.add_trade(received_at, sol_amount);
            
            // Accumulate fees (all-time, from both pump.fun and pump_amm)
            // fee is in lamports, convert to SOL
            let fee_sol = trade.fee as f64 / 1_000_000_000.0;
            token_state.total_fees_sol += fee_sol;
            
            // Populate trade with current volume metrics (24h volume for backward compatibility)
            trade.volume_sol = token_state.volume_metrics.volume_24h;
            trade.tx_count = token_state.volume_metrics.tx_count_24h;
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
