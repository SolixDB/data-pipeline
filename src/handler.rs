use crate::config::Config;
use crate::db;
use crate::events::{TradeDirection, Migration};
use crate::health::HealthState;
use crate::messages::{ServerMessage, TokenLaunchPayload, TokenSecurityUpdate};
use crate::multi_parser;
use crate::storage::{StorageMessage, SnapshotState, TokenState, MigratedEntry};
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
    rpc_url: Option<String>,
) -> anyhow::Result<()> {
    let mut last_ping = std::time::Instant::now();
    let ping_interval = std::time::Duration::from_secs(30);
    let launch_count = Arc::new(AtomicU64::new(0));
    let trade_count = Arc::new(AtomicU64::new(0));
    let migration_count = Arc::new(AtomicU64::new(0));
    let known_mints = Arc::new(RwLock::new(HashSet::<String>::new()));

    if let Some(state_lock) = &snapshot_state {
        let state = state_lock.read().await;
        let mut mints = known_mints.write().await;
        for launch in &state.launches {
            mints.insert(launch.mint.clone());
        }
        info!("Initialized known_mints with {} tokens from snapshot", mints.len());
    }

    let mut last_cursor_slot: u64 = 0;
    let mut last_cursor_tx_index: u64 = 0;
    if let Some(state_lock) = &snapshot_state {
        let state = state_lock.read().await;
        last_cursor_slot = state.last_processed_slot;
        last_cursor_tx_index = state.last_processed_tx_index;
        if last_cursor_slot > 0 {
            info!(
                "Stream cursor loaded: slot={}, tx_index={}",
                last_cursor_slot, last_cursor_tx_index
            );
        }
    }

    let mut reorder_buffer: std::collections::BTreeMap<(u64, u64), yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction> = std::collections::BTreeMap::new();
    let max_buffer_size = 1000;

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(UpdateOneof::Transaction(tx_update)) => {
                        let txn_slot = tx_update.slot;
                        let txn_index = tx_update
                            .transaction
                            .as_ref()
                            .map(|info| info.index)
                            .unwrap_or(0);

                        if txn_slot < last_cursor_slot || (txn_slot == last_cursor_slot && txn_index <= last_cursor_tx_index) {
                             debug!("Dropping replayed tx at slot={}, index={}", txn_slot, txn_index);
                             continue;
                        }

                        reorder_buffer.insert((txn_slot, txn_index), tx_update);

                        while let Some(entry) = reorder_buffer.first_key_value() {
                             let ((s, i), _) = entry;
                             let slot = *s;
                             let idx = *i;

                             let is_stable = if let Some((last_s, _)) = reorder_buffer.last_key_value() {
                                 last_s.0 > slot + 2
                             } else {
                                false
                             };

                             if is_stable || reorder_buffer.len() > max_buffer_size {
                                 let (_, tx) = reorder_buffer.pop_first().unwrap();

                                if let Err(e) = handle_transaction(
                                    &tx,
                                    &*parser_map,
                                    &*launch_count,
                                    &*trade_count,
                                    &*migration_count,
                                    ws_server.clone(),
                                    metrics_client.clone(),
                                    &storage_tx,
                                    health_state.as_deref(),
                                    snapshot_state.as_ref().map(|s| s.as_ref()),
                                    known_mints.clone(),
                                    redis_client.clone(),
                                    rpc_url.as_deref(),
                                )
                                .await
                                {
                                    warn!("Transaction error: {:?}", e);
                                } else {
                                    last_cursor_slot = slot;
                                    last_cursor_tx_index = idx;
                                    if let Some(state_lock) = &snapshot_state {
                                        let mut w = state_lock.write().await;
                                        w.last_processed_slot = slot;
                                        w.last_processed_tx_index = idx;
                                    }
                                }
                             } else {
                                 break;
                             }
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
    ws_server: Arc<WsServer>,
    metrics_client: Option<Arc<clickhouse::Client>>,
    storage_tx: &mpsc::Sender<StorageMessage>,
    health_state: Option<&HealthState>,
    snapshot_state: Option<&RwLock<SnapshotState>>,
    known_mints: Arc<RwLock<HashSet<String>>>,
    redis_client: Option<Arc<redis::Client>>,
    rpc_url: Option<&str>,
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
            count, launch.name, launch.symbol, &launch.mint
        );
        launch.sent_at = unix_timestamp_micros();

        let grpc_ms = (received_at as f64 / 1000.0) - (launch.txn_timestamp as f64);
        let server_us = (launch.sent_at - received_at) as f64;

        let mint_for_tracking = launch.mint.clone();
        let bonding_curve_for_tracking = launch
            .pump_accounts
            .bonding_curve
            .clone()
            .unwrap_or_default();

        let payload = TokenLaunchPayload {
            signature: launch.signature.clone(),
            slot: launch.slot,
            mint: launch.mint.clone(),
            name: launch.name.clone(),
            symbol: launch.symbol.clone(),
            uri: launch.uri.clone(),
            creator: launch.creator.clone(),
            pump_accounts: launch.pump_accounts.clone(),
            token_decimals: launch.token_decimals,
            initial_bonding_curve_state: launch.initial_bonding_curve_state,
            pump_constants: launch.pump_constants.clone(),
            security: launch.security.clone(),
        };

        if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::TokenLaunch(payload)) {
            ws_server.broadcast("launches".to_string(), bytes);
        }
        debug!("{} | clients={}", log_msg, ws_server.client_count());

        let metrics_client_bg = metrics_client.clone();
        let storage_tx_bg = storage_tx.clone();
        let known_mints_bg = known_mints.clone();
        let redis_client_bg = redis_client.clone();
        let ws_server_bg = ws_server.clone();
        let rpc_url_bg = rpc_url.map(|s| s.to_string());
        let mint_for_tracking_bg = mint_for_tracking.clone();
        let bonding_curve_for_tracking_bg = bonding_curve_for_tracking.clone();
        let mut launch_bg = launch.clone();

        tokio::spawn(async move {
            if let Some(url) = rpc_url_bg.as_deref() {
                let mint_for_security = launch_bg.mint.clone();
                match tokio::time::timeout(
                    std::time::Duration::from_millis(500),
                    crate::security::fetch_mint_security(url, &mint_for_security),
                )
                .await
                {
                    Ok(Ok(sec)) => {
                        launch_bg.security = Some(sec.clone());
                        let update = TokenSecurityUpdate {
                            mint: launch_bg.mint.clone(),
                            security: sec,
                        };
                        if let Ok(bytes) =
                            rmp_serde::to_vec_named(&ServerMessage::TokenSecurityUpdate(update))
                        {
                            ws_server_bg.broadcast(launch_bg.mint.clone(), bytes);
                        }
                    }
                    Ok(Err(e)) => {
                        debug!("Failed to fetch security for {}: {}", mint_for_security, e);
                    }
                    Err(_) => {
                        debug!("Security fetch timeout for {}", mint_for_security);
                    }
                }
            }

            db::update_metrics_aggregates_optional(
                metrics_client_bg.as_deref(),
                grpc_ms,
                server_us,
            )
            .await;

            if let Err(e) = storage_tx_bg
                .send(StorageMessage::TokenLaunch(launch_bg.clone()))
                .await
            {
                debug!("Storage channel closed while sending TokenLaunch: {:?}", e);
            }

            if !bonding_curve_for_tracking_bg.is_empty() {
                if let Some(ref r) = redis_client_bg {
                    if let Ok(mut conn) = r.get_multiplexed_async_connection().await {
                        let _: redis::RedisResult<()> = conn
                            .hset(
                                "bonding_curves",
                                &mint_for_tracking_bg,
                                &bonding_curve_for_tracking_bg,
                            )
                            .await;
                    }
                }
            }

            known_mints_bg
                .write()
                .await
                .insert(mint_for_tracking_bg);
        });
    }

    for mut trade in trades {
        let mut migration_event: Option<Migration> = None;
        if !known_mints.read().await.contains(&trade.mint) {
             continue;
         }

        let mut finalized_candle: Option<crate::events::Ohclv> = None;
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
        if let Some(state_lock) = snapshot_state {
            let mut w = state_lock.write().await;
            let token_state = w.tickers.entry(trade.mint.clone())
                .or_insert_with(|| TokenState::new(trade.mint.clone()));

            token_state.price = trade.price;
            token_state.market_cap = trade.market_cap;
            token_state.liquidity_sol = trade.liquidity_sol;
            token_state.last_slot = trade.slot;

            let sol_amount = trade.sol_amount as f64 / 1_000_000_000.0;
            finalized_candle = token_state.add_trade(received_at, sol_amount, trade.price);

            if sol_amount > 100.0 {
                 info!("signal: VOLUME_SPIKE | mint={} | amount={} SOL", trade.mint, sol_amount);
            }
            if trade.price_change_5m.abs() > 20.0 {
                info!("signal: VOLATILITY_SPIKE | mint={} | 5m_change={}%", trade.mint, trade.price_change_5m);
            }

            let fee_sol = trade.fee as f64 / 1_000_000_000.0;
            token_state.total_fees_sol += fee_sol;

            trade.volume_sol = token_state.volume_metrics.volume_24h;
            trade.volume_5m = token_state.volume_metrics.volume_5m;
            trade.volume_1h = token_state.volume_metrics.volume_1h;
            trade.volume_6h = token_state.volume_metrics.volume_6h;
            
            trade.tx_count = token_state.volume_metrics.tx_count_24h;
            trade.tx_count_5m = token_state.volume_metrics.tx_count_5m;
            trade.tx_count_1h = token_state.volume_metrics.tx_count_1h;
            trade.tx_count_6h = token_state.volume_metrics.tx_count_6h;
            trade.price_change_5m = token_state.volume_metrics.price_change_5m;
            trade.price_change_1h = token_state.volume_metrics.price_change_1h;
            trade.price_change_6h = token_state.volume_metrics.price_change_6h;
            trade.price_change_24h = token_state.volume_metrics.price_change_24h;

            trade.total_fees_sol = token_state.total_fees_sol;

            if !trade.is_bonding_curve && token_state.migrated_to_pool.is_none() {
                 if let Some(pool) = &trade.pool {
                     if !pool.is_empty() {
                         let pool_clone = pool.clone();
                         token_state.migrated_to_pool = Some(pool_clone.clone());

                         w.migrated.push(MigratedEntry {
                             mint: trade.mint.clone(),
                             pool: pool_clone.clone(),
                             signature: trade.signature.clone(),
                             slot: trade.slot,
                         });

                         migration_event = Some(Migration {
                             signature: trade.signature.clone(),
                             slot: trade.slot,
                             mint: trade.mint.clone(),
                             user: trade.trader.clone(),
                             bonding_curve: "".to_string(),
                             pool: pool_clone,
                             token_amount: 0,
                             sol_amount: 0,
                             fee: 0,
                             txn_timestamp: trade.txn_timestamp,
                             received_at,
                             sent_at: unix_timestamp_micros(),
                         });
                     }
                 }
            }
        }

        if let Some(c) = finalized_candle {
            if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::Ohclv(c.clone())) {
                ws_server.broadcast(c.mint.clone(), bytes);
            }
            if let Err(e) = storage_tx.send(StorageMessage::Ohclv(c)).await {
                 warn!("Failed to send OHLCV to storage: {:?}", e);
            }
        }

        if let Some(mut m) = migration_event {
             if let Some(ref r) = redis_client {
                if let Ok(mut conn) = r.get_multiplexed_async_connection().await {
                     if let Ok(bc) = conn.hget::<_, _, String>("bonding_curves", &m.mint).await {
                         if !bc.is_empty() {
                             m.bonding_curve = bc;
                         }
                     }
                }
            }
            
            let count = migration_count.fetch_add(1, Ordering::Relaxed) + 1;
            let log_msg = format!(
                "#{} MIGRATE (inferred) | mint={} -> pool={}",
                count,
                &m.mint[..m.mint.len().min(8)],
                &m.pool[..m.pool.len().min(8)]
            );
            
             if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::Migration(m.clone())) {
                ws_server.broadcast(m.mint.clone(), bytes);
            }
            storage_tx.send(StorageMessage::Migration(m)).await
                .map_err(|_| anyhow::anyhow!("Storage channel closed"))?;
            debug!("{} | clients={}", log_msg, ws_server.client_count());
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
            ws_server.broadcast(trade.mint.clone(), bytes);
        }
        storage_tx.send(StorageMessage::Trade(trade)).await
            .map_err(|_| anyhow::anyhow!("Storage channel closed"))?;
        debug!("{} | clients={}", log_msg, ws_server.client_count());
    }

    let migrations = multi_parser::parse_migration_events(&instructions, parser_map, received_at);
    for mut migration in migrations {
        if let Some(state_lock) = snapshot_state {
            let mut w = state_lock.write().await;
            let token_state = w.tickers.entry(migration.mint.clone())
                .or_insert_with(|| TokenState::new(migration.mint.clone()));
            token_state.migrated_to_pool = Some(migration.pool.clone());
        }

        let count = migration_count.fetch_add(1, Ordering::Relaxed) + 1;
        let log_msg = format!(
            "#{} MIGRATE | mint={} -> pool={}",
            count,
            &migration.mint[..migration.mint.len().min(8)],
            &migration.pool[..migration.pool.len().min(8)]
        );
        migration.sent_at = unix_timestamp_micros();
        if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::Migration(migration.clone())) {
            ws_server.broadcast(migration.mint.clone(), bytes);
        }
        storage_tx.send(StorageMessage::Migration(migration)).await
            .map_err(|_| anyhow::anyhow!("Storage channel closed"))?;
        debug!("{} | clients={}", log_msg, ws_server.client_count());
    }

    Ok(())
}

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
