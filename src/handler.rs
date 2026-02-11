use crate::config::Config;
use crate::db;
use crate::events::{TradeDirection, Migration};
use crate::health::HealthState;
use crate::messages::{ServerMessage, TokenLaunchPayload, TokenSecurityUpdate, TokenTicker};
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

// #region agent log
const DEBUG_LOG: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/.cursor/debug.log");
const SIG_CACHE_TTL_MICROS: u64 = 60_000_000; // 60 seconds
fn debug_log(location: &str, message: &str, data: serde_json::Value, hypothesis_id: &str) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let line = serde_json::json!({
        "timestamp": ts,
        "location": location,
        "message": message,
        "data": data,
        "hypothesisId": hypothesis_id
    });
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(DEBUG_LOG) {
        use std::io::Write;
        let _ = writeln!(f, "{}", line);
    }
}
// #endregion

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
    let recent_sigs = Arc::new(RwLock::new(HashMap::<String, u64>::new()));

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

    let volume_delay_secs = crate::storage::volume_display_delay_secs();
    let mut volume_interval = tokio::time::interval(std::time::Duration::from_secs(volume_delay_secs));
    volume_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let _ = volume_interval.tick().await;

    loop {
        tokio::select! {
            msg = stream.next() => {
                let message = match msg {
                    None => break,
                    Some(Err(e)) => {
                        error!("Stream error: {:?}", e);
                        return Err(anyhow::anyhow!("Stream error: {}", e));
                    }
                    Some(Ok(m)) => m,
                };
                match message.update_oneof {
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
                                    recent_sigs.clone(),
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
            _ = volume_interval.tick() => {
                if let Some(ref state_lock) = snapshot_state {
                    let r = state_lock.read().await;
                    let delay = crate::storage::volume_display_delay_secs();
                    let tickers: Vec<TokenTicker> = r.tickers.values()
                        .map(|t| {
                            let vol = t.volume_metrics_delayed(delay);
                            TokenTicker {
                                mint: t.mint.clone(),
                                price: t.price,
                                market_cap: t.market_cap,
                                liquidity_sol: t.liquidity_sol,
                                volume_metrics: vol,
                                total_fees_sol: t.total_fees_sol,
                                last_slot: t.last_slot,
                                migrated_to_pool: t.migrated_to_pool.clone(),
                            }
                        })
                        .collect();
                    drop(r);
                    if !tickers.is_empty() {
                        if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::SnapshotTickers(tickers)) {
                            ws_server.broadcast("tickers".to_string(), bytes);
                        }
                    }
                }
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
    recent_sigs: Arc<RwLock<HashMap<String, u64>>>,
) -> anyhow::Result<()> {
    if let Some(h) = health_state {
        h.record_activity();
    }

    // #region agent log
    if let Some(meta) = &tx_update.transaction.as_ref().and_then(|t| t.meta.as_ref()) {
        if meta.err.is_some() {
            debug_log(
                "handler.rs:skip_failed",
                "skip_failed_tx",
                serde_json::json!({"err": true}),
                "H1",
            );
            return Ok(());
        }
    } else {
        debug_log(
            "handler.rs:no_meta",
            "process_no_meta",
            serde_json::json!({"has_tx": tx_update.transaction.is_some()}),
            "H1",
        );
    }
    // #endregion

    // Deduplicate at transaction level: if we already processed this tx in the last minute, skip the whole tx.
    let now = unix_timestamp_micros();
    if let Some(ref t) = tx_update.transaction {
        if !t.signature.is_empty() {
            let sig_str = bs58::encode(&t.signature).into_string();
            let mut cache = recent_sigs.write().await;
            cache.retain(|_, ts| now.saturating_sub(*ts) <= SIG_CACHE_TTL_MICROS);
            if cache.get(&sig_str).is_some() {
                debug!("Skip duplicate tx: sig={}..", sig_str.chars().take(16).collect::<String>());
                return Ok(());
            }
            cache.insert(sig_str, now);
        }
    }

    let instructions = InstructionUpdate::parse_from_txn(tx_update)
        .map_err(|e| anyhow::anyhow!("parse_from_txn: {:?}", e))?;
    let received_at = unix_timestamp_micros();

    let mut launches = multi_parser::parse_token_launches(&instructions, parser_map, received_at);
    let mut trades = multi_parser::parse_trade_events(&instructions, parser_map, received_at);
    multi_parser::enrich_token_launches(&instructions, parser_map, &mut launches, &trades);

    // Mint -> creator so we can mark dev buy (trader == creator); frontend can show mcap when only dev bought.
    let mut mint_creator: HashMap<String, String> = HashMap::new();
    for launch in &launches {
        mint_creator.insert(launch.mint.clone(), launch.creator.clone());
    }
    if let Some(ref state_lock) = snapshot_state {
        let mut w = state_lock.write().await;
        for launch in &launches {
            w.recent_launches.push_back((launch.mint.clone(), launch.creator.clone(), received_at));
        }
        let cutoff = received_at.saturating_sub(crate::storage::RECENT_LAUNCHES_WINDOW_MICROS);
        while w.recent_launches.front().map(|(_, _, ts)| *ts < cutoff).unwrap_or(false) {
            w.recent_launches.pop_front();
        }
        for (mint, creator, _) in &w.recent_launches {
            mint_creator.entry(mint.clone()).or_insert_with(|| creator.clone());
        }
        for l in &w.launches {
            mint_creator.entry(l.mint.clone()).or_insert_with(|| l.creator.clone());
        }
    }
    for trade in &mut trades {
        trade.is_dev_buy = mint_creator
            .get(&trade.mint)
            .map(|c| c == &trade.trader)
            .unwrap_or(false);
    }

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

        // Add to known_mints immediately so trades in this tx (e.g. dev buy) are not skipped.
        known_mints.write().await.insert(launch.mint.clone());

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

        let (price, market_cap, liquidity_sol) = launch
            .initial_bonding_curve_state
            .map(|s| {
                let v_sol = s.virtual_sol_reserves as f64;
                let v_tok = s.virtual_token_reserves as f64;
                let price = if v_tok > 0.0 {
                    (v_sol / v_tok) / 1000.0
                } else {
                    0.0
                };
                let market_cap = price * 1_000_000_000.0;
                let liquidity_sol = s.real_sol_reserves as f64 / 1_000_000_000.0;
                (price, market_cap, liquidity_sol)
            })
            .unwrap_or((0.0, 0.0, 0.0));

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
            price,
            market_cap,
            liquidity_sol,
        };

        // Send launch (and mcap/price/liquidity) immediately without awaiting; accurate and fast.
        // Security fetch, storage, Redis run in a background task so they never block mcap.
        if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::TokenLaunch(payload)) {
            ws_server.broadcast("launches".to_string(), bytes);
        }
        debug!("{} | clients={}", log_msg, ws_server.client_count());

        let metrics_client_bg = metrics_client.clone();
        let storage_tx_bg = storage_tx.clone();
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
        });
    }

    // Only send trades for mints we've sent a launch for (known_mints is updated synchronously above).
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
            // #region agent log
            let vol_before = token_state.volume_metrics.volume_24h;
            if vol_before > 1e6_f64 || sol_amount > 50.0 || trade.sol_amount > 50_000_000_000 {
                debug_log(
                    "handler.rs:add_trade_before",
                    "add_trade",
                    serde_json::json!({
                        "mint": trade.mint.as_str().get(..16).unwrap_or(&trade.mint),
                        "sig": trade.signature.as_str().get(..16).unwrap_or(&trade.signature),
                        "sol_amount_lamports": trade.sol_amount,
                        "sol_amount_sol": sol_amount,
                        "volume_24h_before": vol_before,
                        "trade_history_len": token_state.trade_history.len()
                    }),
                    "H2_H3_H5",
                );
            }
            // #endregion
            let trade_ts_micros = trade.txn_timestamp.saturating_mul(1000);
            finalized_candle = token_state.add_trade(trade_ts_micros, sol_amount, trade.price);
            // #region agent log
            let vol_after = token_state.volume_metrics.volume_24h;
            if vol_after > 1e9_f64 || sol_amount > 1e9_f64 {
                debug_log(
                    "handler.rs:add_trade_after",
                    "suspicious_volume",
                    serde_json::json!({
                        "mint": trade.mint.as_str().get(..20).unwrap_or(&trade.mint),
                        "volume_24h_after": vol_after,
                        "sol_amount_sol": sol_amount,
                        "sol_amount_lamports": trade.sol_amount
                    }),
                    "H2",
                );
            }
            // #endregion

            if sol_amount > 100.0 {
                 info!("signal: VOLUME_SPIKE | mint={} | amount={} SOL", trade.mint, sol_amount);
            }
            if trade.price_change_5m.abs() > 20.0 {
                info!("signal: VOLATILITY_SPIKE | mint={} | 5m_change={}%", trade.mint, trade.price_change_5m);
            }

            let fee_sol = trade.fee as f64 / 1_000_000_000.0;
            token_state.total_fees_sol += fee_sol;

            let delayed = token_state.volume_metrics_delayed(crate::storage::volume_display_delay_secs());
            trade.volume_sol = delayed.volume_24h;
            trade.volume_5m = delayed.volume_5m;
            trade.volume_1h = delayed.volume_1h;
            trade.volume_6h = delayed.volume_6h;

            trade.tx_count = delayed.tx_count_24h;
            trade.tx_count_5m = delayed.tx_count_5m;
            trade.tx_count_1h = delayed.tx_count_1h;
            trade.tx_count_6h = delayed.tx_count_6h;
            trade.price_change_5m = delayed.price_change_5m;
            trade.price_change_1h = delayed.price_change_1h;
            trade.price_change_6h = delayed.price_change_6h;
            trade.price_change_24h = delayed.price_change_24h;

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
        // #region agent log
        if trade.volume_sol > 1e9_f64 {
            debug_log(
                "handler.rs:send_trade",
                "send_volume_sol",
                serde_json::json!({
                    "mint": trade.mint.as_str().get(..20).unwrap_or(&trade.mint),
                    "volume_sol": trade.volume_sol,
                    "volume_24h": trade.volume_sol
                }),
                "H4",
            );
        }
        // #endregion
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
