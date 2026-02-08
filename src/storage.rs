//! Storage worker: receives launch/trade/migration from bounded channel, writes to ClickHouse,
//! maintains migrated + soon_migrating snapshot. Updates in-memory snapshot for WS connect;
//! optionally writes to Redis when REDIS_URL is set.
//!
//! Scale: Snapshot state is bounded (configurable launch/trade limits, 10k migrations; curve_reserves
//! only for those launch mints). One Redis connection reused. ClickHouse writes are batched (N or 50ms).
//! Handler uses send().await for backpressure when the storage channel is full.

use crate::db::{self, MigrationRow, TokenLaunchRow, TradeRow};
use crate::events::{Migration, TokenLaunch, Trade, TradeDirection};
use crate::messages::TokenLaunchPayload;
use clickhouse::Client;
use redis::AsyncCommands;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::Instant;
use tracing::{error, info};

/// Initial Pump.fun bonding curve virtual token reserves (1.073e9 * 1e6).
const INITIAL_VIRTUAL_TOKEN_RESERVES: u64 = 1_073_000_000_000_000;
/// Progress toward migration above this (0–100) is "soon migrating".
const SOON_MIGRATING_PROGRESS_PCT: f64 = 80.0;
/// Max migrated entries kept in snapshot (avoids unbounded growth).
const MAX_RECENT_MIGRATIONS: usize = 10_000;
/// Flush ClickHouse when any buffer reaches this size.
const BATCH_SIZE: usize = 100;
/// Or when this much time has passed since last flush.
const FLUSH_INTERVAL_MS: u64 = 50;

pub enum StorageMessage {
    TokenLaunch(TokenLaunch),
    Trade(Trade),
    Migration(Migration),
}

/// One entry in the migrated snapshot (sent to clients on connect).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MigratedEntry {
    pub mint: String,
    pub pool: String,
    pub signature: String,
    pub slot: u64,
}

/// One entry in the soon_migrating snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SoonMigratingEntry {
    pub mint: String,
    pub progress_pct: f64,
    pub virtual_token_reserves: u64,
}

pub const REDIS_KEY_MIGRATED: &str = "snapshot:migrated";
pub const REDIS_KEY_SOON_MIGRATING: &str = "snapshot:soon_migrating";
pub const REDIS_KEY_LAUNCHES: &str = "snapshot:launches";
pub const REDIS_KEY_TRADES: &str = "snapshot:trades";

/// In-memory snapshot sent to clients on connect (no Redis required).
#[derive(Clone, Default)]
pub struct SnapshotState {
    pub migrated: Vec<MigratedEntry>,
    pub soon_migrating: Vec<SoonMigratingEntry>,
    pub launches: Vec<TokenLaunchPayload>,
    pub trades: Vec<Trade>,
}

/// Spawns the storage worker. Runs until the channel is closed.
/// When `snapshot_state` is set, it is updated each loop so WS can send snapshots on connect without Redis.
/// ClickHouse writes are batched (BATCH_SIZE rows or FLUSH_INTERVAL_MS).
pub fn spawn_storage_worker(
    mut rx: mpsc::Receiver<StorageMessage>,
    clickhouse: Option<Arc<Client>>,
    redis_url: Option<String>,
    snapshot_state: Option<Arc<RwLock<SnapshotState>>>,
    snapshot_launch_limit: usize,
    snapshot_trade_limit: usize,
) {
    let launch_limit = snapshot_launch_limit.max(1).min(10_000);
    let trade_limit = snapshot_trade_limit.max(1).min(50_000);

    tokio::spawn(async move {
        let mut migrated_list: Vec<MigratedEntry> = Vec::new();
        let mut curve_reserves: HashMap<String, u64> = HashMap::new();
        let mut last_launches: VecDeque<TokenLaunchPayload> = VecDeque::with_capacity(launch_limit + 1);
        let mut recent_trades: VecDeque<Trade> = VecDeque::with_capacity(trade_limit + 1);

        let mut launch_buffer: Vec<TokenLaunchRow> = Vec::with_capacity(BATCH_SIZE);
        let mut trade_buffer: Vec<TradeRow> = Vec::with_capacity(BATCH_SIZE);
        let mut migration_buffer: Vec<MigrationRow> = Vec::with_capacity(BATCH_SIZE);
        let mut last_flush = Instant::now();

        let mut redis_conn = None;
        if let Some(ref url) = redis_url {
            if let Ok(client) = redis::Client::open(url.as_str()) {
                if let Ok(conn) = client.get_multiplexed_async_connection().await {
                    redis_conn = Some(conn);
                }
            }
        }

        while let Some(msg) = rx.recv().await {
            match &msg {
                StorageMessage::TokenLaunch(launch) => {
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
                    };
                    last_launches.push_front(payload);
                    while last_launches.len() > launch_limit {
                        last_launches.pop_back();
                    }
                    let recent_mints: HashSet<String> = last_launches.iter().map(|l| l.mint.clone()).collect();
                    recent_trades.retain(|t| recent_mints.contains(&t.mint));
                    while recent_trades.len() > trade_limit {
                        recent_trades.pop_front();
                    }
                    if clickhouse.is_some() {
                        let row = TokenLaunchRow {
                            signature: launch.signature.clone(),
                            slot: launch.slot,
                            mint: launch.mint.clone(),
                            name: launch.name.clone(),
                            symbol: launch.symbol.clone(),
                            uri: launch.uri.clone(),
                            creator: launch.creator.clone(),
                            received_at: launch.received_at,
                            sent_at: launch.sent_at,
                        };
                        launch_buffer.push(row);
                    }
                    if let Some(ref state) = launch.initial_bonding_curve_state {
                        curve_reserves.insert(launch.mint.clone(), state.virtual_token_reserves);
                    }
                }
                StorageMessage::Trade(trade) => {
                    let recent_mints: HashSet<String> = last_launches.iter().map(|l| l.mint.clone()).collect();
                    if recent_mints.contains(&trade.mint) {
                        recent_trades.push_back(trade.clone());
                        while recent_trades.len() > trade_limit {
                            recent_trades.pop_front();
                        }
                    }
                    if clickhouse.is_some() {
                        let row = TradeRow {
                            signature: trade.signature.clone(),
                            slot: trade.slot,
                            mint: trade.mint.clone(),
                            trader: trade.trader.clone(),
                            direction: match trade.direction {
                                TradeDirection::Buy => "buy".to_string(),
                                TradeDirection::Sell => "sell".to_string(),
                            },
                            token_amount: trade.token_amount,
                            sol_amount: trade.sol_amount,
                            virtual_token_reserves: trade.virtual_token_reserves,
                            is_create: if trade.is_create { 1 } else { 0 },
                            pool: trade.pool.clone(),
                            received_at: trade.received_at,
                            sent_at: trade.sent_at,
                        };
                        trade_buffer.push(row);
                    }
                    if trade.pool.is_none() {
                        curve_reserves.insert(trade.mint.clone(), trade.virtual_token_reserves);
                    }
                }
                StorageMessage::Migration(migration) => {
                    if clickhouse.is_some() {
                        let row = MigrationRow {
                            signature: migration.signature.clone(),
                            slot: migration.slot,
                            mint: migration.mint.clone(),
                            user: migration.user.clone(),
                            bonding_curve: migration.bonding_curve.clone(),
                            pool: migration.pool.clone(),
                            token_amount: migration.token_amount,
                            sol_amount: migration.sol_amount,
                            fee: migration.fee,
                            received_at: migration.received_at,
                            sent_at: migration.sent_at,
                        };
                        migration_buffer.push(row);
                    }
                    curve_reserves.remove(&migration.mint);
                    migrated_list.push(MigratedEntry {
                        mint: migration.mint.clone(),
                        pool: migration.pool.clone(),
                        signature: migration.signature.clone(),
                        slot: migration.slot,
                    });
                    while migrated_list.len() > MAX_RECENT_MIGRATIONS {
                        migrated_list.remove(0);
                    }
                }
            }

            // Flush ClickHouse buffers when full or after interval.
            let should_flush = launch_buffer.len() >= BATCH_SIZE
                || trade_buffer.len() >= BATCH_SIZE
                || migration_buffer.len() >= BATCH_SIZE
                || last_flush.elapsed() >= Duration::from_millis(FLUSH_INTERVAL_MS);
            if should_flush {
                if let Some(ref ch) = clickhouse {
                    if !launch_buffer.is_empty() {
                        if let Err(e) = db::insert_token_launches_batch(ch.as_ref(), &launch_buffer).await {
                            error!("ClickHouse insert_token_launches_batch failed: {}", e);
                        }
                        launch_buffer.clear();
                    }
                    if !trade_buffer.is_empty() {
                        if let Err(e) = db::insert_trades_batch(ch.as_ref(), &trade_buffer).await {
                            error!("ClickHouse insert_trades_batch failed: {}", e);
                        }
                        trade_buffer.clear();
                    }
                    if !migration_buffer.is_empty() {
                        if let Err(e) = db::insert_migrations_batch(ch.as_ref(), &migration_buffer).await {
                            error!("ClickHouse insert_migrations_batch failed: {}", e);
                        }
                        migration_buffer.clear();
                    }
                }
                last_flush = Instant::now();
            }

            // Only track curve reserves for mints in last_launches so soon_migrating stays bounded.
            let recent_mints: HashSet<String> = last_launches.iter().map(|l| l.mint.clone()).collect();
            curve_reserves.retain(|mint, _| recent_mints.contains(mint));

            let soon_migrating: Vec<SoonMigratingEntry> = curve_reserves
                .iter()
                .filter_map(|(mint, &reserves)| {
                    let progress = 1.0 - (reserves as f64 / INITIAL_VIRTUAL_TOKEN_RESERVES as f64);
                    let progress_pct = progress * 100.0;
                    if progress_pct >= SOON_MIGRATING_PROGRESS_PCT {
                        Some(SoonMigratingEntry {
                            mint: mint.clone(),
                            progress_pct,
                            virtual_token_reserves: reserves,
                        })
                    } else {
                        None
                    }
                })
                .collect();

            if let Some(ref state) = snapshot_state {
                let launches_vec: Vec<_> = last_launches.iter().cloned().collect();
                let trades_vec: Vec<_> = recent_trades.iter().cloned().collect();
                let mut w = state.write().await;
                w.migrated = migrated_list.clone();
                w.soon_migrating = soon_migrating.clone();
                w.launches = launches_vec;
                w.trades = trades_vec;
            }

            if let Some(ref mut conn) = redis_conn {
                let _: Result<(), _> = conn.set(REDIS_KEY_MIGRATED, serde_json::to_string(&migrated_list).unwrap_or_default()).await;
                let _: Result<(), _> = conn.set(REDIS_KEY_SOON_MIGRATING, serde_json::to_string(&soon_migrating).unwrap_or_default()).await;
                let launches_vec: Vec<_> = last_launches.iter().cloned().collect();
                let trades_vec: Vec<_> = recent_trades.iter().cloned().collect();
                let _: Result<(), _> = conn.set(REDIS_KEY_LAUNCHES, serde_json::to_string(&launches_vec).unwrap_or_default()).await;
                let _: Result<(), _> = conn.set(REDIS_KEY_TRADES, serde_json::to_string(&trades_vec).unwrap_or_default()).await;
            }
        }
        info!("Storage worker channel closed, exiting");
    });
}
