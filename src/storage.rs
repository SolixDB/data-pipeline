use crate::db::{self, MigrationRow, TokenLaunchRow, TradeRow};
use crate::events::{Migration, TokenLaunch, Trade, TradeDirection};
use crate::messages::TokenLaunchPayload;

use clickhouse::Client;
use redis::AsyncCommands;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, Instant};

use tracing::{info, warn};

/// Initial Pump.fun bonding curve virtual token reserves
const INITIAL_VIRTUAL_TOKEN_RESERVES: u64 = 1_073_000_000_000_000;
/// Above this percent, token is considered "soon migrating"
const SOON_MIGRATING_PROGRESS_PCT: f64 = 80.0;
/// Bound in-memory migrated snapshot
const MAX_RECENT_MIGRATIONS: usize = 10_000;
/// Flush ClickHouse when any buffer reaches this size
const BATCH_SIZE: usize = 100;
/// Or when this much time has passed
const FLUSH_INTERVAL_MS: u64 = 50;

pub enum StorageMessage {
    TokenLaunch(TokenLaunch),
    Trade(Trade),
    Migration(Migration),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MigratedEntry {
    pub mint: String,
    pub pool: String,
    pub signature: String,
    pub slot: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SoonMigratingEntry {
    pub mint: String,
    pub progress_pct: f64,
    pub virtual_token_reserves: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TokenState {
    pub mint: String,
    pub price: f64,
    pub market_cap: f64,
    pub liquidity_sol: f64,
    pub volume_sol: f64,
    pub tx_count: u64,
    pub last_slot: u64,
}

impl TokenState {
    pub fn new(mint: String) -> Self {
        Self {
            mint,
            price: 0.0,
            market_cap: 0.0,
            liquidity_sol: 0.0,
            volume_sol: 0.0,
            tx_count: 0,
            last_slot: 0,
        }
    }
}

pub const REDIS_KEY_MIGRATED: &str = "snapshot:migrated";
pub const REDIS_KEY_SOON_MIGRATING: &str = "snapshot:soon_migrating";
pub const REDIS_KEY_LAUNCHES: &str = "snapshot:launches";
pub const REDIS_KEY_TRADES: &str = "snapshot:trades";

#[derive(Clone, Default)]
pub struct SnapshotState {
    pub migrated: Vec<MigratedEntry>,
    pub soon_migrating: Vec<SoonMigratingEntry>,
    pub launches: Vec<TokenLaunchPayload>,
    pub trades: Vec<Trade>,
    pub tickers: HashMap<String, TokenState>,
}

/// Internal helpers
async fn flush_with_retry<F, Fut>(mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = clickhouse::error::Result<()>>,
{
    for attempt in 0..3 {
        match f().await {
            Ok(_) => return,
            Err(e) => {
                warn!("ClickHouse flush failed (attempt {}): {}", attempt + 1, e);
                sleep(Duration::from_millis(50 * (attempt + 1))).await;
            }
        }
    }
}

/// Storage worker
pub fn spawn_storage_worker(
    mut rx: mpsc::Receiver<StorageMessage>,
    clickhouse: Option<Arc<Client>>,
    redis_url: Option<String>,
    snapshot_state: Option<Arc<RwLock<SnapshotState>>>,
    snapshot_launch_limit: usize,
    snapshot_trade_limit: usize,
) {
    let launch_limit = snapshot_launch_limit.clamp(1, 10_000);
    let trade_limit = snapshot_trade_limit.clamp(1, 50_000);

    tokio::spawn(async move {
        // In-memory state
        let mut migrated_list: Vec<MigratedEntry> = Vec::new();
        let mut curve_reserves: HashMap<String, u64> = HashMap::new();

        let mut last_launches: VecDeque<TokenLaunchPayload> =
            VecDeque::with_capacity(launch_limit + 1);
        let mut recent_trades: VecDeque<Trade> = VecDeque::with_capacity(trade_limit + 1);

        let mut recent_mints: HashSet<String> = HashSet::new();

        // ClickHouse buffers
        let mut launch_buffer: Vec<TokenLaunchRow> = Vec::with_capacity(BATCH_SIZE);
        let mut trade_buffer: Vec<TradeRow> = Vec::with_capacity(BATCH_SIZE);
        let mut migration_buffer: Vec<MigrationRow> = Vec::with_capacity(BATCH_SIZE);

        let mut last_flush = Instant::now();

        // Redis (optional)
        let mut redis_conn = None;
        if let Some(ref url) = redis_url {
            if let Ok(client) = redis::Client::open(url.as_str()) {
                if let Ok(conn) = client.get_multiplexed_async_connection().await {
                    redis_conn = Some(conn);
                    info!("Redis connected for snapshots");
                }
            }
        }

        // Main loop
        while let Some(msg) = rx.recv().await {
            match msg {
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

                    recent_mints.clear();
                    for l in &last_launches {
                        recent_mints.insert(l.mint.clone());
                    }

                    recent_trades.retain(|t| recent_mints.contains(&t.mint));
                    while recent_trades.len() > trade_limit {
                        recent_trades.pop_front();
                    }

                    if clickhouse.is_some() {
                        launch_buffer.push(TokenLaunchRow {
                            signature: launch.signature,
                            slot: launch.slot,
                            mint: launch.mint.clone(),
                            name: launch.name,
                            symbol: launch.symbol,
                            uri: launch.uri,
                            creator: launch.creator,
                            received_at: launch.received_at,
                            sent_at: launch.sent_at,
                        });
                    }

                    if let Some(state) = launch.initial_bonding_curve_state {
                        curve_reserves.insert(launch.mint, state.virtual_token_reserves);
                    }
                }

                StorageMessage::Trade(trade) => {
                    if recent_mints.contains(&trade.mint) {
                        recent_trades.push_back(trade.clone());
                        while recent_trades.len() > trade_limit {
                            recent_trades.pop_front();
                        }
                    }

                    if clickhouse.is_some() {
                        trade_buffer.push(TradeRow {
                            signature: trade.signature,
                            slot: trade.slot,
                            mint: trade.mint.clone(),
                            trader: trade.trader,
                            direction: match trade.direction {
                                TradeDirection::Buy => "buy".into(),
                                TradeDirection::Sell => "sell".into(),
                            },
                            token_amount: trade.token_amount,
                            sol_amount: trade.sol_amount,
                            virtual_token_reserves: trade.virtual_token_reserves,
                            is_create: trade.is_create as u8,
                            pool: trade.pool.clone(),
                            received_at: trade.received_at,
                            sent_at: trade.sent_at,
                        });
                    }

                    if trade.pool.is_none() {
                        curve_reserves.insert(trade.mint, trade.virtual_token_reserves);
                    }
                }

                StorageMessage::Migration(migration) => {
                    if clickhouse.is_some() {
                        migration_buffer.push(MigrationRow {
                            signature: migration.signature.clone(),
                            slot: migration.slot,
                            mint: migration.mint.clone(),
                            user: migration.user,
                            bonding_curve: migration.bonding_curve,
                            pool: migration.pool.clone(),
                            token_amount: migration.token_amount,
                            sol_amount: migration.sol_amount,
                            fee: migration.fee,
                            received_at: migration.received_at,
                            sent_at: migration.sent_at,
                        });
                    }

                    curve_reserves.remove(&migration.mint);
                    migrated_list.push(MigratedEntry {
                        mint: migration.mint,
                        pool: migration.pool,
                        signature: migration.signature,
                        slot: migration.slot,
                    });

                    while migrated_list.len() > MAX_RECENT_MIGRATIONS {
                        migrated_list.remove(0);
                    }
                }
            }

            // Flush logic
            let should_flush = launch_buffer.len() >= BATCH_SIZE
                || trade_buffer.len() >= BATCH_SIZE
                || migration_buffer.len() >= BATCH_SIZE
                || last_flush.elapsed() >= Duration::from_millis(FLUSH_INTERVAL_MS);

            if should_flush {
                if let Some(ref ch) = clickhouse {
                    if !launch_buffer.is_empty() {
                        let rows = std::mem::take(&mut launch_buffer);
                        flush_with_retry(|| db::insert_token_launches_batch(ch, &rows)).await;
                    }
                    if !trade_buffer.is_empty() {
                        let rows = std::mem::take(&mut trade_buffer);
                        flush_with_retry(|| db::insert_trades_batch(ch, &rows)).await;
                    }
                    if !migration_buffer.is_empty() {
                        let rows = std::mem::take(&mut migration_buffer);
                        flush_with_retry(|| db::insert_migrations_batch(ch, &rows)).await;
                    }
                } else {
                    launch_buffer.clear();
                    trade_buffer.clear();
                    migration_buffer.clear();
                }

                last_flush = Instant::now();
            }

            // Derived snapshots
            curve_reserves.retain(|mint, _| recent_mints.contains(mint));

            let soon_migrating: Vec<SoonMigratingEntry> = curve_reserves
                .iter()
                .filter_map(|(mint, &reserves)| {
                    let progress = 1.0 - (reserves as f64 / INITIAL_VIRTUAL_TOKEN_RESERVES as f64);
                    let pct = (progress * 100.0).clamp(0.0, 100.0);
                    (pct >= SOON_MIGRATING_PROGRESS_PCT).then(|| SoonMigratingEntry {
                        mint: mint.clone(),
                        progress_pct: pct,
                        virtual_token_reserves: reserves,
                    })
                })
                .collect();

            // SnapshotState (WS)
            if let Some(ref state) = snapshot_state {
                let mut w = state.write().await;
                w.migrated = migrated_list.clone();
                w.soon_migrating = soon_migrating.clone();
                w.launches = last_launches.iter().cloned().collect();
                w.trades = recent_trades.iter().cloned().collect();
            }

            // Redis snapshots (optional)
            if let Some(ref mut conn) = redis_conn {
                let _: redis::RedisResult<()> = conn
                    .set(
                        REDIS_KEY_MIGRATED,
                        serde_json::to_string(&migrated_list).unwrap_or_default(),
                    )
                    .await;

                let _: redis::RedisResult<()> = conn
                    .set(
                        REDIS_KEY_SOON_MIGRATING,
                        serde_json::to_string(&soon_migrating).unwrap_or_default(),
                    )
                    .await;

                let launches: Vec<_> = last_launches.iter().cloned().collect();
                let trades: Vec<_> = recent_trades.iter().cloned().collect();

                let _: redis::RedisResult<()> = conn
                    .set(
                        REDIS_KEY_LAUNCHES,
                        serde_json::to_string(&launches).unwrap_or_default(),
                    )
                    .await;

                let _: redis::RedisResult<()> = conn
                    .set(
                        REDIS_KEY_TRADES,
                        serde_json::to_string(&trades).unwrap_or_default(),
                    )
                    .await;
            }
        }

        info!("Storage worker exiting, final flush");

        if let Some(ref ch) = clickhouse {
            let _ = db::insert_token_launches_batch(ch, &launch_buffer).await;
            let _ = db::insert_trades_batch(ch, &trade_buffer).await;
            let _ = db::insert_migrations_batch(ch, &migration_buffer).await;
        }
    });
}
