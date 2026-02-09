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
const SOON_MIGRATING_PROGRESS_PCT: f64 = 75.0;
/// Bound in-memory migrated snapshot
const MAX_RECENT_MIGRATIONS: usize = 10_000;
/// Flush ClickHouse when any buffer reaches this size
const BATCH_SIZE: usize = 100;
/// Or when this much time has passed
const FLUSH_INTERVAL_MS: u64 = 50;
/// Redis key for bonding curve mapping (for enrichment)
const REDIS_KEY_BONDING_CURVES: &str = "bonding_curves";
/// Redis key for ticker validation
pub const REDIS_KEY_TICKERS: &str = "snapshot:tickers";

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

/// Trade entry for time-windowed volume calculation
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TradeEntry {
    pub timestamp_micros: u64,
    pub sol_amount: f64,
}

/// Time-windowed volume metrics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VolumeMetrics {
    pub volume_5m: f64,
    pub volume_1h: f64,
    pub volume_6h: f64,
    pub volume_24h: f64,
    pub tx_count_5m: u64,
    pub tx_count_1h: u64,
    pub tx_count_6h: u64,
    pub tx_count_24h: u64,
}

impl Default for VolumeMetrics {
    fn default() -> Self {
        Self {
            volume_5m: 0.0,
            volume_1h: 0.0,
            volume_6h: 0.0,
            volume_24h: 0.0,
            tx_count_5m: 0,
            tx_count_1h: 0,
            tx_count_6h: 0,
            tx_count_24h: 0,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TokenState {
    pub mint: String,
    pub price: f64,
    pub market_cap: f64,
    pub liquidity_sol: f64,
    
    // Time-windowed metrics
    #[serde(flatten)]
    pub volume_metrics: VolumeMetrics,
    
    // Cumulative fees earned by token creator (all-time, from both pump.fun and pump_amm)
    #[serde(default)]
    pub total_fees_sol: f64,
    
    // Circular buffer for trade history (last 24h max, ~2880 trades if 30s avg)
    // We keep this reasonably sized - 5000 trades should cover high-frequency tokens
    #[serde(default)]
    pub trade_history: VecDeque<TradeEntry>,
    
    pub last_slot: u64,
    
    // Last cleanup timestamp to avoid recalculating on every trade
    #[serde(default)]
    pub last_cleanup_micros: u64,
}

impl TokenState {
    pub fn new(mint: String) -> Self {
        Self {
            mint,
            price: 0.0,
            market_cap: 0.0,
            liquidity_sol: 0.0,
            volume_metrics: VolumeMetrics::default(),
            total_fees_sol: 0.0,
            trade_history: VecDeque::with_capacity(5000),
            last_slot: 0,
            last_cleanup_micros: 0,
        }
    }
    
    /// Add a trade and update time-windowed volumes
    /// This is O(1) amortized - cleanup only runs periodically
    pub fn add_trade(&mut self, timestamp_micros: u64, sol_amount: f64) {
        // Add to circular buffer
        self.trade_history.push_back(TradeEntry {
            timestamp_micros,
            sol_amount,
        });
        
        // Limit buffer size (drop oldest if exceeds capacity)
        if self.trade_history.len() > 5000 {
            self.trade_history.pop_front();
        }
        
        // Cleanup old trades periodically (every 60 seconds)
        let should_cleanup = self.last_cleanup_micros == 0 
            || timestamp_micros - self.last_cleanup_micros > 60_000_000;
        
        if should_cleanup {
            self.cleanup_and_recalculate(timestamp_micros);
            self.last_cleanup_micros = timestamp_micros;
        } else {
            // Fast path: add to appropriate windows based on current time
            // Use current system time as reference to handle out-of-order trades correctly
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64;
            
            let trade_age = current_time.saturating_sub(timestamp_micros);
            
            // Time windows in microseconds
            const WINDOW_5M: u64 = 5 * 60 * 1_000_000;
            const WINDOW_1H: u64 = 60 * 60 * 1_000_000;
            const WINDOW_6H: u64 = 6 * 60 * 60 * 1_000_000;
            const WINDOW_24H: u64 = 24 * 60 * 60 * 1_000_000;
            
            // Only add to windows where the trade actually belongs
            if trade_age <= WINDOW_5M {
                self.volume_metrics.volume_5m += sol_amount;
                self.volume_metrics.tx_count_5m += 1;
            }
            if trade_age <= WINDOW_1H {
                self.volume_metrics.volume_1h += sol_amount;
                self.volume_metrics.tx_count_1h += 1;
            }
            if trade_age <= WINDOW_6H {
                self.volume_metrics.volume_6h += sol_amount;
                self.volume_metrics.tx_count_6h += 1;
            }
            if trade_age <= WINDOW_24H {
                self.volume_metrics.volume_24h += sol_amount;
                self.volume_metrics.tx_count_24h += 1;
            }
        }
    }
    
    /// Remove trades older than 24h and recalculate all windows
    /// Called periodically (every 60s) to keep metrics accurate
    fn cleanup_and_recalculate(&mut self, current_time_micros: u64) {
        // Time windows in microseconds
        const WINDOW_5M: u64 = 5 * 60 * 1_000_000;
        const WINDOW_1H: u64 = 60 * 60 * 1_000_000;
        const WINDOW_6H: u64 = 6 * 60 * 60 * 1_000_000;
        const WINDOW_24H: u64 = 24 * 60 * 60 * 1_000_000;
        
        // Remove trades older than 24h
        while let Some(oldest) = self.trade_history.front() {
            if current_time_micros - oldest.timestamp_micros > WINDOW_24H {
                self.trade_history.pop_front();
            } else {
                break;
            }
        }
        
        // Recalculate all windows from scratch
        let mut metrics = VolumeMetrics::default();
        
        for trade in &self.trade_history {
            let age = current_time_micros.saturating_sub(trade.timestamp_micros);
            
            if age <= WINDOW_5M {
                metrics.volume_5m += trade.sol_amount;
                metrics.tx_count_5m += 1;
            }
            if age <= WINDOW_1H {
                metrics.volume_1h += trade.sol_amount;
                metrics.tx_count_1h += 1;
            }
            if age <= WINDOW_6H {
                metrics.volume_6h += trade.sol_amount;
                metrics.tx_count_6h += 1;
            }
            if age <= WINDOW_24H {
                metrics.volume_24h += trade.sol_amount;
                metrics.tx_count_24h += 1;
            }
        }
        
        self.volume_metrics = metrics;
    }
    
    /// Force recalculation (useful after loading from Redis)
    pub fn recalculate_volumes(&mut self, current_time_micros: u64) {
        self.cleanup_and_recalculate(current_time_micros);
        self.last_cleanup_micros = current_time_micros;
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
    clear_redis: bool,
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

        // Clear Redis if requested
        if clear_redis {
            if let Some(ref mut conn) = redis_conn {
                use redis::AsyncCommands;
                let keys_to_delete = vec![REDIS_KEY_LAUNCHES, REDIS_KEY_TRADES, REDIS_KEY_MIGRATED, REDIS_KEY_BONDING_CURVES, REDIS_KEY_TICKERS];
                for key in keys_to_delete {
                    if let Err(e) = conn.del::<_, ()>(key).await {
                        warn!("Failed to delete Redis key {}: {:?}", key, e);
                    }
                }
                info!("Redis snapshot keys cleared");
            } else {
                warn!("CLEAR_REDIS is set but Redis is not connected");
            }
        }

        if let Some(ref mut conn) = redis_conn {
            info!("Hydrating state from Redis...");
            
            // 1. Launches
            if let Ok(json) = conn.get::<_, String>(REDIS_KEY_LAUNCHES).await {
                if let Ok(launches) = serde_json::from_str::<Vec<TokenLaunchPayload>>(&json) {
                    for launch in launches.into_iter().rev() { // iterate oldest to newest
                        last_launches.push_front(launch.clone());
                        recent_mints.insert(launch.mint.clone());
                        if let Some(state) = launch.initial_bonding_curve_state {
                            curve_reserves.insert(launch.mint.clone(), state.virtual_token_reserves);
                        }
                        // Populate bonding curve hash in Redis (idempotent)
                        if let Some(ref bc) = launch.pump_accounts.bonding_curve {
                            let _: redis::RedisResult<()> = conn.hset(REDIS_KEY_BONDING_CURVES, &launch.mint, bc).await;
                        }
                    }
                    // Trim to limit
                    while last_launches.len() > launch_limit {
                         last_launches.pop_back();
                    }
                    info!("Loaded {} launches from Redis", last_launches.len());
                }
            }

            // 2. Trades
            if let Ok(json) = conn.get::<_, String>(REDIS_KEY_TRADES).await {
                 if let Ok(trades) = serde_json::from_str::<Vec<Trade>>(&json) {
                    for trade in trades {
                        if recent_mints.contains(&trade.mint) {
                            recent_trades.push_back(trade.clone());
                            // Update curve reserves from trade
                            if trade.is_bonding_curve {
                                curve_reserves.insert(trade.mint.clone(), trade.virtual_token_reserves);
                            }
                        }
                    }
                     // Trim
                    while recent_trades.len() > trade_limit {
                        recent_trades.pop_front();
                    }
                }
            }

            // 3. Tickers
            if let Ok(json) = conn.get::<_, String>(REDIS_KEY_TICKERS).await {
                if let Ok(mut loaded_tickers) = serde_json::from_str::<HashMap<String, TokenState>>(&json) {
                    if let Some(ref state) = snapshot_state {
                        // Recalculate volumes for all loaded tickers based on current time
                        let current_time = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_micros() as u64;
                        
                        for ticker in loaded_tickers.values_mut() {
                            ticker.recalculate_volumes(current_time);
                        }
                        
                        let mut w = state.write().await;
                        // Use extend to merge loaded tickers with any that might have been created by live handler already
                        w.tickers.extend(loaded_tickers.clone()); 
                        info!("Loaded {} tickers from Redis (volumes recalculated)", loaded_tickers.len());
                    }
                }
            }

            // 4. Migrated
            if let Ok(json) = conn.get::<_, String>(REDIS_KEY_MIGRATED).await {
                if let Ok(migrated) = serde_json::from_str::<Vec<MigratedEntry>>(&json) {
                    migrated_list = migrated;
                    // Remove migrated from curve_reserves
                    for m in &migrated_list {
                        curve_reserves.remove(&m.mint);
                    }
                    info!("Loaded {} migrations from Redis", migrated_list.len());
                }
            }
            
            // Update initial snapshot state
            if let Some(ref state) = snapshot_state {
                 let mut w = state.write().await;
                 w.migrated = migrated_list.clone();
                 w.launches = last_launches.iter().cloned().collect();
                 w.trades = recent_trades.iter().cloned().collect();
                 
                 // Recompute soon_migrating
                 w.soon_migrating = curve_reserves
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
                            bonding_curve: launch.pump_accounts.bonding_curve.clone(),
                            associated_bonding_curve: launch.pump_accounts.associated_bonding_curve.clone(),
                            global: launch.pump_accounts.global.clone(),
                            fee_recipient: launch.pump_accounts.fee_recipient.clone(),
                            metadata: launch.pump_accounts.metadata.clone(),
                            creator_vault: launch.pump_accounts.creator_vault.clone(),
                            fee_config: launch.pump_accounts.fee_config.clone(),
                            event_authority: launch.pump_accounts.event_authority.clone(),
                            token_decimals: launch.token_decimals,
                            virtual_sol_reserves: launch.initial_bonding_curve_state.as_ref().map(|s| s.virtual_sol_reserves),
                            virtual_token_reserves: launch.initial_bonding_curve_state.as_ref().map(|s| s.virtual_token_reserves),
                            real_sol_reserves: launch.initial_bonding_curve_state.as_ref().map(|s| s.real_sol_reserves),
                            real_token_reserves: launch.initial_bonding_curve_state.as_ref().map(|s| s.real_token_reserves),
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
                            token_decimals: trade.token_decimals,
                            suggested_compute_unit_limit: trade.suggested_compute_unit_limit,
                            received_at: trade.received_at,
                            sent_at: trade.sent_at,
                        });
                    }

                    if trade.is_bonding_curve {
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
            let mut tickers_map_clone = HashMap::new();
            if let Some(ref state) = snapshot_state {
                let mut w = state.write().await;
                w.migrated = migrated_list.clone();
                w.soon_migrating = soon_migrating.clone();
                w.launches = last_launches.iter().cloned().collect();
                w.trades = recent_trades.iter().cloned().collect();
                
                // Clone map quickly while holding lock (fast)
                tickers_map_clone = w.tickers.clone();
            } // Lock released

            // Redis snapshots (async to avoid blocking)
            if let Some(ref mut conn) = redis_conn {
                // Serialize OUTSIDE the lock (cpu intensive)
                let migrated_json = serde_json::to_string(&migrated_list).unwrap_or_default();
                let soon_migrating_json = serde_json::to_string(&soon_migrating).unwrap_or_default();
                let launches_json = serde_json::to_string(&last_launches.iter().cloned().collect::<Vec<_>>()).unwrap_or_default();
                let trades_json = serde_json::to_string(&recent_trades.iter().cloned().collect::<Vec<_>>()).unwrap_or_default();
                let tickers_json = serde_json::to_string(&tickers_map_clone).unwrap_or_default();

                // Get a MultiplexedConnection clone which is cheap and thread-safe
                let mut conn_clone = conn.clone();
                tokio::spawn(async move {
                    let _: redis::RedisResult<()> = conn_clone.set(REDIS_KEY_MIGRATED, migrated_json).await;
                    let _: redis::RedisResult<()> = conn_clone.set(REDIS_KEY_SOON_MIGRATING, soon_migrating_json).await;
                    let _: redis::RedisResult<()> = conn_clone.set(REDIS_KEY_LAUNCHES, launches_json).await;
                    let _: redis::RedisResult<()> = conn_clone.set(REDIS_KEY_TRADES, trades_json).await;
                    if !tickers_json.is_empty() {
                         let _: redis::RedisResult<()> = conn_clone.set(REDIS_KEY_TICKERS, tickers_json).await;
                    }
                });
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
