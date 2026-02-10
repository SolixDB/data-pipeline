use crate::db::{self, MigrationRow, TokenLaunchRow, TradeRow, OhclvRow, VolumeAggregateRow};
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

const INITIAL_VIRTUAL_TOKEN_RESERVES: u64 = 1_073_000_000_000_000;
const SOON_MIGRATING_PROGRESS_PCT: f64 = 75.0;
const MAX_RECENT_MIGRATIONS: usize = 10_000;
const BATCH_SIZE: usize = 5000;
const FLUSH_INTERVAL_MS: u64 = 50;
const REDIS_KEY_BONDING_CURVES: &str = "bonding_curves";
pub const REDIS_KEY_TICKERS: &str = "snapshot:tickers";
pub const REDIS_KEY_LAST_SLOT: &str = "snapshot:last_slot";
pub const REDIS_KEY_LAST_TX_INDEX: &str = "snapshot:last_tx_index";

pub enum StorageMessage {
    TokenLaunch(TokenLaunch),
    Trade(Trade),
    Migration(Migration),
    Ohclv(crate::events::Ohclv),
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
pub struct TradeEntry {
    pub timestamp_micros: u64,
    pub sol_amount: f64,
    #[serde(default)]
    pub price: f64,
}

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
    #[serde(default)]
    pub price_change_5m: f64,
    #[serde(default)]
    pub price_change_1h: f64,
    #[serde(default)]
    pub price_change_6h: f64,
    #[serde(default)]
    pub price_change_24h: f64,
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
            price_change_5m: 0.0,
            price_change_1h: 0.0,
            price_change_6h: 0.0,
            price_change_24h: 0.0,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TokenState {
    pub mint: String,
    pub price: f64,
    pub market_cap: f64,
    pub liquidity_sol: f64,
    #[serde(flatten)]
    pub volume_metrics: VolumeMetrics,
    #[serde(default)]
    pub total_fees_sol: f64,
    #[serde(default)]
    pub trade_history: VecDeque<TradeEntry>,
    pub last_slot: u64,
    #[serde(default)]
    pub last_cleanup_micros: u64,
    #[serde(default)]
    pub migrated_to_pool: Option<String>,
    #[serde(skip)]
    pub current_candle: Option<crate::events::Ohclv>,
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
            trade_history: VecDeque::with_capacity(1000),
            last_slot: 0,
            last_cleanup_micros: 0,
            migrated_to_pool: None,
            current_candle: None,
        }
    }

    pub fn add_trade(&mut self, timestamp_micros: u64, sol_amount: f64, price: f64) -> Option<crate::events::Ohclv> {
        self.trade_history.push_back(TradeEntry {
            timestamp_micros,
            sol_amount,
            price,
        });

        if self.trade_history.len() > 1000 {
            self.trade_history.pop_front();
        }

        let should_cleanup = self.last_cleanup_micros == 0
            || timestamp_micros - self.last_cleanup_micros > 60_000_000;
        
        if should_cleanup {
            self.cleanup_and_recalculate(timestamp_micros);
            self.last_cleanup_micros = timestamp_micros;
        } else {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64;
            
            let trade_age = current_time.saturating_sub(timestamp_micros);

            const WINDOW_5M: u64 = 5 * 60 * 1_000_000;
            const WINDOW_1H: u64 = 60 * 60 * 1_000_000;
            const WINDOW_6H: u64 = 6 * 60 * 60 * 1_000_000;
            const WINDOW_24H: u64 = 24 * 60 * 60 * 1_000_000;

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

            self.calculate_price_changes(current_time, price);
        }

        let timestamp_sec = timestamp_micros / 1_000_000;
        let candle_start_time = (timestamp_sec / 60) * 60;
        let mut finalized_candle = None;

        if let Some(ref mut c) = self.current_candle {
            if candle_start_time == c.start_time {
                c.high = c.high.max(price);
                c.low = c.low.min(price);
                c.close = price;
                c.volume += sol_amount;
            } else if candle_start_time > c.start_time {
                finalized_candle = Some(c.clone());

                self.current_candle = Some(crate::events::Ohclv {
                    mint: self.mint.clone(),
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: sol_amount,
                    start_time: candle_start_time,
                    duration: 60,
                });
            }
        } else {
            self.current_candle = Some(crate::events::Ohclv {
                mint: self.mint.clone(),
                open: price,
                high: price,
                low: price,
                close: price,
                volume: sol_amount,
                start_time: candle_start_time,
                duration: 60,
            });
        }
        
        finalized_candle
    }

    fn calculate_price_changes(&mut self, current_time_micros: u64, current_price: f64) {
        if current_price == 0.0 || self.trade_history.is_empty() {
            return;
        }

        // Time windows in microseconds
        const WINDOW_5M: u64 = 5 * 60 * 1_000_000;
        const WINDOW_1H: u64 = 60 * 60 * 1_000_000;
        const WINDOW_6H: u64 = 6 * 60 * 60 * 1_000_000;
        const WINDOW_24H: u64 = 24 * 60 * 60 * 1_000_000;
        
        let find_price_ago = |window: u64, history: &VecDeque<TradeEntry>| -> f64 {
            let target_time = current_time_micros.saturating_sub(window);
            let mut closest_price = 0.0;
            let mut min_diff = u64::MAX;
            for trade in history {
                let diff = if trade.timestamp_micros > target_time {
                    trade.timestamp_micros - target_time
                } else {
                    target_time.saturating_sub(trade.timestamp_micros)
                };
                if diff < min_diff {
                    min_diff = diff;
                    closest_price = trade.price;
                }
            }
            closest_price
        };

        let p_5m = find_price_ago(WINDOW_5M, &self.trade_history);
        let p_1h = find_price_ago(WINDOW_1H, &self.trade_history);
        let p_6h = find_price_ago(WINDOW_6H, &self.trade_history);
        let p_24h = find_price_ago(WINDOW_24H, &self.trade_history);

        let calc_pct = |old: f64, new: f64| -> f64 {
            if old == 0.0 { 0.0 } else { ((new - old) / old) * 100.0 }
        };

        self.volume_metrics.price_change_5m = calc_pct(p_5m, current_price);
        self.volume_metrics.price_change_1h = calc_pct(p_1h, current_price);
        self.volume_metrics.price_change_6h = calc_pct(p_6h, current_price);
        self.volume_metrics.price_change_24h = calc_pct(p_24h, current_price);
    }

    fn cleanup_and_recalculate(&mut self, current_time_micros: u64) {
        const WINDOW_5M: u64 = 5 * 60 * 1_000_000;
        const WINDOW_1H: u64 = 60 * 60 * 1_000_000;
        const WINDOW_6H: u64 = 6 * 60 * 60 * 1_000_000;
        const WINDOW_24H: u64 = 24 * 60 * 60 * 1_000_000;

        while let Some(oldest) = self.trade_history.front() {
            if current_time_micros - oldest.timestamp_micros > WINDOW_24H {
                self.trade_history.pop_front();
            } else {
                break;
            }
        }

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
        let current_price = self.price;
        self.calculate_price_changes(current_time_micros, current_price);
    }

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
    pub last_processed_slot: u64,
    pub last_processed_tx_index: u64,
}

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
        let mut migrated_list: Vec<MigratedEntry> = Vec::new();
        let mut curve_reserves: HashMap<String, u64> = HashMap::new();

        let mut last_launches: VecDeque<TokenLaunchPayload> =
            VecDeque::with_capacity(launch_limit + 1);
        let mut recent_trades: VecDeque<Trade> = VecDeque::with_capacity(trade_limit + 1);

        let mut recent_mints: HashSet<String> = HashSet::new();

        let mut launch_buffer: Vec<TokenLaunchRow> = Vec::with_capacity(BATCH_SIZE);
        let mut trade_buffer: Vec<TradeRow> = Vec::with_capacity(BATCH_SIZE);
        let mut migration_buffer: Vec<MigrationRow> = Vec::with_capacity(BATCH_SIZE);
        let mut ohlcv_buffer: Vec<OhclvRow> = Vec::with_capacity(BATCH_SIZE);

        let mut last_flush = Instant::now();
        let mut last_aggregate_flush = Instant::now();

        let mut redis_conn = None;
        if let Some(ref url) = redis_url {
            if let Ok(client) = redis::Client::open(url.as_str()) {
                if let Ok(conn) = client.get_multiplexed_async_connection().await {
                    redis_conn = Some(conn);
                    info!("Redis connected for snapshots");
                }
            }
        }

        if clear_redis {
            if let Some(ref mut conn) = redis_conn {
                use redis::AsyncCommands;
                let keys_to_delete = vec![
                    REDIS_KEY_LAUNCHES,
                    REDIS_KEY_TRADES,
                    REDIS_KEY_MIGRATED,
                    REDIS_KEY_BONDING_CURVES,
                    REDIS_KEY_TICKERS,
                    REDIS_KEY_LAST_SLOT,
                    REDIS_KEY_LAST_TX_INDEX,
                ];
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

            if let Ok(json) = conn.get::<_, String>(REDIS_KEY_LAUNCHES).await {
                if let Ok(launches) = serde_json::from_str::<Vec<TokenLaunchPayload>>(&json) {
                    for launch in launches.into_iter().rev() {
                        last_launches.push_front(launch.clone());
                        recent_mints.insert(launch.mint.clone());
                        if let Some(state) = launch.initial_bonding_curve_state {
                            curve_reserves.insert(launch.mint.clone(), state.virtual_token_reserves);
                        }
                        if let Some(ref bc) = launch.pump_accounts.bonding_curve {
                            let _: redis::RedisResult<()> = conn.hset(REDIS_KEY_BONDING_CURVES, &launch.mint, bc).await;
                        }
                    }
                    while last_launches.len() > launch_limit {
                         last_launches.pop_back();
                    }
                    info!("Loaded {} launches from Redis", last_launches.len());
                }
            }

            if let Ok(json) = conn.get::<_, String>(REDIS_KEY_TRADES).await {
                 if let Ok(trades) = serde_json::from_str::<Vec<Trade>>(&json) {
                    for trade in trades {
                        if recent_mints.contains(&trade.mint) {
                            recent_trades.push_back(trade.clone());
                            if trade.is_bonding_curve {
                                curve_reserves.insert(trade.mint.clone(), trade.virtual_token_reserves);
                            }
                        }
                    }
                    while recent_trades.len() > trade_limit {
                        recent_trades.pop_front();
                    }
                }
            }

            if let Ok(json) = conn.get::<_, String>(REDIS_KEY_TICKERS).await {
                if let Ok(mut loaded_tickers) = serde_json::from_str::<HashMap<String, TokenState>>(&json) {
                    if let Some(ref state) = snapshot_state {
                        let current_time = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_micros() as u64;
                        
                        for ticker in loaded_tickers.values_mut() {
                            ticker.recalculate_volumes(current_time);
                        }

                        let mut w = state.write().await;
                        w.tickers.extend(loaded_tickers.clone());
                        info!("Loaded {} tickers from Redis (volumes recalculated)", loaded_tickers.len());
                    }
                }
            }

            if let Ok(json) = conn.get::<_, String>(REDIS_KEY_MIGRATED).await {
                if let Ok(migrated) = serde_json::from_str::<Vec<MigratedEntry>>(&json) {
                    migrated_list = migrated;
                    for m in &migrated_list {
                        curve_reserves.remove(&m.mint);
                    }
                    info!("Loaded {} migrations from Redis", migrated_list.len());
                }
            }

            if let Some(ref state) = snapshot_state {
                 let mut w = state.write().await;
                 w.migrated = migrated_list.clone();
                 w.launches = last_launches.iter().cloned().collect();
                 w.trades = recent_trades.iter().cloned().collect();
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

            let mut loaded_slot: Option<u64> = None;
            let mut loaded_tx_index: Option<u64> = None;
            if let Ok(slot_str) = conn.get::<_, String>(REDIS_KEY_LAST_SLOT).await {
                if let Ok(slot) = slot_str.parse::<u64>() {
                    loaded_slot = Some(slot);
                }
            }
            if let Ok(tx_str) = conn.get::<_, String>(REDIS_KEY_LAST_TX_INDEX).await {
                if let Ok(tx_index) = tx_str.parse::<u64>() {
                    loaded_tx_index = Some(tx_index);
                }
            }
            if let Some(ref state) = snapshot_state {
                if loaded_slot.is_some() || loaded_tx_index.is_some() {
                    let mut w = state.write().await;
                    if let Some(slot) = loaded_slot {
                        w.last_processed_slot = slot;
                    }
                    if let Some(tx_index) = loaded_tx_index {
                        w.last_processed_tx_index = tx_index;
                    }
                    if w.last_processed_slot > 0 {
                        info!(
                            "Loaded last processed cursor from Redis: slot={}, tx_index={}",
                            w.last_processed_slot, w.last_processed_tx_index
                        );
                    }
                }
            }
        }

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
                        security: launch.security.clone(),
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
                
                StorageMessage::Ohclv(ohlcv) => {
                    if clickhouse.is_some() {
                        ohlcv_buffer.push(OhclvRow {
                            mint: ohlcv.mint,
                            open: ohlcv.open,
                            high: ohlcv.high,
                            low: ohlcv.low,
                            close: ohlcv.close,
                            volume: ohlcv.volume,
                            start_time: ohlcv.start_time,
                            duration: ohlcv.duration,
                            received_at: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_micros() as u64,
                        });
                    }
                }
            }

            let should_flush = launch_buffer.len() >= BATCH_SIZE
                || trade_buffer.len() >= BATCH_SIZE
                || migration_buffer.len() >= BATCH_SIZE
                || ohlcv_buffer.len() >= BATCH_SIZE
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
                    if !ohlcv_buffer.is_empty() {
                        let rows = std::mem::take(&mut ohlcv_buffer);
                        flush_with_retry(|| db::insert_ohlcv_batch(ch, &rows)).await;
                    }
                } else {
                    launch_buffer.clear();
                    trade_buffer.clear();
                    migration_buffer.clear();
                    ohlcv_buffer.clear();
                }

                last_flush = Instant::now();
            }

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

            let mut tickers_map_clone = HashMap::new();
            let mut current_slot = 0;
            let mut current_tx_index = 0;
            if let Some(ref state) = snapshot_state {
                let mut w = state.write().await;
                w.migrated = migrated_list.clone();
                w.soon_migrating = soon_migrating.clone();
                w.launches = last_launches.iter().cloned().collect();
                w.trades = recent_trades.iter().cloned().collect();
                
                // Clone map quickly while holding lock (fast)
                tickers_map_clone = w.tickers.clone();
                current_slot = w.last_processed_slot;
                current_tx_index = w.last_processed_tx_index;
            }

            if let Some(ref mut conn) = redis_conn {
                let migrated_json = serde_json::to_string(&migrated_list).unwrap_or_default();
                let soon_migrating_json = serde_json::to_string(&soon_migrating).unwrap_or_default();
                let launches_json = serde_json::to_string(&last_launches.iter().cloned().collect::<Vec<_>>()).unwrap_or_default();
                let trades_json = serde_json::to_string(&recent_trades.iter().cloned().collect::<Vec<_>>()).unwrap_or_default();
                let tickers_json = serde_json::to_string(&tickers_map_clone).unwrap_or_default();
                let slot_str = current_slot.to_string();
                let tx_str = current_tx_index.to_string();

                let mut conn_clone = conn.clone();
                tokio::spawn(async move {
                    let _: redis::RedisResult<()> = conn_clone.set(REDIS_KEY_MIGRATED, migrated_json).await;
                    let _: redis::RedisResult<()> = conn_clone.set(REDIS_KEY_SOON_MIGRATING, soon_migrating_json).await;
                    let _: redis::RedisResult<()> = conn_clone.set(REDIS_KEY_LAUNCHES, launches_json).await;
                    let _: redis::RedisResult<()> = conn_clone.set(REDIS_KEY_TRADES, trades_json).await;
                    if !tickers_json.is_empty() {
                         let _: redis::RedisResult<()> = conn_clone.set(REDIS_KEY_TICKERS, tickers_json).await;
                    }
                    if current_slot > 0 {
                        let _: redis::RedisResult<()> = conn_clone.set(REDIS_KEY_LAST_SLOT, slot_str).await;
                    }
                    if current_tx_index > 0 {
                        let _: redis::RedisResult<()> = conn_clone.set(REDIS_KEY_LAST_TX_INDEX, tx_str).await;
                    }
                });
            }

            if last_aggregate_flush.elapsed() >= Duration::from_secs(60) {
                 if let Some(ref ch) = clickhouse {
                     let rows: Vec<VolumeAggregateRow> = tickers_map_clone.values().map(|t| VolumeAggregateRow {
                         mint: t.mint.clone(),
                         volume_5m: t.volume_metrics.volume_5m,
                         volume_1h: t.volume_metrics.volume_1h,
                         volume_6h: t.volume_metrics.volume_6h,
                         volume_24h: t.volume_metrics.volume_24h,
                         tx_count_5m: t.volume_metrics.tx_count_5m,
                         tx_count_1h: t.volume_metrics.tx_count_1h,
                         tx_count_6h: t.volume_metrics.tx_count_6h,
                         tx_count_24h: t.volume_metrics.tx_count_24h,
                         price_change_5m: t.volume_metrics.price_change_5m,
                         price_change_1h: t.volume_metrics.price_change_1h,
                         price_change_6h: t.volume_metrics.price_change_6h,
                         price_change_24h: t.volume_metrics.price_change_24h,
                         ts: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_micros() as u64,
                     }).collect();

                     if !rows.is_empty() {
                         let ch_clone = ch.clone();
                         tokio::spawn(async move {
                             for chunk in rows.chunks(10_000) {
                                let _ = db::insert_volume_aggregates_batch(&ch_clone, chunk).await;
                             }
                         });
                     }
                 }
                 last_aggregate_flush = Instant::now();
            }
        }

        info!("Storage worker exiting, final flush");

        if let Some(ref ch) = clickhouse {
            let _ = db::insert_token_launches_batch(ch, &launch_buffer).await;
            let _ = db::insert_trades_batch(ch, &trade_buffer).await;
            let _ = db::insert_migrations_batch(ch, &migration_buffer).await;
            let _ = db::insert_ohlcv_batch(ch, &ohlcv_buffer).await;
        }
    });
}
