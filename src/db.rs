use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use tracing::{debug, error};

const TABLE: &str = "pipeline_metrics";

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct MetricRow {
    pub key: String,
    pub value: f64,
    pub updated_at: i64,
}

const KEY_LAUNCH_COUNT: &str = "launch_count";
const KEY_GRPC_SUM_MS: &str = "grpc_sum_ms";
const KEY_GRPC_MIN_MS: &str = "grpc_min_ms";
const KEY_GRPC_MAX_MS: &str = "grpc_max_ms";
const KEY_SERVER_SUM_US: &str = "server_sum_us";
const KEY_SERVER_MIN_US: &str = "server_min_us";
const KEY_SERVER_MAX_US: &str = "server_max_us";

pub fn make_client(url: &str, user: Option<&str>, password: Option<&str>) -> Client {
    let mut client = Client::default().with_url(url);
    if let Some(u) = user {
        client = client.with_user(u);
    }
    if let Some(p) = password {
        client = client.with_password(p);
    }
    client
}

pub async fn ensure_table(client: &Client) -> clickhouse::error::Result<()> {
    let ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {TABLE} (
            key LowCardinality(String),
            value Float64 CODEC(ZSTD(3)),
            updated_at DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3))
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY key
        "#
    );
    client.query(&ddl).execute().await?;
    Ok(())
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct MetricKeyValue {
    pub key: String,
    pub value: f64,
}

pub async fn get_current_metrics(
    client: &Client,
) -> clickhouse::error::Result<std::collections::HashMap<String, f64>> {
    let mut cursor = client
        .query(
            "SELECT key, argMax(value, updated_at) AS value
             FROM pipeline_metrics
             GROUP BY key",
        )
        .fetch::<MetricKeyValue>()?;
    let mut out = std::collections::HashMap::new();
    while let Some(row) = cursor.next().await? {
        out.insert(row.key, row.value);
    }
    Ok(out)
}

pub async fn update_metrics_aggregates(
    client: &Client,
    grpc_ms: f64,
    server_us: f64,
) -> clickhouse::error::Result<()> {
    let current = get_current_metrics(client).await?;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let launch_count = current.get(KEY_LAUNCH_COUNT).copied().unwrap_or(0.0) + 1.0;
    let grpc_sum_ms = current.get(KEY_GRPC_SUM_MS).copied().unwrap_or(0.0) + grpc_ms;
    let grpc_min_ms = current
        .get(KEY_GRPC_MIN_MS)
        .map(|v| v.min(grpc_ms))
        .unwrap_or(grpc_ms);
    let grpc_max_ms = current
        .get(KEY_GRPC_MAX_MS)
        .map(|v| v.max(grpc_ms))
        .unwrap_or(grpc_ms);
    let server_sum_us = current.get(KEY_SERVER_SUM_US).copied().unwrap_or(0.0) + server_us;
    let server_min_us = current
        .get(KEY_SERVER_MIN_US)
        .map(|v| v.min(server_us))
        .unwrap_or(server_us);
    let server_max_us = current
        .get(KEY_SERVER_MAX_US)
        .map(|v| v.max(server_us))
        .unwrap_or(server_us);

    let rows = [
        MetricRow { key: KEY_LAUNCH_COUNT.into(), value: launch_count, updated_at: now_ms },
        MetricRow { key: KEY_GRPC_SUM_MS.into(), value: grpc_sum_ms, updated_at: now_ms },
        MetricRow { key: KEY_GRPC_MIN_MS.into(), value: grpc_min_ms, updated_at: now_ms },
        MetricRow { key: KEY_GRPC_MAX_MS.into(), value: grpc_max_ms, updated_at: now_ms },
        MetricRow { key: KEY_SERVER_SUM_US.into(), value: server_sum_us, updated_at: now_ms },
        MetricRow { key: KEY_SERVER_MIN_US.into(), value: server_min_us, updated_at: now_ms },
        MetricRow { key: KEY_SERVER_MAX_US.into(), value: server_max_us, updated_at: now_ms },
    ];

    let mut insert = client.insert::<MetricRow>(TABLE).await?;
    for row in &rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

pub async fn update_metrics_aggregates_optional(
    client: Option<&Client>,
    grpc_ms: f64,
    server_us: f64,
) {
    if let Some(c) = client {
        if let Err(e) = update_metrics_aggregates(c, grpc_ms, server_us).await {
            error!("ClickHouse metrics update failed: {}", e);
        }
    }
}

const TABLE_TOKEN_LAUNCHES: &str = "token_launches";
const TABLE_TRADES: &str = "trades";
const TABLE_MIGRATIONS: &str = "migrations";

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct TokenLaunchRow {
    pub signature: String,
    pub slot: u64,
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: String,
    pub bonding_curve: Option<String>,
    pub associated_bonding_curve: Option<String>,
    pub global: Option<String>,
    pub fee_recipient: Option<String>,
    pub metadata: Option<String>,
    pub creator_vault: Option<String>,
    pub fee_config: Option<String>,
    pub event_authority: Option<String>,
    pub token_decimals: u8,
    pub virtual_sol_reserves: Option<u64>,
    pub virtual_token_reserves: Option<u64>,
    pub real_sol_reserves: Option<u64>,
    pub real_token_reserves: Option<u64>,
    pub received_at: u64,
    pub sent_at: u64,
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct TradeRow {
    pub signature: String,
    pub slot: u64,
    pub mint: String,
    pub trader: String,
    pub direction: String,
    pub token_amount: u64,
    pub sol_amount: u64,
    pub virtual_token_reserves: u64,
    pub is_create: u8,
    pub pool: Option<String>,
    pub token_decimals: u8,
    pub suggested_compute_unit_limit: u32,
    pub received_at: u64,
    pub sent_at: u64,
    /// On-chain time (ms) for volume windows; 0 means use received_at.
    pub txn_timestamp: u64,
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct MigrationRow {
    pub signature: String,
    pub slot: u64,
    pub mint: String,
    pub user: String,
    pub bonding_curve: String,
    pub pool: String,
    pub token_amount: u64,
    pub sol_amount: u64,
    pub fee: u64,
    pub received_at: u64,
    pub sent_at: u64,
}

const TABLE_OHLCV_1M: &str = "ohlcv_1m";

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct OhclvRow {
    pub mint: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub start_time: u64,
    pub duration: u64,
    pub received_at: u64,
}

const TABLE_VOLUME_AGGREGATES: &str = "volume_aggregates";

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct VolumeAggregateRow {
    pub mint: String,
    pub volume_5m: f64,
    pub volume_1h: f64,
    pub volume_6h: f64,
    pub volume_24h: f64,
    pub tx_count_5m: u64,
    pub tx_count_1h: u64,
    pub tx_count_6h: u64,
    pub tx_count_24h: u64,
    pub price_change_5m: f64,
    pub price_change_1h: f64,
    pub price_change_6h: f64,
    pub price_change_24h: f64,
    pub ts: u64,
}

pub async fn ensure_volume_aggregates_table(client: &Client) -> clickhouse::error::Result<()> {
    let ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {TABLE_VOLUME_AGGREGATES} (
            mint LowCardinality(String),
            volume_5m Float64 CODEC(Delta, ZSTD(3)),
            volume_1h Float64 CODEC(Delta, ZSTD(3)),
            volume_6h Float64 CODEC(Delta, ZSTD(3)),
            volume_24h Float64 CODEC(Delta, ZSTD(3)),
            tx_count_5m UInt64 CODEC(Delta, ZSTD(3)),
            tx_count_1h UInt64 CODEC(Delta, ZSTD(3)),
            tx_count_6h UInt64 CODEC(Delta, ZSTD(3)),
            tx_count_24h UInt64 CODEC(Delta, ZSTD(3)),
            price_change_5m Float64 CODEC(Delta, ZSTD(3)),
            price_change_1h Float64 CODEC(Delta, ZSTD(3)),
            price_change_6h Float64 CODEC(Delta, ZSTD(3)),
            price_change_24h Float64 CODEC(Delta, ZSTD(3)),
            ts UInt64 CODEC(Delta, ZSTD(3))
        )
        ENGINE = ReplacingMergeTree(ts)
        PARTITION BY toYYYYMM(toDateTime(ts / 1000000))
        ORDER BY (mint, ts)
        "#
    );
    client.query(&ddl).execute().await?;
    debug!("ClickHouse table {} ensured", TABLE_VOLUME_AGGREGATES);
    Ok(())
}

pub async fn ensure_ohlcv_table(client: &Client) -> clickhouse::error::Result<()> {
    let ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {TABLE_OHLCV_1M} (
            mint LowCardinality(String),
            open Float64 CODEC(Delta, ZSTD(3)),
            high Float64 CODEC(Delta, ZSTD(3)),
            low Float64 CODEC(Delta, ZSTD(3)),
            close Float64 CODEC(Delta, ZSTD(3)),
            volume Float64 CODEC(Delta, ZSTD(3)),
            start_time UInt64 CODEC(Delta, ZSTD(3)),
            duration UInt64 CODEC(Delta, ZSTD(3)),
            received_at UInt64 CODEC(Delta, ZSTD(3))
        )
        ENGINE = ReplacingMergeTree(received_at)
        PARTITION BY toYYYYMM(toDateTime(start_time))
        ORDER BY (mint, start_time)
        "#
    );
    client.query(&ddl).execute().await?;
    debug!("ClickHouse table {} ensured", TABLE_OHLCV_1M);
    Ok(())
}

pub async fn ensure_token_launches_table(client: &Client) -> clickhouse::error::Result<()> {
    let ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {TABLE_TOKEN_LAUNCHES} (
            signature String,
            slot UInt64 CODEC(Delta, ZSTD(3)),
            mint LowCardinality(String),
            name String,
            symbol LowCardinality(String),
            uri String,
            creator LowCardinality(String),
            bonding_curve Nullable(String),
            associated_bonding_curve Nullable(String),
            global Nullable(String),
            fee_recipient Nullable(String),
            metadata Nullable(String),
            creator_vault Nullable(String),
            fee_config Nullable(String),
            event_authority Nullable(String),
            token_decimals UInt8,
            virtual_sol_reserves Nullable(UInt64),
            virtual_token_reserves Nullable(UInt64),
            real_sol_reserves Nullable(UInt64),
            real_token_reserves Nullable(UInt64),
            received_at UInt64 CODEC(Delta, ZSTD(3)),
            sent_at UInt64 CODEC(Delta, ZSTD(3))
        )
        ENGINE = ReplacingMergeTree(received_at)
        PARTITION BY toYYYYMM(toDateTime(received_at / 1000000))
        ORDER BY signature
        "#
    );
    client.query(&ddl).execute().await?;
    debug!("ClickHouse table {} ensured", TABLE_TOKEN_LAUNCHES);
    Ok(())
}

pub async fn ensure_trades_table(client: &Client) -> clickhouse::error::Result<()> {
    let ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {TABLE_TRADES} (
            signature String,
            slot UInt64 CODEC(Delta, ZSTD(3)),
            mint LowCardinality(String),
            trader LowCardinality(String),
            direction LowCardinality(String),
            token_amount UInt64 CODEC(Delta, ZSTD(3)),
            sol_amount UInt64 CODEC(Delta, ZSTD(3)),
            virtual_token_reserves UInt64 CODEC(Delta, ZSTD(3)),
            is_create UInt8,
            pool LowCardinality(Nullable(String)),
            token_decimals UInt8,
            suggested_compute_unit_limit UInt32,
            received_at UInt64 CODEC(Delta, ZSTD(3)),
            sent_at UInt64 CODEC(Delta, ZSTD(3)),
            txn_timestamp UInt64 DEFAULT 0 CODEC(Delta, ZSTD(3))
        )
        ENGINE = ReplacingMergeTree(received_at)
        PARTITION BY toYYYYMM(toDateTime(received_at / 1000000))
        ORDER BY signature
        "#
    );
    client.query(&ddl).execute().await?;
    debug!("ClickHouse table {} ensured", TABLE_TRADES);
    // If the table already existed without txn_timestamp, run: ALTER TABLE trades ADD COLUMN txn_timestamp UInt64 DEFAULT 0
    Ok(())
}

pub async fn ensure_migrations_table(client: &Client) -> clickhouse::error::Result<()> {
    let ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {TABLE_MIGRATIONS} (
            signature String,
            slot UInt64 CODEC(Delta, ZSTD(3)),
            mint LowCardinality(String),
            user LowCardinality(String),
            bonding_curve LowCardinality(String),
            pool LowCardinality(String),
            token_amount UInt64 CODEC(Delta, ZSTD(3)),
            sol_amount UInt64 CODEC(Delta, ZSTD(3)),
            fee UInt64 CODEC(Delta, ZSTD(3)),
            received_at UInt64 CODEC(Delta, ZSTD(3)),
            sent_at UInt64 CODEC(Delta, ZSTD(3))
        )
        ENGINE = ReplacingMergeTree(received_at)
        PARTITION BY toYYYYMM(toDateTime(received_at / 1000000))
        ORDER BY signature
        "#
    );
    client.query(&ddl).execute().await?;
    debug!("ClickHouse table {} ensured", TABLE_MIGRATIONS);
    Ok(())
}

/// Minimal token metadata for HTTP API (e.g. /symbols). Fetched from token_launches by mint.
#[derive(Debug, Row, Serialize, Deserialize)]
pub struct TokenMetadataRow {
    pub name: String,
    pub symbol: String,
    pub uri: String,
}

/// Token launch metadata for HTTP fallback (includes creator). Fetched from token_launches by mint.
#[derive(Debug, Row, Serialize, Deserialize)]
pub struct TokenMetadataWithCreatorRow {
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: String,
}

/// Fetches name, symbol, uri for a mint from token_launches (latest by received_at). Returns None if not found.
pub async fn fetch_token_metadata(
    client: &Client,
    mint: &str,
) -> clickhouse::error::Result<Option<TokenMetadataRow>> {
    let mut cursor = client
        .query(&format!(
            "SELECT name, symbol, uri FROM {TABLE_TOKEN_LAUNCHES} WHERE mint = ? ORDER BY received_at DESC LIMIT 1"
        ))
        .bind(mint)
        .fetch::<TokenMetadataRow>()?;
    let first = cursor.next().await?;
    Ok(first)
}

/// Fetches name, symbol, uri, creator for a mint from token_launches (latest by received_at). Returns None if not found.
pub async fn fetch_token_metadata_with_creator(
    client: &Client,
    mint: &str,
) -> clickhouse::error::Result<Option<TokenMetadataWithCreatorRow>> {
    let mut cursor = client
        .query(&format!(
            "SELECT name, symbol, uri, creator FROM {TABLE_TOKEN_LAUNCHES} WHERE mint = ? ORDER BY received_at DESC LIMIT 1"
        ))
        .bind(mint)
        .fetch::<TokenMetadataWithCreatorRow>()?;
    let first = cursor.next().await?;
    Ok(first)
}

/// Token metadata with mint, for batch API response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenMetadataWithCreatorAndMint {
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: String,
}

/// Fetches metadata (name, symbol, uri, creator) for multiple mints. Returns only mints that exist; order not guaranteed.
pub async fn fetch_token_metadata_batch(
    client: &Client,
    mints: &[String],
) -> clickhouse::error::Result<Vec<TokenMetadataWithCreatorAndMint>> {
    let mut out = Vec::with_capacity(mints.len());
    for mint in mints {
        if let Ok(Some(meta)) = fetch_token_metadata_with_creator(client, mint).await {
            out.push(TokenMetadataWithCreatorAndMint {
                mint: mint.clone(),
                name: meta.name,
                symbol: meta.symbol,
                uri: meta.uri,
                creator: meta.creator,
            });
        }
    }
    Ok(out)
}

pub async fn insert_ohlcv_batch(
    client: &Client,
    rows: &[OhclvRow],
) -> clickhouse::error::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert::<OhclvRow>(TABLE_OHLCV_1M).await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

pub async fn fetch_aggregated_ohlcv(
    client: &Client,
    mint: &str,
    interval_minutes: u64,
    from: u64,
    to: u64,
) -> clickhouse::error::Result<Vec<OhclvRow>> {
    let query = format!(
        r#"
        SELECT
            mint,
            toUnixTimestamp(toStartOfInterval(toDateTime(start_time / 1000000), INTERVAL {interval} MINUTE)) * 1000000 as start_time,
            argMin(open, start_time) as open,
            max(high) as high,
            min(low) as low,
            argMax(close, start_time) as close,
            sum(volume) as volume
        FROM {table}
        WHERE mint = ? AND start_time >= ? AND start_time <= ?
        GROUP BY mint, start_time
        ORDER BY start_time ASC
        "#,
        interval = interval_minutes,
        table = TABLE_OHLCV_1M
    );

    let cursor = client
        .query(&query)
        .bind(mint)
        .bind(from)
        .bind(to)
        .fetch::<OhclvRow>()?;

    let mut rows = Vec::new();
    let mut stream = cursor;
    while let Some(row) = stream.next().await? {
        rows.push(row);
    }
    Ok(rows)
}

/// Result row for volume aggregation from trades (no price_change; v1 uses in-memory for that).
#[derive(Debug, Row, Serialize, Deserialize)]
pub struct VolumeMetricsResultRow {
    pub mint: String,
    pub volume_5m: f64,
    pub volume_1h: f64,
    pub volume_6h: f64,
    pub volume_24h: f64,
    pub tx_count_5m: u64,
    pub tx_count_1h: u64,
    pub tx_count_6h: u64,
    pub tx_count_24h: u64,
}

/// Fetch volume and tx_count aggregates from trades for the given mints. Uses received_at (micros)
/// for time windows. Excludes the last `delay_secs` seconds so display aligns with delayed terminals.
pub async fn fetch_volume_metrics_batch(
    client: &Client,
    mints: &[String],
    delay_secs: u64,
) -> clickhouse::error::Result<Vec<VolumeMetricsResultRow>> {
    if mints.is_empty() {
        return Ok(Vec::new());
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    let end = now.saturating_sub(delay_secs * 1_000_000);
    const US_5M: u64 = 5 * 60 * 1_000_000;
    const US_1H: u64 = 60 * 60 * 1_000_000;
    const US_6H: u64 = 6 * 60 * 60 * 1_000_000;
    const US_24H: u64 = 24 * 60 * 60 * 1_000_000;
    let end_5m_start = end.saturating_sub(US_5M);
    let end_1h_start = end.saturating_sub(US_1H);
    let end_6h_start = end.saturating_sub(US_6H);
    let end_24h_start = end.saturating_sub(US_24H);

    let in_list = mints
        .iter()
        .map(|m| format!("'{}'", m.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");
    let query = format!(
        r#"
        SELECT mint,
            sumIf(sol_amount, received_at >= ? AND received_at <= ?) / 1000000000.0 AS volume_5m,
            countIf(received_at >= ? AND received_at <= ?) AS tx_count_5m,
            sumIf(sol_amount, received_at >= ? AND received_at <= ?) / 1000000000.0 AS volume_1h,
            countIf(received_at >= ? AND received_at <= ?) AS tx_count_1h,
            sumIf(sol_amount, received_at >= ? AND received_at <= ?) / 1000000000.0 AS volume_6h,
            countIf(received_at >= ? AND received_at <= ?) AS tx_count_6h,
            sumIf(sol_amount, received_at >= ? AND received_at <= ?) / 1000000000.0 AS volume_24h,
            countIf(received_at >= ? AND received_at <= ?) AS tx_count_24h
        FROM {table}
        WHERE mint IN ({in_list})
        GROUP BY mint
        "#,
        table = TABLE_TRADES,
        in_list = in_list
    );
    let mut cursor = client
        .query(&query)
        .bind(end_5m_start)
        .bind(end)
        .bind(end_5m_start)
        .bind(end)
        .bind(end_1h_start)
        .bind(end)
        .bind(end_1h_start)
        .bind(end)
        .bind(end_6h_start)
        .bind(end)
        .bind(end_6h_start)
        .bind(end)
        .bind(end_24h_start)
        .bind(end)
        .bind(end_24h_start)
        .bind(end)
        .fetch::<VolumeMetricsResultRow>()?;
    let mut rows = Vec::new();
    while let Some(row) = cursor.next().await? {
        rows.push(row);
    }
    Ok(rows)
}

pub async fn drop_tables(client: &Client) -> clickhouse::error::Result<()> {
    for table in [TABLE, TABLE_TOKEN_LAUNCHES, TABLE_TRADES, TABLE_MIGRATIONS, TABLE_OHLCV_1M, TABLE_VOLUME_AGGREGATES] {
        client
            .query(&format!("DROP TABLE IF EXISTS {}", table))
            .execute()
            .await?;
        debug!("ClickHouse table {} dropped", table);
    }
    Ok(())
}

pub async fn insert_volume_aggregates_batch(
    client: &Client,
    rows: &[VolumeAggregateRow],
) -> clickhouse::error::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert::<VolumeAggregateRow>(TABLE_VOLUME_AGGREGATES).await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

pub async fn insert_token_launches_batch(
    client: &Client,
    rows: &[TokenLaunchRow],
) -> clickhouse::error::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert::<TokenLaunchRow>(TABLE_TOKEN_LAUNCHES).await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

pub async fn insert_trades_batch(
    client: &Client,
    rows: &[TradeRow],
) -> clickhouse::error::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert::<TradeRow>(TABLE_TRADES).await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

pub async fn insert_migrations_batch(
    client: &Client,
    rows: &[MigrationRow],
) -> clickhouse::error::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert::<MigrationRow>(TABLE_MIGRATIONS).await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}
