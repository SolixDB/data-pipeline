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
    debug!("ClickHouse table {} ensured", TABLE);
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
            sent_at UInt64 CODEC(Delta, ZSTD(3))
        )
        ENGINE = ReplacingMergeTree(received_at)
        PARTITION BY toYYYYMM(toDateTime(received_at / 1000000))
        ORDER BY signature
        "#
    );
    client.query(&ddl).execute().await?;
    debug!("ClickHouse table {} ensured", TABLE_TRADES);
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

pub async fn drop_tables(client: &Client) -> clickhouse::error::Result<()> {
    for table in [TABLE, TABLE_TOKEN_LAUNCHES, TABLE_TRADES, TABLE_MIGRATIONS] {
        client
            .query(&format!("DROP TABLE IF EXISTS {}", table))
            .execute()
            .await?;
        debug!("ClickHouse table {} dropped", table);
    }
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
