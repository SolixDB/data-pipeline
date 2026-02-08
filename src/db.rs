use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

const TABLE: &str = "pipeline_metrics";

/// One row in pipeline_metrics. ReplacingMergeTree(updated_at) keeps latest per key.
#[derive(Debug, Row, Serialize, Deserialize)]
pub struct MetricRow {
    pub key: String,
    pub value: f64,
    pub updated_at: i64,
}

/// Keys we maintain for launch aggregates.
const KEY_LAUNCH_COUNT: &str = "launch_count";
const KEY_GRPC_SUM_MS: &str = "grpc_sum_ms";
const KEY_GRPC_MIN_MS: &str = "grpc_min_ms";
const KEY_GRPC_MAX_MS: &str = "grpc_max_ms";
const KEY_SERVER_SUM_US: &str = "server_sum_us";
const KEY_SERVER_MIN_US: &str = "server_min_us";
const KEY_SERVER_MAX_US: &str = "server_max_us";

/// Create ClickHouse client from URL and optional credentials.
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

/// Create pipeline_metrics table if not exists (ReplacingMergeTree + ZSTD codecs).
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
    client.query(ddl.as_str()).execute().await?;
    info!("ClickHouse table {} ensured", TABLE);
    Ok(())
}

/// Row returned by the current-state query (key + value only).
#[derive(Debug, Row, Serialize, Deserialize)]
pub struct MetricKeyValue {
    pub key: String,
    pub value: f64,
}

/// Fetch current metric values (argMax per key). Returns map key -> value; missing keys = 0.
pub async fn get_current_metrics(client: &Client) -> clickhouse::error::Result<std::collections::HashMap<String, f64>> {
    let mut cursor = client
        .query("SELECT key, argMax(value, updated_at) AS value FROM pipeline_metrics GROUP BY key")
        .fetch::<MetricKeyValue>()?;
    let mut out = std::collections::HashMap::new();
    while let Some(row) = cursor.next().await? {
        out.insert(row.key, row.value);
    }
    Ok(out)
}

/// Update launch aggregates: count += 1, sum += x, min = min(current, x), max = max(current, x).
/// Inserts one row per key; ReplacingMergeTree will keep latest per key.
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
        MetricRow {
            key: KEY_LAUNCH_COUNT.to_string(),
            value: launch_count,
            updated_at: now_ms,
        },
        MetricRow {
            key: KEY_GRPC_SUM_MS.to_string(),
            value: grpc_sum_ms,
            updated_at: now_ms,
        },
        MetricRow {
            key: KEY_GRPC_MIN_MS.to_string(),
            value: grpc_min_ms,
            updated_at: now_ms,
        },
        MetricRow {
            key: KEY_GRPC_MAX_MS.to_string(),
            value: grpc_max_ms,
            updated_at: now_ms,
        },
        MetricRow {
            key: KEY_SERVER_SUM_US.to_string(),
            value: server_sum_us,
            updated_at: now_ms,
        },
        MetricRow {
            key: KEY_SERVER_MIN_US.to_string(),
            value: server_min_us,
            updated_at: now_ms,
        },
        MetricRow {
            key: KEY_SERVER_MAX_US.to_string(),
            value: server_max_us,
            updated_at: now_ms,
        },
    ];

    let mut insert = client.insert::<MetricRow>(TABLE).await?;
    for row in &rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

/// Optional metrics: if None, update is no-op. Used when CLICKHOUSE_URL is not set.
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

// ----- Token launches, trades, migrations (storage worker writes; handler sends to channel) -----

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
            slot UInt64,
            mint String,
            name String,
            symbol String,
            uri String,
            creator String,
            received_at UInt64,
            sent_at UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (mint, slot)
        "#
    );
    client.query(ddl.as_str()).execute().await?;
    info!("ClickHouse table {} ensured", TABLE_TOKEN_LAUNCHES);
    Ok(())
}

pub async fn ensure_trades_table(client: &Client) -> clickhouse::error::Result<()> {
    let ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {TABLE_TRADES} (
            signature String,
            slot UInt64,
            mint String,
            trader String,
            direction String,
            token_amount UInt64,
            sol_amount UInt64,
            virtual_token_reserves UInt64,
            is_create UInt8,
            pool Nullable(String),
            received_at UInt64,
            sent_at UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (mint, slot)
        "#
    );
    client.query(ddl.as_str()).execute().await?;
    info!("ClickHouse table {} ensured", TABLE_TRADES);
    Ok(())
}

pub async fn ensure_migrations_table(client: &Client) -> clickhouse::error::Result<()> {
    let ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {TABLE_MIGRATIONS} (
            signature String,
            slot UInt64,
            mint String,
            user String,
            bonding_curve String,
            pool String,
            token_amount UInt64,
            sol_amount UInt64,
            fee UInt64,
            received_at UInt64,
            sent_at UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (mint, slot)
        "#
    );
    client.query(ddl.as_str()).execute().await?;
    info!("ClickHouse table {} ensured", TABLE_MIGRATIONS);
    Ok(())
}

/// DROP then recreate all pipeline tables when CLEAR_CLICKHOUSE=1 so schema matches current code.
pub async fn drop_tables(client: &Client) -> clickhouse::error::Result<()> {
    for table in [TABLE, TABLE_TOKEN_LAUNCHES, TABLE_TRADES, TABLE_MIGRATIONS] {
        client
            .query(format!("DROP TABLE IF EXISTS {}", table).as_str())
            .execute()
            .await?;
        info!("ClickHouse table {} dropped", table);
    }
    Ok(())
}

pub async fn insert_token_launch(client: &Client, row: &TokenLaunchRow) -> clickhouse::error::Result<()> {
    let mut insert = client.insert::<TokenLaunchRow>(TABLE_TOKEN_LAUNCHES).await?;
    insert.write(row).await?;
    insert.end().await?;
    Ok(())
}

pub async fn insert_trade(client: &Client, row: &TradeRow) -> clickhouse::error::Result<()> {
    let mut insert = client.insert::<TradeRow>(TABLE_TRADES).await?;
    insert.write(row).await?;
    insert.end().await?;
    Ok(())
}

pub async fn insert_migration(client: &Client, row: &MigrationRow) -> clickhouse::error::Result<()> {
    let mut insert = client.insert::<MigrationRow>(TABLE_MIGRATIONS).await?;
    insert.write(row).await?;
    insert.end().await?;
    Ok(())
}

// ----- Holder stats (trading-derived: balance = sum(buys) - sum(sells) per mint/trader) -----

/// One row: trader address and their balance for a mint (from trades only; no transfers).
#[allow(dead_code)] // public API, see docs/RUNBOOK.md
#[derive(Debug, Row, Serialize, Deserialize)]
pub struct HolderBalanceRow {
    pub trader: String,
    pub balance: i64,
}

/// Top 10 holders % and dev holding % for a mint. Query uses trades + token_launches (creator).
/// Balances are trading-derived (buys - sells); dev % uses creator from token_launches.
#[allow(dead_code)] // public API, see docs/RUNBOOK.md
pub async fn query_holder_stats(
    client: &Client,
    mint: &str,
) -> clickhouse::error::Result<(f64, f64, Vec<(String, u64)>)> {
    // Balance per trader: sum(buy amount) - sum(sell amount). Signed to allow sells.
    let top10_query = format!(
        r#"
        WITH balances AS (
            SELECT
                trader,
                sum(if(direction = 'buy', toInt64(token_amount), -toInt64(token_amount))) AS bal
            FROM {TABLE_TRADES}
            WHERE mint = ?
            GROUP BY trader
            HAVING bal > 0
        ),
        total AS (SELECT sum(bal) AS t FROM balances),
        ranked AS (
            SELECT trader, bal, row_number() OVER (ORDER BY bal DESC) AS rn
            FROM balances
        )
        SELECT trader, bal AS balance
        FROM ranked
        WHERE rn <= 10
        ORDER BY rn
        "#
    );
    let mut cursor = client
        .query(top10_query.as_str())
        .bind(mint)
        .fetch::<HolderBalanceRow>()?;
    let mut top10: Vec<(String, u64)> = Vec::new();
    let mut sum_top10: u64 = 0;
    while let Some(row) = cursor.next().await? {
        let bal = row.balance.max(0) as u64;
        sum_top10 += bal;
        top10.push((row.trader, bal));
    }

    // Total supply (sum of all positive balances) for this mint
    #[derive(Row, Serialize, Deserialize)]
    struct TotalRow { tot: i64 }
    let total_query = format!(
        r#"
        SELECT sum(if(direction = 'buy', toInt64(token_amount), -toInt64(token_amount))) AS tot
        FROM {TABLE_TRADES}
        WHERE mint = ?
        "#
    );
    let mut total_cursor = client
        .query(total_query.as_str())
        .bind(mint)
        .fetch::<TotalRow>()?;
    let total: i64 = total_cursor.next().await?.map(|r| r.tot).unwrap_or(0);
    let total_positive = total.max(0) as u64;
    let top10_pct = if total_positive > 0 {
        (sum_top10 as f64 / total_positive as f64) * 100.0
    } else {
        0.0
    };

    // Creator (dev) for this mint from token_launches
    #[derive(Row, Serialize, Deserialize)]
    struct CreatorRow { creator: String }
    let creator_query = format!(
        r#"SELECT creator FROM {TABLE_TOKEN_LAUNCHES} WHERE mint = ? LIMIT 1"#
    );
    let mut creator_cursor = client
        .query(creator_query.as_str())
        .bind(mint)
        .fetch::<CreatorRow>()?;
    let creator: Option<String> = creator_cursor.next().await?.map(|r| r.creator);
    #[derive(Row, Serialize, Deserialize)]
    struct BalRow { bal: i64 }
    let dev_pct = if let Some(creator) = creator {
        let dev_bal_query = format!(
            r#"
            SELECT sum(if(direction = 'buy', toInt64(token_amount), -toInt64(token_amount))) AS bal
            FROM {TABLE_TRADES}
            WHERE mint = ? AND trader = ?
            "#
        );
        let mut dev_cursor = client
            .query(dev_bal_query.as_str())
            .bind(mint)
            .bind(creator.as_str())
            .fetch::<BalRow>()?;
        let dev_bal: i64 = dev_cursor.next().await?.map(|r| r.bal).unwrap_or(0);
        let dev_bal_u = dev_bal.max(0) as u64;
        if total_positive > 0 {
            (dev_bal_u as f64 / total_positive as f64) * 100.0
        } else {
            0.0
        }
    } else {
        0.0
    };

    Ok((top10_pct, dev_pct, top10))
}
