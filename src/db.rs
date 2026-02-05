use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

const TABLE: &str = "pipeline_metrics";

/// One row in pipeline_metrics. ReplacingMergeTree(updated_at) keeps latest per key.
#[derive(Debug, Row, Serialize, Deserialize)]
pub struct MetricRow {
    pub key: String,
    pub value: f64,
    /// Unix timestamp in milliseconds (DateTime64(3)).
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
