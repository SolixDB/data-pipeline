use std::env;
use yellowstone_grpc_proto::geyser::CommitmentLevel;

#[derive(Clone, Debug)]
pub struct Config {
    pub endpoint: String,
    pub api_token: Option<String>,
    pub commitment: CommitmentLevel,
    pub max_decoding_message_size: usize,
    pub reconnect_initial_interval_ms: u64,
    pub reconnect_max_interval_ms: u64,
    pub reconnect_max_elapsed_time_ms: Option<u64>,
    pub clickhouse_url: Option<String>,
    pub clickhouse_user: Option<String>,
    pub clickhouse_password: Option<String>,
    #[allow(dead_code)] // read from main.rs (redis client, storage worker)
    pub redis_url: Option<String>,
    /// When true, TRUNCATE pipeline ClickHouse tables on startup (for dev).
    pub clear_clickhouse: bool,
    /// When true, do not connect or write to ClickHouse (Redis/WS still work).
    pub skip_clickhouse: bool,
    /// When true, clear ClickHouse (drop + recreate tables) then do not write any data this run.
    pub clear_clickhouse_only: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: env::var("GRPC_ENDPOINT")
                .expect("GRPC_ENDPOINT environment variable must be set"),
            api_token: env::var("GRPC_API_KEY").ok(),
            commitment: CommitmentLevel::Processed,
            max_decoding_message_size: 1024 * 1024 * 1024,
            reconnect_initial_interval_ms: 500,
            reconnect_max_interval_ms: 30000,
            reconnect_max_elapsed_time_ms: None,
            clickhouse_url: env::var("CLICKHOUSE_URL").ok().filter(|s| !s.is_empty()),
            clickhouse_user: env::var("CLICKHOUSE_USER").ok().filter(|s| !s.is_empty()),
            clickhouse_password: env::var("CLICKHOUSE_PASSWORD").ok().filter(|s| !s.is_empty()),
            redis_url: env::var("REDIS_URL").ok().filter(|s| !s.is_empty()),
            clear_clickhouse: env::var("CLEAR_CLICKHOUSE")
                .ok()
                .map(|s| matches!(s.to_lowercase().as_str(), "1" | "true" | "yes"))
                .unwrap_or(false),
            skip_clickhouse: env::var("SKIP_CLICKHOUSE")
                .ok()
                .map(|s| matches!(s.to_lowercase().as_str(), "1" | "true" | "yes"))
                .unwrap_or(false),
            clear_clickhouse_only: env::var("CLEAR_CLICKHOUSE_ONLY")
                .ok()
                .map(|s| matches!(s.to_lowercase().as_str(), "1" | "true" | "yes"))
                .unwrap_or(false),
        }
    }
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.endpoint.is_empty() {
          anyhow::bail!("Endpoint cannot be empty");
        } 
        if !self.endpoint.starts_with("http://") && !self.endpoint.starts_with("https://") {
            anyhow::bail!("Endpoint must start with http:// or https://");
        }
        if self.clear_clickhouse && !self.skip_clickhouse && self.clickhouse_url.as_ref().map_or(true, |u| u.is_empty()) {
            anyhow::bail!("CLEAR_CLICKHOUSE is set but CLICKHOUSE_URL is missing or empty");
        }
        if self.clear_clickhouse_only && self.clickhouse_url.as_ref().map_or(true, |u| u.is_empty()) {
            anyhow::bail!("CLEAR_CLICKHOUSE_ONLY is set but CLICKHOUSE_URL is missing or empty");
        }
        Ok(())
    }
}