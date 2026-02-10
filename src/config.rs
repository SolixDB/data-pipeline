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
    #[allow(dead_code)]
    pub redis_url: Option<String>,
    pub clear_clickhouse: bool,
    pub clear_redis: bool,
    pub skip_clickhouse: bool,
    pub clear_clickhouse_only: bool,
    #[allow(dead_code)]
    pub health_port: u16,
    #[allow(dead_code)]
    pub health_grpc_max_age_secs: u64,
    pub snapshot_launch_limit: usize,
    pub snapshot_trade_limit: usize,
    #[allow(dead_code)]
    pub cursor_overlap_slots: u64,
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
            clear_redis: env::var("CLEAR_REDIS")
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
            health_port: env::var("HEALTH_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5567),
            health_grpc_max_age_secs: env::var("HEALTH_GRPC_MAX_AGE_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(120),
            snapshot_launch_limit: env::var("WS_INITIAL_SNAPSHOT_LAUNCH_LIMIT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            snapshot_trade_limit: env::var("WS_INITIAL_SNAPSHOT_TRADE_LIMIT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000),
            cursor_overlap_slots: env::var("CURSOR_OVERLAP_SLOTS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(2),
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