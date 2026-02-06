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
        Ok(())
    }
}