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