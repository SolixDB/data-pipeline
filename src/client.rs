use crate::config::Config;
use anyhow::Context;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient, Interceptor};

pub struct GrpcClient {
    config: Config,
}

impl GrpcClient {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub async fn connect(
      &self
    ) -> anyhow::Result<GeyserGrpcClient<impl Interceptor + Clone>> {
        let mut builder = GeyserGrpcClient::build_from_shared(self.config.endpoint.clone())
            .context("Failed to create gRPC client builder")?
            .max_decoding_message_size(self.config.max_decoding_message_size.clone());

        if let Some(token) = &self.config.api_token {
            builder = builder
                .x_token(Some(token.clone()))
                .context("Failed to set API token")?;
        }

        if self.config.endpoint.to_lowercase().starts_with("https://") {
            builder = builder
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .context("Failed to configure TLS")?;
        }

        builder
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to gRPC endpoint: {}", e))
    }
}
