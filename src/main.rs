mod config;
mod client;
mod messages;
mod ws_server;

use std::sync::Arc;
use crate::{config::Config, ws_server::WsServer};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  // TLS crypto provider required by rustls
  rustls::crypto::aws_lc_rs::default_provider()
    .install_default()
    .expect("Failed to install rustls crypto provider");
  
  // Logging setup - uses RUST_LOG env var, defaults to info level
  tracing_subscriber::fmt()
    .with_env_filter(
      tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    )
    .with_target(false)
    .with_thread_ids(false)
    .with_file(false)
    .with_line_number(false)
    .init();
  
  // Initialize configuration
  let config = Config::default();
  config.validate()?;
  
  // Websocket server
  let ws_port: u16 = std::env::var("WS_PORT")
    .ok()
    .and_then(|s| s.parse().ok())
    .unwrap_or(5566);
  
  let ws_config = ws_server::WsConfig::default();
  let ws_server = Arc::new(WsServer::new(ws_config));
  let ws_handle = ws_server.start(ws_port).await;
  // Keep process alive: run WebSocket server until it exits (e.g. on fatal error)
  ws_handle.await.expect("WebSocket server task panicked");

  Ok(())
}
