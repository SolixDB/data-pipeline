//! HTTP health endpoint: GET /health returns 200 if gRPC activity is recent.

use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing::{debug, error, info};

/// Shared state for health checks. Updated by the gRPC handler on each transaction.
pub struct HealthState {
    /// Last gRPC activity time as Unix timestamp (seconds since epoch).
    pub last_activity_secs: AtomicU64,
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            last_activity_secs: AtomicU64::new(0),
        }
    }

    /// Call from handler when a transaction is received.
    pub fn record_activity(&self) {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_activity_secs.store(secs, Ordering::Relaxed);
    }
}

/// Spawns the health HTTP server. If `port` is 0, does nothing.
pub fn spawn_health_server(port: u16, max_age_secs: u64, state: Arc<HealthState>) {
    if port == 0 {
        return;
    }
    let addr = (Ipv4Addr::UNSPECIFIED, port);
    tokio::spawn(async move {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Health server bind 0.0.0.0:{}: {}", port, e);
                return;
            }
        };
        info!("Health server listening on 0.0.0.0:{}", port);
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    error!("Health accept: {}", e);
                    continue;
                }
            };
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                if let Err(e) = serve_health(stream, max_age_secs, &*state).await {
                    debug!("Health request error: {}", e);
                }
            });
        }
    });
}

async fn serve_health(
    mut stream: tokio::net::TcpStream,
    max_age_secs: u64,
    state: &HealthState,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(&mut stream);
    let mut first_line = String::new();
    reader.read_line(&mut first_line).await?;
    let first_line = first_line.trim();
    let ok = first_line.starts_with("GET ") && first_line.contains("/health");
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let last = state.last_activity_secs.load(Ordering::Relaxed);
    let recent = last > 0 && now_secs.saturating_sub(last) <= max_age_secs;
    let status = if ok && recent { 200 } else { 503 };
    let body = if status == 200 { "OK" } else { "unhealthy" };
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        if status == 200 { "OK" } else { "Service Unavailable" },
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}
