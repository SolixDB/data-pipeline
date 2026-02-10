use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

pub struct HealthState {
    last_activity_secs: AtomicU64,
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            last_activity_secs: AtomicU64::new(now_secs()),
        }
    }

    pub fn record_activity(&self) {
        self.last_activity_secs
            .store(now_secs(), Ordering::Release);
    }

    #[allow(dead_code)]
    fn last_activity(&self) -> u64 {
        self.last_activity_secs.load(Ordering::Acquire)
    }
}

#[allow(dead_code)]
pub fn spawn_health_server(port: u16, max_age_secs: u64, state: Arc<HealthState>) {
    if port == 0 {
        return;
    }

    let addr = (Ipv4Addr::UNSPECIFIED, port);

    tokio::spawn(async move {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("health bind 0.0.0.0:{}: {}", port, e);
                return;
            }
        };

        info!("health server listening on 0.0.0.0:{}", port);

        loop {
            let (stream, _) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    error!("health accept: {}", e);
                    continue;
                }
            };

            let state = Arc::clone(&state);

            tokio::spawn(async move {
                if let Err(e) = serve_health(stream, max_age_secs, &state).await {
                    debug!("health request error: {}", e);
                }
            });
        }
    });
}

#[allow(dead_code)]
async fn serve_health(
    mut stream: TcpStream,
    max_age_secs: u64,
    state: &HealthState,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(&mut stream);
    let mut first_line = String::new();
    reader.read_line(&mut first_line).await?;

    let is_health = is_health_request(&first_line);
    let now = now_secs();
    let last = state.last_activity();

    let recent = now.saturating_sub(last) <= max_age_secs;
    let healthy = is_health && recent;

    if !healthy {
        debug!(
            "health unhealthy: is_health={} last={} now={} max_age={}",
            is_health, last, now, max_age_secs
        );
    }

    let (status, status_text, body) = if healthy {
        (200, "OK", "OK")
    } else {
        (503, "Service Unavailable", "unhealthy")
    };

    let response = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        status,
        status_text,
        body.len(),
        body
    );

    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

#[allow(dead_code)]
fn is_health_request(first_line: &str) -> bool {
    let line = first_line.trim().trim_start_matches('\u{feff}');
    let upper = line.to_ascii_uppercase();

    if !upper.starts_with("GET ") {
        return false;
    }

    let path = upper
        .get(4..)
        .and_then(|s| s.split_whitespace().next())
        .unwrap_or("");

    path == "/" || path.starts_with("/HEALTH")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
