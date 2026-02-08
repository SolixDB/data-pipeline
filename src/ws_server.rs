use crate::messages::{ConnectedPayload, ServerMessage};
use crate::storage::SnapshotState;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock, Semaphore};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};
use redis::AsyncCommands;

/// Broadcast channel capacity - handles burst traffic
const BROADCAST_CAPACITY: usize = 4096;

/// Configuration for the WebSocket server (trading terminal central server).
#[derive(Clone, Debug)]
pub struct WsConfig {
    /// Max concurrent connections; None = unlimited.
    pub max_connections: Option<usize>,
    /// If a client drops this many messages in one lag event, disconnect them so they reconnect for fresh state.
    /// None = never disconnect on lag (only log).
    pub max_lag_drop: Option<u64>,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            max_connections: std::env::var("WS_MAX_CONNECTIONS")
                .ok()
                .and_then(|s| s.parse().ok())
                .and_then(|n: usize| if n == 0 { None } else { Some(n) }),
            max_lag_drop: std::env::var("WS_MAX_LAG_DROP")
                .ok()
                .and_then(|s| s.parse().ok())
                .and_then(|n: u64| if n == 0 { None } else { Some(n) }),
        }
    }
}

/// Bind with SO_REUSEADDR so restarts don't get "address in use"; optional larger backlog.
fn bind_reuse(addr: SocketAddr) -> std::io::Result<TcpListener> {
    use socket2::{Domain, Socket, Type};
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None)?;
    socket.set_reuse_address(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    let std_listener = std::net::TcpListener::from(socket);
    std_listener.set_nonblocking(true)?;
    TcpListener::from_std(std_listener)
}

/// Guard that holds a connection slot when max_connections is set; dropped when connection closes.
enum ConnectionGuard {
    Unlimited,
    #[allow(dead_code)]
    Limited(tokio::sync::OwnedSemaphorePermit),
}

pub struct WsServer {
    broadcast_tx: broadcast::Sender<Bytes>,
    next_id: AtomicU64,
    client_count: AtomicU64,
    semaphore: Option<Arc<Semaphore>>,
    max_lag_drop: Option<u64>,
    /// When set, on connect we send the four snapshots from this state (no Redis required).
    snapshot_state: Option<Arc<RwLock<SnapshotState>>>,
    /// When set and snapshot_state is None, on connect we send snapshots from Redis.
    redis_client: Option<Arc<redis::Client>>,
}

impl WsServer {
    pub fn new(
        config: WsConfig,
        snapshot_state: Option<Arc<RwLock<SnapshotState>>>,
        redis_client: Option<Arc<redis::Client>>,
    ) -> Self {
        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let semaphore = config
            .max_connections
            .map(Semaphore::new)
            .map(Arc::new);
        Self {
            broadcast_tx,
            next_id: AtomicU64::new(0),
            client_count: AtomicU64::new(0),
            semaphore,
            max_lag_drop: config.max_lag_drop,
            snapshot_state,
            redis_client,
        }
    }

    /// Acquire a connection slot if limit is set; returns None when at capacity.
    fn try_acquire_connection(self: &Arc<Self>) -> Option<ConnectionGuard> {
        match &self.semaphore {
            Some(s) => s
                .clone()
                .try_acquire_owned()
                .ok()
                .map(ConnectionGuard::Limited),
            None => Some(ConnectionGuard::Unlimited),
        }
    }

    /// Starts TCP listener and spawns connection handler for each client.
    /// Returns a join handle that runs until the listener is closed; await it to keep the process alive.
    pub async fn start(self: &Arc<Self>, port: u16) -> tokio::task::JoinHandle<()> {
        let addr: SocketAddr = ([0, 0, 0, 0], port).into();
        let listener = bind_reuse(addr).expect("Failed to bind WebSocket server");
        info!("WebSocket server listening on ws://{}", addr);

        let server = Arc::clone(self);

        tokio::spawn(async move {
            while let Ok((stream, addr)) = listener.accept().await {
                let server = Arc::clone(&server);

                let guard = match server.try_acquire_connection() {
                    Some(g) => g,
                    None => {
                        warn!("Rejected connection from {}: max connections reached", addr);
                        drop(stream);
                        continue;
                    }
                };

                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, server, guard).await {
                        error!("WebSocket error from {}: {}", addr, e);
                    }
                });
            }
        })
    }

    /// O(1) broadcast - just writes to channel, no iteration or locks
    /// Accepts pre-serialized MessagePack binary; uses Bytes for zero-copy clone to subscribers
    #[inline]
    pub fn broadcast(&self, message: Vec<u8>) {
        match self.broadcast_tx.send(Bytes::from(message)) {
            Ok(_) => {}
            Err(_) => {
                // No active receivers - this is fine, just means no clients connected
            }
        }
    }

    /// Returns current client count (lock-free atomic read)
    #[inline]
    pub fn client_count(&self) -> u64 {
        self.client_count.load(Ordering::Relaxed)
    }

    /// Subscribe to broadcast channel (called by each client handler)
    fn subscribe(&self) -> broadcast::Receiver<Bytes> {
        self.broadcast_tx.subscribe()
    }

    /// Allocate next client ID (lock-free atomic increment)
    fn next_client_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Increment client count
    fn client_connected(&self) {
        self.client_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement client count
    fn client_disconnected(&self) {
        self.client_count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Handles individual WebSocket connection lifecycle
async fn handle_connection(
    stream: TcpStream,
    server: Arc<WsServer>,
    _guard: ConnectionGuard,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // TCP optimizations
    stream.set_nodelay(true)?;

    let ws_stream = tokio_tungstenite::accept_async(stream).await?;
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Get client ID and subscribe to broadcast (both lock-free)
    let client_id = server.next_client_id();
    let mut broadcast_rx = server.subscribe();
    
    server.client_connected();
    let count = server.client_count();
    info!("Client {} connected ({} total)", client_id, count);

    // Send welcome message (MessagePack binary)
    let welcome = ServerMessage::Connected(ConnectedPayload { client_id });
    let welcome_msg = rmp_serde::to_vec(&welcome).unwrap();
    ws_sender.send(Message::Binary(welcome_msg.into())).await?;

    // Snapshot on connect: from in-memory state (no Redis) or from Redis when configured
    if let Some(ref state) = server.snapshot_state {
        // Avoid race: if snapshot is empty, give storage worker a short time to populate (e.g. first connect)
        let mut snap = state.read().await.clone();
        for attempt in 0..5 {
            if !snap.launches.is_empty() {
                break;
            }
            drop(snap);
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            snap = state.read().await.clone();
            if attempt > 0 {
                info!("Snapshot was empty, retry {} for client {}", attempt + 1, client_id);
            }
        }
        info!(
            "Sending snapshot to client {}: {} launches, {} trades",
            client_id,
            snap.launches.len(),
            snap.trades.len()
        );
        if let Ok(bytes) = rmp_serde::to_vec(&ServerMessage::SnapshotMigrated(snap.migrated)) {
            let _ = ws_sender.send(Message::Binary(bytes.into())).await;
        }
        if let Ok(bytes) = rmp_serde::to_vec(&ServerMessage::SnapshotSoonMigrating(snap.soon_migrating)) {
            let _ = ws_sender.send(Message::Binary(bytes.into())).await;
        }
        if let Ok(bytes) = rmp_serde::to_vec(&ServerMessage::SnapshotLaunches(snap.launches.clone())) {
            let _ = ws_sender.send(Message::Binary(bytes.into())).await;
        }
        if let Ok(bytes) = rmp_serde::to_vec(&ServerMessage::SnapshotTrades(snap.trades)) {
            let _ = ws_sender.send(Message::Binary(bytes.into())).await;
        }
    } else if let Some(ref redis) = server.redis_client {
        if let Ok(mut conn) = redis.get_multiplexed_async_connection().await {
            let migrated: Result<String, _> = conn.get(crate::storage::REDIS_KEY_MIGRATED).await;
            let soon: Result<String, _> = conn.get(crate::storage::REDIS_KEY_SOON_MIGRATING).await;
            if let Ok(json) = migrated {
                if let Ok(entries) = serde_json::from_str::<Vec<crate::storage::MigratedEntry>>(&json) {
                    if let Ok(bytes) = rmp_serde::to_vec(&ServerMessage::SnapshotMigrated(entries)) {
                        let _ = ws_sender.send(Message::Binary(bytes.into())).await;
                    }
                }
            }
            if let Ok(json) = soon {
                if let Ok(entries) = serde_json::from_str::<Vec<crate::storage::SoonMigratingEntry>>(&json) {
                    if let Ok(bytes) = rmp_serde::to_vec(&ServerMessage::SnapshotSoonMigrating(entries)) {
                        let _ = ws_sender.send(Message::Binary(bytes.into())).await;
                    }
                }
            }
            let launches: Result<String, _> = conn.get(crate::storage::REDIS_KEY_LAUNCHES).await;
            let trades: Result<String, _> = conn.get(crate::storage::REDIS_KEY_TRADES).await;
            if let Ok(json) = launches {
                if let Ok(entries) = serde_json::from_str::<Vec<crate::messages::TokenLaunchPayload>>(&json) {
                    if let Ok(bytes) = rmp_serde::to_vec(&ServerMessage::SnapshotLaunches(entries)) {
                        let _ = ws_sender.send(Message::Binary(bytes.into())).await;
                    }
                }
            }
            if let Ok(json) = trades {
                if let Ok(entries) = serde_json::from_str::<Vec<crate::events::Trade>>(&json) {
                    if let Ok(bytes) = rmp_serde::to_vec(&ServerMessage::SnapshotTrades(entries)) {
                        let _ = ws_sender.send(Message::Binary(bytes.into())).await;
                    }
                }
            }
        }
    }

    // Event loop: receive from broadcast channel and handle client messages
    loop {
        tokio::select! {
            // Receive broadcast messages (from the shared channel)
            result = broadcast_rx.recv() => {
                match result {
                    Ok(msg) => {
                        // Bytes::clone() is refcount-only; no buffer copy
                        if ws_sender.send(Message::Binary(msg)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        let n_u64 = n as u64;
                        if let Some(limit) = server.max_lag_drop {
                            if n_u64 >= limit {
                                warn!(
                                    "Client {} lagged {} messages (limit {}), disconnecting for fresh state",
                                    client_id, n_u64, limit
                                );
                                break;
                            }
                        }
                        warn!("Client {} lagged, dropped {} messages", client_id, n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
            // Handle incoming client messages (ping/pong/close)
            msg = ws_receiver.next() => {
                match msg {
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws_sender.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    Some(Err(_)) => {
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    server.client_disconnected();
    let count = server.client_count();
    info!("Client {} disconnected ({} remaining)", client_id, count);

    Ok(())
}
