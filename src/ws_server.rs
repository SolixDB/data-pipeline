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
use tracing::{debug, error, info, warn};
use redis::AsyncCommands;
use serde_json;

const BROADCAST_CAPACITY: usize = 4096;

#[derive(Clone, Debug)]
pub struct BroadcastMessage {
    pub topic: String,
    pub payload: Bytes,
}

#[derive(Clone, Debug)]
pub struct WsConfig {
    pub max_connections: Option<usize>,
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

enum ConnectionGuard {
    Unlimited,
    #[allow(dead_code)]
    Limited(tokio::sync::OwnedSemaphorePermit),
}

pub struct WsServer {
    broadcast_tx: broadcast::Sender<Arc<BroadcastMessage>>,
    next_id: AtomicU64,
    client_count: AtomicU64,
    semaphore: Option<Arc<Semaphore>>,
    max_lag_drop: Option<u64>,
    snapshot_state: Option<Arc<RwLock<SnapshotState>>>,
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

    #[inline]
    pub fn broadcast(&self, topic: String, message: Vec<u8>) {
        let msg = Arc::new(BroadcastMessage {
            topic,
            payload: Bytes::from(message),
        });
        let _ = self.broadcast_tx.send(msg);
    }

    #[inline]
    pub fn client_count(&self) -> u64 {
        self.client_count.load(Ordering::Relaxed)
    }

    fn subscribe(&self) -> broadcast::Receiver<Arc<BroadcastMessage>> {
        self.broadcast_tx.subscribe()
    }

    fn next_client_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn client_connected(&self) {
        self.client_count.fetch_add(1, Ordering::Relaxed);
    }

    fn client_disconnected(&self) {
        self.client_count.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn handle_connection(
    stream: TcpStream,
    server: Arc<WsServer>,
    _guard: ConnectionGuard,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stream.set_nodelay(true)?;

    let ws_stream = tokio_tungstenite::accept_async(stream).await?;
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    let client_id = server.next_client_id();
    let mut broadcast_rx = server.subscribe();
    
    server.client_connected();
    let count = server.client_count();
    info!("Client {} connected ({} total)", client_id, count);

    let welcome = ServerMessage::Connected(ConnectedPayload { client_id });
    let welcome_msg = rmp_serde::to_vec_named(&welcome).unwrap();
    ws_sender.send(Message::Binary(welcome_msg.into())).await?;

    if let Some(ref state) = server.snapshot_state {
        let mut snap = state.read().await.clone();
        for attempt in 0..20 {
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
        if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::SnapshotMigrated(snap.migrated)) {
            let _ = ws_sender.send(Message::Binary(bytes.into())).await;
        }
        if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::SnapshotSoonMigrating(snap.soon_migrating)) {
            let _ = ws_sender.send(Message::Binary(bytes.into())).await;
        }
        if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::SnapshotLaunches(snap.launches.clone())) {
            let _ = ws_sender.send(Message::Binary(bytes.into())).await;
        }
        if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::SnapshotTrades(snap.trades)) {
            let _ = ws_sender.send(Message::Binary(bytes.into())).await;
        }
        let tickers: Vec<_> = snap.tickers.values().map(crate::messages::TokenTicker::from).collect();
        if !tickers.is_empty() {
            if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::SnapshotTickers(tickers)) {
                let _ = ws_sender.send(Message::Binary(bytes.into())).await;
            }
        }
    } else if let Some(ref redis) = server.redis_client {
        if let Ok(mut conn) = redis.get_multiplexed_async_connection().await {
            let migrated: Result<String, _> = conn.get(crate::storage::REDIS_KEY_MIGRATED).await;
            let soon: Result<String, _> = conn.get(crate::storage::REDIS_KEY_SOON_MIGRATING).await;
            if let Ok(json) = migrated {
                if let Ok(entries) = serde_json::from_str::<Vec<crate::storage::MigratedEntry>>(&json) {
                    if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::SnapshotMigrated(entries)) {
                        let _ = ws_sender.send(Message::Binary(bytes.into())).await;
                    }
                }
            }
            if let Ok(json) = soon {
                if let Ok(entries) = serde_json::from_str::<Vec<crate::storage::SoonMigratingEntry>>(&json) {
                    if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::SnapshotSoonMigrating(entries)) {
                        let _ = ws_sender.send(Message::Binary(bytes.into())).await;
                    }
                }
            }
            let launches: Result<String, _> = conn.get(crate::storage::REDIS_KEY_LAUNCHES).await;
            let trades: Result<String, _> = conn.get(crate::storage::REDIS_KEY_TRADES).await;
            if let Ok(json) = launches {
                if let Ok(entries) = serde_json::from_str::<Vec<crate::messages::TokenLaunchPayload>>(&json) {
                    if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::SnapshotLaunches(entries)) {
                        let _ = ws_sender.send(Message::Binary(bytes.into())).await;
                    }
                }
            }
            if let Ok(json) = trades {
                if let Ok(entries) = serde_json::from_str::<Vec<crate::events::Trade>>(&json) {
                    if let Ok(bytes) = rmp_serde::to_vec_named(&ServerMessage::SnapshotTrades(entries)) {
                        let _ = ws_sender.send(Message::Binary(bytes.into())).await;
                    }
                }
            }
        }
    }

    let mut subscriptions = std::collections::HashSet::new();

    loop {
        tokio::select! {
            result = broadcast_rx.recv() => {
                match result {
                    Ok(msg) => {
                        let should_send = subscriptions.is_empty() || subscriptions.contains(&msg.topic);
                        
                        if should_send {
                            if ws_sender.send(Message::Binary(msg.payload.clone())).await.is_err() {
                                break;
                            }
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
            msg = ws_receiver.next() => {
                match msg {
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws_sender.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    Some(Ok(Message::Text(text))) => {
                         if let Ok(client_msg) = serde_json::from_str::<crate::messages::ClientMessage>(&text) {
                            match client_msg {
                                crate::messages::ClientMessage::Subscribe(topics) => {
                                    for t in topics {
                                        subscriptions.insert(t);
                                    }
                                    debug!("Client {} subscribed to {} topics", client_id, subscriptions.len());
                                }
                                crate::messages::ClientMessage::Unsubscribe(topics) => {
                                    for t in topics {
                                        subscriptions.remove(&t);
                                    }
                                    debug!("Client {} unsubscribed, remaining: {}", client_id, subscriptions.len());
                                }
                            }
                         }
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
