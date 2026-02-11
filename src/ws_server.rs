use crate::messages::{ConnectedPayload, ServerMessage, ServerMessageEnvelope};
use crate::storage::SnapshotState;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};
use redis::AsyncCommands;
use serde_json;

const BROADCAST_CAPACITY: usize = 16384;
/// Max messages kept for selective-repeat retransmit. Should be >= BROADCAST_CAPACITY.
const REPLAY_BUFFER_LEN: usize = 16384;
const RETRANSMIT_INTERVAL_MS: u64 = 100;
const RETRANSMIT_MAX_PER_CLIENT_PER_TICK: usize = 200;

/// Bounded replay buffer for selective repeat: seq -> payload. Evicts oldest when full.
struct ReplayBuffer {
    map: HashMap<u64, Bytes>,
    order: VecDeque<u64>,
    max_len: usize,
}

impl ReplayBuffer {
    fn new(max_len: usize) -> Self {
        Self {
            map: HashMap::with_capacity(max_len),
            order: VecDeque::with_capacity(max_len),
            max_len,
        }
    }

    fn push(&mut self, seq: u64, payload: Bytes) {
        while self.order.len() >= self.max_len {
            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            }
        }
        if self.map.insert(seq, payload).is_none() {
            self.order.push_back(seq);
        }
    }

    fn get(&self, seq: u64) -> Option<Bytes> {
        self.map.get(&seq).cloned()
    }

    fn max_seq(&self) -> u64 {
        self.order.back().copied().unwrap_or(0)
    }

    #[allow(dead_code)]
    fn min_seq(&self) -> u64 {
        self.order.front().copied().unwrap_or(0)
    }
}

/// Per-client state for selective repeat: last cumulative ACK and channel to push retransmits.
struct ClientHandle {
    last_acked: AtomicU64,
    retransmit_tx: mpsc::Sender<Bytes>,
}

#[derive(Clone, Debug)]
pub struct BroadcastMessage {
    pub topic: String,
    pub payload: Bytes,
}

#[derive(Clone, Debug)]
pub struct WsConfig {
    pub max_lag_drop: Option<u64>,
    /// When true, send snapshot_* (launches, trades, tickers, migrated, soon_migrating) on each client connect.
    pub send_snapshot_on_connect: bool,
}

impl Default for WsConfig {
    fn default() -> Self {
        let send_snapshot = std::env::var("WS_SEND_SNAPSHOT_ON_CONNECT")
            .ok()
            .map(|s| matches!(s.to_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(true);
        Self {
            max_lag_drop: std::env::var("WS_MAX_LAG_DROP")
                .ok()
                .and_then(|s| s.parse().ok())
                .and_then(|n: u64| if n == 0 { None } else { Some(n) }),
            send_snapshot_on_connect: send_snapshot,
        }
    }
}

async fn retransmit_loop(server: Arc<WsServer>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(RETRANSMIT_INTERVAL_MS));
    loop {
        interval.tick().await;
        let clients_to_retransmit: Vec<(u64, u64, mpsc::Sender<Bytes>)> = {
            let buf = match server.replay_buffer.try_read() {
                Ok(b) => b,
                Err(_) => continue,
            };
            let max_seq = buf.max_seq();
            if max_seq == 0 {
                continue;
            }
            let reg = match server.client_registry.try_read() {
                Ok(r) => r,
                Err(_) => continue,
            };
            reg.iter()
                .filter(|(_, h)| h.last_acked.load(Ordering::Acquire) + 1 < max_seq)
                .map(|(&id, h)| (id, h.last_acked.load(Ordering::Acquire), h.retransmit_tx.clone()))
                .collect()
        };
        for (client_id, last_acked, retransmit_tx) in clients_to_retransmit {
            let buf = match server.replay_buffer.try_read() {
                Ok(b) => b,
                Err(_) => break,
            };
            let max_seq = buf.max_seq();
            let mut sent = 0;
            for seq in (last_acked + 1)..=max_seq {
                if sent >= RETRANSMIT_MAX_PER_CLIENT_PER_TICK {
                    break;
                }
                if let Some(payload) = buf.get(seq) {
                    if retransmit_tx.try_send(payload).is_ok() {
                        sent += 1;
                    }
                }
            }
            if sent > 0 {
                debug!("Selective repeat: sent {} messages to client {}", sent, client_id);
            }
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

pub struct WsServer {
    broadcast_tx: broadcast::Sender<Arc<BroadcastMessage>>,
    next_id: AtomicU64,
    next_seq: AtomicU64,
    client_count: AtomicU64,
    max_lag_drop: Option<u64>,
    send_snapshot_on_connect: bool,
    snapshot_state: Option<Arc<RwLock<SnapshotState>>>,
    redis_client: Option<Arc<redis::Client>>,
    /// Replay buffer for selective repeat: retransmit to clients that didn't ACK.
    replay_buffer: Arc<RwLock<ReplayBuffer>>,
    /// Per-client last acked seq and retransmit channel.
    client_registry: Arc<RwLock<HashMap<u64, ClientHandle>>>,
}

impl WsServer {
    pub fn new(
        config: WsConfig,
        snapshot_state: Option<Arc<RwLock<SnapshotState>>>,
        redis_client: Option<Arc<redis::Client>>,
    ) -> Self {
        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            broadcast_tx,
            next_id: AtomicU64::new(0),
            next_seq: AtomicU64::new(0),
            client_count: AtomicU64::new(0),
            max_lag_drop: config.max_lag_drop,
            send_snapshot_on_connect: config.send_snapshot_on_connect,
            snapshot_state,
            redis_client,
            replay_buffer: Arc::new(RwLock::new(ReplayBuffer::new(REPLAY_BUFFER_LEN))),
            client_registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns next monotonic sequence number for a message (increments counter).
    pub fn next_seq(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::SeqCst)
    }

    /// Broadcasts a server message with an assigned seq; stores in replay buffer for selective repeat.
    pub fn broadcast_message(&self, topic: String, message: ServerMessage) {
        let seq = self.next_seq();
        let envelope = ServerMessageEnvelope { message, seq };
        if let Ok(bytes) = rmp_serde::to_vec_named(&envelope) {
            let payload = Bytes::from(bytes.clone());
            if let Ok(mut buf) = self.replay_buffer.try_write() {
                buf.push(seq, payload);
            }
            self.broadcast(topic, bytes);
        }
    }

    pub async fn start(self: &Arc<Self>, port: u16) -> tokio::task::JoinHandle<()> {
        let addr: SocketAddr = ([0, 0, 0, 0], port).into();
        let listener = bind_reuse(addr).expect("Failed to bind WebSocket server");
        info!("WebSocket server listening on ws://{}", addr);

        let server = Arc::clone(self);
        let server_retransmit = Arc::clone(&server);

        tokio::spawn(async move {
            // Selective repeat: retransmit to clients that haven't ACKed.
            tokio::spawn(async move {
                retransmit_loop(server_retransmit).await;
            });

            while let Ok((stream, addr)) = listener.accept().await {
                let server = Arc::clone(&server);

                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, server).await {
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

    /// Register a client for selective repeat; call when client connects.
    fn register_client(&self, client_id: u64, handle: ClientHandle) {
        if let Ok(mut r) = self.client_registry.try_write() {
            r.insert(client_id, handle);
        }
    }

    /// Unregister when client disconnects.
    pub fn unregister_client(&self, client_id: u64) {
        if let Ok(mut r) = self.client_registry.try_write() {
            r.remove(&client_id);
        }
    }

    /// Update client's last acked seq (cumulative); used for selective repeat.
    pub fn update_client_ack(&self, client_id: u64, seq: u64) {
        if let Ok(r) = self.client_registry.try_read() {
            if let Some(h) = r.get(&client_id) {
                let mut prev = h.last_acked.load(Ordering::Acquire);
                while prev < seq {
                    match h.last_acked.compare_exchange_weak(
                        prev,
                        seq,
                        Ordering::SeqCst,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(p) => prev = p,
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    server: Arc<WsServer>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stream.set_nodelay(true)?;

    let ws_stream = tokio_tungstenite::accept_async(stream).await?;
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    let client_id = server.next_client_id();
    let mut broadcast_rx = server.subscribe();
    let (retransmit_tx, mut retransmit_rx) = mpsc::channel::<Bytes>(256);
    server.register_client(
        client_id,
        ClientHandle {
            last_acked: AtomicU64::new(0),
            retransmit_tx,
        },
    );

    server.client_connected();
    let count = server.client_count();
    info!("Client {} connected ({} total)", client_id, count);

    let welcome = ServerMessage::Connected(ConnectedPayload {
        client_id,
        recommended_topics: vec!["launches".to_string(), "tickers".to_string()],
    });
    let welcome_envelope = ServerMessageEnvelope {
        message: welcome,
        seq: server.next_seq(),
    };
    let welcome_msg = rmp_serde::to_vec_named(&welcome_envelope).unwrap();
    ws_sender.send(Message::Binary(welcome_msg.into())).await?;

    if server.send_snapshot_on_connect {
        let mut sent_from_memory = false;
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
            sent_from_memory = !snap.launches.is_empty();
            if sent_from_memory {
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
            }
        }
        // When in-memory snapshot is missing (no state or empty launches), fall back to Redis
        if !sent_from_memory {
            if let Some(ref redis) = server.redis_client {
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
        }
    }

    let mut subscriptions = std::collections::HashSet::new();

    loop {
        tokio::select! {
            Some(payload) = retransmit_rx.recv() => {
                if ws_sender.send(Message::Binary(payload.into())).await.is_err() {
                    break;
                }
            }
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
                                // Close so client reconnects and gets fresh snapshot (+ selective repeat will repair on reconnect).
                                let _ = ws_sender.send(Message::Close(None)).await;
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
                        if let Ok(client_msg) =
                            serde_json::from_str::<crate::messages::ClientMessage>(&text)
                        {
                            match client_msg {
                                crate::messages::ClientMessage::Subscribe(topics) => {
                                    for topic in topics {
                                        let is_new = subscriptions.insert(topic.clone());

                                        // On first subscribe to a mint-like topic, proactively send the latest
                                        // TokenLaunchPayload so the client has token details before any trades.
                                        // We skip special aggregate topics like "launches" and "tickers".
                                        if is_new && topic != "launches" && topic != "tickers" {
                                            if let Some(ref state_lock) = server.snapshot_state {
                                                let launch_opt = {
                                                    let snap = state_lock.read().await;
                                                    snap.latest_launch_by_mint
                                                        .get(&topic)
                                                        .cloned()
                                                };
                                                if let Some(payload) = launch_opt {
                                                    if let Ok(bytes) = rmp_serde::to_vec_named(
                                                        &ServerMessage::TokenLaunch(payload),
                                                    ) {
                                                        let _ = ws_sender
                                                            .send(Message::Binary(bytes.into()))
                                                            .await;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    debug!(
                                        "Client {} subscribed to {} topics",
                                        client_id,
                                        subscriptions.len()
                                    );
                                }
                                crate::messages::ClientMessage::Unsubscribe(topics) => {
                                    for t in topics {
                                        subscriptions.remove(&t);
                                    }
                                    debug!(
                                        "Client {} unsubscribed, remaining: {}",
                                        client_id,
                                        subscriptions.len()
                                    );
                                }
                                crate::messages::ClientMessage::Ack(seq) => {
                                    server.update_client_ack(client_id, seq);
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

    server.unregister_client(client_id);
    server.client_disconnected();
    let count = server.client_count();
    info!("Client {} disconnected ({} remaining)", client_id, count);

    Ok(())
}
