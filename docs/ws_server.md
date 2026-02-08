# ws_server.rs — WebSocket server

**Source:** `src/ws_server.rs`

Single broadcast channel; each client gets the same stream of binary (MessagePack) frames. Handles accept loop, connection limit, lag-based disconnect, ping/pong, and snapshot-on-connect (in-memory or Redis). All behaviour is described here; message types are in [messages.md](messages.md).

---

## Constants and config

- **BROADCAST_CAPACITY:** 4096. Tokio broadcast channel capacity; slow clients that fall behind will get RecvError::Lagged and may be disconnected if WS_MAX_LAG_DROP is set.
- **WsConfig:** From env (Default): max_connections (WS_MAX_CONNECTIONS, 0 = unlimited), max_lag_drop (WS_MAX_LAG_DROP, 0 = never disconnect on lag).

---

## WsServer (fields)

- **broadcast_tx:** broadcast::Sender<Bytes>. Single producer; handler is the only sender.
- **next_id:** AtomicU64 — next client id.
- **client_count:** AtomicU64 — current number of connected clients.
- **semaphore:** Option<Arc<Semaphore>> — when set, try_acquire_owned() before accepting; reject when at capacity.
- **max_lag_drop:** Option<u64> — when set, if a client’s recv returns Lagged(n) and n >= this, disconnect that client.
- **snapshot_state:** Option<Arc<RwLock<SnapshotState>>> — when set, on connect we send the four snapshots from this in-memory state (updated by storage worker). See [storage.md](storage.md).
- **redis_client:** Option<Arc<redis::Client>> — when snapshot_state is None and this is set, snapshots are read from Redis (snapshot:migrated, snapshot:soon_migrating, snapshot:launches, snapshot:trades).

---

## start(self: &Arc<Self>, port: u16)

- Binds 0.0.0.0:port with SO_REUSEADDR (bind_reuse), listen backlog 1024.
- Spawns a task that loops: accept() → try_acquire_connection() (if semaphore, try_acquire_owned; else always succeed). If at capacity, drop stream and continue. Otherwise spawn handle_connection(stream, server, guard). Guard holds the semaphore permit until the connection closes (ConnectionGuard::Limited(permit) or Unlimited).
- Returns JoinHandle that runs until the listener is closed. main holds this handle so the process doesn’t exit.

---

## broadcast(&self, message: Vec<u8>)

Sends Bytes::from(message) on broadcast_tx. If there are no receivers (no clients), send returns an error and we ignore it. No lock; O(1). Handler calls this for every token_launch, trade, and migration.

---

## client_count(&self)

Returns client_count.load(Relaxed). Used for logs (“clients=N”).

---

## handle_connection (per client)

1. set_nodelay(true) on the TCP stream.
2. tokio_tungstenite::accept_async(stream) — WebSocket handshake.
3. Split into sender/receiver. Allocate client_id (next_id.fetch_add(1)+1), subscribe() to broadcast (new receiver for this client), increment client_count, log “Client X connected (Y total)”.
4. Send one ServerMessage::Connected(ConnectedPayload { client_id }) as MessagePack binary (welcome).
5. Snapshot on connect: if snapshot_state is set, read it (with short retries if launches empty), then send ServerMessage::SnapshotMigrated, SnapshotSoonMigrating, SnapshotLaunches, SnapshotTrades in that order. Else if redis_client is set, read the four Redis keys and send the same four message types. See [messages.md](messages.md) for connection order.
6. Loop (tokio::select!):
   - **broadcast_rx.recv():** On Ok(msg), send Message::Binary(msg) to client; on Lagged(n), if max_lag_drop is set and n >= limit, break (disconnect); else log and continue. On Closed, break.
   - **ws_receiver.next():** On Ping, send Pong. On Close or None or Err, break. Other frames (e.g. text) ignored.
7. Decrement client_count, log “Client X disconnected (Y remaining)”, return Ok.

So each client gets: one Connected, then the four snapshots (when snapshot_state or redis_client is set), then every broadcast message in order. If the client can’t keep up, it may be disconnected when Lagged(n) >= max_lag_drop. ConnectionGuard is held for the duration so the semaphore permit is released when the task exits.

---

## Dependencies

- **crate::messages:** ConnectedPayload, ServerMessage (for welcome and snapshots).
- **crate::storage:** SnapshotState (for snapshot_state field and on-connect read).
- **bytes::Bytes** — zero-copy clone for broadcast.
- **tokio** — TcpListener, TcpStream, spawn, sync (broadcast, Semaphore, RwLock).
- **tokio_tungstenite** — accept_async, Message (Binary, Ping, Pong, Close).
- **socket2** — SO_REUSEADDR, bind, listen (for bind_reuse).
- **redis** — AsyncCommands (when redis_client is set, for snapshot keys).

No config struct; WsConfig reads env in Default. WsServer::new(config, snapshot_state, redis_client) is called from main.
