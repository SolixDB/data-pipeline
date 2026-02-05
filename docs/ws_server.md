# `ws_server.rs` — WebSocket Server

**Path:** `src/ws_server.rs`

## Purpose

Implements the **central WebSocket server** for trading terminals: accept many clients, assign each a `client_id`, and **broadcast** the same binary stream (token/trade data) to all connected clients with low latency and minimal copying. Tuned for a single producer (the data pipeline) and many consumers (terminals).

---

## WsConfig

Configuration for the WebSocket server; built from environment or passed explicitly.

| Field | Env var | Description |
|-------|---------|-------------|
| **`max_connections`** | `WS_MAX_CONNECTIONS` | Max concurrent connections. `0` or unset = unlimited. When at capacity, new TCP connections are rejected before the WebSocket handshake. |
| **`max_lag_drop`** | `WS_MAX_LAG_DROP` | If a client lags and drops **≥ this many messages in one lag event**, the server **disconnects** that client so they reconnect and get fresh state. `0` or unset = never disconnect on lag (only log). |

`WsConfig::default()` reads these env vars; you can also build a `WsConfig` in code and pass it to `WsServer::new(config)`.

---

## WsServer

### Core idea

- **One broadcast channel** (`tokio::sync::broadcast`) with capacity `BROADCAST_CAPACITY` (4096).
- The **producer** (future pipeline that consumes gRPC and pushes token/trade data) calls **`broadcast(message: Vec<u8>)`** once per update; the channel fans out to every connected client.
- Each **client** has its own task: it receives from the channel and writes to its WebSocket. Uses **`Bytes`** so clones in the hot path are refcount-only, not buffer copies.

### Main fields

- **`broadcast_tx`** — Sender for the broadcast channel; messages are `Bytes`.
- **`next_id`** — Atomic counter for assigning `client_id` to new connections.
- **`client_count`** — Atomic count of connected clients (for logging/metrics).
- **`semaphore`** — Optional; when set (from `max_connections`), limits how many connections can be active. Rejected connections are dropped before the WebSocket handshake.
- **`max_lag_drop`** — Optional; when set, any client that lags past this many dropped messages in one event is disconnected for fresh state.

### Key methods

- **`new(config: WsConfig)`** — Creates the broadcast channel and, if `max_connections` is set, a semaphore.
- **`start(self: &Arc<Self>, port: u16)`** — Binds the listener (with SO_REUSEADDR and backlog 1024), spawns the accept loop, and **returns a `JoinHandle`**. The caller (e.g. `main`) must await this handle to keep the process alive.
- **`broadcast(&self, message: Vec<u8>)`** — Pushes one message to all subscribers. Converts to `Bytes` and sends on `broadcast_tx`; no-op if there are no subscribers.
- **`client_count(&self)`** — Lock-free read of current connection count.

Internal helpers: `try_acquire_connection` (semaphore), `subscribe`, `next_client_id`, `client_connected`, `client_disconnected`.

---

## Accept loop and connection handling

1. **Bind** — `bind_reuse(addr)` uses `socket2` to set **SO_REUSEADDR** and a listen backlog of 1024, then wraps the listener in `tokio::net::TcpListener`.
2. **Accept** — For each accepted TCP stream:
   - **Connection limit:** If `max_connections` is set and no permit is available, the stream is dropped and a warning is logged.
   - Otherwise a **`ConnectionGuard`** (permit or unlimited) is acquired and passed into the per-connection task. The guard is dropped when the connection closes, releasing the slot.
3. **Per connection** — Spawns a task that runs **`handle_connection(stream, server, guard)`**.

---

## handle_connection (per-client lifecycle)

1. **TCP** — `stream.set_nodelay(true)` for lower latency.
2. **WebSocket** — `accept_async(stream)`, then split into `ws_sender` / `ws_receiver`.
3. **Identity and subscription** — Allocates `client_id`, subscribes to the broadcast channel, increments `client_count`, logs connection.
4. **Welcome** — Sends one `ServerMessage::Connected(ConnectedPayload { client_id })` as MessagePack binary.
5. **Event loop** — `tokio::select!`:
   - **From broadcast:** On `recv()`:
     - **Ok(msg)** — Sends `Message::Binary(msg)` to the client. If send fails (e.g. client gone), exits loop.
     - **Lagged(n)** — Client fell behind and dropped `n` messages. If `max_lag_drop` is set and `n >= limit`, **disconnect** (break) so the client can reconnect for fresh state; otherwise just log and continue.
     - **Closed** — Channel closed; exit loop.
   - **From client:** On `ws_receiver.next()`:
     - **Ping** — Reply with Pong.
     - **Close** or **None** or **error** — Exit loop.
     - Other frames — Ignored.
6. **Cleanup** — Decrements `client_count`, logs disconnect. Dropping `_guard` releases the connection slot if limited.

---

## Design choices (why it’s fast and safe)

- **Broadcast channel** — O(1) for the producer; each subscriber gets its own copy of the message (via `Bytes`, so only refcount is cloned).
- **Lock-free counters** — `client_id` and `client_count` use atomics so the hot path has no mutexes.
- **One task per connection** — Slow or stuck clients don’t block others or the accept loop.
- **Lag handling** — Clients that fall behind can be disconnected when they exceed `max_lag_drop`, so terminals get fresh data after reconnect instead of stale token/trade stream.
- **Connection limit** — Semaphore prevents unbounded connections and avoids “address in use” on restart (SO_REUSEADDR + capped clients).

---

## Environment variables (summary)

| Variable | Default | Effect |
|----------|---------|--------|
| `WS_MAX_CONNECTIONS` | no limit | Max concurrent WebSocket connections; reject above this. |
| `WS_MAX_LAG_DROP` | no disconnect | Disconnect client if it drops ≥ this many messages in one lag event. |

(WebSocket port is read in `main` from `WS_PORT`, default 5566.)

---

## See also

- [main.md](main.md) — How the server is started and kept alive
- [messages.md](messages.md) — `ServerMessage`, `ConnectedPayload`, and wire format
- [client.md](client.md) — Future producer of data that will call `broadcast()`
