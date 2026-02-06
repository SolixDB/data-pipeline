# WebSocket server

**Source:** `src/ws_server.rs`

This document describes the WebSocket server: how connections are accepted, how data is broadcast to clients, and how connection limits and lag are handled.

---

## Purpose

The server accepts many WebSocket connections, assigns each a unique `client_id`, and **broadcasts** the same stream of binary messages (token launches and trades) to all connected clients. It is designed for one producer (the data pipeline) and many consumers (terminals or APIs), with low latency and minimal copying. Connection count and client lag can be limited so that slow clients do not exhaust resources or receive stale data indefinitely.

---

## WsConfig

Configuration for the WebSocket server. Built from the environment or passed explicitly when constructing the server.

| Field | Environment variable | Description |
|-------|----------------------|-------------|
| `max_connections` | `WS_MAX_CONNECTIONS` | Maximum number of concurrent connections. `0` or unset means no limit. When the limit is reached, new TCP connections are rejected before the WebSocket handshake. |
| `max_lag_drop` | `WS_MAX_LAG_DROP` | If a client falls behind and the broadcast channel delivers a "lagged" result with at least this many dropped messages in one lag event, the server disconnects that client. The client can reconnect and receive fresh data. `0` or unset means the server never disconnects on lag (it only logs). |

`WsConfig::default()` reads these from the environment. You can also build a `WsConfig` in code and pass it to `WsServer::new(config)`.

---

## WsServer

### Model

- A single **broadcast channel** (e.g. `tokio::sync::broadcast`) with a fixed capacity (e.g. 4096 messages).
- The **producer** (the pipeline that receives gRPC updates and parses them) calls **`broadcast(message: Vec<u8>)`** once per event. The channel fans out to every subscribed client.
- Each **client** has its own task: it receives from the channel and writes to its WebSocket. Messages are stored as **`Bytes`** so that clones in the hot path are refcount-only.

### Main fields

- **broadcast_tx:** Sender for the broadcast channel. Messages are sent as `Bytes`.
- **next_id:** Atomic counter used to assign a unique `client_id` to each new connection.
- **client_count:** Atomic count of currently connected clients (for logging and metrics).
- **semaphore:** Optional. When `max_connections` is set, the semaphore limits how many connections can be active. New connections that cannot acquire a permit are closed before the WebSocket handshake.
- **max_lag_drop:** Optional. When set, any client that lags and drops at least this many messages in one lag event is disconnected.

### Methods

- **`new(config: WsConfig)`:** Creates the broadcast channel and, if `max_connections` is set, the semaphore.
- **`start(self: Arc<Self>, port: u16)`:** Binds the TCP listener (with SO_REUSEADDR and a listen backlog), spawns the accept loop in a background task, and **returns the task’s `JoinHandle`**. The caller (e.g. `main`) must await this handle to keep the process running.
- **`broadcast(&self, message: Vec<u8>)`:** Sends one message to all current subscribers. The message is converted to `Bytes` and sent on the broadcast sender. If there are no subscribers, the send may be a no-op depending on the channel semantics.
- **`client_count(&self)`:** Returns the current number of connected clients (lock-free).

Internal helpers (e.g. for acquiring a connection permit, subscribing a client, or updating counts) are used by the accept loop and per-connection handler.

---

## Accept loop and connection handling

1. **Bind:** The listener is bound with SO_REUSEADDR and a listen backlog (e.g. 1024), then wrapped in a `tokio::net::TcpListener`.
2. **Accept:** For each accepted TCP stream:
   - If `max_connections` is set and no permit is available, the stream is closed and a warning is logged.
   - Otherwise a **connection guard** (permit or placeholder for "unlimited") is acquired and passed into the per-connection task. The guard is released when the connection closes.
3. **Per connection:** A new task is spawned that runs **`handle_connection(stream, server, guard)`**.

---

## handle_connection (per-client lifecycle)

1. **TCP:** The stream may be tuned (e.g. `set_nodelay(true)`) for lower latency.
2. **WebSocket:** The stream is upgraded with `accept_async(stream)` and then split into send and receive halves.
3. **Identity and subscription:** A `client_id` is allocated, the task subscribes to the broadcast channel, and the client count is incremented. Connection is logged.
4. **Welcome message:** The server sends one **connected** message (e.g. `ServerMessage::Connected(ConnectedPayload { client_id })`) as a MessagePack binary frame.
5. **Event loop:** A loop runs until the connection ends:
   - **From broadcast channel:** When a message is received:
     - **Ok(msg):** The message is sent to the client as a WebSocket binary frame. If the send fails (e.g. client disconnected), the loop exits.
     - **Lagged(n):** The client fell behind and the channel dropped `n` messages. If `max_lag_drop` is set and `n >= max_lag_drop`, the client is disconnected (loop exits) so it can reconnect and get a fresh stream; otherwise the lag is only logged.
     - **Closed:** The channel is closed; the loop exits.
   - **From client:** When a WebSocket frame is received:
     - **Ping:** The server replies with **Pong**.
     - **Close** or **None** or **error:** The loop exits.
     - **Other frames:** Ignored (e.g. text or binary from client are not parsed as commands yet).
6. **Cleanup:** The client count is decremented and disconnection is logged. Dropping the connection guard releases the slot if a connection limit is in effect.

---

## Design choices

- **Broadcast channel:** The producer pays O(1) per message; each subscriber gets a copy (via `Bytes`, so only a refcount is incremented). Slow clients do not block the producer or other clients.
- **Lock-free counters:** `client_id` and `client_count` use atomics so the hot path avoids mutexes.
- **One task per connection:** Each client is handled in its own task so a slow or stuck client does not block the accept loop or other connections.
- **Lag handling:** When a client falls behind, the broadcast channel may drop messages. If `max_lag_drop` is set and the number of dropped messages in one lag event reaches the limit, the client is disconnected so it can reconnect and receive a consistent, up-to-date stream.
- **Connection limit:** The semaphore plus SO_REUSEADDR prevents unbounded connections and avoids port/address issues on restart.

---

## Environment variables

| Variable | Default | Effect |
|----------|---------|--------|
| `WS_MAX_CONNECTIONS` | No limit | Maximum concurrent WebSocket connections; new connections beyond this are rejected. |
| `WS_MAX_LAG_DROP` | No disconnect | Disconnect a client if it drops at least this many messages in one lag event. |

The listen port is read in `main` from `WS_PORT` (default 5566).

---

## See also

- [main.md](main.md) — How the server is started and how the join handle is used.
- [messages.md](messages.md) — Format of the **connected** message and other broadcast payloads.
- [client.md](client.md) — Producer side: gRPC client that feeds the pipeline which calls `broadcast()`.
