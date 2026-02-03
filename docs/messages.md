# `messages.rs` — WebSocket Message Types

**Path:** `src/messages.rs`

## Purpose

Defines the **wire format** for WebSocket messages between the server and trading terminals. All server→client and client→server message types are serialized (e.g. MessagePack) and sent as binary WebSocket frames.

## Server → Client

### `ServerMessage` (enum)

Tagged with `type` and `data` (e.g. JSON/MessagePack). Variants:

| Variant | Use |
|--------|-----|
| **`Connected(ConnectedPayload)`** | Sent once after a client connects. Carries `client_id` so the terminal knows its ID. |
| **`Error(ErrorPayload)`** | For future use: structured errors with `code` and `message`. |

### `ConnectedPayload`

- **`client_id: u64`** — Unique ID assigned by the server for this connection (used in logs and for future per-client features).

### `ErrorPayload` (reserved)

- **`code: String`**, **`message: String`** — For future error responses.

---

## Client → Server (reserved)

### `ClientMessage` (enum)

Planned client commands; not yet handled by the server.

| Variant | Purpose |
|--------|---------|
| **`Ping(PingPayload)`** | Keepalive / latency check (`timestamp`). |
| **`Subscribe(SubscribePayload)`** | Future: subscribe to a `channel` (e.g. symbol or stream). |
| **`Unsubscribe(UnsubscribePayload)`** | Future: unsubscribe from a `channel`. |

Payload structs: `PingPayload { timestamp }`, `SubscribePayload { channel }`, `UnsubscribePayload { channel }`.

The WebSocket server currently only reacts to protocol-level **Ping/Pong/Close**; it does not yet parse or dispatch `ClientMessage`. When you add channel subscription, you will parse incoming binary frames as MessagePack `ClientMessage` and act on `Subscribe`/`Unsubscribe`.

## Serialization

- **Format:** MessagePack (binary), via `rmp_serde`.
- **Usage:** In [ws_server.rs](ws_server.md), the server sends `ServerMessage::Connected(...)` as MessagePack binary in the welcome message; other server messages (e.g. token/trade data) will be serialized the same way and sent via `WsServer::broadcast()`.

## Naming

All enum tags use `snake_case` via `#[serde(rename_all = "snake_case")]` (e.g. `connected`, `error`, `ping`, `subscribe`, `unsubscribe`).

## See also

- [ws_server.md](ws_server.md) — Where `ServerMessage::Connected` is sent and where `broadcast` data is sent as binary
