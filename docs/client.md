# `client.rs` — gRPC (Geyser) Client

**Path:** `src/client.rs`

## Purpose

Builds and connects a **Yellowstone Geyser gRPC client** using [config](config.md). This client is intended to consume blockchain/Geyser streams; the pipeline will later forward that data to the WebSocket server (e.g. via `WsServer::broadcast`).

## Struct: `GrpcClient`

Wraps a `Config` and exposes:

- **`new(config: Config)`** — creates a client that will use the given config for connection.
- **`connect(&self)`** — async; returns a connected `GeyserGrpcClient<impl Interceptor + Clone>` or an error.

## Connection logic (`connect`)

1. **Builder**  
   `GeyserGrpcClient::build_from_shared(config.endpoint)` with:
   - `max_decoding_message_size(config.max_decoding_message_size)` (1 GiB from config).

2. **Auth**  
   If `config.api_token` is set, applies `.x_token(Some(token))` to the builder.

3. **TLS**  
   If `config.endpoint` starts with `https://` (case-insensitive), configures TLS with `ClientTlsConfig::new().with_native_roots()`.

4. **Connect**  
   `builder.connect().await`; errors are wrapped with `anyhow` and a clear message.

## Usage in the project

The binary does not yet call `GrpcClient::new` or `connect()`; it only loads and validates `Config`. When you add the data pipeline that subscribes to Geyser streams and pushes token/trade data to the WebSocket server, you will:

- Construct `GrpcClient::new(config)` (with the same `Config` used in `main`).
- Call `client.connect().await` and use the returned client to subscribe to the needed streams.
- Serialize updates (e.g. MessagePack) and call `ws_server.broadcast(message)` so all connected terminals receive them.

## See also

- [config.md](config.md) — Source of `Config`
- [ws_server.md](ws_server.md) — `broadcast()` for pushing data to WebSocket clients
- [messages.md](messages.md) — Wire format for server/client messages
