# Geyser gRPC client

**Source:** `src/client.rs`

This document describes how the data-pipeline connects to the Yellowstone Geyser gRPC service and how that connection is used to feed the WebSocket stream.

---

## Purpose

The client builds and connects a **Yellowstone Geyser gRPC client** using the configuration from [config.md](config.md). The pipeline subscribes to transaction streams for the programs it cares about (Pump.fun, Pump AMM); parsed token launches and trades are then serialized and pushed to all WebSocket clients via `WsServer::broadcast`.

---

## GrpcClient

**Construction:** `GrpcClient::new(config)` creates a client that will use the given config for the connection.

**Connection:** `connect(&self)` is an async function that returns a connected Geyser gRPC client (or an error). The caller uses this client to subscribe to transaction updates.

---

## Connection logic (connect)

1. **Builder:** The client is built from `config.endpoint` with `max_decoding_message_size` set from config (e.g. 1 GiB).

2. **Authentication:** If `config.api_token` is set, the builder is configured with that token (e.g. via an interceptor or header).

3. **TLS:** If the endpoint URL starts with `https://` (case-insensitive), the client is configured to use TLS with default roots (e.g. `ClientTlsConfig::new().with_native_roots()`).

4. **Connect:** The builder’s `connect().await` is called. Errors are wrapped with `anyhow` and a clear message so the pipeline can log or retry.

---

## How it fits in the pipeline

The handler (or a dedicated task) typically:

1. Constructs `GrpcClient::new(config)` with the same `Config` used in `main`.
2. Calls `client.connect().await` and subscribes to the needed transaction filters (Pump.fun and Pump AMM program IDs).
3. For each transaction update, parses instructions, extracts token launches and trades, enriches launches from same-tx data, then serializes the events (e.g. with MessagePack) and calls `ws_server.broadcast(message)` so every connected WebSocket client receives them.

---

## See also

- [config.md](config.md) — Source of `Config`.
- [ws_server.md](ws_server.md) — `broadcast()` and how messages reach WebSocket clients.
- [messages.md](messages.md) — Wire format of broadcast messages.
