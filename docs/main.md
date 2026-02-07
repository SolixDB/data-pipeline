# main.rs — entry point and process lifecycle

**Source:** `src/main.rs`

This document walks through the binary from startup to shutdown. No external references; everything is in-repo.

---

## What main does (overview)

1. Install TLS provider and init logging.
2. Load and validate config (env); exit if invalid.
3. Start WebSocket server (listener task) on `WS_PORT`.
4. Build optional ClickHouse client; if present, ensure `pipeline_metrics` table.
5. Set up shutdown (Ctrl+C) and gRPC retry backoff.
6. Retry loop: connect gRPC, subscribe to transactions (Pump.fun + Pump AMM), run `handle_stream_updates` until stream ends or shutdown. On stream error, backoff and reconnect.
7. Process exits when retry gives up or shutdown is requested.

---

## Step-by-step

### 1. TLS and logging

- `rustls::crypto::aws_lc_rs::default_provider().install_default()` — required for gRPC TLS (https endpoints).
- `tracing_subscriber::fmt()` — log format. Filter from `RUST_LOG` env or default `info`; no target/thread/file/line in output.

### 2. Config

- `Config::default()` — reads env: `GRPC_ENDPOINT` (required), `GRPC_API_KEY`, `WS_*`, `CLICKHOUSE_*`, etc. See [config.md](config.md).
- `config.validate()` — endpoint non-empty and starts with `http://` or `https://`. Returns `anyhow::Result`; main `?` so invalid config exits.

### 3. WebSocket server

- `WS_PORT`: env or default 5566.
- `WsConfig::default()` — from env: `WS_MAX_CONNECTIONS`, `WS_MAX_LAG_DROP`.
- `WsServer::new(ws_config)` then `ws_server.start(ws_port).await` — binds `0.0.0.0:port`, spawns accept loop, returns a join handle. Main holds that handle so the process doesn’t exit; the loop runs until the process exits. See [ws_server.md](ws_server.md).

### 4. gRPC client and ClickHouse

- `GrpcClient::new(config)` — wrapper around config; actual connect is in the retry loop. See [client.md](client.md).
- If `config.clickhouse_url` is set: `db::make_client(url, user, password)`, then `db::ensure_table(client)` to create `pipeline_metrics` if not exists. `metrics_client` is `Option<Arc<clickhouse::Client>>`; cloned into the retry closure for the handler.

### 5. Backoff and shutdown

- `ExponentialBackoff`: initial 500 ms, max 30 s, multiplier 1.5; optional max elapsed from config. Used by `future::retry` in the loop.
- `watch::channel(false)` — shutdown signal. A task waits for Ctrl+C and sends `true`. The retry closure checks `shutdown_rx.borrow()` before connect and after; if true, returns permanent error and the loop exits.

### 6. Retry loop (closure)

Each attempt:

1. If shutdown, return `Permanent` error (stops retry).
2. `client.connect().await` — build and connect Geyser gRPC client.
3. If shutdown again, return `Permanent`.
4. `multi_parser::build_parser_map()` — map program id bytes → `"pump_fun"` / `"pump_amm"`. Keys are base58-decoded program IDs; subscription needs base58 strings, so keys are encoded to strings for the request.
5. Build `SubscribeRequest`: only `transactions` filter; `account_include` = Pump.fun and Pump AMM program IDs (from parser_map keys); `vote: false`; commitment from config (Processed). No slots/accounts/entry/blocks.
6. `grpc.subscribe_with_request(Some(subscribe_request)).await` — returns `(subscribe_tx, stream)`.
7. `tokio::select!`: either `handle_stream_updates(stream, subscribe_tx, config, ws_server, parser_map, metrics_client)` completes (then map result to transient/permanent error for retry), or `shutdown_rx.changed()` (then return permanent error).

So: one long-lived stream; when the stream errors or ends, `handle_stream_updates` returns, the error is treated as transient, and `future::retry` runs the closure again after backoff. Shutdown makes the error permanent so retry stops.

### 7. Top-level select and exit

- `tokio::select! { run => ..., ws_handle => ... }` — wait for either the retry future to finish or the WebSocket server handle (which never completes in normal operation). If run fails with non-shutdown error, it’s logged. Process then exits with `Ok(())`.

---

## Dependencies between modules (from main)

- **config** — Config, validation.
- **client** — GrpcClient, connect.
- **db** — make_client, ensure_table; handler uses update_metrics_aggregates_optional.
- **handler** — handle_stream_updates(stream, subscribe_tx, config, ws_server, parser_map, metrics_client).
- **multi_parser** — build_parser_map.
- **ws_server** — WsServer, WsConfig, start.

All other modules are used indirectly (handler uses messages, events, protocols via multi_parser, etc.).
