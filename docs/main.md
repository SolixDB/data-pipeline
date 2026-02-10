# main.rs — entry point and process lifecycle

**Source:** `src/main.rs`

This document walks through the binary from startup to shutdown. No external references; everything is in-repo.

---

## What main does (overview)

1. Install TLS provider and init logging.
2. Load and validate config (env); exit if invalid.
3. Build in-memory snapshot state (Arc<RwLock<SnapshotState>>); optional Redis client from REDIS_URL.
4. Start WebSocket server (listener task) on `WS_PORT` with snapshot state and optional Redis client.
5. Create `HealthState` and spawn the combined HTTP API + health server (`api::ApiServer::start`) on `HEALTH_PORT`.
6. Build optional ClickHouse client (unless SKIP_CLICKHOUSE); ensure pipeline_metrics and storage tables; if CLEAR_CLICKHOUSE or CLEAR_CLICKHOUSE_ONLY, drop and recreate tables; if CLEAR_CLICKHOUSE_ONLY set metrics_client to None for the rest of the run.
7. Spawn storage worker (channel receiver, ClickHouse client, redis_url, snapshot state, config.snapshot_launch_limit, config.snapshot_trade_limit).
8. Set up shutdown (Ctrl+C) and gRPC retry backoff.
9. Retry loop: connect gRPC, call health_state.record_activity(), subscribe to transactions (Pump.fun + Pump AMM), run `handle_stream_updates` (with storage_tx, health_state, snapshot_state, Redis client, and optional RPC_URL for Rug Check) until stream ends or shutdown. On stream error, backoff and reconnect.
10. Process exits when retry gives up or shutdown is requested.

---

## Step-by-step

### 1. TLS and logging

- `rustls::crypto::aws_lc_rs::default_provider().install_default()` — required for gRPC TLS (https endpoints).
- `tracing_subscriber::fmt()` — log format. Filter from `RUST_LOG` env or default `info`; no target/thread/file/line in output.

### 2. Config

- `Config::default()` — reads env: `GRPC_ENDPOINT` (required), `GRPC_API_KEY`, `WS_*`, `CLICKHOUSE_*`, etc. See [config.md](config.md).
- `config.validate()` — endpoint non-empty and starts with `http://` or `https://`. Returns `anyhow::Result`; main `?` so invalid config exits.

### 3. Snapshot state and WebSocket server

- `snapshot_state = Arc::new(RwLock::new(SnapshotState::default()))` — shared in-memory snapshot; storage worker updates it; ws_server reads it on each new client connect.
- `redis_client`: from `config.redis_url` if set; passed to WsServer for optional Redis-backed snapshots.
- `WS_PORT`: env or default 5566.
- `WsConfig::default()` — from env: `WS_MAX_CONNECTIONS`, `WS_MAX_LAG_DROP`.
- `WsServer::new(ws_config, Some(snapshot_state), redis_client)` then `ws_server.start(ws_port).await` — binds `0.0.0.0:port`, spawns accept loop, returns a join handle. See [ws_server.md](ws_server.md).

### 4. gRPC client and ClickHouse

- `GrpcClient::new(config)` — wrapper around config; actual connect is in the retry loop. See [client.md](client.md).
- If `config.skip_clickhouse` is true: `metrics_client = None` (no ClickHouse; log "ClickHouse disabled").
- Else if `config.clickhouse_url` is set: `db::make_client(url, user, password)`, then `db::ensure_table` and `ensure_token_launches_table`, `ensure_trades_table`, `ensure_migrations_table`. If `clear_clickhouse` or `clear_clickhouse_only`: `db::drop_tables` then ensure all four again. If `clear_clickhouse_only`: set `metrics_client = None` so no further writes this run.
- `metrics_client` is cloned into the retry closure for the handler and into the storage worker.

### 5. HTTP API + health server

- `health_state = Arc::new(health::HealthState::new())`.
- `ApiServer::start(port, clickhouse.clone(), health_state.clone())` is spawned on a background task:
  - Port comes from `HEALTH_PORT` env or defaults to 5567.
  - Provides `/config`, `/symbols`, `/history`, `/health`, and `/`.
  - `/history` uses ClickHouse OHLCV when configured.
- `health_state` is cloned into the retry closure and passed to `handle_stream_updates`, so you can tighten `/health` semantics later if desired.

### 6. Storage worker

- `(storage_tx, storage_rx) = mpsc::channel(50_000)`.
- `storage::spawn_storage_worker(storage_rx, metrics_client, config.redis_url, Some(snapshot_state), config.snapshot_launch_limit, config.snapshot_trade_limit)` — consumes channel; buffers then batch-inserts to ClickHouse (token_launches, trades, migrations) when client is Some; updates snapshot_state (migrated, soon_migrating, last N launches and their trades per config); optionally writes same to Redis when redis_url is set. See [storage.md](storage.md).
- `storage_tx` is cloned into the retry closure and passed to `handle_stream_updates`.

### 7. Backoff and shutdown

- `ExponentialBackoff`: initial 500 ms, max 30 s, multiplier 1.5; optional max elapsed from config. Used by `future::retry` in the loop.
- `watch::channel(false)` — shutdown signal. A task waits for Ctrl+C and sends `true`. The retry closure checks `shutdown_rx.borrow()` before connect and after; if true, returns permanent error and the loop exits.

### 8. Retry loop (closure)

Each attempt:

1. If shutdown, return `Permanent` error (stops retry).
2. `client.connect().await` — build and connect Geyser gRPC client.
3. If shutdown again, return `Permanent`.
4. `health_state.record_activity()` — so health is 200 right after connect.
5. `multi_parser::build_parser_map()` — map program id bytes → `"pump_fun"` / `"pump_amm"`. Keys are base58-decoded program IDs; subscription needs base58 strings, so keys are encoded to strings for the request.
6. Build `SubscribeRequest`: only `transactions` filter; `account_include` = Pump.fun and Pump AMM program IDs (from parser_map keys); `vote: false`; commitment from config (Processed). If a cursor is available from snapshot state, slots filter and from_slot are filled to resume with overlap.
7. `grpc.subscribe_with_request(Some(subscribe_request)).await` — returns `(subscribe_tx, stream)`.
8. `tokio::select!`: either
   - `handle_stream_updates(stream, subscribe_tx, config, ws_server, parser_map, metrics_client, storage_tx, Some(health_state), Some(snapshot_state), redis_client_retry, std::env::var("RPC_URL").ok())` completes (then map result to transient/permanent error for retry), or
   - `shutdown_rx.changed()` (then return permanent error).

So: one long-lived stream; when the stream errors or ends, `handle_stream_updates` returns, the error is treated as transient, and `future::retry` runs the closure again after backoff. Shutdown makes the error permanent so retry stops.

### 9. Top-level select and exit

- `tokio::select! { run => ..., ws_handle => ... }` — wait for either the retry future to finish or the WebSocket server handle (which never completes in normal operation). If run fails with non-shutdown error, it’s logged. Process then exits with `Ok(())`.

---

## Dependencies between modules (from main)

- **config** — Config, validation.
- **client** — GrpcClient, connect.
- **db** — make_client, ensure_table, ensure_token_launches_table, ensure_trades_table, ensure_migrations_table, drop_tables; handler uses update_metrics_aggregates_optional; storage uses insert_*.
- **handler** — handle_stream_updates(stream, subscribe_tx, config, ws_server, parser_map, metrics_client, storage_tx).
- **multi_parser** — build_parser_map.
- **storage** — SnapshotState, spawn_storage_worker; handler sends StorageMessage on channel.
- **ws_server** — WsServer, WsConfig, start; WsServer holds snapshot_state and optional redis_client.

All other modules are used indirectly (handler uses messages, events, protocols via multi_parser, etc.).
