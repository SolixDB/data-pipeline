# handler.rs — stream loop and transaction handling

**Source:** `src/handler.rs`

Consumes the gRPC subscribe stream, parses each transaction into launches/trades/migrations, optionally writes metrics to ClickHouse, and broadcasts MessagePack to WebSocket clients. This doc describes the flow and types; parsing is in [multi_parser.md](multi_parser.md), wire types in [messages.md](messages.md), events in [events.md](events.md).

---

## handle_stream_updates (signature and role)

Arguments:

- `stream`: gRPC stream of `Result<SubscribeUpdate, tonic::Status>`.
- `subscribe_tx`: sink for `SubscribeRequest` (used to send pings).
- `config`: used only for ping (commitment).
- `ws_server`: broadcast target.
- `parser_map`: program id → `"pump_fun"` / `"pump_amm"` for multi_parser.
- `metrics_client`: `Option<Arc<clickhouse::Client>>`; if None, metrics writes are no-op.
- `storage_tx`: `mpsc::Sender<StorageMessage>`; handler sends TokenLaunch, Trade, Migration for each event with **backpressure** (`send().await`; blocks when channel full so the gRPC stream slows instead of dropping). Consumed by storage worker. See [storage.md](storage.md).
- `health_state`: `Option<Arc<HealthState>>`; if Some, `record_activity()` is called at the start of each `handle_transaction` so the health endpoint reflects recent gRPC activity.

Runs until the stream ends or errors. Returns `anyhow::Result<()>`; stream errors or storage channel closed are returned as Err, so main can retry.

---

## Outer loop

- `while let Some(message) = stream.next().await`:
  - **Ok(msg):** match `msg.update_oneof`:
    - `Transaction(tx_update)`: call `handle_transaction(...)`. On error log and continue (no stream exit).
    - `Ping(_)`: call `send_ping`.
    - Other: ignore.
  - Every iteration: if 30 s elapsed since last ping, call `send_ping` and reset timer.
  - **Err(e):** return `Err(anyhow!(...))` so main retries.
- When stream ends (None), log “Stream ended” and return Ok.

---

## handle_transaction (per transaction)

1. **Parse instructions:** `InstructionUpdate::parse_from_txn(tx_update)` — yields a tree of instructions (outer + inner). Return error if parse fails.
2. **Timestamps:** `received_at = unix_timestamp_micros()` (server wall clock, μs).
3. **Parse events:**
   - `launches = multi_parser::parse_token_launches(instructions, parser_map, received_at)` — Pump.fun Create only.
   - `trades = multi_parser::parse_trade_events(...)` — Pump.fun and Pump AMM Buy/Sell.
   - `multi_parser::enrich_token_launches(instructions, parser_map, &mut launches, &trades)` — fill fee_recipient and initial_bonding_curve_state where possible.
0. **Health:** If `health_state` is Some, call `health_state.record_activity()` at the start of handle_transaction (so GET /health reflects recent gRPC activity).
4. **Launches:** For each launch:
   - Compute metrics: `grpc_ms = (received_at/1000) - txn_timestamp` (ms), `server_us = sent_at - received_at` (μs). Then `db::update_metrics_aggregates_optional(metrics_client, grpc_ms, server_us)` (no-op if metrics_client is None).
   - Set `launch.sent_at = unix_timestamp_micros()`.
   - `storage_tx.send(StorageMessage::TokenLaunch(launch.clone())).await` (backpressure; returns Err if channel closed).
   - Build `TokenLaunchPayload` from launch (same-named fields; see [messages.md](messages.md)).
   - `rmp_serde::to_vec(&ServerMessage::TokenLaunch(payload))` and `ws_server.broadcast(bytes)`.
   - Log at debug level: `#N LAUNCH | name (symbol) | mint=... | clients=...`.
5. **Trades:** For each trade: set `sent_at`, `storage_tx.send(StorageMessage::Trade(trade)).await`, serialize `ServerMessage::Trade(trade)`, broadcast, log at debug.
6. **Migrations:** `migrations = multi_parser::parse_migration_events(...)`; for each: set `sent_at`, `storage_tx.send(StorageMessage::Migration(migration)).await`, serialize and broadcast, log at debug.

Order: all launches first, then all trades, then all migrations. Counters (launch_count, trade_count, migration_count) are atomic and only used for log text.

---

## send_ping

Builds a `SubscribeRequest` with only `commitment` and `ping: Some(SubscribeRequestPing { id: 1 })`, sends it on `subscribe_tx`. Keeps the gRPC subscription alive. Called on Ping from server or every 30 s.

---

## unix_timestamp_micros()

`SystemTime::now().duration_since(UNIX_EPOCH).as_micros() as u64`. Used for received_at and sent_at.

---

## Dependencies

- **config:** Config (ping).
- **db:** update_metrics_aggregates_optional.
- **events:** TradeDirection (for log string).
- **messages:** ServerMessage, TokenLaunchPayload.
- **multi_parser:** parse_token_launches, parse_trade_events, enrich_token_launches, parse_migration_events.
- **storage:** StorageMessage (TokenLaunch, Trade, Migration).
- **ws_server:** WsServer (broadcast, client_count).
- **yellowstone_vixen_core::instruction::InstructionUpdate:** parse_from_txn.
- **yellowstone_grpc_proto:** SubscribeUpdate, UpdateOneof, SubscribeRequest, SubscribeRequestPing.

Per-event logs (LAUNCH, BUY/SELL, MIGRATE) use `debug!` so at RUST_LOG=info they do not flood; set RUST_LOG=debug for per-event visibility.
