# storage.rs — storage worker and snapshots

**Source:** `src/storage.rs`

Consumes launch/trade/migration events from a bounded channel; writes to ClickHouse (token_launches, trades, migrations) when a client is set; maintains in-memory snapshot state (migrated, soon_migrating, last 50 launches, their trades) for WebSocket on-connect; optionally writes the same snapshot keys to Redis when REDIS_URL is set. All behaviour is described here; message types and connection order are in [messages.md](messages.md), row types in [db.md](db.md).

---

## StorageMessage enum

- **TokenLaunch(TokenLaunch)** — one per Pump.fun Create (same as handler receives).
- **Trade(Trade)** — one per Buy/Sell.
- **Migration(Migration)** — one per Migrate.

Handler sends these on the channel via `storage_tx.send(...).await` (backpressure: if the channel is full, the gRPC stream blocks instead of dropping events). Storage worker receives on `storage_rx`.

---

## Snapshot types (wire and Redis)

- **MigratedEntry:** mint, pool, signature, slot. Pushed when a Migration is processed.
- **SoonMigratingEntry:** mint, progress_pct (f64), virtual_token_reserves (u64). Derived from curve reserves; progress ≥ 75% is “soon migrating”.
- **TokenState:** mint, price, market_cap, liquidity_sol, volume_metrics (volume_5m/1h/6h/24h, tx_count_5m/1h/6h/24h), total_fees_sol (cumulative creator fees from all trades), trade_history (circular buffer), last_slot. Time-windowed volumes calculated from circular buffer with automatic cleanup every 60s.
- **SnapshotState:** in-memory struct with migrated (Vec<MigratedEntry>), soon_migrating (Vec<SoonMigratingEntry>), launches (Vec<TokenLaunchPayload>), trades (Vec<Trade>), tickers (HashMap<String, TokenState>). Updated each loop; ws_server reads it on each new client connect.

Redis keys (when redis_url is set): `snapshot:migrated`, `snapshot:soon_migrating`, `snapshot:launches`, `snapshot:trades`. JSON-serialized. Used by ws_server when snapshot_state is not the primary source (e.g. multi-instance) or as fallback.

---

## spawn_storage_worker(rx, clickhouse, redis_url, snapshot_state, snapshot_launch_limit, snapshot_trade_limit)

- **rx:** mpsc::Receiver<StorageMessage>. Closed when main drops all senders.
- **clickhouse:** Option<Arc<Client>>. When Some, rows are pushed into in-memory buffers; buffers are flushed with **batch inserts** when any buffer reaches 100 rows or 50 ms have passed since last flush (insert_token_launches_batch, insert_trades_batch, insert_migrations_batch). When None (e.g. SKIP_CLICKHOUSE or CLEAR_CLICKHOUSE_ONLY), no DB writes.
- **redis_url:** Option<String>. When Some, after processing each message we write the four snapshot keys to Redis (JSON). Optional; in-memory snapshot_state is always updated when provided.
- **snapshot_state:** Option<Arc<RwLock<SnapshotState>>>. When Some, each loop we write the current migrated_list, soon_migrating, last_launches (as Vec), recent_trades (as Vec) into the RwLock. main passes the same Arc to WsServer so new clients get this state.
- **snapshot_launch_limit**, **snapshot_trade_limit:** Bounds for last_launches and recent_trades (from config; defaults 30 and 1000). Clamped to 1–10_000 and 1–50_000.

Runs in a tokio::spawn until rx is closed; then logs “Storage worker channel closed, exiting”.

---

## Per-message behaviour

- **TokenLaunch:** Build TokenLaunchRow and push to launch_buffer; build TokenLaunchPayload; push to last_launches (front), trim to snapshot_launch_limit; update curve_reserves from initial_bonding_curve_state if present; then update snapshot_state and Redis.
- **Trade:** If mint is in the set of last N launch mints, push TradeRow to trade_buffer and push to recent_trades, trim to snapshot_trade_limit. Update curve_reserves if pool is None; then update snapshot_state and Redis.
- **Migration:** Push MigrationRow to migration_buffer; remove mint from curve_reserves; push MigratedEntry to migrated_list; then update snapshot_state and Redis.

After each message, if any buffer length ≥ 100 or 50 ms have passed since last flush, call the three batch insert functions for non-empty buffers, clear those buffers, and reset the flush timer. Soon_migrating is computed from curve_reserves: progress_pct = (1 - reserves/initial_reserves)*100; entries with progress_pct ≥ 80 are included.

---

## Constants

- **INITIAL_VIRTUAL_TOKEN_RESERVES:** 1_073_000_000_000_000 (Pump.fun initial curve).
- **SOON_MIGRATING_PROGRESS_PCT:** 80.0.
- **BATCH_SIZE:** 100 (flush when any buffer reaches this).
- **FLUSH_INTERVAL_MS:** 50 (or flush when this many ms since last flush).
- **MAX_RECENT_MIGRATIONS:** 10_000 (cap on migrated_list).

---

## Where storage is used

- **main:** Creates snapshot_state (Arc<RwLock<SnapshotState>>), passes it to WsServer and to spawn_storage_worker; creates storage_tx/storage_rx, passes storage_rx, metrics_client, redis_url, snapshot_state, config.snapshot_launch_limit, and config.snapshot_trade_limit to spawn_storage_worker; passes storage_tx into the retry closure for handle_stream_updates.
- **handler:** storage_tx.send(StorageMessage::TokenLaunch|Trade|Migration).await for each event (backpressure). See [handler.md](handler.md).
- **ws_server:** Reads snapshot_state on each handle_connection (or Redis when snapshot_state is None and redis_client is set). See [ws_server.md](ws_server.md).
