# Data pipeline documentation

Everything you need to understand and run the data-pipeline is in this directory. No external references required.

---

## What this service does

The binary subscribes to a Solana Geyser gRPC stream (Yellowstone), filters for transactions that touch **Pump.fun** or **Pump AMM** programs, and parses them into:

- Token launches (Create)
- Trades (Buy/Sell)
- Migrations (Migrate)
- OHLCV candles (1m)

It then:

- Broadcasts events over a WebSocket as MessagePack `ServerMessage` values.
- Runs a Rug Check per mint (SPL mint security) and pushes follow‑up `token_security_update` messages.
- Writes launches/trades/migrations/ohlcv and volume aggregates to ClickHouse when configured.
- Maintains in‑memory + optional Redis snapshots for fast warm‑starts.
- Exposes a small HTTP API (`/config`, `/symbols`, `/history`, `/health`) for charts and health.

- **Input:** Geyser gRPC (transactions).
- **Output:** WebSocket on `WS_PORT` (default 5566) and HTTP API on `HEALTH_PORT` (default 5567).

---

## Documentation index

| Document | Contents |
|----------|----------|
| [main.md](main.md) | Entry point: TLS, logging, config, ClickHouse setup, WebSocket + HTTP API server start, gRPC retry loop, shutdown. |
| [config.md](config.md) | Config struct, every field, env vars, validation, where it’s used. |
| [client.md](client.md) | Geyser gRPC client: build, TLS, auth, connect. |
| [handler.md](handler.md) | Stream loop, transaction handling, parsing pipeline, metrics, broadcast, ping. |
| [multi_parser.md](multi_parser.md) | Parser map, flatten_instructions, parse_token_launches, parse_trade_events, parse_migration_events, enrich_token_launches. |
| [events.md](events.md) | Domain types: TokenLaunch (with MintSecurity), Trade (with volume/price windows), Migration, Ohclv, BondingCurveState, PumpFunBondingCurveAccounts, PumpProtocolConstants, TradeDirection. |
| [messages.md](messages.md) | Wire format: ServerMessage variants (`token_launch`, `token_security_update`, `trade`, `migration`, `ohclv`, `snapshot_*`), payload fields, connection order. |
| [ws_server.md](ws_server.md) | WsConfig, WsServer, broadcast channel, accept loop, handle_connection, connection limit, lag disconnect, snapshot-on-connect behaviour. |
| [db.md](db.md) | ClickHouse client, pipeline_metrics and storage tables (token_launches, trades, migrations, ohlcv, volume_aggregates), drop_tables, insert_*, query_holder_stats. |
| [storage.md](storage.md) | Storage worker: channel consumer, batch ClickHouse writes, SnapshotState (migrated, soon_migrating, launches, trades, tickers), OHLCV ingestion, periodic volume aggregates, optional Redis. |
| [protocols.md](protocols.md) | pump_fun: Create/trade/migration detection and parsing (logs + instruction data). pump_amm: AMM buy/sell event layout and parsing. |
| [api.md](api.md) | HTTP API: /config, /symbols, /history, /health. UDF semantics and ClickHouse integration. |
| [security.md](security.md) | Rug Check: MintSecurity, fetch_mint_security, how results flow into launches and token_security_update. |
| `FRONTEND.md` | High‑level integration guide for frontend/WebSocket + HTTP consumers. |
| [RUNBOOK.md](RUNBOOK.md) | Operations: gRPC/data flow, ClickHouse (tables, CLEAR/SKIP/CLEAR_ONLY), parse errors, WebSocket clients. |

**Storage and snapshots:** Handler sends each launch/trade/migration/ohlcv to a bounded channel with backpressure (`send().await`; the gRPC stream slows instead of dropping). The storage worker buffers rows and bulk‑inserts to ClickHouse and updates in‑memory `SnapshotState` (migrated, soon_migrating, launches, trades, tickers); optionally it also persists JSON snapshots and the last processed cursor to Redis. On WebSocket connect the server sends snapshot messages so clients start from a consistent view. See [storage.md](storage.md) and [messages.md](messages.md).

**New to the codebase:** Start with [main.md](main.md) for the big picture, then [handler.md](handler.md) and [multi_parser.md](multi_parser.md) for the data path. Use [messages.md](messages.md), [events.md](events.md), and `FRONTEND.md` when integrating clients or building Pump.fun transactions.

---

## How to run

**Prerequisites:** Rust (e.g. `rustup default stable`), a Solana Geyser gRPC endpoint. From the repo root, run from the `data-pipeline` directory.

**Required env:** `GRPC_ENDPOINT` (Geyser gRPC URL, e.g. `https://your-rpc.example.com`).

### Minimal (WebSocket only, no persistence)

No ClickHouse or Redis; snapshots come from in-memory state. Good for testing.

```bash
cd data-pipeline
export GRPC_ENDPOINT="https://your-geyser-endpoint"
cargo run --release
```

WebSocket: `ws://0.0.0.0:5566`. Optional: `GRPC_API_KEY`, `WS_PORT`, `RUST_LOG`.

### With ClickHouse (save launches, trades, migrations)

Persist to ClickHouse; snapshots still in-memory (Redis optional).

```bash
cd data-pipeline
export GRPC_ENDPOINT="https://your-geyser-endpoint"
export CLICKHOUSE_URL="http://localhost:8123"
# optional: CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
cargo run --release
```

### With Redis (snapshot cache)

When you set `REDIS_URL`, the storage worker also writes snapshot keys to Redis. Useful if you run multiple instances or want snapshots to survive restarts.

```bash
cd data-pipeline
export GRPC_ENDPOINT="https://your-geyser-endpoint"
export CLICKHOUSE_URL="http://localhost:8123"
export REDIS_URL="redis://127.0.0.1:6379"
cargo run --release
```

### Dev: clear ClickHouse on every start

Drop and recreate all pipeline tables on startup so schema and data match the current code.

```bash
export GRPC_ENDPOINT="https://your-geyser-endpoint"
export CLICKHOUSE_URL="http://localhost:8123"
export CLEAR_CLICKHOUSE=1
export RUST_LOG=info
cargo run --release
```

### Dev: clear ClickHouse once, then don’t save

Connect to ClickHouse, drop and recreate tables, then run without writing any data (WebSocket and in-memory snapshots only).

```bash
export GRPC_ENDPOINT="https://your-geyser-endpoint"
export CLICKHOUSE_URL="http://localhost:8123"
export CLEAR_CLICKHOUSE_ONLY=1
export RUST_LOG=info
cargo run --release
```

### No ClickHouse (Redis + WebSocket only)

Disable ClickHouse entirely; no metrics or storage writes.

```bash
export GRPC_ENDPOINT="https://your-geyser-endpoint"
export REDIS_URL="redis://127.0.0.1:6379"
export SKIP_CLICKHOUSE=1
cargo run --release
```

### All optional env vars

| Env | Default | Description |
|-----|---------|-------------|
| `GRPC_ENDPOINT` | (required) | Geyser gRPC URL. |
| `GRPC_API_KEY` | — | Optional gRPC auth token. |
| `WS_PORT` | 5566 | WebSocket listen port. |
| `WS_MAX_CONNECTIONS` | 0 (unlimited) | Max concurrent WS clients. |
| `WS_MAX_LAG_DROP` | 0 (off) | Disconnect client if it lags this many messages. |
| `CLICKHOUSE_URL` | — | ClickHouse HTTP URL (e.g. `http://localhost:8123`). |
| `CLICKHOUSE_USER` | — | ClickHouse user. |
| `CLICKHOUSE_PASSWORD` | — | ClickHouse password. |
| `REDIS_URL` | — | Redis URL for snapshot cache (e.g. `redis://127.0.0.1:6379`). |
| `CLEAR_CLICKHOUSE` | 0 | Set to 1 to drop and recreate ClickHouse tables on startup. |
| `SKIP_CLICKHOUSE` | 0 | Set to 1 to disable ClickHouse (no connect, no writes). |
| `CLEAR_CLICKHOUSE_ONLY` | 0 | Set to 1 to clear tables then run without saving. |
| `HEALTH_PORT` | 5567 | HTTP health server port; 0 = disabled. |
| `HEALTH_GRPC_MAX_AGE_SECS` | 120 | Health returns 200 only if last gRPC activity is within this many seconds. |
| `WS_INITIAL_SNAPSHOT_LAUNCH_LIMIT` | 50 | Max token launches in the snapshot sent to new WebSocket clients. |
| `WS_INITIAL_SNAPSHOT_TRADE_LIMIT` | 1000 | Max trades (for those mints) in the snapshot. |
| `RUST_LOG` | info | Log level (e.g. `debug`, `info`). |

After start: WebSocket at `ws://0.0.0.0:<WS_PORT>`. If `HEALTH_PORT` is not 0, health at `http://0.0.0.0:<HEALTH_PORT>/health` (GET; 200 if gRPC activity is recent, else 503). Logs: `Connected to blockchain gRPC`, `WebSocket server listening on ws://...`, `Health server listening on 0.0.0.0:<port>` when health is enabled. Per-event lines are at `debug` level; use `RUST_LOG=debug` to see them.

---

## Architecture (one paragraph)

**main** builds config, snapshot state, and optional Redis; starts the WebSocket server (with snapshot state and optional Redis). It builds an optional ClickHouse client (unless SKIP_CLICKHOUSE), ensures pipeline_metrics and storage tables; if CLEAR_CLICKHOUSE or CLEAR_CLICKHOUSE_ONLY it drops and recreates them; if CLEAR_CLICKHOUSE_ONLY it then sets the client to None. If HEALTH_PORT is not 0 it spawns the **health** HTTP server (GET /health → 200 if gRPC activity is recent). It spawns the **storage** worker (channel, ClickHouse, redis_url, snapshot state, snapshot launch/trade limits), then runs a retry loop: connect gRPC (**client**), record health activity, build **multi_parser**’s program-id map, subscribe to transactions that include Pump.fun and Pump AMM program IDs, and run **handler**’s `handle_stream_updates` (with storage_tx and health_state). **handler** parses transactions, updates health on each transaction, sends events to **storage** with backpressure (send().await), optionally writes metrics (**db**), serializes to MessagePack (**messages**), and broadcasts via **ws_server**. **storage** buffers then bulk-inserts to ClickHouse (100 rows or 50 ms) and updates snapshot state (and optionally Redis). **ws_server** sends the four snapshots on each new client connect. **multi_parser** delegates to **protocols** (pump_fun, pump_amm) for Create/trade/migration parsing. All types are in **events** and **messages**.
