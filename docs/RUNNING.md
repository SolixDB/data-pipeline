# Running the Data Pipeline

## 1. Development

For local development, use `cargo run` (debug build: faster compile, slower execution).

```bash
# Minimum: only GRPC_ENDPOINT is required
export GRPC_ENDPOINT="https://your-solana-grpc-endpoint.com"

# Optional: persistence and snapshots
export REDIS_URL="redis://127.0.0.1:6379"
export CLICKHOUSE_URL="http://127.0.0.1:8123"

# Optional: Solana JSON-RPC (e.g. metadata backfill; Rug Check not currently used)
export RPC_URL="https://api.mainnet-beta.solana.com"

# Dev only: wipe state on start (do not use in prod)
# export CLEAR_REDIS=1
# export CLEAR_CLICKHOUSE=1

cargo run
```

---

## 2. Production

Build a **release** binary (optimizations from `Cargo.toml`).

### Required environment (prod)

| Variable | Description |
|----------|-------------|
| `GRPC_ENDPOINT` | **Required.** Yellowstone/Geyser gRPC URL (e.g. `https://your-geyser.example.com`). Process exits if unset. |

### Recommended for production

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | — | Redis URL. Enables snapshot + cursor persistence across restarts; without it, snapshots are in-memory only. |
| `CLICKHOUSE_URL` | — | ClickHouse HTTP URL. Enables storage (launches, trades, migrations, OHLCV, volume aggregates) and HTTP `/history` API. |
| `RPC_URL` | — | Solana JSON-RPC URL (e.g. metadata backfill). Rug Check is not currently run; no security updates are sent. `SOLANA_RPC_URL` is not used by this binary (reserved for helpers). |
| `GRPC_API_KEY` | — | Optional gRPC auth token (e.g. Helius, Triton). |
| `HEALTH_PORT` | 5567 | HTTP API port (`/config`, `/symbols`, `/history`, `/health`). Set to `0` to disable (not recommended in prod). |
| `WS_PORT` | 5566 | WebSocket server port. |
| `RUST_LOG` | info | Log level (`error`, `warn`, `info`, `debug`). |

### Optional tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `WS_INITIAL_SNAPSHOT_LAUNCH_LIMIT` | 30 | Max launches in connect snapshot. |
| `WS_INITIAL_SNAPSHOT_TRADE_LIMIT` | 1000 | Max trades (for those mints) in connect snapshot. |
| `CURSOR_OVERLAP_SLOTS` | 2 | Slots to rewind when resuming from stored cursor. |
| `WS_MAX_CONNECTIONS` | 0 (unlimited) | Max concurrent WebSocket clients. |
| `WS_MAX_LAG_DROP` | 0 (off) | Disconnect client if it lags this many messages. |
| `CLICKHOUSE_USER` | — | ClickHouse user. |
| `CLICKHOUSE_PASSWORD` | — | ClickHouse password. |

### Do not use in production

| Variable | Description |
|----------|-------------|
| `CLEAR_REDIS` | Set `1` to wipe Redis snapshot keys on startup. Dev only. |
| `CLEAR_CLICKHOUSE` | Set `1` to TRUNCATE/drop+recreate ClickHouse tables on startup. Dev only. |
| `CLEAR_CLICKHOUSE_ONLY` | Set `1` to clear tables once then run without writing. Dev only. |
| `SKIP_CLICKHOUSE` | Set `1` to disable ClickHouse (no connect, no writes). Use only if you intentionally run without persistence. |

---

### Step A: Build

```bash
cargo build --release
# Binary: ./target/release/data-pipeline
```

### Step B: Quick start (nohup)

```bash
# Required
export GRPC_ENDPOINT="https://your-solana-grpc-endpoint.com"

# Recommended for prod
export REDIS_URL="redis://127.0.0.1:6379"
export CLICKHOUSE_URL="http://127.0.0.1:8123"
export RPC_URL="https://api.mainnet-beta.solana.com"
export RUST_LOG=info

# Optional
# export GRPC_API_KEY="your-key"
# export HEALTH_PORT=5567
# export WS_PORT=5566

nohup ./target/release/data-pipeline >> pipeline.log 2>&1 &
ps aux | grep data-pipeline
```

### Step C: Systemd (recommended for production)

1. Create `/etc/systemd/system/data-pipeline.service`:

```ini
[Unit]
Description=Kleos Data Pipeline
After=network.target
# If you use Redis/ClickHouse on the same host, add:
# After=network.target redis-server.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/data-pipeline
ExecStart=/home/ubuntu/data-pipeline/target/release/data-pipeline
Restart=always
RestartSec=3

# Required
Environment="GRPC_ENDPOINT=https://your-solana-grpc-endpoint.com"

# Recommended for prod
Environment="REDIS_URL=redis://127.0.0.1:6379"
Environment="CLICKHOUSE_URL=http://127.0.0.1:8123"
Environment="RPC_URL=https://api.mainnet-beta.solana.com"
Environment="RUST_LOG=info"

# Optional
Environment="WS_PORT=5566"
Environment="HEALTH_PORT=5567"
# Environment="GRPC_API_KEY=your-grpc-key"
# Environment="CLICKHOUSE_USER=default"
# Environment="CLICKHOUSE_PASSWORD=password"
# Environment="WS_INITIAL_SNAPSHOT_LAUNCH_LIMIT=30"
# Environment="WS_INITIAL_SNAPSHOT_TRADE_LIMIT=1000"
# Environment="CURSOR_OVERLAP_SLOTS=2"
# Environment="WS_MAX_CONNECTIONS=0"
# Environment="WS_MAX_LAG_DROP=0"

LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

2. Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable data-pipeline
sudo systemctl start data-pipeline
```

3. Logs:

```bash
journalctl -u data-pipeline -f
```

---

## Environment variables reference (full)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GRPC_ENDPOINT` | **Yes** | — | Yellowstone gRPC URL. Process exits if unset. |
| `GRPC_API_KEY` | No | — | gRPC auth token (x_token). |
| `WS_PORT` | No | 5566 | WebSocket server port. |
| `HEALTH_PORT` | No | 5567 | HTTP API port (UDF + health). |
| `REDIS_URL` | No | — | Redis URL; enables snapshot + cursor persistence. |
| `CLICKHOUSE_URL` | No | — | ClickHouse HTTP URL; enables storage + `/history`. |
| `CLICKHOUSE_USER` | No | — | ClickHouse user. |
| `CLICKHOUSE_PASSWORD` | No | — | ClickHouse password. |
| `RPC_URL` | No | — | Solana JSON-RPC URL (e.g. metadata backfill). |
| `RUST_LOG` | No | info | Log level. |
| `WS_INITIAL_SNAPSHOT_LAUNCH_LIMIT` | No | 30 | Max launches in connect snapshot. |
| `WS_INITIAL_SNAPSHOT_TRADE_LIMIT` | No | 1000 | Max trades in connect snapshot. |
| `CURSOR_OVERLAP_SLOTS` | No | 2 | Slot overlap on resume. |
| `WS_MAX_CONNECTIONS` | No | 0 | Max WS clients (0 = unlimited). |
| `WS_MAX_LAG_DROP` | No | 0 | Disconnect client after this many lagged messages (0 = off). |
| `CLEAR_REDIS` | No | 0 | Set 1/true/yes to clear Redis snapshot on start (dev). |
| `CLEAR_CLICKHOUSE` | No | 0 | Set 1/true/yes to drop+recreate ClickHouse tables on start (dev). |
| `CLEAR_CLICKHOUSE_ONLY` | No | 0 | Set 1/true/yes to clear tables then run without writing (dev). |
| `SKIP_CLICKHOUSE` | No | 0 | Set 1/true/yes to disable ClickHouse. |

After start: WebSocket at `ws://0.0.0.0:<WS_PORT>`, HTTP API at `http://0.0.0.0:<HEALTH_PORT>` (unless `HEALTH_PORT=0`).
