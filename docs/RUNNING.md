# Running the Data Pipeline

## 1. Development
For local development, use `cargo run`. This compiles in debug mode (faster compile, slower execution).

```bash
# Basic run with essential env vars
export GRPC_ENDPOINT="https://your-solana-grpc-endpoint.com"
export REDIS_URL="redis://127.0.0.1:6379"

# Optional: Clear DBs on start
export CLEAR_REDIS=1
export CLEAR_CLICKHOUSE=1

cargo run
```

---

## 2. Production

For production, you must build a **release** binary. This includes optimizations (LTO, strip) defined in `Cargo.toml`.

### Step A: Build
```bash
cargo build --release
# Binary will be at: ./target/release/data-pipeline
```

### Step B: Quick Start (nohup)
If you just need to keep it running in the background without a process manager:

```bash
export GRPC_ENDPOINT="https://your-solana-grpc-endpoint.com"
export REDIS_URL="redis://127.0.0.1:6379"
export CLICKHOUSE_URL="http://127.0.0.1:8123"
export RUST_LOG=info

# Run in background, append logs to pipeline.log
nohup ./target/release/data-pipeline > pipeline.log 2>&1 &

# Check it's running
ps aux | grep data-pipeline
```

### Step C: Robust Deployment (Systemd) - RECOMMENDED
For a real production environment (Linux), use `systemd` to handle restarts, logging, and startup dependency.

1. **Create service file:** `/etc/systemd/system/data-pipeline.service`

```ini
[Unit]
Description=Kleos Data Pipeline
After=network.target redis.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/data-pipeline
ExecStart=/home/ubuntu/data-pipeline/target/release/data-pipeline
Restart=always
RestartSec=3

# Environment Variables
Environment="RUST_LOG=info"
Environment="GRPC_ENDPOINT=https://your-solana-grpc-endpoint.com"
Environment="WS_PORT=5566"
Environment="REDIS_URL=redis://127.0.0.1:6379"
Environment="CLICKHOUSE_URL=http://127.0.0.1:8123"
# Environment="CLICKHOUSE_USER=default"
# Environment="CLICKHOUSE_PASSWORD=password"

# Limits
Limitnofile=65536

[Install]
WantedBy=multi-user.target
```

2. **Enable and Start:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable data-pipeline
sudo systemctl start data-pipeline
```

3. **View Logs:**
```bash
journalctl -u data-pipeline -f
```

## Environment Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GRPC_ENDPOINT` | **Yes** | - | Yellowstone gRPC URL |
| `REDIS_URL` | **Yes** | - | Redis connection string (needed for persistence) |
| `WS_PORT` | No | 5566 | WebSocket server port |
| `CLICKHOUSE_URL` | No | - | ClickHouse HTTP interface URL |
| `SKIP_CLICKHOUSE` | No | 0 | Set `1` to disable ClickHouse completely |
| `CLEAR_REDIS` | No | 0 | Set `1` to wipe Redis Snapshot on startup |
| `WS_MAX_CONNECTIONS` | No | - | Max concurrent WS clients |
