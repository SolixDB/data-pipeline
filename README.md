# Data Pipeline

Central WebSocket server for streaming token and trade data to trading terminals. Clients connect over WebSocket and receive a single broadcast stream; the server is tuned for low latency and many concurrent connections.

## Overview

- **Role:** Single server that many terminals connect to; one producer (the data pipeline) broadcasts the same stream to all connected clients.
- **Stack:** Rust, Tokio, WebSocket (tokio-tungstenite), MessagePack. gRPC (Yellowstone Geyser) client is in place for the future pipeline that will feed the WebSocket.
- **Repo layout:** Main binary and WebSocket logic live in `src/`; shared crates (core, proto, proc-macro) live in `crates/`.

## Prerequisites

- Rust (e.g. latest stable)
- `GRPC_ENDPOINT` set (required by config validation at startup), e.g. `https://your-grpc-endpoint`

## Build & run

```bash
cargo build --release
```

```bash
# Required for config validation
export GRPC_ENDPOINT="https://..."

# Optional
export WS_PORT=5566
export WS_MAX_CONNECTIONS=2000
export WS_MAX_LAG_DROP=5000
export RUST_LOG=info

cargo run --release
```

The process stays running and serves WebSocket on `ws://0.0.0.0:<WS_PORT>` (default 5566).

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GRPC_ENDPOINT` | Yes | — | gRPC endpoint URL (http/https). |
| `GRPC_API_KEY` | No | — | Optional gRPC auth token. |
| `WS_PORT` | No | 5566 | WebSocket listen port. |
| `WS_MAX_CONNECTIONS` | No | no limit | Max concurrent WebSocket clients; excess connections are rejected. |
| `WS_MAX_LAG_DROP` | No | no disconnect | If a client drops ≥ this many messages in one lag event, disconnect them so they reconnect for fresh data. |
| `RUST_LOG` | No | info | Log level (e.g. `debug`, `info`, `warn`). |
| `CLICKHOUSE_URL` | No | — | If set, pipeline_metrics are written to ClickHouse. |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | No | — | ClickHouse auth. |

## Code documentation

**[docs/README.md](docs/README.md)** is the index. It links to detailed split docs (main, config, client, handler, multi_parser, events, messages, ws_server, db, protocols, pumpfun_accounts, RUNBOOK). Each doc is self-contained so a new Rust dev can understand the codebase from these docs only.
