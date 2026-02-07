# Data pipeline documentation

Everything you need to understand and run the data-pipeline is in this directory. No external references required.

---

## What this service does

The binary subscribes to a Solana Geyser gRPC stream (Yellowstone), filters for transactions that touch **Pump.fun** or **Pump AMM** programs, parses them into token launches (Create), trades (Buy/Sell), and migrations (Migrate). It broadcasts these events over a WebSocket as MessagePack. If `CLICKHOUSE_URL` is set, it also writes aggregate metrics (launch count, gRPC/server latency) to ClickHouse.

- **Input:** Geyser gRPC (transactions).
- **Output:** WebSocket on `WS_PORT` (default 5566). Each frame is MessagePack: `ServerMessage` (`connected`, `token_launch`, `trade`, `migration`).

---

## Documentation index

| Document | Contents |
|----------|----------|
| [main.md](main.md) | Entry point: TLS, logging, config, ClickHouse setup, WebSocket server start, gRPC retry loop, shutdown. Step-by-step. |
| [config.md](config.md) | Config struct, every field, env vars, validation, where it’s used. |
| [client.md](client.md) | Geyser gRPC client: build, TLS, auth, connect. |
| [handler.md](handler.md) | Stream loop, transaction handling, parsing pipeline, metrics, broadcast, ping. |
| [multi_parser.md](multi_parser.md) | Parser map, flatten_instructions, parse_token_launches, parse_trade_events, parse_migration_events, enrich_token_launches. |
| [events.md](events.md) | Domain types: TokenLaunch, Trade, Migration, BondingCurveState, PumpFunBondingCurveAccounts, PumpProtocolConstants, TradeDirection. Field-by-field. |
| [messages.md](messages.md) | Wire format: ServerMessage, TokenLaunchPayload, ConnectedPayload. MessagePack, when each message is sent, payload fields. |
| [ws_server.md](ws_server.md) | WsConfig, WsServer, broadcast channel, accept loop, handle_connection, connection limit, lag disconnect. |
| [db.md](db.md) | ClickHouse client, pipeline_metrics table, metric keys, get_current_metrics, update_metrics_aggregates, update_metrics_aggregates_optional. |
| [protocols.md](protocols.md) | pump_fun: Create/trade/migration detection and parsing (logs + instruction data). pump_amm: AMM buy/sell event layout and parsing. |
| [pumpfun_accounts.md](pumpfun_accounts.md) | Program IDs, PDA seeds, Create/Buy/Sell account order, what the pipeline sends vs what you derive. For building transactions. |
| [RUNBOOK.md](RUNBOOK.md) | Operations: how to confirm gRPC/data flow, ClickHouse, parse errors, WebSocket clients. |

**New to the codebase:** Start with [main.md](main.md) for the big picture, then [handler.md](handler.md) and [multi_parser.md](multi_parser.md) for the data path. Use [messages.md](messages.md) and [pumpfun_accounts.md](pumpfun_accounts.md) when integrating clients or building Pump.fun transactions.

---

## How to run

```bash
cd data-pipeline
export GRPC_ENDPOINT="https://your-geyser-endpoint"
# optional: GRPC_API_KEY, WS_PORT, WS_MAX_CONNECTIONS, WS_MAX_LAG_DROP, CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, RUST_LOG
cargo run --release
```

WebSocket: `ws://0.0.0.0:5566` (or your `WS_PORT`). Logs: `Connected to blockchain gRPC`, `WebSocket server listening on ws://...`, and per-event lines (LAUNCH, BUY/SELL, MIGRATE).

---

## Architecture (one paragraph)

**main** builds config, optional ClickHouse client (and ensures `pipeline_metrics` table), starts the WebSocket server, then runs a retry loop: connect gRPC (**client**), build **multi_parser**’s program-id map, subscribe to transactions that include Pump.fun and Pump AMM program IDs, and run **handler**’s `handle_stream_updates` on the stream. **handler** parses each transaction into instructions, calls **multi_parser** to get launches/trades/migrations and enriches launches; for each event it optionally writes metrics (**db**), serializes to MessagePack (**messages**), and broadcasts via **ws_server**. **multi_parser** delegates to **protocols** (pump_fun, pump_amm) for Create/trade/migration parsing. All types are in **events** and **messages**.
