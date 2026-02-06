# Data pipeline documentation

This directory documents the **data-pipeline** service: what it does, how to run it, and how to consume its WebSocket stream. It is written for both operators and developers integrating with the pipeline.

---

## What this service does

The data-pipeline subscribes to a Solana Geyser gRPC stream (Yellowstone), parses Pump.fun and Pump AMM transactions, and broadcasts **token launch** and **trade** events over a WebSocket. Clients receive real-time, structured data they can use to build Create/Buy/Sell transactions without extra RPC lookups.

- **Input:** Geyser gRPC (transactions for Pump.fun and Pump AMM programs).
- **Output:** WebSocket on a configurable port; each frame is a MessagePack-encoded server message (e.g. `token_launch`, `trade`).
- **Parsed events:** New token creation (Pump.fun Create) and every buy/sell on the bonding curve or AMM, with accounts and curve state needed for transaction building.

---

## Documentation index

| Document | Audience | Contents |
|----------|----------|----------|
| [messages.md](messages.md) | Frontend/backend integrating with the WebSocket | Wire format: message types, payloads, field semantics, serialization. How to interpret and use each event. |
| [pumpfun_accounts.md](pumpfun_accounts.md) | Frontend/backend building Pump.fun transactions | Program IDs, PDAs, bonding curve vs AMM, what the pipeline sends vs what you derive. Before/after migration. |
| [main.md](main.md) | Developers working on the binary | Entry point, startup flow, TLS and logging. |
| [config.md](config.md) | Operators and developers | Configuration and environment variables (gRPC, commitment). |
| [client.md](client.md) | Developers wiring the pipeline | Geyser gRPC client construction and connection. |
| [ws_server.md](ws_server.md) | Developers working on the server | WebSocket server: accept loop, broadcast, connection limits, lag handling. |

**If you are integrating a frontend or backend with the WebSocket:** read [messages.md](messages.md) first, then [pumpfun_accounts.md](pumpfun_accounts.md) when you need to build or interpret Pump.fun transactions.

**If you are modifying the data-pipeline code:** start with [main.md](main.md) and the source-file table in this README, then open the doc for the file you are changing.

---

## Architecture (high level)

```
  Geyser gRPC (Solana)     data-pipeline                WebSocket clients
  ───────────────────     ─────────────                ─────────────────
        │                       │                             │
        │  Subscribe (Pump.fun  │   Parse Create / Buy /     │
        │  + Pump AMM)          │   Sell; decode tradeEvent   │
        │──────────────────────►│   Enrich (fee_recipient,    │
        │                       │   initial curve state)      │
        │                       │                             │
        │                       │   broadcast(MessagePack)    │
        │                       │────────────────────────────►│  Terminal / API
        │                       │                             │
```

- **handler.rs** turns parsed transactions into `TokenLaunch` and `Trade` events, enriches launches from same-tx data, then serializes and broadcasts.
- **ws_server.rs** holds the broadcast channel and per-client tasks; each client gets the same stream.

---

## Source files and their docs

| Source file | Documentation | Summary |
|-------------|---------------|---------|
| `src/main.rs` | [main.md](main.md) | Entry point, TLS, logging, config validation, starts WebSocket server and keeps process alive. |
| `src/config.rs` | [config.md](config.md) | Config struct, env vars (GRPC_ENDPOINT, GRPC_API_KEY), validation. |
| `src/client.rs` | [client.md](client.md) | Geyser gRPC client build, connect, TLS, auth. |
| `src/messages.rs` | [messages.md](messages.md) | ServerMessage / ClientMessage types, payloads, MessagePack wire format. |
| `src/ws_server.rs` | [ws_server.md](ws_server.md) | WsConfig, WsServer, broadcast channel, accept loop, handle_connection, connection limit, lag disconnect. |
| `src/handler.rs` | (see main + messages) | Parses transaction updates, produces launches and trades, enriches, broadcasts. |
| `src/multi_parser.rs` | (see pumpfun_accounts) | Dispatches to Pump.fun parser, enrich_token_launches. |
| `src/protocols/pump_fun.rs` | [pumpfun_accounts.md](pumpfun_accounts.md) | Create/tradeEvent parsing, account indices. |

---

## Environment variables (reference)

| Variable | Used in | Default | Purpose |
|----------|---------|---------|---------|
| `GRPC_ENDPOINT` | config | required | Geyser gRPC server URL. |
| `GRPC_API_KEY` | config | — | Optional gRPC auth token. |
| `WS_PORT` | main | 5566 | WebSocket listen port. |
| `WS_MAX_CONNECTIONS` | ws_server | no limit | Max concurrent WebSocket clients. |
| `WS_MAX_LAG_DROP` | ws_server | no disconnect | Disconnect client if it drops at least this many messages in one lag event. |
| `RUST_LOG` | main | info | Log level filter. |

---

## Running the service

Build and run from the data-pipeline directory. Set at least `GRPC_ENDPOINT`; other env vars are optional (see [config.md](config.md) and [ws_server.md](ws_server.md)).

```bash
cargo run
```

For project-level build/run and repository layout, see the [repository README](../README.md) at the project root.
