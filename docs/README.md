# Data Pipeline — Code Documentation

This directory is **for developers**: it explains the **data-pipeline** Rust code in detail (types, control flow, design). For project overview, build/run, and env vars, see the [repo README](../README.md) at the project root.

Here, each document below maps to a source file and describes its role, main types, and how it fits into the system.

## Quick overview

- **Entry point:** [main.rs](main.md) — TLS/logging setup, config validation, starts the WebSocket server and keeps the process alive.
- **Configuration:** [config.rs](config.md) — gRPC-related env (endpoint, API key, limits).
- **gRPC client:** [client.rs](client.md) — Builds and connects the Yellowstone Geyser gRPC client (for future pipeline that feeds the WebSocket).
- **Wire format:** [messages.rs](messages.md) — Server/client message types (e.g. `Connected`, `Error`; future `Subscribe`/`Unsubscribe`).
- **WebSocket server:** [ws_server.rs](ws_server.md) — Accept loop, broadcast channel, per-client handling, connection limit, lag-based disconnect.

## How to use this documentation

1. **New to the project?**  
   Start with [main.md](main.md) to see how the binary boots, then read [ws_server.md](ws_server.md) to understand how clients connect and receive data.

2. **Adding the data pipeline?**  
   You’ll wire [client.md](client.md) (gRPC) to [ws_server.md](ws_server.md) (`broadcast`), and use the types in [messages.md](messages.md) for the stream format.

3. **Changing config or env vars?**  
   See [config.md](config.md) for gRPC and [ws_server.md](ws_server.md#wsconfig) for WebSocket (e.g. `WS_MAX_CONNECTIONS`, `WS_MAX_LAG_DROP`).

4. **Debugging connections or broadcasts?**  
   [ws_server.md](ws_server.md) describes the accept loop, `handle_connection`, lag handling, and connection limits.

## Source file → doc mapping

| Source file      | Documentation   | What it covers |
|------------------|-----------------|----------------|
| `src/main.rs`    | [main.md](main.md)       | Entry point, TLS, logging, config validation, starting and awaiting the WebSocket server. |
| `src/config.rs` | [config.md](config.md)   | `Config` fields, env vars (`GRPC_ENDPOINT`, `GRPC_API_KEY`), `validate()`. |
| `src/client.rs` | [client.md](client.md)  | `GrpcClient`, `connect()`, TLS and token setup. |
| `src/messages.rs`| [messages.md](messages.md) | `ServerMessage`, `ClientMessage`, payloads, serialization (MessagePack). |
| `src/ws_server.rs` | [ws_server.md](ws_server.md) | `WsConfig`, `WsServer`, broadcast channel, accept loop, `handle_connection`, connection limit, lag disconnect. |

## Architecture (high level)

```
                    ┌─────────────────────────────────────┐
                    │  main.rs                            │
                    │  - TLS, logging, config validation  │
                    │  - WsServer::start(port)            │
                    │  - await server task (keep alive)   │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │  ws_server.rs                       │
                    │  - TcpListener (SO_REUSEADDR)       │
                    │  - Accept → try_acquire → spawn     │
                    │  - handle_connection per client     │
                    │  - broadcast_tx → all clients       │
                    └──────────────┬──────────────────────┘
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         │                         │                         │
         ▼                         ▼                         ▼
   Terminal 1               Terminal 2               Terminal N
   (client_id, rx)          (client_id, rx)            (client_id, rx)
```

The **producer** of the broadcast stream is not yet connected: it will be the pipeline that uses [client.rs](client.md) to subscribe to Geyser gRPC and call `WsServer::broadcast()` with serialized token/trade updates (see [messages.md](messages.md)).

## Environment variables (reference)

| Variable | Used in | Default | Purpose |
|----------|---------|---------|---------|
| `GRPC_ENDPOINT` | config | (required) | gRPC server URL. |
| `GRPC_API_KEY` | config | — | Optional gRPC auth token. |
| `WS_PORT` | main | 5566 | WebSocket listen port. |
| `WS_MAX_CONNECTIONS` | ws_server | no limit | Max concurrent WebSocket clients. |
| `WS_MAX_LAG_DROP` | ws_server | no disconnect | Disconnect client if it drops ≥ N messages in one lag event. |
| `RUST_LOG` | main | info | Log level filter. |

---

For file-by-file explanations, open the linked `.md` files above.
