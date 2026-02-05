# `main.rs` — Application Entry Point

**Path:** `src/main.rs`

## Purpose

The binary entry point: initializes the process, configures logging and TLS, validates config, then starts and keeps the WebSocket server running.

## Flow

1. **TLS / crypto**  
   Installs the default rustls crypto provider (e.g. `aws_lc_rs`) so any TLS used later (e.g. gRPC client) works.

2. **Logging**  
   Sets up `tracing_subscriber` with:
   - Filter from `RUST_LOG` (default: `info`)
   - No target / thread ID / file / line in the log format (keeps logs short)

3. **Configuration**  
   Builds `Config` from environment (see [config.md](config.md)), then calls `config.validate()?` so the process fails fast if required env (e.g. `GRPC_ENDPOINT`) is missing or invalid.

4. **WebSocket server**  
   - Reads `WS_PORT` (default `5566`).
   - Builds `WsConfig` from env (see [ws_server.md](ws_server.md#wsconfig)).
   - Creates `WsServer` with that config and calls `start(ws_port)`.
   - **Awaits the returned join handle** so the process stays alive and the WebSocket server runs until it exits or panics.

## Why the join handle is awaited

`WsServer::start()` spawns the accept loop in a background task and returns its `JoinHandle`. If `main` did not await that handle, it would reach `Ok(())` and exit, shutting down the runtime and killing the WebSocket server. Awaiting the handle keeps the process running for the lifetime of the server.

## Dependencies

- **Modules:** `config`, `client`, `messages`, `ws_server`
- **Config:** Used for validation only in this binary; the gRPC `client` is not started here (see [client.md](client.md)).

## See also

- [config.md](config.md) — Env-based configuration
- [ws_server.md](ws_server.md) — WebSocket server and `WsConfig`
