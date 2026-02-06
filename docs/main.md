# Application entry point

**Source:** `src/main.rs`

This document describes how the data-pipeline binary starts and what it does before the WebSocket server is running.

---

## Purpose

The binary is the process entry point. It initializes cryptography and logging, loads and validates configuration, then starts the WebSocket server and keeps the process alive until the server task exits or the process is terminated.

---

## Startup flow

**1. TLS and cryptography**

The default rustls crypto provider (e.g. `aws_lc_rs`) is installed so that any TLS used later (for example by the Geyser gRPC client) works correctly.

**2. Logging**

`tracing_subscriber` is configured with:

- A filter from the `RUST_LOG` environment variable (default: `info`).
- A format that omits target, thread ID, file, and line so log lines stay short.

**3. Configuration**

`Config` is built from the environment (see [config.md](config.md)). `config.validate()` is called so the process exits immediately with an error if required settings (such as `GRPC_ENDPOINT`) are missing or invalid.

**4. WebSocket server**

- The listen port is read from `WS_PORT` (default: 5566).
- `WsConfig` is built from the environment (see [ws_server.md](ws_server.md)).
- `WsServer` is created with that config and `start(ws_port)` is called.
- The function returns a **join handle** for the server task. The main function **awaits** this handle so the process does not exit while the server is running.

---

## Why the join handle is awaited

`WsServer::start()` spawns the accept loop in a background task and returns its `JoinHandle`. If `main` did not await that handle, execution would reach the end of `main`, the process would exit, and the runtime would shut down, stopping the WebSocket server. Awaiting the handle keeps the process running for the lifetime of the server task.

---

## Dependencies

- **Modules:** `config`, `client`, `messages`, `ws_server`, and the handler/multi_parser pipeline.
- **Config:** Used for validation in `main`; the gRPC client is constructed and used elsewhere (see [client.md](client.md)). The handler subscribes to Geyser and pushes parsed events to the WebSocket server.

---

## See also

- [config.md](config.md) — Environment-based configuration.
- [ws_server.md](ws_server.md) — WebSocket server and `WsConfig`.
