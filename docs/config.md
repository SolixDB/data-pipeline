# Configuration

**Source:** `src/config.rs`

This document describes how the data-pipeline is configured and which environment variables it reads.

---

## Purpose

Configuration is loaded from the environment and used when building and connecting the Geyser gRPC client (see [client.md](client.md)). Validation runs at startup so the process fails fast if required values are missing or invalid.

---

## Config struct

| Field | Environment variable | Description |
|-------|----------------------|-------------|
| `endpoint` | `GRPC_ENDPOINT` | **Required.** URL of the Geyser gRPC endpoint (e.g. `https://...` or `http://...`). |
| `api_token` | `GRPC_API_KEY` | Optional. Authentication token for the gRPC service. |
| `commitment` | — | Set in code. Default is `Processed`. |
| `max_decoding_message_size` | — | Set in code. Maximum size of decoded gRPC messages (e.g. 1 GiB). |
| `reconnect_initial_interval_ms` | — | Set in code. Initial backoff interval for reconnection (e.g. 500 ms). |
| `reconnect_max_interval_ms` | — | Set in code. Maximum backoff interval (e.g. 30 s). |
| `reconnect_max_elapsed_time_ms` | — | Set in code. Optional cap on total time spent reconnecting. |

---

## Default and validation

**Default:** `Config::default()` reads from the environment: `GRPC_ENDPOINT` is required (missing value causes a panic or error depending on usage); `GRPC_API_KEY` is optional.

**Validation:** `validate()` checks that:

- `endpoint` is non-empty.
- `endpoint` starts with `http://` or `https://`.

It returns `anyhow::Result<()>`. The binary calls it in `main` so invalid configuration stops the process immediately.

---

## Usage

The binary builds `Config` via `Config::default()` and calls `validate()` in `main`. The same config is passed to the code that constructs the gRPC client and subscribes to Geyser; parsed updates are then broadcast over the WebSocket (see [client.md](client.md) and [ws_server.md](ws_server.md)).

---

## See also

- [client.md](client.md) — Uses `Config` to build and connect the gRPC client.
- [README.md](README.md) — Environment variable reference table.
