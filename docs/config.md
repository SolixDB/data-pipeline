# `config.rs` — Configuration

**Path:** `src/config.rs`

## Purpose

Holds gRPC-related configuration loaded from environment variables. Used when building and connecting the Geyser gRPC client (see [client.md](client.md)).

## Struct: `Config`

| Field | Env var | Description |
|-------|---------|-------------|
| `endpoint` | `GRPC_ENDPOINT` | **Required.** gRPC endpoint URL (e.g. `https://...` or `http://...`). |
| `api_token` | `GRPC_API_KEY` | Optional. Auth token for the gRPC service. |
| `commitment` | — | Set to `Processed` in `Default`. |
| `max_decoding_message_size` | — | 1 GiB. Max size for decoded gRPC messages. |
| `reconnect_initial_interval_ms` | — | 500 ms. For future reconnect backoff. |
| `reconnect_max_interval_ms` | — | 30 s. For future reconnect backoff. |
| `reconnect_max_elapsed_time_ms` | — | `None`. Optional cap on total reconnect time. |

## Default implementation

`Config::default()` reads from the environment:

- **`GRPC_ENDPOINT`** — required; `.expect()` if missing.
- **`GRPC_API_KEY`** — optional; `.ok()`.

Other fields are fixed in code (see table above).

## Validation: `validate()`

- `endpoint` must be non-empty.
- `endpoint` must start with `http://` or `https://`.

Returns `anyhow::Result<()>`; use in `main` so the process fails fast on bad config.

## Usage

Currently used when constructing the gRPC client (see [client.md](client.md)). The binary calls `Config::default()` and `validate()` in [main.rs](main.md); the actual client is not yet wired in `main`, so these fields may appear unused until the pipeline that consumes gRPC and calls `WsServer::broadcast` is connected.

## See also

- [client.md](client.md) — Uses `Config` to build and connect the gRPC client
