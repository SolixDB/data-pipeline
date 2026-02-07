# config.rs — configuration

**Source:** `src/config.rs`

Config is loaded from the environment and validated at startup. This doc lists every field, env var, and how it’s used. No external references.

---

## Config struct (all fields)

| Field | Type | Env var | Meaning |
|-------|------|---------|--------|
| `endpoint` | `String` | `GRPC_ENDPOINT` | Geyser gRPC URL. Required. Must start with `http://` or `https://`. |
| `api_token` | `Option<String>` | `GRPC_API_KEY` | Optional auth token; sent as x_token on gRPC. |
| `commitment` | `CommitmentLevel` | — | Fixed in code: `Processed`. Used in subscribe request and ping. |
| `max_decoding_message_size` | `usize` | — | Fixed: 1024 * 1024 * 1024 (1 GiB). Passed to gRPC client builder. |
| `reconnect_initial_interval_ms` | `u64` | — | Fixed: 500. First backoff delay when gRPC stream fails. |
| `reconnect_max_interval_ms` | `u64` | — | Fixed: 30000. Cap on backoff interval. |
| `reconnect_max_elapsed_time_ms` | `Option<u64>` | — | Fixed: None. If set, backoff would stop after this total time. |
| `clickhouse_url` | `Option<String>` | `CLICKHOUSE_URL` | If set and non-empty, ClickHouse client is created and metrics are written. |
| `clickhouse_user` | `Option<String>` | `CLICKHOUSE_USER` | Optional; empty string filtered out. |
| `clickhouse_password` | `Option<String>` | `CLICKHOUSE_PASSWORD` | Optional; empty string filtered out. |

---

## Default (Config::default())

- `endpoint`: `env::var("GRPC_ENDPOINT")` — panics (expect) if unset.
- `api_token`: `env::var("GRPC_API_KEY").ok()`.
- `commitment`: `CommitmentLevel::Processed`.
- `max_decoding_message_size`: 1 GiB.
- `reconnect_*`: as in table above.
- `clickhouse_*`: from env; `.filter(|s| !s.is_empty())` so empty string is treated as unset.

---

## Validation (validate())

- `endpoint` must not be empty.
- `endpoint` must start with `http://` or `https://`.

Returns `anyhow::Result<()>`. Called in main before starting the server; invalid config exits the process.

---

## Where config is used

- **main**: validation; WS_PORT from env separately (not in Config); backoff built from reconnect_*; passes config clone into retry closure.
- **client**: endpoint, api_token, max_decoding_message_size, and endpoint scheme (https → TLS). See [client.md](client.md).
- **handler**: received as argument; used only for `send_ping` (commitment and ping id). See [handler.md](handler.md).

WS_PORT, WS_MAX_CONNECTIONS, WS_MAX_LAG_DROP are read in ws_server (WsConfig::default()) and in main (for port only); they are not fields of Config.
