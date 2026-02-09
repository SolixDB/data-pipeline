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
| `clickhouse_url` | `Option<String>` | `CLICKHOUSE_URL` | If set and non-empty (and not skip_clickhouse), ClickHouse client is created; metrics and storage writes. |
| `clickhouse_user` | `Option<String>` | `CLICKHOUSE_USER` | Optional; empty string filtered out. |
| `clickhouse_password` | `Option<String>` | `CLICKHOUSE_PASSWORD` | Optional; empty string filtered out. |
| `redis_url` | `Option<String>` | `REDIS_URL` | Optional. When set, storage worker writes snapshot keys (migrated, soon_migrating, launches, trades); WS server can send snapshots from Redis when in-memory state is not used. |
| `clear_clickhouse` | `bool` | `CLEAR_CLICKHOUSE` | When true (1/true/yes), drop and recreate all pipeline ClickHouse tables on startup. Requires CLICKHOUSE_URL. |
| `skip_clickhouse` | `bool` | `SKIP_CLICKHOUSE` | When true (1/true/yes), no ClickHouse client; no metrics or storage writes. Redis and WebSocket unchanged. |
| `clear_clickhouse_only` | `bool` | `CLEAR_CLICKHOUSE_ONLY` | When true (1/true/yes), connect to ClickHouse, drop and recreate tables, then set client to None for the rest of the run (no further writes). Requires CLICKHOUSE_URL. |
| `health_port` | `u16` | `HEALTH_PORT` | HTTP health server port; 0 = disabled. Default 5567. |
| `health_grpc_max_age_secs` | `u64` | `HEALTH_GRPC_MAX_AGE_SECS` | Health returns 200 only if last gRPC activity is within this many seconds. Default 120. |
| `snapshot_launch_limit` | `usize` | `WS_INITIAL_SNAPSHOT_LAUNCH_LIMIT` | Max token launches in WS connect snapshot. Default 30. |
| `snapshot_trade_limit` | `usize` | `WS_INITIAL_SNAPSHOT_TRADE_LIMIT` | Max trades (for those mints) in WS connect snapshot. Default 1000. |

---

## Default (Config::default())

- `endpoint`: `env::var("GRPC_ENDPOINT")` — panics (expect) if unset.
- `api_token`: `env::var("GRPC_API_KEY").ok()`.
- `commitment`: `CommitmentLevel::Processed`.
- `max_decoding_message_size`: 1 GiB.
- `reconnect_*`: as in table above.
- `clickhouse_*`: from env; `.filter(|s| !s.is_empty())` so empty string is treated as unset.
- `redis_url`: from env `REDIS_URL`; empty filtered out.
- `clear_clickhouse`, `skip_clickhouse`, `clear_clickhouse_only`: from env (1/true/yes → true).

---

## Validation (validate())

- `endpoint` must not be empty.
- `endpoint` must start with `http://` or `https://`.
- If `clear_clickhouse` is true and `skip_clickhouse` is false, `clickhouse_url` must be set (non-empty).
- If `clear_clickhouse_only` is true, `clickhouse_url` must be set (non-empty).

Returns `anyhow::Result<()>`. Called in main before starting the server; invalid config exits the process.

---

## Where config is used

- **main**: validation; WS_PORT from env separately (not in Config); backoff built from reconnect_*; creates health_state and spawns health server when health_port != 0; passes config clone and health_state into retry closure; uses redis_url for Redis client and storage worker; uses clickhouse_* and clear/skip flags for ClickHouse client lifecycle; passes snapshot_launch_limit and snapshot_trade_limit to spawn_storage_worker.
- **client**: endpoint, api_token, max_decoding_message_size, and endpoint scheme (https → TLS). See [client.md](client.md).
- **handler**: received as argument; used only for `send_ping` (commitment and ping id). See [handler.md](handler.md).

WS_PORT, WS_MAX_CONNECTIONS, WS_MAX_LAG_DROP are read in ws_server (WsConfig::default()) and in main (for port only); they are not fields of Config.
