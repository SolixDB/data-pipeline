# api.rs — HTTP API (charts, health)

**Source:** `src/api.rs`

Small Axum HTTP server for charts and health. It exposes a TradingView‑style UDF API backed by ClickHouse, plus a simple health endpoint.

---

## AppState

```rust
pub struct AppState {
    pub clickhouse: Option<Arc<Client>>,
    pub health_state: Arc<crate::health::HealthState>,
}
```

- `clickhouse` – when `Some`, `/history` queries OHLCV from ClickHouse; when `None`, `/history` returns `503 Service Unavailable`.
- `health_state` – shared health state (updated by the gRPC handler); used by `/health` if you want to make it stricter in the future.

`main.rs` constructs `AppState` and passes it into `ApiServer::start`.

---

## Endpoints

Base URL: `http://0.0.0.0:<HEALTH_PORT>` (default 5567).

Routes:

- `GET /config` → UDF config.
- `GET /symbols` → symbol metadata (by `symbol` query param).
- `GET /history` → OHLCV history for a mint.
- `GET /health` → basic health.
- `GET /` → alias for `/health`.

### GET /config

Returns static TradingView UDF config:

- `supported_resolutions`: `["1","5","60","D"]`
- `supports_group_request`: `false`
- `supports_marks`: `false`
- `supports_search`: `true`
- `supports_timescale_marks`: `false`

### GET /symbols?symbol=<MINT>

Query params:

- `symbol`: base58 mint (required).

Response fields (subset):

- `name`, `full_name`, `description`
- `type_ = "crypto"`
- `session = "24x7"`
- `exchange = "DEX"`
- `listed_exchange = "DEX"`
- `timezone = "Etc/UTC"`
- `format = "price"`
- `pricescale = 1_000_000_000` (9‑decimals support)
- `minmov = 1`
- `supported_resolutions = ["1","5","60","D"]`
- `volume_precision = 2`
- `data_status = "streaming"`

All values are generic; the pipeline does not currently look up per‑mint metadata here.

### GET /history

Query params (`UdfHistoryParams`):

- `symbol: String` – mint (base58).
- `resolution: String` – `"1"`, `"5"`, `"15"`, `"30"`, `"60"`, `"1H"`, `"240"`, `"4H"`, `"D"`, `"1D"`.
- `from: u64` – start time (Unix seconds).
- `to: u64` – end time (Unix seconds).
- `countback: Option<u64>` – reserved for future use (currently ignored).

Resolution mapping (minutes):

- `"1"` → 1
- `"5"` → 5
- `"15"` → 15
- `"30"` → 30
- `"60"` or `"1H"` → 60
- `"240"` or `"4H"` → 240
- `"D"` or `"1D"` → 1440
- other → 1

Behaviour:

- If `state.clickhouse.is_none()`: returns `503 Service Unavailable` with `"ClickHouse not configured"`.
- Else: calls `db::fetch_aggregated_ohlcv(client, symbol, interval_minutes, from*1_000_000, to*1_000_000)`.
  - Input times are seconds; ClickHouse stores microseconds.
  - Response rows are converted back to seconds.

Response (`UdfHistoryResponse`):

```json
{
  "s": "ok" | "no_data" | "error",
  "t": [u64],
  "o": [f64],
  "h": [f64],
  "l": [f64],
  "c": [f64],
  "v": [f64],
  "nextTime": u64 | null
}
```

- `"ok"` – candles found; arrays are populated.
- `"no_data"` – no rows; arrays are empty.
- `"error"` – internal error (returns 500 with a plain string body).

### GET /health and GET /

Signature:

```rust
async fn get_health(State(_state): State<AppState>) -> impl IntoResponse
```

Currently returns:

- `200 OK`, body `"OK"`.

This is a simple liveness/readiness endpoint. You can harden it later by checking `health_state` and returning 503 when gRPC has been idle too long.

---

## ApiServer::start

```rust
pub struct ApiServer;

impl ApiServer {
    pub async fn start(
        port: u16,
        clickhouse: Option<Arc<Client>>,
        health_state: Arc<crate::health::HealthState>,
    )
}
```

Behaviour:

- Builds `AppState { clickhouse, health_state }`.
- Assembles an Axum `Router` with the routes above.
- Binds `0.0.0.0:port`.
- Logs:
  - `API/Health/Metrics server listening on http://0.0.0.0:<port>`
- Runs `axum::serve` until the process shuts down.

`main.rs` spawns this in a background task; it is independent from the WebSocket server.

