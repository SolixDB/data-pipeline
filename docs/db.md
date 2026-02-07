# db.rs — ClickHouse metrics

**Source:** `src/db.rs`

Single table `pipeline_metrics` for aggregate metrics (launch count, gRPC latency, server latency). Used only when `CLICKHOUSE_URL` is set. No other tables or features. All behaviour is described here.

---

## Public API

- **make_client(url, user, password)** — build ClickHouse `Client`; optional user/password.
- **ensure_table(client)** — create `pipeline_metrics` if not exists (ReplacingMergeTree(updated_at)).
- **update_metrics_aggregates_optional(client, grpc_ms, server_us)** — if client is Some, read current metrics, update aggregates, insert new rows. Logs on error but does not return it (no-op or log).

`get_current_metrics` and `update_metrics_aggregates` are used only inside this module.

---

## make_client

- `Client::default().with_url(url)`.
- If user: `.with_user(u)`.
- If password: `.with_password(p)`.

Returns the client; no connection test. main calls this once when building `metrics_client`; handler receives `Option<Arc<Client>>`.

---

## Table: pipeline_metrics

- **Columns:** `key` (LowCardinality(String)), `value` (Float64), `updated_at` (DateTime64(3, 'UTC')).
- **Engine:** ReplacingMergeTree(updated_at), ORDER BY key.
- **Semantics:** One row per metric key; ReplacingMergeTree keeps the row with latest `updated_at` per key after merges. So we use it as a key-value store with “last write wins”.

---

## Metric keys (constants)

- `launch_count` — number of launches (incremented by 1 per launch).
- `grpc_sum_ms`, `grpc_min_ms`, `grpc_max_ms` — sum/min/max of (received_at/1000 - txn_timestamp) in ms.
- `server_sum_us`, `server_min_us`, `server_max_us` — sum/min/max of (sent_at - received_at) in μs.

All are updated together in one batch per launch (handler passes one grpc_ms and one server_us per launch).

---

## get_current_metrics(client)

- Query: `SELECT key, argMax(value, updated_at) AS value FROM pipeline_metrics GROUP BY key`.
- Returns `HashMap<String, f64>`; missing keys are simply absent (caller treats as 0).

Used by update_metrics_aggregates to read current state before writing.

---

## update_metrics_aggregates(client, grpc_ms, server_us)

1. `current = get_current_metrics(client).await`.
2. `now_ms` = current time in ms (for updated_at).
3. Compute new values: launch_count + 1, grpc_sum + grpc_ms, grpc_min = min(current min, grpc_ms), grpc_max = max(current max, grpc_ms); same for server_*.
4. Build seven `MetricRow { key, value, updated_at }` and insert into pipeline_metrics.

ReplacingMergeTree will eventually collapse duplicates by key to the latest updated_at; reads use argMax so we always get the latest value per key.

---

## update_metrics_aggregates_optional(client, grpc_ms, server_us)

If `client` is None: do nothing. If Some: call `update_metrics_aggregates`; on error log “ClickHouse metrics update failed” and do not propagate. So handler can call this unconditionally; when ClickHouse is not configured it’s a no-op.

---

## Row types

- **MetricRow:** key, value (f64), updated_at (i64 ms). Used for inserts.
- **MetricKeyValue:** key, value. Used for the SELECT result (cursor.next()).

---

## Where db is used

- **main:** make_client, ensure_table when CLICKHOUSE_URL is set; passes metrics_client into handle_stream_updates.
- **handler:** update_metrics_aggregates_optional for each launch (grpc_ms and server_us from that launch).

No health checks or other endpoints; if ClickHouse is down, the insert fails and we log.
