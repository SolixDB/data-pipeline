# Runbook — operations

How to confirm the pipeline is healthy and what to do when something goes wrong. No health HTTP server in this codebase; use logs and optional ClickHouse.

---

## Confirming gRPC is connected

- **Logs:** On successful connect you should see: `Connected to blockchain gRPC`. If the stream later errors, you’ll see `Connection failed, retrying: ...` and then a new `Connected to blockchain gRPC` after backoff.
- If you never see `Connected to blockchain gRPC`, check `GRPC_ENDPOINT` and `GRPC_API_KEY` (if required). Endpoint must be http:// or https://.

---

## Confirming data is flowing

- **Logs:** Per-event lines: `#N LAUNCH | name (symbol) | mint=... | clients=K`, `#N BUY/SELL | ... | clients=K`, `#N MIGRATE | ... | clients=K`. If gRPC is connected but no such lines, the subscription filter may not match (we subscribe to transactions that include Pump.fun or Pump AMM program IDs). Commitment is Processed.
- **WebSocket:** Connect to ws://host:WS_PORT; you should receive one `connected` then a stream of `token_launch`, `trade`, `migration` as the pipeline processes transactions.
- **Parse errors:** A transaction that fails to parse (e.g. InstructionUpdate::parse_from_txn) logs `Transaction error: ...` and the handler continues; no counter or HTTP endpoint. Check logs for repeated errors.

---

## ClickHouse (optional)

- **When used:** Only when `CLICKHOUSE_URL` is set. main builds the client and calls `db::ensure_table` (creates `pipeline_metrics` if not exists). Handler calls `update_metrics_aggregates_optional` for each launch; if ClickHouse is unset, it’s a no-op.
- **Table:** `pipeline_metrics` (key, value, updated_at). Keys: launch_count, grpc_sum_ms, grpc_min_ms, grpc_max_ms, server_sum_us, server_min_us, server_max_us. See [db.md](db.md).
- **If writes fail:** We log `ClickHouse metrics update failed: ...`. No retry or circuit breaker in the current code; stream and WebSocket are unaffected.
- **Confirming writes:** Query the DB (e.g. `SELECT key, argMax(value, updated_at) FROM pipeline_metrics GROUP BY key`). There is no in-process health endpoint.

---

## WebSocket clients

- **Count:** Logs: `Client X connected (Y total)` and `Client X disconnected (Y remaining)`. No HTTP metrics endpoint.
- **Connection limit:** When `WS_MAX_CONNECTIONS` is set, new connections beyond that are rejected; log: `Rejected connection from ... max connections reached`.
- **Lag:** When a client drops messages (broadcast channel lag), we log `Client X lagged, dropped N messages`. If `WS_MAX_LAG_DROP` is set and N >= that value, we disconnect that client and log `Client X lagged N messages (limit M), disconnecting for fresh state`.
- **Port:** Use `WS_PORT` (default 5566) for WebSocket only. There is no separate health port; do not point a health check at the WebSocket port expecting HTTP.

---

## No launches / no data

1. **gRPC:** Ensure `Connected to blockchain gRPC` appears and no permanent disconnect. Check endpoint and auth.
2. **Filter:** Subscription is transactions with account_include = [Pump.fun program id, Pump AMM program id]. So only transactions that touch those programs are received. If the chain has little Pump.fun/AMM traffic, you’ll see few or no events.
3. **Parse errors:** Look for `Transaction error` in logs. If many, the transaction format or parser may need adjustment.
4. **Commitment:** We use Processed. If the provider doesn’t support it or filters differently, behaviour may differ.

---

## Summary

| Check | How |
|-------|-----|
| gRPC connected? | Logs: "Connected to blockchain gRPC". |
| Data flowing? | Logs: LAUNCH / BUY/SELL / MIGRATE lines; or connect a WS client and see messages. |
| ClickHouse OK? | Query pipeline_metrics; or check logs for "ClickHouse metrics update failed". |
| Why no launches? | Verify endpoint, filter (program IDs), commitment; check "Transaction error" in logs. |
