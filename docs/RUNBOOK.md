# Runbook — operations

How to confirm the pipeline is healthy and what to do when something goes wrong. No health HTTP server in this codebase; use logs and optional ClickHouse.

---

## How to run (quick reference)

Full run options and env table: [README.md](README.md#how-to-run).

| Goal | Command |
|------|--------|
| Minimal (WS only) | `GRPC_ENDPOINT=https://... cargo run --release` |
| Save to ClickHouse | `GRPC_ENDPOINT=... CLICKHOUSE_URL=http://localhost:8123 cargo run --release` |
| Dev: clear DB each run | Add `CLEAR_CLICKHOUSE=1` (requires CLICKHOUSE_URL). |
| Dev: clear once, no save | Add `CLEAR_CLICKHOUSE_ONLY=1` (requires CLICKHOUSE_URL). |
| No ClickHouse | Add `SKIP_CLICKHOUSE=1`. |

Run from the `data-pipeline` directory. WebSocket default port: 5566 (`WS_PORT`).

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

- **When used:** When `CLICKHOUSE_URL` is set and `SKIP_CLICKHOUSE` is not set. main ensures pipeline_metrics and storage tables (token_launches, trades, migrations). Handler writes metrics; storage worker writes launches/trades/migrations.
- **Env flags:** `CLEAR_CLICKHOUSE=1` — drop and recreate all four tables on startup. `SKIP_CLICKHOUSE=1` — no ClickHouse client. `CLEAR_CLICKHOUSE_ONLY=1` — drop and recreate then do not write for the rest of the run.
- **Tables:** `pipeline_metrics` (key, value, updated_at); `token_launches`, `trades`, `migrations`. See [db.md](db.md).
- **If writes fail:** We log `ClickHouse metrics update failed` or `insert_* failed`. No retry; stream and WebSocket are unaffected.
- **Confirming writes:** Query the DB (e.g. `SELECT key, argMax(value, updated_at) FROM pipeline_metrics GROUP BY key` or `SELECT count() FROM token_launches`). No in-process health endpoint.

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

---

## Holder stats (top 10 % and dev %) from the DB

Balances are **trading-derived** only (buys − sells per mint/trader); we do not see SPL transfers. Dev is the **creator** from `token_launches`.

Use `db::query_holder_stats(client, mint)` (in Rust). It returns:

- `top10_pct`: share of supply held by the top 10 holders (0–100).
- `dev_pct`: share of supply held by the creator (0–100).
- `top10`: list of `(trader, balance)` for the top 10.

Example (e.g. from an API or script that has a ClickHouse client):

```rust
let (top10_pct, dev_pct, top10) = db::query_holder_stats(&client, "MintBase58Here").await?;
```

Equivalent logic in SQL: aggregate `trades` by `mint` and `trader` with `sum(if(direction='buy', token_amount, -token_amount))`, take positive balances only; join `token_launches` for `creator`; then top 10 and creator balance as % of total.
