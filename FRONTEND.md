# Frontend integration guide

This document is for the frontend engineer wiring the UI into the data‑pipeline. It focuses only on what you need to connect, consume data, and build features quickly.

---

## Endpoints and ports

- **WebSocket (real‑time):** `ws://<host>:WS_PORT`
  - Default `WS_PORT` = `5566`.
  - Env: `WS_PORT` on the pipeline process.
- **HTTP API (charts, health):** `http://<host>:HEALTH_PORT`
  - Default `HEALTH_PORT` = `5567`.
  - Endpoints:
    - `GET /config` – TradingView UDF config.
    - `GET /symbols?symbol=<MINT>` – symbol metadata.
    - `GET /history?symbol=<MINT>&resolution=<RES>&from=<TS>&to=<TS>` – OHLCV candles.
    - `GET /health` and `GET /` – simple health (200 = OK).

All HTTP responses are JSON. WebSocket frames are MessagePack `ServerMessage` values.

---

## WebSocket contract

### Connect

1. Open `ws://<host>:WS_PORT`.
2. Server immediately sends:
   1. `connected`
   2. `snapshot_migrated`
   3. `snapshot_soon_migrating`
   4. `snapshot_launches`
   5. `snapshot_trades`
   6. `snapshot_tickers`
3. After snapshots, live messages stream: `token_launch`, `token_security_update`, `trade`, `migration`, `ohclv`.

Frames are MessagePack; you should:

- Decode as a tagged enum:
  - `type: "connected" | "token_launch" | "token_security_update" | "trade" | "migration" | "ohclv" | "snapshot_*"`
  - `data: { ... }` is the payload.

See `docs/messages.md` for exact shapes; below is the minimal subset you usually care about.

### Topics and subscriptions

The server supports **topic‑based filtering** via JSON text frames:

- Subscribe:

```json
{ "method": "subscribe", "params": ["launches", "<MINT1>", "<MINT2>"] }
```

- Unsubscribe:

```json
{ "method": "unsubscribe", "params": ["<MINT1>"] }
```

Behaviour:

- If **no topics** are subscribed, the client is in **firehose mode** and receives everything.
- If topics are present:
  - `topic = "launches"` → global launch feed.
  - `topic = "<mint>"` → per‑mint channel:
    - `trade` for that mint
    - `migration` for that mint
    - `token_security_update` for that mint
    - `ohclv` candles for that mint
  - `topic = "tickers"` → periodic **snapshot_tickers** with **accurate volume** (recalculated from trade history with a delay; see “Volume and market cap” below).

The server does not send acks for subscribe/unsubscribe; just start / stop getting those topics.

---

## Message types (what to render)

### 1. `connected`

```json
{ "type": "connected", "data": { "client_id": 123 } }
```

Use only for logging / debugging.

### 2. `snapshot_*` on connect

- `snapshot_migrated`: list of `{ mint, pool, signature, slot }`.
- `snapshot_soon_migrating`: list of `{ mint, progress_pct, virtual_token_reserves }`.
- `snapshot_launches`: array of `TokenLaunchPayload` (see below).
- `snapshot_trades`: array of `Trade`.
- `snapshot_tickers`: array of `TokenTicker`:
  - `mint`, `price`, `market_cap`, `liquidity_sol`
  - `volume_5m`, `volume_1h`, `volume_6h`, `volume_24h`
  - `tx_count_5m`, `tx_count_1h`, `tx_count_6h`, `tx_count_24h`
  - `price_change_5m`, `price_change_1h`, `price_change_6h`, `price_change_24h`
  - `total_fees_sol`
  - `last_slot`
  - `migrated_to_pool?`

**Recommended UI flow on connect:**

- Replace your in‑memory state with these snapshots (don’t merge heuristically).
- Use `snapshot_tickers` to populate the main list (no blank screen).
- Use `snapshot_launches` / `snapshot_trades` to backfill feeds.

### 3. `token_launch`

Payload (`TokenLaunchPayload`). Same shape is used for **live** `token_launch` and for **snapshot_launches** on connect.

- **Identity:** `signature`, `slot`, `mint`
- **Display:** `name`, `symbol`, `uri`, `creator`
- **Creator vs user:** `creator` (token creator); `user` (wallet that performed the create; may equal creator). Use `user` for “who launched” when you need the signer.
- **Accounts:** `pump_accounts` (bonding curve PDAs etc.)
- **Constants:** `pump_constants` (program IDs, token_decimals)
- **Curve:** `initial_bonding_curve_state?` (virtual/real reserves at create)
- **Security:** `security?` (usually `null` on first message; filled later via `token_security_update`)
- **Price / liquidity:** `price`, `market_cap`, `liquidity_sol` (derived from curve state; may be 0)
- **Create-event fields (from Pump.fun CreateEvent):**
  - `txn_timestamp` (u64): create time in **milliseconds** (Unix). Use for “launched at” and sorting.
  - `is_token_2022` (bool): `true` if the token was created with the Token-2022 program.
  - `token_program?` (string): token program pubkey (e.g. `TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA` vs Token-2022). Omitted when not decoded.
  - `token_total_supply?` (u64): total supply in **base units** (raw, same as SPL mint supply). Omitted when not decoded.
  - `is_mayhem_mode?` (bool): Pump.fun “mayhem” mode flag. Omitted when not decoded.

Use this to:

- Add a row to the “new tokens” list.
- Show “launched at” using `txn_timestamp` and “Token-2022” badge using `is_token_2022` (or `token_program`).
- Pre‑fill transaction‑builder forms (PDAs and program IDs).
- Display supply or “mayhem” indicator when present.

### 4. `token_security_update`

Payload:

- `mint`
- `security` (`MintSecurity`):
  - `mint`
  - `mint_authority?`
  - `freeze_authority?`
- `supply`, `decimals`, `is_initialized`
- `mint_authority_renounced` (bool)
- `freeze_authority_renounced` (bool)

**Supply and “top N hold %”:**
- `security.supply` is in **base units** (raw token amount, same as the SPL mint account).
- To show “top 10 hold X%”: get the top 10 token account balances in **base units**, then:
  - `top10_pct = (sum_of_balances_base / security.supply) * 100`
  - Clamp the result to `0`–`100`. If you see >100%, the cause is usually:
    - Using **supply in human form** (supply / 10^decimals) as the denominator while balances are in base units, or
    - Using the **inverted ratio** (supply / sum) instead of (sum / supply).

Semantics:

- Sent **once per mint** when Rug Check successfully reads the on‑chain SPL mint.
- Delivered on topic = `mint`.
- `token_launch.security` may still be `null` when this arrives; treat this as the source of truth for security in the live session.

**Recommended UX:**

- Show obvious “mint/freeze authority still present” vs “fully renounced”.
- You **do not** need to call RPC for this; it is done once in the pipeline.

### 5. `trade`

Key fields:

- `mint`, `trader`, `direction` (`"buy"` / `"sell"`)
- `token_amount`, `sol_amount`, `token_decimals`
- `price`, `market_cap`, `liquidity_sol`
- `volume_5m/1h/6h/24h`, `tx_count_*`
- `price_change_*`
- `total_fees_sol`
- `is_bonding_curve`, `pool?`
- `is_dev_buy` (bool): `true` when `trader` is the token creator. Use this to **show market cap even when only the dev has bought** (no other trades yet); otherwise the UI might show mcap as nil.

**Volume semantics:** All volume fields are in **SOL** (not lamports). They are rolling windows from **on-chain trade time** (5m, 1h, 6h, 24h). The pipeline deduplicates by transaction (same tx is never counted twice). Use `volume_sol` or `volume_24h` for “total volume” display; both are the 24h window.

**Volume and market cap:**
- **Market cap** is sent with the launch and with every trade **without awaiting** any slow work (security fetch, DB, Redis run in the background). So mcap appears as soon as we have it and matches Pump.fun.
- **Volume** is computed **accurately** by recalculating from trade history (not incremental), with a configurable delay (default 10s via `VOLUME_DISPLAY_DELAY_SECS`) so the number aligns with terminal-style updates. You get volume in two ways:
  1. **On each trade:** the `trade` message includes delayed volume fields (`volume_sol`, `volume_5m`, etc.) so the UI can update on every trade (value excludes the last N seconds).
  2. **Periodic tickers:** subscribe to topic `"tickers"` to receive `snapshot_tickers` every N seconds with the same accurate delayed volume for all tokens. Prefer this for a single source of truth for volume if you want smooth, batched updates.

Use this to:

- Stream the trade feed.
- Update per‑token cards/rows in near‑real‑time (price, volume, tx count).

### 6. `migration`

Key fields:

- `mint`, `user`
- `bonding_curve`, `pool`

Use this to:

- Flip routing from bonding curve → AMM for a mint (`pool` is the AMM address).

### 7. `ohclv`

Finalized 1‑minute candle:

- `mint`
- `open`, `high`, `low`, `close`
- `volume` (SOL)
- `start_time` (Unix seconds)
- `duration` (always 60)

Use this to:

- Incrementally update price charts without backfilling from HTTP unless you need long history.

---

## HTTP API (TradingView‑style)

Base URL: `http://<host>:HEALTH_PORT`

### GET /config

TradingView UDF config. Example:

```json
{
  "supported_resolutions": ["1", "5", "60", "D"],
  "supports_group_request": false,
  "supports_marks": false,
  "supports_search": true,
  "supports_timescale_marks": false
}
```

### GET /symbols?symbol=<MINT>

Basic metadata for a symbol. Example fields:

- `name`, `full_name`, `description`
- `type_` (`"crypto"`)
- `exchange`, `listed_exchange`
- `timezone` (`"Etc/UTC"`)
- `format` (`"price"`)
- `pricescale` (1e9 for 9‑decimals)
- `supported_resolutions`
- `volume_precision`
- `data_status` (`"streaming"`)

### GET /history

Query:

- `symbol=<MINT>`
- `resolution=1|5|15|30|60|1H|240|4H|D|1D`
- `from=<unix_seconds>`
- `to=<unix_seconds>`

Response shape (TradingView UDF):

- `s`: `"ok"` | `"no_data"` | `"error"`
- `t`: `[u64]` times (seconds)
- `o/h/l/c/v`: `[f64]` arrays (open/high/low/close/volume)
- `nextTime?`: optional `u64`

Backed by ClickHouse OHLCV; frontends can use this for initial chart history and then switch to streaming `ohclv` over WebSocket.

### GET /health and GET /

- Return `200 OK` and `"OK"` body while the process is healthy.
- Use as a cheap liveness/readiness probe.

---

## Minimal integration checklist

- **WebSocket**
  - [ ] Connect to `ws://host:WS_PORT`.
  - [ ] Decode MessagePack `ServerMessage` frames.
  - [ ] On connect, replace state from `snapshot_*` messages.
  - [ ] Handle live `token_launch`, `token_security_update`, `trade`, `migration`, `ohclv`.
  - [ ] Use `token_launch` / `snapshot_launches` fields: `user`, `txn_timestamp`, `is_token_2022`, `token_total_supply?`, `token_program?`, `is_mayhem_mode?` where needed (see §3).
  - [ ] Use subscribe/unsubscribe messages to limit per‑mint traffic.

- **Charts**
  - [ ] Use `/history` for backfill.
  - [ ] Apply streaming `ohclv` on top for live candles.

- **Security**
  - [ ] Show Rug Check state from `token_security_update.security`.
  - [ ] Fall back to `TokenLaunchPayload.security` from snapshots for reconnects.

With this, you can build a full token list + detail view + charts + Rug Check without touching RPC directly.

