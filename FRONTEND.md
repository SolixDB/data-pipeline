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
3. After snapshots, live messages stream: `token_launch`, `trade`, `migration`, `ohclv`.

Frames are MessagePack; you should:

- Decode as a tagged enum:
  - `type: "connected" | "token_launch" | "trade" | "migration" | "ohclv" | "snapshot_*"`
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
    - `token_launch` for that mint (token details / metadata)
    - `trade` for that mint
    - `migration` for that mint
    - `ohclv` candles for that mint
  - `topic = "tickers"` → periodic **snapshot_tickers** with **accurate volume** (recalculated from trade history with a delay; see “Volume and market cap” below).

The server does not send acks for subscribe/unsubscribe; just start / stop getting those topics.

---

## Token details: single source of truth (critical contract)

**Pump.fun launches** include a CreateEvent in logs, so the pipeline normally sends `token_launch` with token details (`name`, `symbol`, `uri`) from that CreateEvent as soon as they are decoded.

**PumpAMM direct launches** (a token is minted “normally” and then a pool is created via `CreatePool`) may arrive as a `token_launch` with **placeholder metadata** (often `name = mint`, `symbol = ""`, `uri = ""`) so the mint enters the list and AMM trades can be tracked immediately. The pipeline then does an **RPC metadata fetch** and will **rebroadcast** `token_launch` with real `name/symbol/uri` as soon as it can.

- **Token metadata** (name, symbol, uri, creator, user, PDAs, curve state, etc.) comes **only** from:
  - Live `token_launch` messages (topic `"launches"` or per‑mint topic), and
  - `snapshot_launches` on connect.
- **Trades, migrations, tickers** carry `mint`, price, volume, etc., but **do not** carry name/symbol/uri. Do not create or show a “token row” from a trade/migration/ticker alone without merging in launch data.
- **Rule (backend invariant):**
  - The backend **only sends trades/migrations/tickers for mints it has launched** (tracked Pump tokens).
  - The backend **does not guarantee** that a `trade` for a mint is always delivered *after* the very first `token_launch` in every possible reconnect/subscription scenario.
  - Instead, the backend guarantees that **whenever it knows token metadata, it will send `token_launch`** (snapshots on connect, live `token_launch`, and a one‑off per‑mint snapshot when you subscribe to a specific mint topic).
- **Rule (frontend responsibility):** For any mint you display, maintain a **launch cache keyed by mint**:
  - When you receive `token_launch` or an item from `snapshot_launches`, **upsert `launchCache[mint] = payload`** (create or replace). This cache is the only source of truth for `name/symbol/uri/creator/...`.
  - When you receive `trade`, `migration`, or `snapshot_tickers`, **update price/volume/state for that mint** but **never overwrite or ignore the cached launch payload** for that mint.
  - If `launchCache[mint]` is missing, you **must treat the token details as unknown** and:
    - Render a placeholder (e.g. mint shorthand or “Loading…”), and
    - Optionally call `/token?mint=<MINT>` as a backfill (see below).
- **Fallback when WS is missed:** If you have a mint from `trade` / `migration` / `snapshot_tickers` but `launchCache[mint]` is still missing after a short grace period (e.g. 1–3 seconds), call the HTTP API to backfill metadata:
  - `GET /token?mint=<MINT>` → returns `{ name, symbol, uri, creator }` from ClickHouse `token_launches` (latest by received_at).
  - Then upsert into your `launchCache[mint]` (and keep waiting for WS `token_launch` as the authoritative full payload).

---

## Message types (what to render and how to join)

### 1. `connected`

```json
{ "type": "connected", "data": { "client_id": 123 } }
```

Use only for logging / debugging.

### 2. `snapshot_*` on connect (initial state)

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

**Recommended UI flow on connect: strict state reset**

- Replace your in‑memory state with these snapshots (don’t merge heuristically).
- Use `snapshot_tickers` to populate the main list (no blank screen).
- Use `snapshot_launches` / `snapshot_trades` to backfill feeds.

### Frontend implementation: token details and list state

Follow these steps so mints never appear without token details **once the pipeline has sent a launch for them**, while still behaving correctly if a trade is seen before launch due to reconnects.

1. **State**
   - Keep a **launch cache**: `Map<mint, TokenLaunchPayload>` (or equivalent). This is the source of truth for `name`, `symbol`, `uri`, `creator`, `user`, `pump_accounts`, `initial_bonding_curve_state`, etc.
   - Keep separate structures for live price/volume/state (e.g. from trades and tickers) keyed by mint.

2. **On connect (snapshots)**
   - When you receive **`snapshot_launches`**, for each item in the array: **set `launchCache[item.mint] = item`** (replace if already present). Do not skip; do not merge only “new” fields.
   - When you receive **`snapshot_trades`** or **`snapshot_tickers`**, update your trade/ticker state by mint. For the UI, **for each mint you display**, resolve display name/symbol/uri from **`launchCache.get(mint)`**:
     - If present, use it for name/symbol/uri/etc.
     - If missing, show a placeholder (e.g. mint slice or “Loading…”) until a launch for that mint appears or `/token` returns details.

3. **Live `token_launch`**
   - On every **`token_launch`** message: **set `launchCache[data.mint] = data`** (full replace). Then update the list/card for that mint so the row shows `data.name`, `data.symbol`, `data.uri`, etc.
   - This applies to:
     - The initial launch.
     - Any later `token_launch` rebroadcasts (e.g. after RPC metadata fetch or before a migration). Treat them as authoritative and overwrite your cache.

4. **Live `trade` / `migration` / `ohclv`**
   - Update price, volume, and migration state by mint.
   - **Do not** create a new list entry from a trade or migration alone and assume you have full token details.
   - If you add a row for a new mint when you see a trade/migration:
     - Use `launchCache.get(mint)` for token details if present.
     - If `launchCache[mint]` is missing, render the row with:
       - A placeholder name/symbol/uri (e.g. truncated mint), and
       - A loading state while you wait for `token_launch` / `snapshot_launches` or `/token`.

5. **Display rule**
   - For any “new tokens” or “launches” list: **prefer mints that exist in `launchCache`**.
   - If you show mints from tickers/trades first, **render name/symbol/uri strictly from `launchCache`** and never from trade/ticker (they don’t carry it). If the cache is missing, display the mint as a placeholder and backfill details via WS (`token_launch`) or HTTP (`/token`).

### 3. `token_launch`

Payload (`TokenLaunchPayload`). Same shape is used for **live** `token_launch` and for **snapshot_launches** on connect.

- **Pump.fun**: token details come from CreateEvent, so `name/symbol/uri` are expected to be filled immediately.
- **PumpAMM direct CreatePool**: the first `token_launch` may have placeholder `name/symbol/uri`, followed by a later `token_launch` update after the pipeline backfills metadata via RPC.

Use this payload as the single source of truth for token metadata (see “Token details: single source of truth” and “Frontend implementation” above).

- **Identity:** `signature`, `slot`, `mint`
- **Display:** `name`, `symbol`, `uri`, `creator`
- **Creator vs user:** `creator` (token creator); `user` (wallet that performed the create; may equal creator). Use `user` for “who launched” when you need the signer.
- **Accounts:** `pump_accounts` (bonding curve PDAs etc.)
- **Constants:** `pump_constants` (program IDs, token_decimals)
- **Curve:** `initial_bonding_curve_state?` (virtual/real reserves at create)
- **Security:** `security?` (currently always `null`; no live security updates are sent)
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

### 4. `trade`

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
- **Volume** is computed **accurately** by recalculating from trade history (not incremental), with a configurable delay (default 3s via `VOLUME_DISPLAY_DELAY_SECS`; set to `0` for instant volume). You get volume in two ways:
  1. **On each trade:** the `trade` message includes delayed volume fields (`volume_sol`, `volume_5m`, etc.) so the UI can update on every trade (value excludes the last N seconds).
  2. **Periodic tickers:** subscribe to topic `"tickers"` to receive `snapshot_tickers` every N seconds with the same accurate delayed volume for all tokens. Prefer this for a single source of truth for volume if you want smooth, batched updates.

Use this to:

- Stream the trade feed.
- Update per‑token cards/rows in near‑real‑time (price, volume, tx count).

### 5. `migration`

**Order of messages:** For each migration the pipeline sends a **`token_launch`** update first (same shape as §3), with current `price`, `market_cap`, and `liquidity_sol` for that mint, then sends the **`migration`** message. Use the launch update so the token card shows correct price/mcap/liquidity and “migrated” state before you apply the migration.

Key fields:

- `mint`, `user`
- `bonding_curve`, `pool`
- `token_amount`, `sol_amount`, `fee`, `txn_timestamp`
- `bonding_curve_state?`: final bonding curve state at migration (curve drained). When present:
  - `virtual_sol_reserves`, `virtual_token_reserves` are 0
  - `real_sol_reserves`, `real_token_reserves` are from the event (e.g. SOL migrated). Use to show “100% complete” or progress.
- **From pump_amm createPoolEvent** (when migration is from CreatePool logs), optional:
  - `quote_mint?` (e.g. WSOL), `lp_mint?`, `coin_creator?`
  - `is_mayhem_mode?` (bool)
  - `pool_base_amount?`, `pool_quote_amount?`, `initial_liquidity?` (pool state at creation)

Use this to:

- Flip routing from bonding curve → AMM for a mint (`pool` is the AMM address).
- Show “migrated” and avoid showing “bonding curve 100%” by handling the preceding `token_launch` update and then applying the `migration` event.

### 6. `ohclv`

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
  - [ ] Handle live `token_launch`, `trade`, `migration`, `ohclv`.
  - [ ] Use `token_launch` / `snapshot_launches` fields: `user`, `txn_timestamp`, `is_token_2022`, `token_total_supply?`, `token_program?`, `is_mayhem_mode?` where needed (see §3).
  - [ ] Use subscribe/unsubscribe messages to limit per‑mint traffic.

- **Charts**
  - [ ] Use `/history` for backfill.
  - [ ] Apply streaming `ohclv` on top for live candles.

- **Security**
  - [ ] Rug Check / mint security is not provided by the pipeline; omit or implement via your own RPC/API if needed.

With this, you can build a full token list + detail view + charts without touching RPC directly.

