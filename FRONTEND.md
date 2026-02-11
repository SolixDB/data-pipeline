# Frontend integration guide

**For frontend agents (AI/automation):** This document is the **strict contract** for the client that connects to the data-pipeline. When implementing or changing the frontend (WebSocket client, HTTP API usage, message handling, ACK, subscriptions, backfill, or any behavior described here):

1. **Follow this document strictly.** Treat it as the source of truth.
2. **Change whatever is implemented** in the frontend so that it matches what is specified here (endpoints, message format, envelope with `seq`, ACK format and frequency, subscription semantics, error handling, HTTP backfill, checklist, etc.).
3. **Do not deviate** from the contract (e.g. do not skip ACK, use a different envelope shape, or assume fields or endpoints not described here) unless the user explicitly asks for a change and the contract is updated.

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
    - `GET /token?mint=<MINT>` – token metadata (name, symbol, uri, creator). Use when WS missed `token_launch`.
    - `GET /tokens?mints=<MINT1>,<MINT2>,...` – batch token metadata (max 100 mints). Use to backfill many mints after reconnect.
    - `GET /history?symbol=<MINT>&resolution=<RES>&from=<TS>&to=<TS>` – OHLCV candles.
    - `GET /health` and `GET /` – simple health (200 = OK).

All HTTP responses are JSON. WebSocket frames are MessagePack `ServerMessage` values.

---

## WebSocket contract

### Connect

1. Open `ws://<host>:WS_PORT`.
2. Server immediately sends:
   1. `connected`
   2. Optionally (if the server has `WS_SEND_SNAPSHOT_ON_CONNECT` enabled, which is the default): `snapshot_migrated`, `snapshot_soon_migrating`, `snapshot_launches`, `snapshot_trades`, `snapshot_tickers`. If snapshots are disabled, you only get `connected` and then the live stream.
3. After that, live messages stream: `token_launch`, `trade`, `migration`, `ohclv`.

Your frontend should handle both cases: with snapshots (populate launch cache from `snapshot_launches`) and without (rely on subscribing to `"launches"` and on HTTP backfill for any mint you see from trades/tickers).

**Message order on connect:** The server sends `connected` first, then (if snapshots are enabled) zero or more `snapshot_*` messages, then the live stream. If the in-memory snapshot is empty (e.g. right after a restart), the server may send snapshot_* from **Redis** when `REDIS_URL` is set; in that case `snapshot_tickers` may be missing or empty—handle missing or empty snapshot messages gracefully.

**Recommended first subscription:** After connect, subscribe to `["launches", "tickers"]` so you receive every new token and periodic volume updates without firehose mode; add specific mint topics when you need per-mint trades.

**Selective repeat (ACK):** Every binary frame is a **ServerMessageEnvelope**: `{ type, data, seq }`. `seq` is monotonic. The server keeps a replay buffer and tracks per-socket the highest `seq` you have ACKed. **The frontend must send ACKs** so the server can retransmit only the missing block to your socket when you fall behind.

**Binary frame format:** Every binary frame is MessagePack with top-level keys: `type` (string), `data` (object), `seq` (number, u64). Decode once; then dispatch on `type`, use `data` as the payload; track the maximum `seq` for ACK.

### Sending ACK (required for selective repeat)

**Yes, the FE must send ACKs.** Without them, selective repeat does not work as intended.

- **Format (JSON text frame):** `{ "method": "ack", "params": <seq> }` — `params` must be a **number** (the seq value).
- **Cumulative:** Sending ack(123) means "I have received every message with seq ≤ 123". Always send the **highest** seq you have received so far.
- **When to send:** At least every **100 ms** or every **20–50 messages**. Do not wait forever.
- **Implementation:** (1) On each binary frame: parse `{ type, data, seq }`; set `lastReceivedSeq = max(lastReceivedSeq, seq)`; dispatch `data` by `type`. (2) On a 100 ms timer (or every 20 messages): if `lastReceivedSeq` changed, send one text frame: `{"method":"ack","params":lastReceivedSeq}`. (3) Retransmitted messages arrive as normal binary envelopes (same `type`/`data`/`seq`); apply them the same way, then send ACK with the new highest seq. (4) If you see `seq !== lastReceivedSeq + 1`, you had a gap; the server will send the missing seqs on the same connection; apply them when they arrive.

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

**Client → server (JSON text frames only):** Subscribe: `{ "method": "subscribe", "params": ["launches", "tickers", "<MINT>", ...] }`. Unsubscribe: `{ "method": "unsubscribe", "params": ["<MINT>", ...] }`. **ACK (required):** `{ "method": "ack", "params": <seq> }` with `params` = highest seq received (number); send at least every 100 ms or every 20 messages.

### Subscription semantics: avoiding missed `token_launch` (clients can drop messages)

The backend sends every `token_launch` on both topic `"launches"` and on the mint’s topic. **Clients can still miss a message** (e.g. network blip, reconnect, or they weren’t subscribed yet). To minimise missing metadata:

1. **Subscribe to `"launches"`** as soon as you connect if you need a global feed of new tokens. Then you receive every `token_launch` and can keep your launch cache up to date. If you only subscribe to specific mints or `"tickers"`, you will **not** receive `token_launch` for mints you haven’t subscribed to, so you may see a **trade** for a mint before ever receiving its **token_launch**.
2. **When you first see a mint** (e.g. from a trade or ticker), **subscribe to that mint**. The server will send a **one-off `token_launch`** for that mint from its cache (per-mint backfill on subscribe), so you get metadata without waiting for the next live event. See “Per-mint backfill on subscribe” below.
3. **Always backfill via HTTP** when `launchCache[mint]` is missing: use `GET /token?mint=<MINT>` or `GET /tokens?mints=...` for multiple mints. This covers reconnects, lag, or any case where the WS message was dropped.

### Per-mint backfill on subscribe

When you send `Subscribe` with a **mint** topic (e.g. `["<MINT>"]`) and the server has a cached launch for that mint in `latest_launch_by_mint`, it **immediately sends one `token_launch` message** for that mint. Use this so that as soon as you care about a mint (e.g. you added a row after seeing a trade), subscribing to that mint gives you metadata without a separate HTTP call. Aggregate topics `"launches"` and `"tickers"` do **not** trigger this one-off send; only mint-like topics do.

---

## Supported launches

The pipeline supports **Pump.fun** token launches only (CreateEvent). Metadata (`name`, `symbol`, `uri`) comes from the chain at decode time. PumpAMM and other launchpads are not supported yet.

---

## Token details: single source of truth (critical contract)

**Pump.fun launches** include a CreateEvent in logs, so the pipeline sends `token_launch` with token details (`name`, `symbol`, `uri`) from that CreateEvent as soon as they are decoded.

**Removed:** PumpAMM direct launches and RPC metadata fetch/rebroadcast are no longer supported.

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
- **Fallback when WS is missed:** If you have a mint from `trade` / `migration` / `snapshot_tickers` but `launchCache[mint]` is still missing after a short grace period (e.g. 1–3 seconds), call the HTTP API to backfill metadata (see **HTTP backfill** below). Then upsert into your `launchCache[mint]` (and keep waiting for WS `token_launch` as the authoritative full payload).

---

## Message types (what to render and how to join)

### 1. `connected`

Each binary frame is an envelope: `{ "type": "connected", "data": { ... }, "seq": <u64> }`. Example:

```json
{ "type": "connected", "data": { "client_id": 123, "recommended_topics": ["launches", "tickers"] }, "seq": 1 }
```

Use `client_id` for logging; use `recommended_topics` as a hint for initial subscription. Use `seq` for gap detection and for sending cumulative ACKs (see **Selective repeat (ACK)** above).

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
   - This applies to the initial launch and any later `token_launch` updates (e.g. before a migration). Treat them as authoritative and overwrite your cache.

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

Token details come from the Pump.fun CreateEvent at decode time.

Use this payload as the single source of truth for token metadata (see “Token details: single source of truth” and “Frontend implementation” above).

- **Identity:** `signature`, `slot`, `mint`
- **Display:** `name`, `symbol`, `uri`, `creator`
- **Creator vs user:** `creator` (token creator); `user` (wallet that performed the create; may equal creator). Use `user` for “who launched” when you need the signer.
- **Accounts:** `pump_accounts` (bonding curve PDAs etc.)
- **Constants:** `pump_constants` (program IDs, token_decimals)
- **Curve:** `initial_bonding_curve_state?` (virtual/real reserves at create)
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
- **Market cap** is sent with the launch and with every trade as soon as we have it (matches Pump.fun).
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

### HTTP backfill: GET /token and GET /tokens

Use these when the WebSocket missed a `token_launch` (e.g. after reconnect or lag) so you can fill `launchCache[mint]`.

**GET /token?mint=&lt;MINT&gt;**

- **Response (200):** JSON `{ "name", "symbol", "uri", "creator" }` (latest by received_at from ClickHouse `token_launches`).
- **404:** Mint not found.
- **400:** Missing or empty `mint` query param (plain text body).
- **503:** ClickHouse not configured (plain text: `"ClickHouse not configured"`).

**GET /tokens?mints=&lt;MINT1&gt;,&lt;MINT2&gt;,...**

- **Response (200):** JSON array of `{ "mint", "name", "symbol", "uri", "creator" }` for each mint that exists. Max **100 mints** per request; only mints that exist are returned; order is not guaranteed.
- **400:** Missing/empty `mints` or more than 100 mints (plain text body).
- **503:** ClickHouse not configured (plain text: `"ClickHouse not configured"`).

### GET /health and GET /

- Return `200 OK` and `"OK"` body while the process is healthy.
- Use as a cheap liveness/readiness probe.

---

## Minimal integration checklist

- **WebSocket**
  - [ ] Connect to `ws://host:WS_PORT`.
  - [ ] Decode each binary frame as a **ServerMessageEnvelope** (`type`, `data`, `seq`). Use `data` as the payload; use `seq` for gap detection and for ACKs.
  - [ ] Send **cumulative ACK** so the server can selectively retransmit: text frame `{ "method": "ack", "params": <seq> }` where `<seq>` is the highest seq received (e.g. every N messages or every 100 ms).
  - [ ] On connect, replace state from `snapshot_*` messages. If `seq` jumps by more than 1, you missed messages; the server will retransmit if you ACK, or reconnect to get a fresh snapshot.
  - [ ] Handle live `token_launch`, `trade`, `migration`, `ohclv`.
  - [ ] Use `token_launch` / `snapshot_launches` fields: `user`, `txn_timestamp`, `is_token_2022`, `token_total_supply?`, `token_program?`, `is_mayhem_mode?` where needed (see §3).
  - [ ] Subscribe to `["launches", "tickers"]` (recommended) then add mint topics as needed; use `recommended_topics` from `connected` as a hint.

- **Charts**
  - [ ] Use `/history` for backfill.
  - [ ] Apply streaming `ohclv` on top for live candles.

With this, you can build a full token list + detail view + charts. Mint security / Rug Check is not provided by the pipeline; implement via your own RPC/API if needed.

