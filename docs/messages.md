# messages.rs — WebSocket wire format

**Source:** `src/messages.rs`

Types sent over the WebSocket: ServerMessage and its payloads. All frames are MessagePack-encoded. This doc describes when each message is sent and every payload field. Event types are in [events.md](events.md); PDA/account semantics in [pumpfun_accounts.md](pumpfun_accounts.md).

---

## Encoding

- **Format:** MessagePack (binary). Serialization: `rmp_serde::to_vec(&ServerMessage::...)`.
- **Shape:** Serde tag `type` (variant name) and content `data` (payload). Variants are snake_case: `connected`, `token_launch`, `trade`, `migration`.
- **Per frame:** One ServerMessage per WebSocket binary frame. No batching.

---

## ServerMessage enum

- **Connected(ConnectedPayload)** — sent once per connection right after handshake.
- **TokenLaunch(TokenLaunchPayload)** — one per Pump.fun Create observed (live stream).
- **Trade(Trade)** — one per Buy/Sell (bonding curve or AMM). Trade is the same struct as in events (live stream).
- **Migration(Migration)** — one per Migrate observed (live stream).
- **SnapshotMigrated(Vec<MigratedEntry>)** — sent once on connect. Full list of migrated tokens.
- **SnapshotSoonMigrating(Vec<SoonMigratingEntry>)** — sent once on connect. Tokens nearing migration.
- **SnapshotLaunches(Vec<TokenLaunchPayload>)** — sent once on connect. Last 50 token launches (same shape as TokenLaunch).
- **SnapshotTrades(Vec<Trade>)** — sent once on connect. Trades for the mints in the last 50 launches (same shape as Trade).
- **SnapshotTickers(Vec<TokenState>)** — sent once on connect. Current state (price, volume, market cap) for all active tokens. This is the **primary source of truth** for initial state.

Snapshots are sent from in-memory state (hydrated from Redis on startup). Client must handle all variants. Snapshot variants use the same payload types as live messages.

---

## ConnectedPayload

| Field | Type | Meaning |
|-------|------|--------|
| client_id | u64 | Unique id for this connection (monotonic). Logging/correlation only. |

Sent by ws_server in handle_connection immediately after accept. See [ws_server.md](ws_server.md).

---

## TokenLaunchPayload

Same information as events::TokenLaunch; handler maps field-by-field. Used so the wire type can evolve independently if needed.

| Field | Type | Meaning |
|-------|------|--------|
| signature | String | Transaction signature (base58). |
| slot | u64 | Slot. |
| mint | String | Token mint (base58). |
| name, symbol, uri | String | From Create instruction. |
| creator | String | Creator pubkey (base58). |
| pump_accounts | PumpFunBondingCurveAccounts | bonding_curve, global, fee_recipient, etc. **metadata** is guaranteed to be the valid Metaplex PDA. |
| token_decimals | u8 | 6. |
| initial_bonding_curve_state | Option<BondingCurveState> | From same-tx trade when available. |
| pump_constants | PumpProtocolConstants | program_ids and token_decimals. |

Clients use this to display the new token and build Create/Buy/Sell without extra RPC when possible. fee_recipient may be null; then fetch Global account at pump_accounts.global or derive per [pumpfun_accounts.md](pumpfun_accounts.md).

---

## Trade (wire)

Same struct as events::Trade. Fields: signature, slot, mint, trader, direction (buy/sell), token_amount, sol_amount, virtual_* and real_* reserves, price, market_cap, liquidity_sol, fee, is_create, **pool** (Always populated: BondingCurve address for pump.fun, Pool address for AMM), suggested_compute_unit_limit, token_decimals, txn_timestamp, received_at, sent_at, volume_sol, tx_count. See [events.md](events.md) for semantics.

---

## Migration (wire)

Same struct as events::Migration. Fields: signature, slot, mint, user, bonding_curve, pool, token_amount, sol_amount, fee, txn_timestamp, received_at, sent_at.

---

## Snapshot payload types (on connect)

**MigratedEntry** (storage): `mint`, `pool`, `signature`, `slot` (all String/u64).

**SoonMigratingEntry** (storage): `mint`, `progress_pct` (f64), `virtual_token_reserves` (u64).

**SnapshotLaunches** and **SnapshotTrades** use the same payload types as live `TokenLaunch` and `Trade`; treat them the same in the UI (e.g. push into the same lists).

**TokenState** (SnapshotTickers): `mint`, `price`, `market_cap`, `liquidity_sol`, `volume_sol`, `tx_count`, `last_slot`.

---

## Connection order (what a new client receives)

1. **connected** — Once, right after WebSocket accept.
2. **snapshot_migrated** — Once. Replace or init migrated list.
3. **snapshot_soon_migrating** — Once. Replace or init soon-migrating list.
4. **snapshot_launches** — Once. Last 50 launches; replace or init launch feed.
5. **snapshot_trades** — Once. Trades for those mints; replace or init trade feed.
6. **snapshot_tickers** — Once. Current state of all active tokens. **Use this to initially populate the UI to prevent blank screens.**
7. **token_launch** / **trade** / **migration** — Live stream from then on. Append to the same lists.

Snapshots are hydrated from Redis on startup. Redis persistence ensures consistent state (volume, price) across server restarts.

---

## When each is sent (live)

- **token_launch** — For each Pump.fun Create the pipeline parses; order is stream order.
- **trade** — For each Buy/Sell (Pump.fun or Pump AMM); order is stream order.
- **migration** — For each Migrate; order is stream order.

No acks or client-to-server message types in this codebase; server only pushes.
