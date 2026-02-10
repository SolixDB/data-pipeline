# messages.rs — WebSocket wire format

**Source:** `src/messages.rs`

Types sent over the WebSocket are `ServerMessage` values encoded as MessagePack. This doc is the contract for WebSocket consumers.

---

## Encoding

- **Format:** MessagePack (binary), via `rmp_serde::to_vec_named(&ServerMessage::...)`.
- **Shape:** Serde tag `type` (snake_case variant) and content `data` (payload).
- **Per frame:** Exactly one `ServerMessage` per binary frame.

---

## ServerMessage variants

- `connected` → `Connected(ConnectedPayload)`
- `token_launch` → `TokenLaunch(TokenLaunchPayload)`
- `token_security_update` → `TokenSecurityUpdate(TokenSecurityUpdate)`
- `trade` → `Trade(crate::events::Trade)`
- `migration` → `Migration(crate::events::Migration)`
- `ohclv` → `Ohclv(crate::events::Ohclv)`
- `snapshot_migrated` → `SnapshotMigrated(Vec<crate::storage::MigratedEntry>)`
- `snapshot_soon_migrating` → `SnapshotSoonMigrating(Vec<crate::storage::SoonMigratingEntry>)`
- `snapshot_launches` → `SnapshotLaunches(Vec<TokenLaunchPayload>)`
- `snapshot_trades` → `SnapshotTrades(Vec<crate::events::Trade>)`
- `snapshot_tickers` → `SnapshotTickers(Vec<TokenTicker>)`

Snapshots are sent once on connect; live variants stream until the connection closes.

---

## ConnectedPayload

| Field | Type | Meaning |
|-------|------|--------|
| client_id | u64 | Unique id for this connection (monotonic). Logging/correlation only. |

Sent by ws_server in handle_connection immediately after accept. See [ws_server.md](ws_server.md).

---

## TokenLaunchPayload

Same information as `events::TokenLaunch`, with an optional embedded Rug Check result.

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
| security | Option<MintSecurity> | Rug Check mint security; usually null on the first `token_launch`. |

Notes:

- On the initial `token_launch` the `security` field is typically `null`; Rug Check runs in a background task.
- On later snapshots (`snapshot_launches`) and for stored launches, `security` is populated when Rug Check succeeded.

See [events.md](events.md) for `MintSecurity`.

---

## TokenSecurityUpdate

Follow-up message when Rug Check completes for a mint.

| Field | Type | Meaning |
|-------|------|--------|
| mint | String | Mint this update applies to. |
| security | MintSecurity | Parsed SPL mint security. |

Behaviour:

- Emitted once per mint when background Rug Check succeeds.
- Broadcast on topic = mint, so subscribers to that mint see the update without reconnecting.

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

**SnapshotLaunches** and **SnapshotTrades** use the same payload types as live `token_launch` and `trade`.

**TokenTicker** (SnapshotTickers) flattens `storage::TokenState` for the wire:

- `mint`
- `price`, `market_cap`, `liquidity_sol`
- `volume_5m`, `volume_1h`, `volume_6h`, `volume_24h`
- `tx_count_5m`, `tx_count_1h`, `tx_count_6h`, `tx_count_24h`
- `price_change_5m`, `price_change_1h`, `price_change_6h`, `price_change_24h`
- `total_fees_sol`
- `last_slot`
- `migrated_to_pool: Option<String>`

---

## Connection order (what a new client receives)

1. **connected** — Once, right after WebSocket accept.
2. **snapshot_migrated** — Once. Replace or init migrated list.
3. **snapshot_soon_migrating** — Once. Replace or init soon-migrating list.
4. **snapshot_launches** — Once. Last 50 launches; replace or init launch feed.
5. **snapshot_trades** — Once. Trades for those mints; replace or init trade feed.
6. **snapshot_tickers** — Once. Current state of all active tokens. **Use this to initially populate the UI to prevent blank screens.**
7. Live stream: `token_launch`, `token_security_update`, `trade`, `migration`, `ohclv`.

Snapshots are hydrated from Redis on startup. Redis persistence ensures consistent state (volume, price) across server restarts.

---

## When each is sent (live)

- **token_launch** — For each Pump.fun Create the pipeline parses; order is stream order.
- **trade** — For each Buy/Sell (Pump.fun or Pump AMM); order is stream order.
- **migration** — For each Migrate; order is stream order.

No acks or client-to-server message types in this codebase; server only pushes.
