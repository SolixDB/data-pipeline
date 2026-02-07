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
- **TokenLaunch(TokenLaunchPayload)** — one per Pump.fun Create observed.
- **Trade(Trade)** — one per Buy/Sell (bonding curve or AMM). Trade is the same struct as in events.
- **Migration(Migration)** — one per Migrate observed.

No other variants. Client receives only these.

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
| pump_accounts | PumpFunBondingCurveAccounts | bonding_curve, global, fee_recipient, metadata, etc. See [events.md](events.md). |
| token_decimals | u8 | 6. |
| initial_bonding_curve_state | Option<BondingCurveState> | From same-tx trade when available. |
| pump_constants | PumpProtocolConstants | program_ids and token_decimals. |

Clients use this to display the new token and build Create/Buy/Sell without extra RPC when possible. fee_recipient may be null; then fetch Global account at pump_accounts.global or derive per [pumpfun_accounts.md](pumpfun_accounts.md).

---

## Trade (wire)

Same struct as events::Trade. Fields: signature, slot, mint, trader, direction (buy/sell), token_amount, sol_amount, virtual_* and real_* reserves, price, market_cap, liquidity_sol, fee, is_create, pool (Some for AMM), suggested_compute_unit_limit, token_decimals, txn_timestamp, received_at, sent_at. See [events.md](events.md) for semantics.

---

## Migration (wire)

Same struct as events::Migration. Fields: signature, slot, mint, user, bonding_curve, pool, token_amount, sol_amount, fee, txn_timestamp, received_at, sent_at.

---

## When each is sent

1. **connected** — Once per client, right after WebSocket accept, before any other message.
2. **token_launch** — For each Pump.fun Create the pipeline parses; order is stream order.
3. **trade** — For each Buy/Sell (Pump.fun or Pump AMM); order is stream order.
4. **migration** — For each Migrate; order is stream order.

No acks or client-to-server message types in this codebase; server only pushes.
