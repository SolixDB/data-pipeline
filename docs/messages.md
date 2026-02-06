# WebSocket message format

**Source:** `src/messages.rs`

This document describes the wire format of messages between the data-pipeline WebSocket server and its clients. It is the reference for anyone implementing a consumer of the stream (frontend, backend API, or terminal).

---

## Overview

- **Direction:** Server pushes messages to clients; client messages are reserved for future use.
- **Encoding:** Every message is a **MessagePack**-encoded binary frame. The logical structure is a tagged enum: a **type** (discriminant) and **data** (payload).
- **When data is sent:** After a client connects, the server sends one **connected** message, then streams **token_launch** and **trade** messages as the pipeline processes transactions from Geyser.

Clients can build Pump.fun Create/Buy/Sell transactions using the accounts and curve state in **token_launch** and **trade** without extra RPC; see [pumpfun_accounts.md](pumpfun_accounts.md) for PDA derivation and phase (bonding curve vs AMM).

---

## Message structure

Each WebSocket frame (binary) is the MessagePack encoding of a **ServerMessage** value.

- **Tag:** The enum variant is serialized with `type` (string) and `data` (payload). In MessagePack this appears as a map with keys `"type"` and `"data"`.
- **Naming:** Variant names use **snake_case** (e.g. `token_launch`, `trade`, `connected`).

Example logical shape (for illustration; actual wire format is MessagePack):

```text
{ "type": "token_launch", "data": { "signature": "...", "mint": "...", ... } }
```

---

## Server message types

### connected

**When sent:** Exactly once per connection, immediately after the WebSocket handshake.

**Purpose:** Tells the client that the connection is accepted and assigns a unique **client_id** for this session (logging, debugging, future per-client features).

**Payload: ConnectedPayload**

| Field | Type | Description |
|-------|------|--------------|
| `client_id` | u64 | Unique identifier for this connection. No semantic meaning for the stream; use for logs or correlation. |

---

### token_launch

**When sent:** For each Pump.fun **Create** instruction the pipeline observes. One message per new token creation.

**Purpose:** Supplies everything needed to display the new token and to build a **Create**, **Buy**, or **Sell** transaction on the bonding curve without extra RPC: PDAs, fee recipient (when available), token decimals, initial curve state (when available), and protocol constants (program IDs).

**Payload: TokenLaunchPayload**

| Field | Type | Description |
|-------|------|--------------|
| `signature` | string | Transaction signature (base58) of the Create transaction. |
| `slot` | u64 | Slot in which the transaction was processed. |
| `mint` | string | Token mint address (base58). Primary key for this token. |
| `name` | string | Token name (from Create instruction). |
| `symbol` | string | Token symbol (from Create instruction). |
| `uri` | string | Metadata URI (from Create instruction). |
| `creator` | string | Creator pubkey (base58). Used to derive creator_vault and for AMM pool lookup after migration. |
| `pump_accounts` | object | See below. Bonding-curve accounts and PDAs for this mint. |
| `token_decimals` | u8 | Always **6** for Pump.fun tokens. Use for human-readable amounts and instruction amounts. |
| `initial_bonding_curve_state` | object or null | When the Create tx also contains a Buy (e.g. initial buy), the first trade in that tx is used to fill this. See BondingCurveState below. Use for price and slippage on the first follow-up buy. |
| `pump_constants` | object | Program IDs and decimals so the client does not need to hardcode them. See PumpProtocolConstants below. |

**pump_accounts (PumpFunBondingCurveAccounts)**

| Field | Type | Description |
|-------|------|--------------|
| `bonding_curve` | string or null | Bonding curve PDA for this mint. Filled from Create instruction. Required for Buy/Sell on the curve. |
| `global` | string or null | Global config PDA (Pump.fun). Filled from Create instruction. Fetch account data here to get fee_recipient if not provided. |
| `fee_recipient` | string or null | Filled when the same transaction includes a Buy instruction (e.g. initial buy). Otherwise the client must fetch the Global account at `global` and read `feeRecipient`. |
| `metadata` | string or null | Metaplex metadata PDA for this mint. Filled from Create instruction. Used for Create flow. |
| `creator_vault` | string or null | Not filled by the pipeline. Derive with seeds `["creator-vault", creator]` (Pump.fun program). See [pumpfun_accounts.md](pumpfun_accounts.md). |
| `fee_config` | string or null | Not filled by the pipeline. Derive with seeds `["fee_config", pump_fun_program_id]` (Pump.fun fee program). See [pumpfun_accounts.md](pumpfun_accounts.md). |

**initial_bonding_curve_state (BondingCurveState), when present**

| Field | Type | Description |
|-------|------|--------------|
| `virtual_sol_reserves` | u64 | Virtual SOL reserves (lamports) after the first trade in the same tx. |
| `virtual_token_reserves` | u64 | Virtual token reserves (raw units) after the first trade. |
| `real_sol_reserves` | u64 | Real SOL reserves (lamports). |
| `real_token_reserves` | u64 | Real token reserves (raw units). |

Use these to compute price and slippage for the next buy/sell without fetching the bonding curve account.

**pump_constants (PumpProtocolConstants)**

| Field | Type | Description |
|-------|------|--------------|
| `token_decimals` | u8 | 6. |
| `program_ids` | object | See below. |

**program_ids (PumpProgramIds)**

| Field | Description |
|-------|-------------|
| `pump_fun` | Pump.fun program ID. |
| `pump_fun_fee_program` | Pump.fun fee program ID. |
| `pump_amm` | Pump AMM program ID (post-migration). |
| `associated_token_program` | SPL Associated Token Program. |
| `token_program` | SPL Token program. |
| `metaplex_metadata` | Metaplex Token Metadata program. |
| `system_program` | System program. |
| `wsol_mint` | wSOL mint (default quote for AMM). |

**Using token_launch:** Store by `mint` for the lifetime of the token. Use `pump_accounts` and `pump_constants` to build Create (if you are not the creator), Buy, or Sell on the **bonding curve**. After migration, the same mint is traded on the AMM; use [pumpfun_accounts.md](pumpfun_accounts.md) to resolve AMM accounts (pool, global_config, etc.) when the bonding curve no longer exists.

---

### trade

**When sent:** For each buy or sell on the Pump.fun bonding curve (or AMM, when the pipeline parses it). One message per trade event decoded from program logs.

**Purpose:** Real-time trade feed with amounts, reserves, price, and fee. Includes hints so the client can build a follow-up buy or sell (compute units, decimals) and already has the latest curve state for slippage.

**Payload: Trade**

| Field | Type | Description |
|-------|------|--------------|
| `signature` | string | Transaction signature (base58). |
| `slot` | u64 | Slot of the transaction. |
| `mint` | string | Token mint. |
| `trader` | string | Trader pubkey (base58). |
| `direction` | string | `"buy"` or `"sell"`. |
| `token_amount` | u64 | Token amount in raw units (use token_decimals for display). |
| `sol_amount` | u64 | SOL amount in lamports. |
| `virtual_sol_reserves` | u64 | Virtual SOL reserves after this trade. |
| `virtual_token_reserves` | u64 | Virtual token reserves after this trade. |
| `real_sol_reserves` | u64 | Real SOL reserves after this trade. |
| `real_token_reserves` | u64 | Real token reserves after this trade. |
| `price` | f64 | Derived price (e.g. SOL per token in listing units). |
| `market_cap` | f64 | Derived market cap. |
| `liquidity_sol` | f64 | Real SOL reserves in SOL (for display). |
| `fee` | u64 | Fee paid (lamports). |
| `is_create` | bool | True if this trade is part of the same transaction as the Create (e.g. initial buy). |
| `suggested_compute_unit_limit` | u32 | 300_000. Use as compute unit limit for a follow-up buy/sell instruction. |
| `token_decimals` | u8 | 6 for Pump.fun. |
| `txn_timestamp` | u64 | On-chain transaction timestamp (milliseconds since epoch). |
| `received_at` | u64 | Server received from gRPC (microseconds since epoch). |
| `sent_at` | u64 | Server sent to WebSocket (microseconds since epoch). |

**Using trade:** To build a follow-up buy or sell, combine with **token_launch** data for the same `mint` (for bonding_curve, fee_recipient, creator_vault, fee_config). If you never received a token_launch for this mint (e.g. token created before subscription), derive PDAs from `mint` and fetch Global for fee_recipient; see [pumpfun_accounts.md](pumpfun_accounts.md).

---

### error

**When sent:** Reserved for future use (e.g. malformed client message, rate limit).

**Payload: ErrorPayload**

| Field | Type | Description |
|-------|------|--------------|
| `code` | string | Error code. |
| `message` | string | Human-readable message. |

---

## Client messages (reserved)

The server does not yet parse or act on client message types. WebSocket protocol-level **Ping** is answered with **Pong**; **Close** is handled. Any other frame is ignored.

Planned client message types (for future channel subscription or control):

| Type | Payload | Purpose |
|------|---------|---------|
| `ping` | `{ "timestamp": u64 }` | Keepalive or latency check. |
| `subscribe` | `{ "channel": string }` | Subscribe to a channel (e.g. by symbol or stream). |
| `unsubscribe` | `{ "channel": string }` | Unsubscribe from a channel. |

When subscription is implemented, incoming binary frames will be decoded as MessagePack **ClientMessage** and dispatched.

---

## Serialization

- **Format:** MessagePack (binary), via `rmp_serde` in Rust.
- **Server usage:** In `ws_server.rs`, the server sends **connected** as MessagePack in the welcome message. All other server messages (token_launch, trade, error) are serialized with `rmp_serde::to_vec` and broadcast as WebSocket binary frames.
- **Decoding on the client:** Deserialize the binary frame as a map with key `"type"` (string) and key `"data"` (payload). Branch on `type` and decode `data` into the corresponding struct (TokenLaunchPayload, Trade, etc.).

---

## See also

- [pumpfun_accounts.md](pumpfun_accounts.md) — PDAs, program IDs, bonding curve vs AMM, and how to build transactions before and after migration.
- [ws_server.md](ws_server.md) — Where the server sends the welcome message and how broadcast works.
