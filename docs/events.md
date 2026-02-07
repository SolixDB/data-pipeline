# events.rs — domain event types

**Source:** `src/events.rs`

All in-memory event types produced by the parsers and sent (after conversion to wire types) to WebSocket clients. This doc lists every type and field. Wire format is in [messages.md](messages.md); PDA/account details in [pumpfun_accounts.md](pumpfun_accounts.md).

---

## BondingCurveState

Reserve state for the bonding curve (virtual and real). Used for price and slippage.

| Field | Type | Meaning |
|-------|------|--------|
| virtual_sol_reserves | u64 | Virtual SOL side. |
| virtual_token_reserves | u64 | Virtual token side. |
| real_sol_reserves | u64 | Real SOL. |
| real_token_reserves | u64 | Real token. |

Copy type; filled from tradeEvent or same-tx trade when enriching a launch.

---

## PumpProtocolConstants

Sent with token_launch so clients don’t hardcode program IDs or decimals.

| Field | Type | Meaning |
|-------|------|--------|
| token_decimals | u8 | Always 6 for Pump.fun. |
| program_ids | PumpProgramIds | All program IDs (see below). |

Default() in code fills program_ids with the known mainnet addresses.

---

## PumpProgramIds

| Field | Example (mainnet) |
|-------|--------------------|
| pump_fun | 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P |
| pump_fun_fee_program | pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ |
| pump_amm | pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA |
| associated_token_program | ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL |
| token_program | TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA |
| metaplex_metadata | metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s |
| system_program | 11111111111111111111111111111111 |
| wsol_mint | So11111111111111111111111111111111111111112 |

---

## PumpFunBondingCurveAccounts

PDAs and accounts for building Create/Buy/Sell on the bonding curve. All Option<String>; base58 addresses.

| Field | Filled by | Meaning |
|-------|-----------|--------|
| bonding_curve | Create instruction accounts[2] | PDA seeds ["bonding-curve", mint]. |
| global | Create accounts[4] | PDA seeds ["global"]. |
| fee_recipient | Same-tx Buy accounts[1], or client reads Global | Not a PDA; from Global account or Buy ix. |
| metadata | Create accounts[6] | Metaplex metadata PDA for mint. |
| creator_vault | Not filled by pipeline | PDA seeds ["creator-vault", creator]. |
| fee_config | Not filled | PDA seeds ["fee_config", pump_fun_program_id] on fee program. |

See [pumpfun_accounts.md](pumpfun_accounts.md) for derivation.

---

## TokenLaunch

One new token (Pump.fun Create). Produced by pump_fun::parse_create; enriched by enrich_token_launches.

| Field | Type | Meaning |
|-------|------|--------|
| signature | String | Transaction signature (base58). |
| slot | u64 | Slot. |
| mint | String | Token mint (base58). |
| name, symbol, uri | String | From Create instruction data. |
| creator | String | Creator pubkey (base58). |
| pump_accounts | PumpFunBondingCurveAccounts | As above. |
| token_decimals | u8 | 6. |
| initial_bonding_curve_state | Option<BondingCurveState> | From same-tx trade if present. |
| pump_constants | PumpProtocolConstants | Program IDs and decimals. |
| txn_timestamp | u64 | Ms since epoch (from logs or received_at). |
| received_at | u64 | Server received (μs). |
| sent_at | u64 | Set by handler before broadcast. |

---

## TradeDirection

Enum: Buy | Sell. Serialized lowercase. Used in Trade and for log strings.

---

## Trade

One buy or sell (bonding curve or AMM). From pump_fun tradeEvent or pump_amm buy/sell events.

| Field | Type | Meaning |
|-------|------|--------|
| signature, slot | String, u64 | Transaction. |
| mint | String | Token. |
| trader | String | User pubkey (base58). |
| direction | TradeDirection | Buy or Sell. |
| token_amount, sol_amount | u64 | Amounts (raw; decimals in token_decimals). |
| virtual_sol_reserves, virtual_token_reserves | u64 | Curve state. |
| real_sol_reserves, real_token_reserves | u64 | Curve state. |
| price | f64 | Derived. |
| market_cap | f64 | Derived. |
| liquidity_sol | f64 | Real SOL reserves in SOL. |
| fee | u64 | Fee (lamports or token). |
| is_create | bool | True for the first buy (create) on the curve. |
| pool | Option<String> | Set for AMM trades (migrated); None for bonding curve. |
| suggested_compute_unit_limit | u32 | 300_000 default. |
| token_decimals | u8 | 6. |
| txn_timestamp, received_at, sent_at | u64 | Timestamps. |

---

## Migration

Token migrated from bonding curve to Pump AMM (Raydium pool). From pump_fun migration event.

| Field | Type |
|-------|------|
| signature, slot | String, u64 |
| mint, user | String |
| bonding_curve, pool | String |
| token_amount, sol_amount, fee | u64 |
| txn_timestamp, received_at, sent_at | u64 |

Clients use mint + pool to route follow-up trades to the AMM.
