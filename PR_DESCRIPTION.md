# Migration events and Pump AMM buy/sell tracking

## Summary

- **Migration events**: Emit token migration (pump.fun bonding curve → pump AMM pool) so the frontend can track when a token moves to the new pool.
- **AMM trades**: Track Buy/Sell on **migrated** tokens (program `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`). These are parsed from `Instruction: Buy` / `Instruction: Sell` and the corresponding `Program data:` buyEvent/sellEvent logs.

## Changes

| Area | Change |
|------|--------|
| **Events** | `Trade` has `pool: Option<String>` (None = bonding curve, Some = AMM pool). New `Migration` event (signature, slot, mint, user, bonding_curve, pool, token_amount, sol_amount, fee, timestamps). |
| **Protocols** | New `pump_amm` module: detect Buy/Sell in logs, decode buyEvent/sellEvent from base64 `Program data:`, derive mint/pool/user from instruction accounts; emit `Trade` with `pool: Some(pool)`. |
| **Multi-parser** | `parse_trade_events` now includes pump_amm instructions (Buy/Sell) and merges AMM trades with bonding-curve trades. `parse_migration_events` for pump.fun Migrate. |
| **Messages** | `ServerMessage::Migration(Migration)` for migration events. |
| **Handler** | Parse migrations, broadcast `ServerMessage::Migration`, track migration count. |

## Behaviour

- **Bonding curve** (pump.fun): Trades unchanged; `pool` is `None`.
- **Migration**: Emitted when a Migrate instruction is seen; event includes destination `pool` from pump.fun.
- **AMM**: Trades on migrated tokens (pAMMBay…) are parsed and broadcast as `ServerMessage::Trade` with `pool: Some(pool)`; same WS channel as other trades.

Subscription already includes the pump_amm program in the gRPC transaction filter; no subscription changes.

## Testing

- `cargo build --release` in `data-pipeline` succeeds.
- Logic validated against real Buy/Sell logs for a migrated token (Instruction: Buy/Sell + Program data: …).
