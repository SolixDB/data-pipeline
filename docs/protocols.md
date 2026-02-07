# protocols/ — Pump.fun and Pump AMM parsing

**Sources:** `src/protocols/mod.rs`, `src/protocols/pump_fun.rs`, `src/protocols/pump_amm.rs`

Protocol-specific detection (logs) and parsing (instruction data and “Program data:” base64). Event types are in [events.md](events.md); account orders and PDAs in [pumpfun_accounts.md](pumpfun_accounts.md). This doc describes how we detect and decode each event type.

---

## pump_fun.rs — Create

**Detection:** `has_create_in_logs(ix)` — any log line contains `"Instruction: Create"` and does not contain `"CreateToken"` or `"CreateIdempotent"`.

**Parsing:** If not Create, return None. Otherwise read instruction `ix.data`: skip 8 bytes (discriminator), then length-prefixed UTF-8 strings (4-byte little-endian length + bytes) for name, symbol, uri; then 32 bytes creator pubkey. Mint from `ix.accounts[0]`. Create instruction account order (IDL): 0=mint, 1=mintAuthority, 2=bondingCurve, 3=associatedBondingCurve, 4=global, 5=mplTokenMetadata, 6=metadata, 7=user. We take bonding_curve, global, metadata from accounts 2, 4, 6. fee_recipient is not in Create; it is filled later by enrich_token_launches from a same-tx Buy (accounts[1]). txn_timestamp: from first “Program data:” line that decodes as a valid tradeEvent (timestamp in reasonable range); else received_at/1000.

---

## pump_fun.rs — Trade (bonding curve)

**Detection:** `has_trade_in_logs(ix)` — any log contains `"Instruction: Buy"` or `"Instruction: Sell"`.

**Parsing:** For each log line starting with `"Program data: "`, base64-decode and parse as tradeEvent. Layout (after 8-byte discriminator): mint(32), sol(8), token(8), is_buy(1), user(32), timestamp(8), virtual_sol(8), virtual_token(8), real_sol(8), real_token(8), 32 bytes, 8 bytes, fee(8). Reject if length &lt; TRADE_EVENT_MIN_SIZE or mint all zeros/all 0xff or timestamp not in 2019–2049. is_create: true if the same instruction has Create in logs (first buy in same tx as Create). Fill Trade with reserves, price (virtual_sol/virtual_token/1000), market_cap, liquidity_sol (real_sol/1e9), pool=None. token_decimals=6, suggested_compute_unit_limit=300_000.

---

## pump_fun.rs — Migration

**Detection:** `has_migrate_in_logs(ix)` — any log contains `"Instruction: Migrate"`.

**Parsing:** For each “Program data:” line, base64-decode. completePumpAmmMigrationEvent layout (after 8-byte disc): user(32), mint(32), mintAmount(8), solAmount(8), fee(8), bondingCurve(32), timestamp(8), pool(32). Reject if length &lt; MIGRATION_EVENT_MIN_SIZE or mint invalid or timestamp out of range. Fill Migration (signature, slot, mint, user, bonding_curve, pool, token_amount, sol_amount, fee, txn_timestamp, received_at, sent_at=0).

---

## pump_amm.rs — Buy/Sell (AMM)

**Detection:** `has_buy_in_logs(ix)` or `has_sell_in_logs(ix)` — log contains `"Instruction: Buy"` or `"Instruction: Sell"`. Program is Pump AMM (parser_map).

**Accounts:** mint = accounts[3], pool = accounts[0], user = accounts[1] (Buy/Sell instruction account order).

**Parsing:** For each “Program data:” line, strip first 8 bytes (discriminator?), then parse buyEvent/sellEvent. Offsets (in payload after 8): baseAmountOut 8, quoteAmountIn 56 (buy); baseAmountIn 8, quoteAmountOut 56 (sell); pool base/quote at 40, 48; fee at 72, 88 (lp + protocol); timestamp in first 8 of payload. Build Trade with direction Buy/Sell, pool=Some(pool), is_create=false, virtual_* = pool_base/pool_quote, price = pool_quote/pool_base/1000, etc.

---

## mod.rs

Re-exports: `pub mod pump_amm; pub mod pump_fun;`. multi_parser uses `protocols::pump_fun::*` and `protocols::pump_amm::*`.
