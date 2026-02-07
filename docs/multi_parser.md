# multi_parser.rs — parsing orchestration

**Source:** `src/multi_parser.rs`

Builds the program-id → parser-name map and orchestrates parsing: flatten instructions, dispatch by program, dedupe launches, enrich launches with fee_recipient and initial_bonding_curve_state. Protocol-specific parsing is in [protocols.md](protocols.md); event types in [events.md](events.md).

---

## build_parser_map()

Returns `HashMap<Vec<u8>, &'static str>`.

- Key: program id as raw bytes (32 bytes). Inserted: base58-decode of Pump AMM and Pump.fun program IDs.
- Value: `"pump_amm"` or `"pump_fun"`.

Program IDs (base58):

- Pump AMM: `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`
- Pump.fun: `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`

Used in main to build the gRPC subscription filter (account_include) and passed to handler, which passes it to parse_* and enrich_token_launches.

---

## flatten_instructions(instructions, out)

Recursive: for each `InstructionUpdate` push it onto `out`, then recurse on `ix.inner`. So we get a flat list of all instructions in the transaction (outer and inner). Used by all parse_* and enrich_token_launches so we don’t miss nested CPI.

---

## parse_token_launches(instructions, parser_map, received_at)

1. Flatten instructions.
2. For each instruction: if `parser_map.get(program) == Some("pump_fun")`, call `protocols::pump_fun::parse_create(ix, received_at)`. If Some(launch), push it.
3. Dedupe by `(signature, mint)` (HashSet); return unique launches.

So only Pump.fun Create instructions (detected by logs + instruction data) produce launches. See [protocols.md](protocols.md) for parse_create.

---

## parse_trade_events(instructions, parser_map, received_at)

1. Flatten instructions.
2. For each instruction:
   - If pump_fun and `has_trade_in_logs(ix)`: extend with `pump_fun::parse_trade_events(ix, received_at)` (decodes “Program data:” base64 as tradeEvent). Then continue (don’t double-check pump_amm for same ix).
   - If pump_amm and (has_buy_in_logs or has_sell_in_logs): extend with `pump_amm::parse_amm_trade_events(ix, received_at)`.

Returns all trades from the transaction (bonding curve + AMM). Trade type is in [events.md](events.md); pool is set for AMM trades.

---

## parse_migration_events(instructions, parser_map, received_at)

1. Flatten instructions.
2. Only pump_fun; only if `has_migrate_in_logs(ix)`.
3. Extend with `pump_fun::parse_migration_events(ix, received_at)`.

Returns list of Migration (signature, slot, mint, user, bonding_curve, pool, amounts, fee, timestamps). See [events.md](events.md).

---

## enrich_token_launches(instructions, parser_map, launches, trades)

Mutates `launches` in place. For each launch:

1. **fee_recipient:** If still None, find first instruction in same tx (same signature) that is pump_fun, has trade in logs, and mint at accounts[2] matches launch.mint. Then fee_recipient = accounts[1] (BuyExactSolIn order: 0=Global, 1=FeeRecipient, 2=Mint).
2. **initial_bonding_curve_state:** If still None, find first trade in `trades` with same signature and same mint; set launch.initial_bonding_curve_state to that trade’s virtual_* and real_* reserves.

So launches get fee_recipient from the same-tx Buy and curve state from the same-tx trade when present. See [pumpfun_accounts.md](pumpfun_accounts.md) for account orders.

---

## Dependencies

- **crate::protocols** — pump_fun (parse_create, has_trade_in_logs, parse_trade_events, has_migrate_in_logs, parse_migration_events), pump_amm (has_buy_in_logs, has_sell_in_logs, parse_amm_trade_events).
- **yellowstone_vixen_core::instruction::InstructionUpdate** — program, shared.signature, shared.slot, accounts, inner.
- **events** — TokenLaunch, Trade, Migration (via protocols).

No config or I/O; pure parsing and enrichment.
