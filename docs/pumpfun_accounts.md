# Pump.fun accounts and transaction building

This document is the reference for building **Pump.fun** and **Pump AMM** transactions (Create, Buy, Sell) using data from the WebSocket stream. It lists program IDs, PDA seeds, and which accounts the pipeline provides so that frontends and backends can build transactions without extra RPC when possible.

**Reference implementation:** The `test/` directory scripts (`create-token.ts`, `buy.ts`, `sell.ts`, `bonding-curve-helpers.ts`, `amm-helpers.ts`) show the exact account order and derivation used on devnet.

---

## Quick reference

| Phase | When | Create | Buy | Sell |
|-------|------|--------|-----|------|
| **Bonding curve** | Token not yet migrated | Create instruction + metadata, bonding_curve, global, user, ATA | BuyExactSolIn / Buy with bonding curve accounts | Sell with bonding curve accounts |
| **AMM** | After Migrate | Not applicable | BuyExactQuoteIn with pool, global_config, ATAs, fee accounts | Sell with pool, global_config, ATAs, fee accounts |

The pipeline emits **token_launch** at Create (bonding curve phase). It does not emit a separate event when a token migrates to the AMM. Clients must resolve phase by checking whether a Pool exists for the mint (see [Resolving bonding curve vs AMM](#resolving-bonding-curve-vs-amm)).

---

## Program IDs and constants

Use these when building instructions or deriving PDAs. The pipeline also sends them in **token_launch** as `pump_constants.program_ids`.

| Name | Address |
|------|---------|
| Pump.fun program | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` |
| Pump.fun fee program | `pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ` |
| Pump AMM program | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` |
| Associated Token Program | `ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL` |
| SPL Token program | `TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA` |
| Metaplex Token Metadata | `metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s` |
| wSOL mint (default quote for AMM) | `So11111111111111111111111111111111111111112` |

Pump.fun tokens use **6 decimals**.

---

## Bonding curve phase (pre-migration)

Tokens start on a bonding curve. Create, Buy, and Sell use the **Pump.fun** program until the curve is complete and liquidity is migrated to the **Pump AMM**.

### PDAs (bonding curve)

Derive these with standard Solana PDA derivation (e.g. `findProgramAddress`): program id, seeds, optional bump.

| Account | Program | Seeds |
|---------|---------|--------|
| bonding_curve | Pump.fun | `["bonding-curve", mint]` — first seed is the UTF-8 bytes of the string `"bonding-curve"`, second is the mint pubkey (32 bytes). |
| global | Pump.fun | `["global"]` — single seed, UTF-8 bytes of the string `"global"`. |
| creator_vault | Pump.fun | `["creator-vault", creator]` — first seed is UTF-8 `"creator-vault"`, second is the creator pubkey (32 bytes). |
| fee_config | Pump.fun fee program | `["fee_config", pump_fun_program_id]` — first seed is UTF-8 `"fee_config"`, second is the Pump.fun program pubkey (32 bytes). |

**fee_recipient** is not a PDA. It is stored in the **Global** account data (field `feeRecipient`). Either the pipeline fills it from a same-tx Buy instruction (see [What the pipeline sends](#what-the-pipeline-sends-on-token_launch)), or the client must fetch the account at the **global** PDA and deserialize the Global struct to read `feeRecipient`.

**User ATA (for buy/sell):** Standard SPL Associated Token Account: seeds `[owner, token_program, mint]`, Associated Token Program. Derive for the trader and the mint to get the account that holds the user’s tokens.

### What the pipeline sends on token_launch

| Field | Source | Notes |
|-------|--------|-------|
| bonding_curve | Create instruction accounts (index 2) | Always set. |
| global | Create instruction accounts (index 4) | Always set. |
| metadata | Create instruction accounts (index 6) | Metaplex metadata PDA for this mint. |
| fee_recipient | Same-tx Buy instruction accounts (index 1) | Set only when the Create transaction also contains a Buy (e.g. initial buy). Otherwise the client must fetch the Global account. |
| creator_vault | Not sent | Derive using seeds above. |
| fee_config | Not sent | Derive using seeds above. |
| token_decimals | Constant | 6. |
| initial_bonding_curve_state | First trade in same tx with same mint | Virtual and real reserves after the first trade (e.g. initial buy). Use for price and slippage. |
| pump_constants | Constant | Program IDs and decimals. |

TypeScript derivation for creator_vault and fee_config: see `test/src/bonding-curve-helpers.ts`.

### Create instruction account order (Pump.fun)

When building or parsing a Create instruction, accounts appear in this order:

| Index | Account |
|-------|---------|
| 0 | mint (signer) |
| 1 | mint_authority |
| 2 | bonding_curve |
| 3 | associated_bonding_curve |
| 4 | global |
| 5 | mpl_token_metadata |
| 6 | metadata |
| 7 | user |
| 8 | system_program |
| 9 | token_program |
| 10 | associated_token_program |
| 11 | rent |
| 12 | event_authority |
| 13 | program |

### Buy / Sell on the bonding curve

**BuyExactSolIn** and **Sell** require: `mint`, `user` (signer), `associatedUser` (user’s ATA for the mint), `feeRecipient`, `creatorVault`, `feeConfig`, plus `bonding_curve`, `global`, system/token programs, and any volume-tracking accounts your client supports. See `test/src/buy.ts` and `test/src/sell.ts` for the exact account list and order.

---

## AMM phase (post-migration)

After the bonding curve completes, liquidity is migrated to the **Pump AMM** (Migrate instruction). The same mint is then traded via the AMM program; Buy and Sell use AMM instructions and AMM accounts instead of the bonding curve.

### PDAs (Pump AMM)

| Account | Program | Seeds |
|---------|---------|--------|
| global_config | Pump AMM | `["global_config"]` — single seed, UTF-8 bytes. |
| pool | Pump AMM | `["pool", index_le_u16, creator, base_mint, quote_mint]` — first seed is UTF-8 `"pool"`, second is pool index as little-endian u16 (typically 0), then creator, base_mint (token), quote_mint (typically wSOL). |

**protocol_fee_recipient:** Read from the **GlobalConfig** account data (e.g. `protocolFeeRecipients[0]`).

**pool_base_token_account**, **pool_quote_token_account**, **coin_creator** (vault authority): Read from the **Pool** account data after fetching the pool at the pool PDA.

Default for new pools: **quote_mint** = wSOL, **index** = 0.

### Resolving bonding curve vs AMM

The pipeline does not emit a "migrated" event. To decide whether to use bonding curve or AMM for a given mint:

1. You need the **creator** (from token_launch or from the bonding curve account data). Default **quote_mint** is wSOL, **index** is 0.
2. Derive the **pool** PDA: `["pool", 0, creator, mint, wSOL]` with the Pump AMM program. Fetch the account.
3. If the account exists and has valid Pool data, use the **AMM** flow (BuyExactQuoteIn / Sell with pool, global_config, and the accounts from Pool and GlobalConfig).
4. Otherwise derive the **bonding_curve** PDA for this mint and try to fetch it. If it exists, use the **bonding curve** flow.

See `test/src/buy.ts` and `test/src/sell.ts`: they call `resolveAmmAccounts` first, then fall back to `resolveBondingCurveAccounts`.

---

## Using Trade events

**Trade** messages contain `mint`, `trader`, amounts, reserves, and fee. To build a follow-up buy or sell:

- If you have a **token_launch** for that `mint`, use its `pump_accounts` (bonding_curve, fee_recipient, etc.) and `pump_constants`. Resolve AMM vs bonding curve as above if the token might have migrated.
- If you never received a token_launch (e.g. token created before your client subscribed), you must derive bonding curve PDAs from `mint` and fetch the **Global** account once to get `fee_recipient`. The **creator** is stored in the bonding curve account data (or you can infer it from a prior trade if you persist it).

---

## Before vs after migration (summary)

| Phase | Create | Buy | Sell |
|-------|--------|-----|------|
| Bonding curve | Create instruction with metadata, bonding_curve, global, user, ATA. | BuyExactSolIn or Buy with bonding curve accounts (mint, user, ATA, fee_recipient, creator_vault, fee_config, …). | Sell with same bonding curve accounts. |
| AMM | Not applicable (token already created). | BuyExactQuoteIn with pool, global_config, user/pool token accounts, protocol fee recipient, coin creator vault. | Sell with same AMM accounts. |

Token_launch from the pipeline always describes the bonding-curve creation. After migration, the same mint is traded on the AMM; the pipeline does not change the token_launch. Clients that store token_launch by mint can keep using it for creator and constants, and resolve AMM vs curve by checking Pool existence as described above.
