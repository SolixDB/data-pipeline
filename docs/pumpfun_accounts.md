# Pump.fun accounts and transaction building

Reference for building Pump.fun and Pump AMM transactions using data from the WebSocket. Program IDs, PDA seeds, and account orders are below. All addresses base58 unless noted. No external links; everything you need is in this repo.

---

## Program IDs

| Name | Address |
|------|---------|
| Pump.fun program | 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P |
| Pump.fun fee program | pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ |
| Pump AMM program | pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA |
| Associated Token Program | ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL |
| SPL Token program | TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA |
| Metaplex Token Metadata | metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s |
| System program | 11111111111111111111111111111111 |
| wSOL mint (AMM quote) | So11111111111111111111111111111111111111112 |

Pump.fun tokens use **6 decimals**. The pipeline sends these in `token_launch.pump_constants.program_ids`.

---

## Bonding curve phase (Create / Buy / Sell before migration)

### PDA seeds (derive with program + seeds; 32-byte pubkeys in seeds)

| Account | Program | Seeds |
|---------|---------|--------|
| bonding_curve | Pump.fun | `["bonding-curve", mint]` — first seed is UTF-8 bytes of the string `"bonding-curve"`, second is mint pubkey (32 bytes). |
| global | Pump.fun | `["global"]` — single seed, UTF-8 bytes of `"global"`. |
| creator_vault | Pump.fun | `["creator-vault", creator]` — UTF-8 `"creator-vault"`, then creator pubkey (32 bytes). |
| fee_config | Pump.fun fee program | `["fee_config", pump_fun_program_id]` — UTF-8 `"fee_config"`, then Pump.fun program pubkey (32 bytes). |

**fee_recipient** is not a PDA. It is stored in the **Global** account data (e.g. field `feeRecipient`). The pipeline fills it when the same transaction contains a Buy instruction (we take accounts[1] of that Buy). Otherwise the client must fetch the account at `global` and read the struct.

### Create instruction (Pump.fun) — account order

0 = mint, 1 = mintAuthority, 2 = bondingCurve, 3 = associatedBondingCurve, 4 = global, 5 = mplTokenMetadata, 6 = metadata, 7 = user, …  

The pipeline sends bonding_curve, global, metadata from this instruction; fee_recipient from same-tx Buy when present.

### Buy/Sell (bonding curve) — account order (e.g. BuyExactSolIn)

0 = Global, 1 = FeeRecipient, 2 = Mint, …  

So we use accounts[1] as fee_recipient when enriching a launch from a same-tx Buy with matching mint (accounts[2] == launch.mint).

---

## What the pipeline sends on token_launch

- **bonding_curve, associated_bonding_curve, global, metadata:** From Create instruction accounts.
- **creator_vault, fee_config:** Derived and filled by the pipeline (so frontend does not need to).
- **fee_recipient:** From same-tx Buy instruction (accounts[1]) when present; otherwise null — client fetches Global or skips.

Use token_launch + trade data to build Create/Buy/Sell without extra RPC when possible. After migration, use mint + pool from migration/trade and AMM program for Buy/Sell.


---

## AMM phase (after migration)

Trades on the Pump AMM use the **pool** address (and global_config, ATAs, fee accounts per AMM IDL). The pipeline sets `trade.pool` for AMM trades. Instruction accounts for AMM Buy/Sell: pool(0), user(1), globalConfig(2), baseMint(3), quoteMint(4), … — we read mint=accounts[3], pool=accounts[0], user=accounts[1] in pump_amm.rs.
