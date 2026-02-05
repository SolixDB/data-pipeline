# multi_parser vs idls – Review

## Summary

**Verdict: The multi_parser is well defined and correctly wired for the two IDLs it uses.** Program IDs and parser names match the IDL `program.name` / `publicKey`. The main gaps are: only 2 of 14 IDLs are used, some dead code in data-pipeline, and no single source of truth when adding new parsers.

---

## What’s correct

1. **IDL → module name**  
   The proc-macro uses `to_snake_case(idl.program.name)`:
   - `pumpfun_swaps.json` → program name `"pumpAmm"` → module `pump_amm` ✓  
   - `pump_fun.json` → program name `"pump_fun"` → module `pump_fun` ✓  

2. **Program IDs**  
   - `pump_amm`: `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` matches `pumpfun_swaps.json` `program.publicKey`.  
   - `pump_fun`: `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` matches `pump_fun.json` `program.publicKey`.  

3. **Parser wiring**  
   `try_parse` uses the same names as `build_parser_map` (`"pump_amm"`, `"pump_fun"`) and the generated modules, so routing is consistent.

4. **Paths**  
   `include_vixen_parser!("idls/...")` is resolved from `CARGO_MANIFEST_DIR` (data-pipeline crate), so `idls/pumpfun_swaps.json` and `idls/pump_fun.json` resolve correctly.

---

## Issues

### 1. Only 2 of 14 IDLs are used

**idls/** contains 14 JSON IDLs; multi_parser includes only 2:

| Used in multi_parser | In idls/ |
|----------------------|----------|
| ✓ pumpfun_swaps.json | pumpfun_swaps.json |
| ✓ pump_fun.json       | pump_fun.json      |
| —                    | drift-v2.json, jupiter_v4.json, jupiter_v6.json, kamino-farms.json, kamino-lending-vault.json, kamino-lending.json, kamino-limo.json, meteora-damm-v2.json, meteora-dlmm.json, meteora-dynamic-bonding-curve.json, orca_whirlpool.json, raydium_amm_v3.json, raydium_cpmm.json |

So either:

- The pipeline is intentionally only parsing Pump AMM and Pump.fun, and the other IDLs are for other tools/future use, or  
- You want to parse more programs and should add more `include_vixen_parser!`, `build_parser_map` entries, and `try_parse` arms.

If the rest are unused by this crate, consider moving them to a shared repo or documenting their purpose so it’s clear they’re not used by multi_parser.

### 2. Dead code in data-pipeline: `build_full_account_list`

- `build_full_account_list` is **not** used anywhere in **data-pipeline** (only in the separate **data** crate).
- In data-pipeline it is reported as dead code by the compiler.

**Options:** remove it from data-pipeline’s multi_parser, or add a short doc comment that it’s kept for API symmetry / future use so the dead-code warning can be explicitly allowed.

### 3. No single source of truth when adding parsers

Adding a new program requires editing three places by hand:

1. `include_vixen_parser!("idls/SomeProgram.json");`  
2. `build_parser_map()` – new program ID + parser name string  
3. `try_parse` – new `"parser_name" => module::InstructionParser ...` arm  

If one is missed or the name is mistyped, you get runtime or compile errors. A small improvement would be to derive the parser name from the same constant or from the map (e.g. only have program IDs and parser names in one place and generate or validate the rest from that).

### 4. Filename vs program name

- File `pumpfun_swaps.json` describes the **Pump AMM** program (`pumpAmm`), not a generic “pumpfun swaps” program.  
- So the IDL filename doesn’t match the program name; the **program.name** in the IDL is the source of truth and is correct.  
- Optional: rename to e.g. `pump_amm.json` to avoid confusion, or add a one-line comment in multi_parser that `pumpfun_swaps.json` is the Pump AMM IDL.

---

## Recommendations

1. **Decide policy for the other 12 IDLs**  
   Either document that they’re not used by this pipeline (or move them) or add them to multi_parser with the same pattern (include, map, try_parse).

2. **Clean up or document dead code**  
   Remove `build_full_account_list` from data-pipeline’s multi_parser or keep it with `#[allow(dead_code)]` and a short doc explaining why.

3. **When adding parsers**  
   Consider a single table (e.g. `(parser_name, program_id_b58)`) and derive `build_parser_map` and possibly the `try_parse` arms from it to avoid mismatches.

4. **Optional**  
   Rename `pumpfun_swaps.json` → `pump_amm.json` or add a comment in `multi_parser.rs` that this file is the Pump AMM IDL.
