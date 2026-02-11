# security.rs — Mint security (Rug Check)

**Source:** `src/security.rs`

This module fetches and parses SPL mint account data from a Solana JSON‑RPC endpoint and derives simple Rug Check flags. **The main pipeline does not currently call it;** the handler no longer runs Rug Check or sends `token_security_update`. The types and helpers remain for optional use (e.g. other binaries or future re‑enablement).

---

## MintSecurity

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MintSecurity {
    pub mint: String,
    pub mint_authority: Option<String>,
    pub freeze_authority: Option<String>,
    pub supply: u64,
    pub decimals: u8,
    pub is_initialized: bool,
    pub mint_authority_renounced: bool,
    pub freeze_authority_renounced: bool,
}
```

- `mint` – SPL mint address (base58).
- `mint_authority` – current mint authority pubkey, if any.
- `freeze_authority` – current freeze authority pubkey, if any.
- `supply` – total supply (base units).
- `decimals` – SPL mint decimals.
- `is_initialized` – SPL mint `is_initialized` flag.
- `mint_authority_renounced` – `true` when `mint_authority` is `None`.
- `freeze_authority_renounced` – `true` when `freeze_authority` is `None`.

`TokenLaunchPayload.security` is an `Option<MintSecurity>`; the pipeline currently leaves it `null`. See [messages.md](messages.md) and [events.md](events.md).

---

## fetch_mint_security(rpc_url, mint)

```rust
pub async fn fetch_mint_security(
    rpc_url: &str,
    mint: &str,
) -> anyhow::Result<MintSecurity>
```

Behaviour:

1. Validates that `rpc_url` is non‑empty.
2. Validates that `mint` decodes from base58 to 32 bytes.
3. Sends a minimal JSON‑RPC `getAccountInfo` request with `encoding: "base64"`.
4. Ensures the owner program is SPL Token (Token / Token‑2022).
5. Decodes base64 account data and calls `parse_mint_account`.

Errors are annotated with `anyhow::Context` at each step (decode, HTTP, JSON, owner mismatch).

---

## fetch_mint_security_from_env(mint)

```rust
#[allow(dead_code)]
pub async fn fetch_mint_security_from_env(mint: &str) -> anyhow::Result<MintSecurity>
```

Convenience helper:

- Reads `RPC_URL`, then `SOLANA_RPC_URL` from the environment.
- Calls `fetch_mint_security(&rpc_url, mint)`.

The main pipeline does **not** use this helper directly; instead it passes `RPC_URL` down to the handler and calls `fetch_mint_security` there. This function is kept as a simple public API for other binaries/tests.

---

## Where Rug Check is used

The pipeline **does not** currently call `fetch_mint_security` or send any security updates. `RPC_URL` is still read in main and passed to the handler (e.g. for metadata backfill); Rug Check is disabled. To re‑enable, you would call `fetch_mint_security` in the handler and optionally broadcast a security payload; the `TokenSecurityUpdate` message type has been removed, so any new design would need a new variant or a different channel.

