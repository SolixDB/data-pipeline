# security.rs — Rug Check (mint security)

**Source:** `src/security.rs`

This module fetches and parses SPL mint account data from a Solana JSON‑RPC endpoint and derives simple Rug Check flags. Results are attached to launches and sent to clients.

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

Frontends receive this via:

- `TokenLaunchPayload.security: Option<MintSecurity>`
- `TokenSecurityUpdate.security: MintSecurity`

See [messages.md](messages.md) for wire details and [events.md](events.md) for how it is attached to `TokenLaunch`.

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

- **main.rs**
  - Reads `RPC_URL` from the environment (optional) and passes it to `handle_stream_updates`.
- **handler.rs**
  - For each launch:
    1. Builds and broadcasts `token_launch` **without** waiting on Rug Check.
    2. Spawns a background task which:
       - Calls `fetch_mint_security(rpc_url, mint)` with a 500ms timeout.
       - If successful:
         - Sets `launch.security = Some(MintSecurity)`.
         - Broadcasts `token_security_update` on topic = `mint`.
       - Irrespective of Rug Check success, continues with metrics + storage + Redis + known_mints.
- **storage.rs**
  - Stores the enriched `security` inside launches so snapshots include Rug Check state for reconnecting clients.

---

## Frontend expectations

- `TokenLaunchPayload.security` may be `null` on the first launch event.
- A follow‑up `token_security_update` will arrive on the mint topic if Rug Check succeeded.
- Snapshots (`snapshot_launches`) will typically contain `security` for older tokens.

Recommended UX:

- Treat `mint_authority_renounced` and `freeze_authority_renounced` as the primary Rug Check flags.
- Show more detailed information (e.g. current authority pubkeys) on hover/tooltips.

