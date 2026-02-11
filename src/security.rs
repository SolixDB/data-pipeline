use anyhow::Context;
use base64::Engine;
use serde::Deserialize;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MintSecurity {
    pub mint: String,
    pub mint_authority: Option<String>,
    pub freeze_authority: Option<String>,
    /// Total supply in **base units** (raw, same as SPL mint account).
    /// For "top N hold %": use (sum of holder token balances in base units) / supply * 100, then clamp to 0..100.
    /// Using supply/10^decimals as denominator while balances are in base units will produce >100%.
    pub supply: u64,
    pub decimals: u8,
    pub is_initialized: bool,
    pub mint_authority_renounced: bool,
    pub freeze_authority_renounced: bool,
}

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    #[allow(dead_code)]
    jsonrpc: String,
    #[allow(dead_code)]
    id: u64,
    result: Option<T>,
}

#[derive(Debug, Deserialize)]
struct AccountInfoResult {
    value: Option<AccountInfoValue>,
}

#[derive(Debug, Deserialize)]
struct AccountInfoValue {
    data: (String, String),
    owner: String,
}

pub async fn fetch_mint_security(
    rpc_url: &str,
    mint: &str,
) -> anyhow::Result<MintSecurity> {
    if rpc_url.is_empty() {
        anyhow::bail!("RPC URL is empty");
    }

    let mint_bytes = bs58::decode(mint)
        .into_vec()
        .context("Failed to decode mint as base58")?;
    if mint_bytes.len() != 32 {
        anyhow::bail!("Mint must be 32 bytes, got {}", mint_bytes.len());
    }

    #[derive(serde::Serialize)]
    struct RpcRequest<'a> {
        jsonrpc: &'static str,
        id: u64,
        method: &'static str,
        params: (&'a str, RpcConfig),
    }

    #[derive(serde::Serialize)]
    struct RpcConfig {
        encoding: &'static str,
    }

    let req_body = RpcRequest {
        jsonrpc: "2.0",
        id: 1,
        method: "getAccountInfo",
        params: (mint, RpcConfig { encoding: "base64" }),
    };

    let client = reqwest::Client::new();
    let resp = client
        .post(rpc_url)
        .json(&req_body)
        .send()
        .await
        .context("RPC request to getAccountInfo failed")?;

    if !resp.status().is_success() {
        anyhow::bail!("RPC getAccountInfo returned HTTP {}", resp.status());
    }

    let rpc_resp: RpcResponse<AccountInfoResult> = resp
        .json()
        .await
        .context("Failed to deserialize getAccountInfo response")?;

    let result = rpc_resp
        .result
        .context("getAccountInfo missing result field")?;
    let value = result
        .value
        .context("getAccountInfo returned null value (account not found)")?;

    if value.owner != "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        && value.owner != "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
    {
        anyhow::bail!("Account owner is not SPL Token program");
    }

    let (data_b64, _enc) = value.data;
    let data = base64::engine::general_purpose::STANDARD
        .decode(data_b64.trim())
        .context("Failed to decode account data base64")?;

    parse_mint_account(&data, mint)
        .context("Failed to parse SPL Token mint account data")
}

#[allow(dead_code)]
pub async fn fetch_mint_security_from_env(mint: &str) -> anyhow::Result<MintSecurity> {
    let rpc_url = std::env::var("RPC_URL")
        .or_else(|_| std::env::var("SOLANA_RPC_URL"))
        .context("RPC_URL or SOLANA_RPC_URL environment variable must be set")?;
    fetch_mint_security(&rpc_url, mint).await
}

fn parse_mint_account(data: &[u8], mint: &str) -> anyhow::Result<MintSecurity> {
    const MIN_MINT_LEN: usize = 4 + 32 + 8 + 1 + 1 + 4 + 32;
    if data.len() < MIN_MINT_LEN {
        anyhow::bail!("Mint account too small: {} bytes", data.len());
    }

    let mut offset = 0usize;

    let mint_auth_option = u32::from_le_bytes(
        data[offset..offset + 4]
            .try_into()
            .expect("slice length already checked"),
    );
    offset += 4;

    let mint_authority = if mint_auth_option == 0 {
        None
    } else {
        let key_bytes = &data[offset..offset + 32];
        offset += 32;
        Some(bs58::encode(key_bytes).into_string())
    };

    if mint_auth_option == 0 {
        offset += 32;
    }

    let supply = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .expect("slice length already checked"),
    );
    offset += 8;

    let decimals = data[offset];
    offset += 1;

    let is_initialized = data[offset] != 0;
    offset += 1;

    let freeze_auth_option = u32::from_le_bytes(
        data[offset..offset + 4]
            .try_into()
            .expect("slice length already checked"),
    );
    offset += 4;

    let freeze_authority = if freeze_auth_option == 0 {
        None
    } else {
        let key_bytes = &data[offset..offset + 32];
        Some(bs58::encode(key_bytes).into_string())
    };

    let mint_authority_renounced = mint_authority.is_none();
    let freeze_authority_renounced = freeze_authority.is_none();

    Ok(MintSecurity {
        mint: mint.to_string(),
        mint_authority,
        freeze_authority,
        supply,
        decimals,
        is_initialized,
        mint_authority_renounced,
        freeze_authority_renounced,
    })
}

/// Safe "top N hold %" from sum of balances and supply (all in base units).
/// Returns 0..100. Use this so the ratio is never inverted and never exceeds 100%.
#[allow(dead_code)]
#[inline]
pub fn holder_pct_clamped(sum_balances_base: u64, supply_base: u64) -> f64 {
    if supply_base == 0 {
        0.0
    } else {
        (sum_balances_base as f64 / supply_base as f64 * 100.0).min(100.0)
    }
}
