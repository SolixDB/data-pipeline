use anyhow::Context;
use base64::Engine;
use serde::Deserialize;
use solana_pubkey::Pubkey;
use std::str::FromStr;

const METAPLEX_METADATA_PROGRAM_ID: &str = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";

#[derive(Debug, Clone)]
pub struct TokenMetadata {
    pub name: String,
    pub symbol: String,
    pub uri: String,
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
}

pub fn metaplex_metadata_pda(mint: &str) -> Option<String> {
    let metadata_program = Pubkey::from_str(METAPLEX_METADATA_PROGRAM_ID).ok()?;
    let mint_pubkey = Pubkey::from_str(mint).ok()?;
    let (pda, _) = Pubkey::find_program_address(
        &[b"metadata", metadata_program.as_ref(), mint_pubkey.as_ref()],
        &metadata_program,
    );
    Some(pda.to_string())
}

/// Best-effort: fetch Metaplex token metadata (name/symbol/uri) from RPC.
/// Returns Ok(None) if metadata account doesn't exist or can't be parsed.
pub async fn fetch_metaplex_metadata(
    rpc_url: &str,
    mint: &str,
) -> anyhow::Result<Option<TokenMetadata>> {
    if rpc_url.is_empty() {
        anyhow::bail!("RPC URL is empty");
    }

    let pda = match metaplex_metadata_pda(mint) {
        Some(p) => p,
        None => return Ok(None),
    };

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
        params: (&pda, RpcConfig { encoding: "base64" }),
    };

    let client = reqwest::Client::new();
    let resp = client
        .post(rpc_url)
        .json(&req_body)
        .send()
        .await
        .context("RPC request to getAccountInfo (metadata PDA) failed")?;

    if !resp.status().is_success() {
        anyhow::bail!("RPC getAccountInfo (metadata PDA) returned HTTP {}", resp.status());
    }

    let rpc_resp: RpcResponse<AccountInfoResult> = resp
        .json()
        .await
        .context("Failed to deserialize getAccountInfo (metadata PDA) response")?;

    let result = match rpc_resp.result {
        Some(r) => r,
        None => return Ok(None),
    };

    let value = match result.value {
        Some(v) => v,
        None => return Ok(None),
    };

    let (data_b64, _enc) = value.data;
    let data = base64::engine::general_purpose::STANDARD
        .decode(data_b64.trim())
        .context("Failed to decode metadata PDA data base64")?;

    Ok(parse_metaplex_metadata_account(&data))
}

fn parse_metaplex_metadata_account(data: &[u8]) -> Option<TokenMetadata> {
    // Metadata account (v1) layout starts with:
    // key: u8
    // update_authority: [u8;32]
    // mint: [u8;32]
    // name: string (borsh: u32 len + bytes)
    // symbol: string
    // uri: string
    let mut offset = 0usize;
    if data.len() < 1 + 32 + 32 {
        return None;
    }
    offset += 1 + 32 + 32;

    let name = read_borsh_string(data, &mut offset)?;
    let symbol = read_borsh_string(data, &mut offset)?;
    let uri = read_borsh_string(data, &mut offset)?;

    let clean = |s: String| s.trim_matches('\0').trim().to_string();
    Some(TokenMetadata {
        name: clean(name),
        symbol: clean(symbol),
        uri: clean(uri),
    })
}

fn read_borsh_string(data: &[u8], offset: &mut usize) -> Option<String> {
    if *offset + 4 > data.len() {
        return None;
    }
    let len = u32::from_le_bytes(data[*offset..*offset + 4].try_into().ok()?) as usize;
    *offset += 4;
    if *offset + len > data.len() {
        return None;
    }
    let bytes = &data[*offset..*offset + len];
    *offset += len;
    Some(String::from_utf8_lossy(bytes).to_string())
}

