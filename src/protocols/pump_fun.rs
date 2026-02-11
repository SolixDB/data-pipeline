use yellowstone_vixen_core::instruction::InstructionUpdate;
use solana_pubkey::Pubkey;
use std::str::FromStr;

const TRADE_EVENT_TIMESTAMP_OFFSET: usize = 8 + 32 + 8 + 8 + 1 + 32;
const TRADE_EVENT_MIN_SIZE: usize = 8 + 32 + 8 + 8 + 1 + 32 + 8 + 8 + 8 + 8 + 8 + 32 + 8 + 8;
const MIGRATION_EVENT_MIN_SIZE: usize = 8 + 32 + 32 + 8 + 8 + 8 + 32 + 8 + 32;

pub const PUMP_FUN_FEE_RECIPIENT: &str = "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM";
pub const PUMP_FUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const PUMP_FUN_FEE_PROGRAM_ID: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";

/// Create instruction discriminator (IDL create).
const CREATE_DISCRIMINATOR: [u8; 8] = [0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77];
/// CreateV2 instruction discriminator (IDL createV2).
const CREATE_V2_DISCRIMINATOR: [u8; 8] = [0xd6, 0x90, 0x4c, 0xec, 0x5f, 0x8b, 0x31, 0xb4];
/// Anchor event discriminator for CreateEvent: first 8 bytes of sha256("event:CreateEvent").
const CREATE_EVENT_DISCRIMINATOR: [u8; 8] = [27, 114, 169, 77, 222, 235, 99, 118];

fn is_create_instruction(ix: &InstructionUpdate) -> bool {
    ix.data.len() >= 8
        && (ix.data[0..8] == CREATE_DISCRIMINATOR || ix.data[0..8] == CREATE_V2_DISCRIMINATOR)
}

pub fn has_create_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared.log_messages.iter().any(|l| {
        (l.contains("Instruction: Create") || l.contains("Instruction: CreateV2"))
            && !l.contains("CreateToken")
            && !l.contains("CreateIdempotent")
    })
}

#[allow(dead_code)]
pub fn has_create_v2_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared.log_messages.iter().any(|l| l.contains("Instruction: CreateV2"))
}

pub fn has_trade_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared
        .log_messages
        .iter()
        .any(|l| {
            l.contains("Instruction: Buy") 
            || l.contains("Instruction: Sell")
            || l.contains("Instruction: BuyExactSolIn")
        })
}

pub fn has_migrate_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared.log_messages.iter().any(|l| l.contains("Instruction: Migrate"))
}

fn txn_timestamp_from_create_logs(ix: &InstructionUpdate) -> Option<u64> {
    use base64::Engine;
    let decoder = base64::engine::general_purpose::STANDARD;
    for log in &ix.shared.log_messages {
        let data_str = log
            .trim()
            .strip_prefix("Program data: ")
            .or_else(|| log.trim().strip_prefix("> Program data: "))?;
        let data = decoder.decode(data_str.trim()).ok()?;
        if data.len() < TRADE_EVENT_MIN_SIZE {
            continue;
        }
        let txn_timestamp_sec = u64::from_le_bytes(
            data[TRADE_EVENT_TIMESTAMP_OFFSET..TRADE_EVENT_TIMESTAMP_OFFSET + 8]
                .try_into()
                .ok()?,
        );
        if (1577836800..=2524608000).contains(&txn_timestamp_sec) {
            return Some(txn_timestamp_sec * 1000);
        }
    }
    None
}

fn read_length_prefixed_string(data: &[u8], offset: &mut usize) -> Option<String> {
    if *offset + 4 > data.len() {
        return None;
    }
    let len = u32::from_le_bytes(data[*offset..*offset + 4].try_into().ok()?) as usize;
    *offset += 4;
    if *offset + len > data.len() {
        return None;
    }
    let s = String::from_utf8_lossy(&data[*offset..*offset + len]).to_string();
    *offset += len;
    Some(s)
}

/// Minimum size for createEvent: discriminator(8) + at least 3 length-prefixed strings + 4*32 + 8.
const CREATE_EVENT_MIN_LEN: usize = 8 + 4 * 3 + 32 * 4 + 8;

/// Valid timestamp range (roughly 2020–2050).
const TIMESTAMP_MIN: u64 = 1577836800;
const TIMESTAMP_MAX: u64 = 2524608000;

/// Parse createEvent from raw bytes. Requires exact CreateEvent discriminator so we never
/// mistake trade/migrate/extend events for create. Layout: discriminator(8), name, symbol, uri,
/// mint(32), bonding_curve(32), user(32), creator(32), timestamp(8).
/// Returns (name, symbol, uri, mint, bonding_curve, creator, timestamp_millis).
fn decode_create_event(data: &[u8]) -> Option<(String, String, String, String, String, String, u64)> {
    if data.len() < CREATE_EVENT_MIN_LEN || data[0..8] != CREATE_EVENT_DISCRIMINATOR {
        return None;
    }
    let mut offset = 8;
    let name = read_length_prefixed_string(data, &mut offset)?;
    let symbol = read_length_prefixed_string(data, &mut offset)?;
    let uri = read_length_prefixed_string(data, &mut offset)?;
    if offset + 32 * 4 + 8 > data.len() {
        return None;
    }
    let mint = bs58::encode(&data[offset..offset + 32]).into_string();
    let bonding_curve = bs58::encode(&data[offset + 32..offset + 64]).into_string();
    let _user = bs58::encode(&data[offset + 64..offset + 96]).into_string();
    let creator = bs58::encode(&data[offset + 96..offset + 128]).into_string();
    let timestamp_sec = i64::from_le_bytes(data[offset + 128..offset + 136].try_into().ok()?);
    if !(TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(&(timestamp_sec as u64)) {
        return None;
    }
    if mint.is_empty() || bonding_curve.is_empty() || creator.is_empty() {
        return None;
    }
    Some((name, symbol, uri, mint, bonding_curve, creator, timestamp_sec as u64 * 1000))
}

/// Parse createEvent from "Program data:" log (same layout as decode_create_event).
/// Used when we have an InstructionUpdate (per-instruction fallback).
fn try_parse_create_event_from_logs(ix: &InstructionUpdate) -> Option<(String, String, String, String, String, String, u64)> {
    use base64::Engine;
    let decoder = base64::engine::general_purpose::STANDARD;
    for log in &ix.shared.log_messages {
        let data_str = log
            .trim()
            .strip_prefix("Program data: ")
            .or_else(|| log.trim().strip_prefix("> Program data: "))?;
        let data = decoder.decode(data_str.trim()).ok()?;
        if let Some(t) = decode_create_event(&data) {
            return Some(t);
        }
    }
    None
}

/// Rigid, log-only CreateV2 launch detection. Does not depend on Geyser giving us the Create
/// instruction or its data. Scans transaction logs for the first "Program data:" that decodes
/// to a valid CreateEvent (discriminator + layout). Use this as the primary source for launches.
pub fn parse_create_from_tx_logs(
    log_messages: &[String],
    signature: &[u8],
    slot: u64,
    received_at: u64,
) -> Option<crate::events::TokenLaunch> {
    use base64::Engine;
    let decoder = base64::engine::general_purpose::STANDARD;
    let signature_str = bs58::encode(signature).into_string();
    for log in log_messages {
        let data_str = log
            .trim()
            .strip_prefix("Program data: ")
            .or_else(|| log.trim().strip_prefix("> Program data: "))?;
        let data = decoder.decode(data_str.trim()).ok()?;
        if let Some((name, symbol, uri, mint, bonding_curve, creator, txn_timestamp)) = decode_create_event(&data) {
            if let Some(mut launch) = build_token_launch_from_event(
                &signature_str,
                slot,
                received_at,
                true,
                name,
                symbol,
                uri,
                mint,
                creator,
                bonding_curve,
            ) {
                launch.txn_timestamp = txn_timestamp;
                return Some(launch);
            }
        }
    }
    None
}

/// Build TokenLaunch from createEvent fields only (no instruction accounts). Used by
/// parse_create_from_tx_logs so we never depend on Geyser instruction data.
fn build_token_launch_from_event(
    signature: &str,
    slot: u64,
    received_at: u64,
    is_token_2022: bool,
    name: String,
    symbol: String,
    uri: String,
    mint: String,
    creator: String,
    bonding_curve: String,
) -> Option<crate::events::TokenLaunch> {
    let metadata_program_id = Pubkey::from_str("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s").unwrap_or_default();
    let mint_pubkey = Pubkey::from_str(&mint).unwrap_or_default();
    let (metadata_pda, _) = Pubkey::find_program_address(
        &[b"metadata", metadata_program_id.as_ref(), mint_pubkey.as_ref()],
        &metadata_program_id,
    );
    let metadata = Some(metadata_pda.to_string());

    let pump_program = Pubkey::from_str(PUMP_FUN_PROGRAM_ID).unwrap_or_default();
    let fee_program = Pubkey::from_str(PUMP_FUN_FEE_PROGRAM_ID).unwrap_or_default();
    let creator_bytes = bs58::decode(&creator).into_vec().ok();
    let creator_slice: &[u8] = match creator_bytes.as_deref() {
        Some(b) if b.len() >= 32 => &b[..32],
        _ => return None,
    };
    let (creator_vault_pda, _) = Pubkey::find_program_address(
        &[b"creator-vault", creator_slice],
        &pump_program,
    );
    let (fee_config_pda, _) = Pubkey::find_program_address(
        &[b"fee_config", pump_program.as_ref()],
        &fee_program,
    );
    let (event_authority_pda, _) = Pubkey::find_program_address(&[b"__event_authority"], &pump_program);

    let pump_accounts = crate::events::PumpFunBondingCurveAccounts {
        bonding_curve: Some(bonding_curve),
        associated_bonding_curve: None,
        global: None,
        fee_recipient: Some(PUMP_FUN_FEE_RECIPIENT.to_string()),
        metadata,
        creator_vault: Some(creator_vault_pda.to_string()),
        fee_config: Some(fee_config_pda.to_string()),
        event_authority: Some(event_authority_pda.to_string()),
    };

    Some(crate::events::TokenLaunch {
        signature: signature.to_string(),
        slot,
        mint,
        name,
        symbol,
        uri,
        creator,
        pump_accounts,
        token_decimals: 6,
        initial_bonding_curve_state: None,
        pump_constants: crate::events::PumpProtocolConstants::default(),
        security: None,
        is_token_2022,
        txn_timestamp: received_at / 1000,
        received_at,
        sent_at: 0,
    })
}

pub fn parse_create(ix: &InstructionUpdate, received_at: u64) -> Option<crate::events::TokenLaunch> {
    let signature = bs58::encode(ix.shared.signature.as_slice()).into_string();
    let txn_timestamp = txn_timestamp_from_create_logs(ix).unwrap_or(received_at / 1000);

    // Path 1: current instruction is Create/CreateV2 (discriminator in ix.data)
    if has_create_in_logs(ix) && is_create_instruction(ix) {
        let is_token_2022 = ix.data.len() >= 8 && ix.data[0..8] == CREATE_V2_DISCRIMINATOR;

        let data = &ix.data;
        if data.len() >= 8 {
            let mut offset = 8;
            if let (Some(n), Some(s), Some(u)) = (
                read_length_prefixed_string(data, &mut offset),
                read_length_prefixed_string(data, &mut offset),
                read_length_prefixed_string(data, &mut offset),
            ) {
                if offset + 32 <= data.len() {
                    let c = bs58::encode(&data[offset..offset + 32]).into_string();
                    let m = ix.accounts.get(0).map(|p| bs58::encode(&p.0).into_string()).unwrap_or_default();
                    let bc = ix.accounts.get(2).map(|p| bs58::encode(&p.0).into_string());
                    let abc = ix.accounts.get(3).map(|p| bs58::encode(&p.0).into_string());
                    let g = ix.accounts.get(4).map(|p| bs58::encode(&p.0).into_string());
                    if !m.is_empty() || !n.is_empty() || !s.is_empty() || !u.is_empty() {
                        if let Some(launch) = build_token_launch(
                            ix,
                            &signature,
                            txn_timestamp,
                            received_at,
                            is_token_2022,
                            n,
                            s,
                            u,
                            m,
                            c,
                            bc,
                            abc,
                            g,
                        ) {
                            return Some(launch);
                        }
                    }
                }
            }
        }

        if is_token_2022 {
            if let Some((n, s, u, m, bc, c, txn_ts)) = try_parse_create_event_from_logs(ix) {
                let abc = ix.accounts.get(3).map(|p| bs58::encode(&p.0).into_string());
                let g = ix.accounts.get(4).map(|p| bs58::encode(&p.0).into_string());
                if let Some(launch) = build_token_launch(
                    ix,
                    &signature,
                    txn_ts,
                    received_at,
                    true,
                    n,
                    s,
                    u,
                    m,
                    c,
                    Some(bc),
                    abc,
                    g,
                ) {
                    return Some(launch);
                }
            }
        }
    }

    // Path 2: CreateV2 in logs but current instruction is not Create (e.g. Buy in same tx).
    // Geyser may not give us CreateV2 ix.data; parse launch from createEvent in logs.
    if has_create_v2_in_logs(ix) {
        if let Some((n, s, u, m, bc, c, txn_ts)) = try_parse_create_event_from_logs(ix) {
            if let Some(launch) = build_token_launch(
                ix,
                &signature,
                txn_ts,
                received_at,
                true,
                n,
                s,
                u,
                m,
                c,
                Some(bc),
                None,
                None,
            ) {
                return Some(launch);
            }
        }
    }

    None
}

fn build_token_launch(
    ix: &InstructionUpdate,
    signature: &str,
    txn_timestamp: u64,
    received_at: u64,
    is_token_2022: bool,
    name: String,
    symbol: String,
    uri: String,
    mint: String,
    creator: String,
    bonding_curve: Option<String>,
    associated_bonding_curve: Option<String>,
    global: Option<String>,
) -> Option<crate::events::TokenLaunch> {
    let metadata_program_id = Pubkey::from_str("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s").unwrap_or_default();
    let mint_pubkey = Pubkey::from_str(&mint).unwrap_or_default();
    let (metadata_pda, _) = Pubkey::find_program_address(
        &[b"metadata", metadata_program_id.as_ref(), mint_pubkey.as_ref()],
        &metadata_program_id,
    );
    let metadata = Some(metadata_pda.to_string());

    let pump_program = Pubkey::from_str(PUMP_FUN_PROGRAM_ID).unwrap_or_default();
    let fee_program = Pubkey::from_str(PUMP_FUN_FEE_PROGRAM_ID).unwrap_or_default();
    let creator_bytes = bs58::decode(&creator).into_vec().ok();
    let creator_slice: &[u8] = match creator_bytes.as_deref() {
        Some(b) if b.len() >= 32 => &b[..32],
        _ => return None,
    };
    let (creator_vault_pda, _) = Pubkey::find_program_address(
        &[b"creator-vault", creator_slice],
        &pump_program,
    );
    let (fee_config_pda, _) = Pubkey::find_program_address(
        &[b"fee_config", pump_program.as_ref()],
        &fee_program,
    );
    let (event_authority_pda, _) = Pubkey::find_program_address(&[b"__event_authority"], &pump_program);

    let pump_accounts = crate::events::PumpFunBondingCurveAccounts {
        bonding_curve,
        associated_bonding_curve,
        global,
        fee_recipient: Some(PUMP_FUN_FEE_RECIPIENT.to_string()),
        metadata,
        creator_vault: Some(creator_vault_pda.to_string()),
        fee_config: Some(fee_config_pda.to_string()),
        event_authority: Some(event_authority_pda.to_string()),
    };

    Some(crate::events::TokenLaunch {
        signature: signature.to_string(),
        slot: ix.shared.slot,
        mint,
        name,
        symbol,
        uri,
        creator,
        pump_accounts,
        token_decimals: 6,
        initial_bonding_curve_state: None,
        pump_constants: crate::events::PumpProtocolConstants::default(),
        security: None,
        is_token_2022,
        txn_timestamp,
        received_at,
        sent_at: 0,
    })
}

/// Decode one pump.fun tradeEvent (after base64 decode).
fn decode_trade_event(
    data: &[u8],
    signature: &str,
    slot: u64,
    is_create: bool,
    bonding_curve: Option<&str>,
    receive_ts: u64,
) -> Option<crate::events::Trade> {
    use crate::events::{Trade, TradeDirection};

    if data.len() < TRADE_EVENT_MIN_SIZE {
        return None;
    }
    let mut offset = 8;

    let mint_bytes = &data[offset..offset + 32];
    if mint_bytes.iter().all(|&b| b == 0) || mint_bytes.iter().all(|&b| b == 255) {
        return None;
    }
    let mint = bs58::encode(mint_bytes).into_string();
    offset += 32;

    let sol_amount = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let token_amount = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let is_buy = data[offset] != 0;
    offset += 1;
    let trader = bs58::encode(&data[offset..offset + 32]).into_string();
    offset += 32;
    let txn_timestamp_sec = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    if !(1577836800..=2524608000).contains(&txn_timestamp_sec) {
        return None;
    }
    let txn_timestamp = txn_timestamp_sec * 1000;

    let virtual_sol_reserves = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let virtual_token_reserves = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let real_sol_reserves = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let real_token_reserves = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    offset += 32;
    offset += 8;
    let fee = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);

    let price = if virtual_token_reserves > 0 {
        (virtual_sol_reserves as f64) / (virtual_token_reserves as f64) / 1000.0
    } else {
        0.0
    };
    let market_cap = price * 1_000_000_000.0;
    let liquidity_sol = real_sol_reserves as f64 / 1_000_000_000.0;

    Some(Trade {
        signature: signature.to_string(),
        slot,
        mint,
        trader,
        direction: if is_buy { TradeDirection::Buy } else { TradeDirection::Sell },
        token_amount,
        sol_amount,
        virtual_sol_reserves,
        virtual_token_reserves,
        real_sol_reserves,
        real_token_reserves,
        price,
        market_cap,
        liquidity_sol,
        fee,
        is_create,
        is_bonding_curve: true,
        pool: bonding_curve.map(|s| s.to_string()),
        suggested_compute_unit_limit: 300_000,
        token_decimals: 6,
        txn_timestamp,
        received_at: receive_ts,
        sent_at: 0,
        volume_sol: 0.0,
        volume_5m: 0.0,
        volume_1h: 0.0,
        volume_6h: 0.0,
        tx_count: 0,
        tx_count_5m: 0,
        tx_count_1h: 0,
        tx_count_6h: 0,
        price_change_5m: 0.0,
        price_change_1h: 0.0,
        price_change_6h: 0.0,
        price_change_24h: 0.0,
        total_fees_sol: 0.0,
    })
}

pub fn parse_trade_events(ix: &InstructionUpdate, received_at: u64) -> Vec<crate::events::Trade> {
    use base64::Engine;
    let decoder = base64::engine::general_purpose::STANDARD;
    let signature = bs58::encode(ix.shared.signature.as_slice()).into_string();
    let is_create = has_create_in_logs(ix);

    // Extract bonding curve address from accounts (accounts[2] per IDL)
    let bonding_curve = ix.accounts.get(2).map(|p| bs58::encode(&p.0).into_string());

    let mut out = Vec::new();
    for log in &ix.shared.log_messages {
        let Some(data_str) = log.strip_prefix("Program data: ") else {
            continue;
        };
        let Ok(data) = decoder.decode(data_str.trim()) else {
            continue;
        };
        if let Some(trade) = decode_trade_event(&data, &signature, ix.shared.slot, is_create, bonding_curve.as_deref(), received_at) {
            if trade.pool.is_none() {
                tracing::debug!(
                    "Parsed Pump.fun trade with NULL pool (bonding_curve not found in accounts). Signature: {}, Accounts len: {}", 
                    signature, 
                    ix.accounts.len()
                );
                // Log accounts to see what we have
                for (i, acc) in ix.accounts.iter().enumerate() {
                    tracing::debug!("Account {}: {}", i, bs58::encode(&acc.0).into_string());
                }
            }
            out.push(trade);
        }
    }
    out
}

fn decode_migration_event(
    data: &[u8],
    signature: &str,
    slot: u64,
    received_at: u64,
) -> Option<crate::events::Migration> {
    if data.len() < MIGRATION_EVENT_MIN_SIZE {
        return None;
    }
    let mut offset = 8;

    let user = bs58::encode(&data[offset..offset + 32]).into_string();
    offset += 32;

    let mint_bytes = &data[offset..offset + 32];
    if mint_bytes.iter().all(|&b| b == 0) || mint_bytes.iter().all(|&b| b == 255) {
        return None;
    }
    let mint = bs58::encode(mint_bytes).into_string();
    offset += 32;

    let token_amount = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let sol_amount = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let fee = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let bonding_curve = bs58::encode(&data[offset..offset + 32]).into_string();
    offset += 32;

    let txn_timestamp_sec = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    if !(1577836800..=2524608000).contains(&txn_timestamp_sec) {
        return None;
    }
    let txn_timestamp = txn_timestamp_sec * 1000;

    let pool = bs58::encode(&data[offset..offset + 32]).into_string();

    Some(crate::events::Migration {
        signature: signature.to_string(),
        slot,
        mint,
        user,
        bonding_curve,
        pool,
        token_amount,
        sol_amount,
        fee,
        txn_timestamp,
        received_at,
        sent_at: 0,
    })
}

pub fn parse_migration_events(ix: &InstructionUpdate, received_at: u64) -> Vec<crate::events::Migration> {
    use base64::Engine;
    let decoder = base64::engine::general_purpose::STANDARD;
    let signature = bs58::encode(ix.shared.signature.as_slice()).into_string();
    let mut out = Vec::new();
    for log in &ix.shared.log_messages {
        let Some(data_str) = log.strip_prefix("Program data: ") else {
            continue;
        };
        let Ok(data) = decoder.decode(data_str.trim()) else {
            continue;
        };
        if let Some(migration) = decode_migration_event(&data, &signature, ix.shared.slot, received_at) {
            out.push(migration);
        }
    }
    out
}

