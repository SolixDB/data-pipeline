use yellowstone_vixen_core::instruction::InstructionUpdate;
use solana_pubkey::Pubkey;
use std::str::FromStr;

/// tradeEvent layout (after 8-byte discriminator):
/// mint(32) + sol(8) + token(8) + is_buy(1) + user(32) + timestamp(8) + reserves/fee...
const TRADE_EVENT_TIMESTAMP_OFFSET: usize = 8 + 32 + 8 + 8 + 1 + 32;
/// Minimum size for pump.fun tradeEvent payload (after base64 decode).
const TRADE_EVENT_MIN_SIZE: usize = 8 + 32 + 8 + 8 + 1 + 32 + 8 + 8 + 8 + 8 + 8 + 32 + 8 + 8;

/// completePumpAmmMigrationEvent: 8(disc) + user(32) + mint(32) + mintAmount(8) + solAmount(8) + fee(8) + bondingCurve(32) + timestamp(8) + pool(32).
/// completePumpAmmMigrationEvent: 8(disc) + user(32) + mint(32) + mintAmount(8) + solAmount(8) + fee(8) + bondingCurve(32) + timestamp(8) + pool(32).
const MIGRATION_EVENT_MIN_SIZE: usize = 8 + 32 + 32 + 8 + 8 + 8 + 32 + 8 + 32;

const PUMP_FUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMP_FUN_FEE_PROGRAM_ID: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";

pub fn has_create_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared.log_messages.iter().any(|l| {
        l.contains("Instruction: Create")
            && !l.contains("CreateToken")
            && !l.contains("CreateIdempotent")
    })
}

pub fn has_trade_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared
        .log_messages
        .iter()
        .any(|l| l.contains("Instruction: Buy") || l.contains("Instruction: Sell"))
}

pub fn has_migrate_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared.log_messages.iter().any(|l| l.contains("Instruction: Migrate"))
}

/// Extract on-chain txn_timestamp (ms) from the first tradeEvent in logs.
/// For Create, the initial buy tradeEvent typically carries the true on-chain timestamp.
fn txn_timestamp_from_create_logs(ix: &InstructionUpdate) -> Option<u64> {
    use base64::Engine;
    let decoder = base64::engine::general_purpose::STANDARD;
    for log in &ix.shared.log_messages {
        let data_str = log.strip_prefix("Program data: ")?;
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

pub fn parse_create(ix: &InstructionUpdate, received_at: u64) -> Option<crate::events::TokenLaunch> {
    if !has_create_in_logs(ix) {
        return None;
    }
    let data = &ix.data;
    if data.len() < 8 {
        return None;
    }
    let mut offset = 8;
    let name = read_length_prefixed_string(data, &mut offset)?;
    let symbol = read_length_prefixed_string(data, &mut offset)?;
    let uri = read_length_prefixed_string(data, &mut offset)?;
    if offset + 32 > data.len() {
        return None;
    }
    let creator = bs58::encode(&data[offset..offset + 32]).into_string();
    let mint = ix
        .accounts
        .get(0)
        .map(|p| bs58::encode(&p.0).into_string())
        .unwrap_or_default();
    // Create instruction account order (pump_fun IDL): 0=mint, 1=mintAuthority, 2=bondingCurve, 3=associatedBondingCurve, 4=global, 5=mplTokenMetadata, 6=metadata, 7=user, ...
    let bonding_curve = ix.accounts.get(2).map(|p| bs58::encode(&p.0).into_string());
    let associated_bonding_curve = ix.accounts.get(3).map(|p| bs58::encode(&p.0).into_string());
    let global = ix.accounts.get(4).map(|p| bs58::encode(&p.0).into_string());
    let metadata = ix.accounts.get(6).map(|p| bs58::encode(&p.0).into_string());
    let signature = bs58::encode(ix.shared.signature.as_slice()).into_string();
    let txn_timestamp = txn_timestamp_from_create_logs(ix).unwrap_or(received_at / 1000);

    // Derive PDAs
    let pump_program = Pubkey::from_str(PUMP_FUN_PROGRAM_ID).unwrap_or_default();
    let fee_program = Pubkey::from_str(PUMP_FUN_FEE_PROGRAM_ID).unwrap_or_default();

    // creator_vault: ["creator-vault", creator_pubkey]
    let creator_bytes = &data[offset..offset + 32];
    let (creator_vault_pda, _) = Pubkey::find_program_address(
        &[b"creator-vault", creator_bytes],
        &pump_program,
    );

    // fee_config: ["fee_config", pump_fun_program_id] - program: fee_program
    // Note: docs say seeds are ["fee_config", pump_fun_program_id]
    let (fee_config_pda, _) = Pubkey::find_program_address(
        &[b"fee_config", pump_program.as_ref()],
        &fee_program,
    );

    let pump_accounts = crate::events::PumpFunBondingCurveAccounts {
        bonding_curve,
        associated_bonding_curve,
        global,
        fee_recipient: None,
        metadata,
        creator_vault: Some(creator_vault_pda.to_string()),
        fee_config: Some(fee_config_pda.to_string()),
    };
    Some(crate::events::TokenLaunch {
        signature,
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
        pool: None,
        suggested_compute_unit_limit: 300_000,
        token_decimals: 6,
        txn_timestamp,
        received_at: receive_ts,
        sent_at: 0,
    })
}

pub fn parse_trade_events(ix: &InstructionUpdate, received_at: u64) -> Vec<crate::events::Trade> {
    use base64::Engine;
    let decoder = base64::engine::general_purpose::STANDARD;
    let signature = bs58::encode(ix.shared.signature.as_slice()).into_string();
    let is_create = has_create_in_logs(ix);

    let mut out = Vec::new();
    for log in &ix.shared.log_messages {
        let Some(data_str) = log.strip_prefix("Program data: ") else {
            continue;
        };
        let Ok(data) = decoder.decode(data_str.trim()) else {
            continue;
        };
        if let Some(trade) = decode_trade_event(&data, &signature, ix.shared.slot, is_create, received_at) {
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

