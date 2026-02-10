use yellowstone_vixen_core::instruction::InstructionUpdate;
use crate::events::{Trade, TradeDirection};

const AMM_EVENT_MIN_SIZE: usize = 144 + 32;
const BUY_TOKEN_OFF: usize = 8;
const BUY_SOL_OFF: usize = 56;
const BUY_POOL_BASE_OFF: usize = 40;
const BUY_POOL_QUOTE_OFF: usize = 48;
const SELL_TOKEN_OFF: usize = 8;
const SELL_SOL_OFF: usize = 56;
const SELL_POOL_BASE_OFF: usize = 40;
const SELL_POOL_QUOTE_OFF: usize = 48;

pub fn has_buy_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared.log_messages.iter().any(|l| l.contains("Instruction: Buy"))
}

pub fn has_sell_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared.log_messages.iter().any(|l| l.contains("Instruction: Sell"))
}

fn decode_buy_event(
    data: &[u8],
    signature: &str,
    slot: u64,
    mint: &str,
    received_at: u64,
) -> Option<Trade> {
    if data.len() < AMM_EVENT_MIN_SIZE {
        return None;
    }
    
    let token_amount = u64::from_le_bytes(data[BUY_TOKEN_OFF..BUY_TOKEN_OFF + 8].try_into().ok()?);
    let sol_amount = u64::from_le_bytes(data[BUY_SOL_OFF..BUY_SOL_OFF + 8].try_into().ok()?);
    let pool_base = u64::from_le_bytes(data[BUY_POOL_BASE_OFF..BUY_POOL_BASE_OFF + 8].try_into().ok()?);
    let pool_quote = u64::from_le_bytes(data[BUY_POOL_QUOTE_OFF..BUY_POOL_QUOTE_OFF + 8].try_into().ok()?);
    let lp_fee = u64::from_le_bytes(data[72..80].try_into().ok()?);
    let protocol_fee = u64::from_le_bytes(data[88..96].try_into().ok()?);
    let fee = lp_fee.saturating_add(protocol_fee);

    let txn_timestamp_sec = i64::from_le_bytes(data[0..8].try_into().ok()?) as u64;
    let txn_timestamp = if (1577836800..=2524608000).contains(&txn_timestamp_sec) {
        txn_timestamp_sec * 1000
    } else {
        received_at / 1000
    };

    if data.len() < 176 {
        return None;
    }

    let pool = bs58::encode(&data[112..144]).into_string();
    let trader = bs58::encode(&data[144..176]).into_string();

    let price = if pool_base > 0 {
        (pool_quote as f64) / (pool_base as f64) / 1000.0
    } else {
        0.0
    };
    let market_cap = price * 1_000_000_000.0;
    let liquidity_sol = pool_quote as f64 / 1_000_000_000.0;

    Some(Trade {
        signature: signature.to_string(),
        slot,
        mint: mint.to_string(),
        trader,
        direction: TradeDirection::Buy,
        token_amount,
        sol_amount,
        virtual_sol_reserves: pool_quote,
        virtual_token_reserves: pool_base,
        real_sol_reserves: pool_quote,
        real_token_reserves: pool_base,
        price,
        market_cap,
        liquidity_sol,
        fee,
        is_create: false,
        is_bonding_curve: false,
        pool: Some(pool),
        suggested_compute_unit_limit: 300_000,
        token_decimals: 6,
        txn_timestamp,
        received_at,
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

fn decode_sell_event(
    data: &[u8],
    signature: &str,
    slot: u64,
    mint: &str,
    received_at: u64,
) -> Option<Trade> {
    if data.len() < AMM_EVENT_MIN_SIZE {
        return None;
    }
    let token_amount = u64::from_le_bytes(data[SELL_TOKEN_OFF..SELL_TOKEN_OFF + 8].try_into().ok()?);
    let sol_amount = u64::from_le_bytes(data[SELL_SOL_OFF..SELL_SOL_OFF + 8].try_into().ok()?);
    let pool_base = u64::from_le_bytes(data[SELL_POOL_BASE_OFF..SELL_POOL_BASE_OFF + 8].try_into().ok()?);
    let pool_quote = u64::from_le_bytes(data[SELL_POOL_QUOTE_OFF..SELL_POOL_QUOTE_OFF + 8].try_into().ok()?);
    let lp_fee = u64::from_le_bytes(data[72..80].try_into().ok()?);
    let protocol_fee = u64::from_le_bytes(data[88..96].try_into().ok()?);
    let fee = lp_fee.saturating_add(protocol_fee);

    let txn_timestamp_sec = i64::from_le_bytes(data[0..8].try_into().ok()?) as u64;
    let txn_timestamp = if (1577836800..=2524608000).contains(&txn_timestamp_sec) {
        txn_timestamp_sec * 1000
    } else {
        received_at / 1000
    };

    if data.len() < 176 {
        return None;
    }

    let pool = bs58::encode(&data[112..144]).into_string();
    let trader = bs58::encode(&data[144..176]).into_string();

    let price = if pool_base > 0 {
        (pool_quote as f64) / (pool_base as f64) / 1000.0
    } else {
        0.0
    };
    let market_cap = price * 1_000_000_000.0;
    let liquidity_sol = pool_quote as f64 / 1_000_000_000.0;

    Some(Trade {
        signature: signature.to_string(),
        slot,
        mint: mint.to_string(),
        trader,
        direction: TradeDirection::Sell,
        token_amount,
        sol_amount,
        virtual_sol_reserves: pool_quote,
        virtual_token_reserves: pool_base,
        real_sol_reserves: pool_quote,
        real_token_reserves: pool_base,
        price,
        market_cap,
        liquidity_sol,
        fee,
        is_create: false,
        is_bonding_curve: false,
        pool: Some(pool),
        suggested_compute_unit_limit: 300_000,
        token_decimals: 6,
        txn_timestamp,
        received_at,
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

fn extract_mint_from_ix(ix: &InstructionUpdate) -> Option<String> {
    if let Some(mint) = ix.accounts.get(3).map(|p| bs58::encode(&p.0).into_string()) {
        return Some(mint);
    }

    let token_program = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
    let token_2022_program = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
    
    for inner_ix in &ix.inner {
        let program_str = bs58::encode(&inner_ix.program.0).into_string();
        if program_str == token_program || program_str == token_2022_program {
            if let Some(mint) = inner_ix.accounts.get(2).map(|p| bs58::encode(&p.0).into_string()) {
                log::debug!("Extracted mint from inner Token Program TransferChecked: {}", mint);
                return Some(mint);
            }
        }
    }

    log::warn!(
        "Failed to extract mint: accounts.len()={}, inner.len()={}, program={}",
        ix.accounts.len(),
        ix.inner.len(),
        bs58::encode(&ix.program.0).into_string()
    );
    None
}

pub fn parse_amm_trade_events(ix: &InstructionUpdate, received_at: u64) -> Vec<Trade> {
    use base64::Engine;
    let decoder = base64::engine::general_purpose::STANDARD;
    let signature = bs58::encode(ix.shared.signature.as_slice()).into_string();

    if ix.accounts.len() < 4 {
        return vec![];
    }

    let mint = match extract_mint_from_ix(ix) {
        Some(m) => m,
        None => return vec![],
    };

    let is_buy = has_buy_in_logs(ix);
    let is_sell = has_sell_in_logs(ix);
    if !is_buy && !is_sell {
        return vec![];
    }

    let mut out = Vec::new();
    for log in &ix.shared.log_messages {
        let Some(data_str) = log.strip_prefix("Program data: ") else {
            continue;
        };
        let Ok(data) = decoder.decode(data_str.trim()) else {
            continue;
        };
        if data.len() < 8 + AMM_EVENT_MIN_SIZE {
            continue;
        };
        let payload = &data[8..];

        if is_buy {
            if let Some(t) = decode_buy_event(payload, &signature, ix.shared.slot, &mint, received_at) {
                out.push(t);
                break;
            }
        }
        if is_sell {
            if let Some(t) = decode_sell_event(payload, &signature, ix.shared.slot, &mint, received_at) {
                out.push(t);
                break;
            }
        }
    }
    out
}
