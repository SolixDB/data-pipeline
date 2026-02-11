use yellowstone_vixen_core::instruction::InstructionUpdate;
use crate::events::{Migration, Trade, TradeDirection};

const AMM_EVENT_MIN_SIZE: usize = 144 + 32;

/// Anchor event discriminator: first 8 bytes of sha256("event:CreatePoolEvent").
const CREATE_POOL_EVENT_DISCRIMINATOR: [u8; 8] = [0xb1, 0x31, 0x0c, 0xd2, 0xa0, 0x76, 0xa7, 0x74];
/// createPoolEvent min size: discriminator(8) + timestamp(8) + index(2) + creator(32) + baseMint(32) + quoteMint(32) + decimals(2) + 6*u64(48) + poolBump(1) + pool(32).
const CREATE_POOL_EVENT_MIN_SIZE: usize = 8 + 8 + 2 + 32 + 32 + 32 + 2 + 48 + 1 + 32;
/// Full createPoolEvent: above + lpMint(32) + userBase(32) + userQuote(32) + coinCreator(32) + isMayhemMode(1) => data.len() >= 334.
const CREATE_POOL_EVENT_FULL_SIZE: usize = 8 + 326;
const BUY_TOKEN_OFF: usize = 8;
const BUY_SOL_OFF: usize = 56;
const BUY_POOL_BASE_OFF: usize = 40;
const BUY_POOL_QUOTE_OFF: usize = 48;
const SELL_TOKEN_OFF: usize = 8;
const SELL_SOL_OFF: usize = 56;
const SELL_POOL_BASE_OFF: usize = 40;
const SELL_POOL_QUOTE_OFF: usize = 48;

/// Anchor event discriminator: first 8 bytes of sha256("event:buyEvent").
const BUY_EVENT_DISCRIMINATOR: [u8; 8] = [239, 95, 203, 206, 161, 114, 144, 248];
/// Anchor event discriminator: first 8 bytes of sha256("event:sellEvent").
const SELL_EVENT_DISCRIMINATOR: [u8; 8] = [38, 126, 39, 120, 33, 101, 62, 244];

fn program_data_payload(log_line: &str) -> Option<&str> {
    const PREFIX: &str = "Program data: ";
    let line = log_line.trim();
    line.find(PREFIX)
        .map(|i| &line[i + PREFIX.len()..])
        .map(|s| s.trim())
}

pub fn has_buy_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared.log_messages.iter().any(|l| {
        l.contains("Instruction: Buy") || l.contains("Instruction: BuyExactQuoteIn")
    })
}

pub fn has_sell_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared.log_messages.iter().any(|l| l.contains("Instruction: Sell"))
}

pub fn has_create_pool_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared
        .log_messages
        .iter()
        .any(|l| l.contains("Instruction: CreatePool"))
}

/// createPoolEvent layout (after 8-byte discriminator): timestamp(8), index(2), creator(32), baseMint(32), quoteMint(32),
/// baseMintDecimals(1), quoteMintDecimals(1), baseAmountIn(8), quoteAmountIn(8), poolBaseAmount(8), poolQuoteAmount(8),
/// minimumLiquidity(8), initialLiquidity(8), lpTokenAmountOut(8), poolBump(1), pool(32), lpMint(32),
/// userBaseTokenAccount(32), userQuoteTokenAccount(32), coinCreator(32), isMayhemMode(1).
fn decode_create_pool_event(
    data: &[u8],
    signature: &str,
    slot: u64,
    received_at: u64,
) -> Option<Migration> {
    if data.len() < CREATE_POOL_EVENT_MIN_SIZE {
        return None;
    }
    let payload = &data[8..];
    let timestamp_sec = i64::from_le_bytes(payload[0..8].try_into().ok()?) as u64;
    let txn_timestamp = if (1577836800..=2524608000).contains(&timestamp_sec) {
        timestamp_sec * 1000
    } else {
        received_at / 1000
    };
    let creator = bs58::encode(&payload[10..42]).into_string();
    let base_mint = bs58::encode(&payload[42..74]).into_string();
    let base_amount_in = u64::from_le_bytes(payload[108..116].try_into().ok()?);
    let quote_amount_in = u64::from_le_bytes(payload[116..124].try_into().ok()?);
    let pool = bs58::encode(&payload[165..197]).into_string();

    let (quote_mint, lp_mint, coin_creator, is_mayhem_mode, pool_base_amount, pool_quote_amount, initial_liquidity) =
        if data.len() >= CREATE_POOL_EVENT_FULL_SIZE {
            let quote_mint = Some(bs58::encode(&payload[74..106]).into_string());
            let pool_base_amount = Some(u64::from_le_bytes(payload[124..132].try_into().ok()?));
            let pool_quote_amount = Some(u64::from_le_bytes(payload[132..140].try_into().ok()?));
            let initial_liquidity = Some(u64::from_le_bytes(payload[148..156].try_into().ok()?));
            let lp_mint = Some(bs58::encode(&payload[197..229]).into_string());
            let coin_creator = Some(bs58::encode(&payload[293..325]).into_string());
            let is_mayhem_mode = Some(payload.get(325).copied().unwrap_or(0) != 0);
            (quote_mint, lp_mint, coin_creator, is_mayhem_mode, pool_base_amount, pool_quote_amount, initial_liquidity)
        } else {
            (None, None, None, None, None, None, None)
        };

    let bonding_curve_state = Some(crate::events::BondingCurveState {
        virtual_sol_reserves: 0,
        virtual_token_reserves: 0,
        real_sol_reserves: quote_amount_in,
        real_token_reserves: 0,
    });
    Some(Migration {
        signature: signature.to_string(),
        slot,
        mint: base_mint,
        user: creator,
        bonding_curve: String::new(),
        pool,
        token_amount: base_amount_in,
        sol_amount: quote_amount_in,
        fee: 0,
        txn_timestamp,
        received_at,
        sent_at: 0,
        bonding_curve_state,
        quote_mint,
        lp_mint,
        coin_creator,
        is_mayhem_mode,
        pool_base_amount,
        pool_quote_amount,
        initial_liquidity,
    })
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
        is_dev_buy: false,
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
        is_dev_buy: false,
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

/// Robust order: 1) buyEvent/sellEvent from logs, 2) Geyser ix data, 3) fallback to program logs.
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

    // 1. buyEvent / sellEvent — from program logs (primary; validate discriminator).
    for log in &ix.shared.log_messages {
        let data_str = match program_data_payload(log) {
            Some(s) => s,
            None => continue,
        };
        let data = match decoder.decode(data_str) {
            Ok(d) => d,
            Err(_) => continue,
        };
        if data.len() < 8 + AMM_EVENT_MIN_SIZE {
            continue;
        }
        let disc = &data[0..8];
        let payload = &data[8..];
        if disc == BUY_EVENT_DISCRIMINATOR && is_buy {
            if let Some(t) = decode_buy_event(payload, &signature, ix.shared.slot, &mint, received_at) {
                out.push(t);
                return out;
            }
        }
        if disc == SELL_EVENT_DISCRIMINATOR && is_sell {
            if let Some(t) = decode_sell_event(payload, &signature, ix.shared.slot, &mint, received_at) {
                out.push(t);
                return out;
            }
        }
    }

    // 2. Geyser ix data — would require Buy/Sell instruction layout; skip for AMM (events are reliable).
    // (Optional: add try_parse_amm_trade_from_ix_data if needed.)

    // 3. Fallback — program logs without strict discriminator.
    for log in &ix.shared.log_messages {
        let data_str = match program_data_payload(log) {
            Some(s) => s,
            None => continue,
        };
        let data = match decoder.decode(data_str) {
            Ok(d) => d,
            Err(_) => continue,
        };
        if data.len() < 8 + AMM_EVENT_MIN_SIZE {
            continue;
        }
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

/// Parse migration (Create Pool) from pump_amm: 1) createPoolEvent from logs, 2) Geyser createPool instruction accounts.
pub fn parse_migration_events_from_amm(
    ix: &InstructionUpdate,
    received_at: u64,
) -> Vec<Migration> {
    use base64::Engine;
    let decoder = base64::engine::general_purpose::STANDARD;
    let signature = bs58::encode(ix.shared.signature.as_slice()).into_string();
    let mut out = Vec::new();

    // 1. Event / logs — createPoolEvent in program logs.
    for log in &ix.shared.log_messages {
        let data_str = match program_data_payload(log) {
            Some(s) => s,
            None => continue,
        };
        let data = match decoder.decode(data_str) {
            Ok(d) => d,
            Err(_) => continue,
        };
        if data.len() < CREATE_POOL_EVENT_MIN_SIZE {
            continue;
        }
        let disc = &data[0..8];
        if disc != CREATE_POOL_EVENT_DISCRIMINATOR {
            continue;
        }
        if let Some(migration) =
            decode_create_pool_event(&data, &signature, ix.shared.slot, received_at)
        {
            out.push(migration);
            return out;
        }
    }

    // 2. Geyser instruction (fallback when event didn't decode) — createPool accounts: pool=0, creator=2, baseMint=3.
    if has_create_pool_in_logs(ix) && ix.accounts.len() >= 4 {
        let pool = bs58::encode(&ix.accounts[0].0).into_string();
        let creator = bs58::encode(&ix.accounts[2].0).into_string();
        let base_mint = bs58::encode(&ix.accounts[3].0).into_string();
        let txn_timestamp = received_at / 1000;
        out.push(Migration {
            signature: signature.clone(),
            slot: ix.shared.slot,
            mint: base_mint,
            user: creator,
            bonding_curve: String::new(),
            pool,
            token_amount: 0,
            sol_amount: 0,
            fee: 0,
            txn_timestamp,
            received_at,
            sent_at: 0,
            bonding_curve_state: None,
            quote_mint: None,
            lp_mint: None,
            coin_creator: None,
            is_mayhem_mode: None,
            pool_base_amount: None,
            pool_quote_amount: None,
            initial_liquidity: None,
        });
    }
    out
}
