use std::collections::{HashMap, HashSet};
use yellowstone_vixen_core::instruction::InstructionUpdate;

use crate::protocols;

pub fn build_parser_map() -> HashMap<Vec<u8>, &'static str> {
    let mut map = HashMap::new();
    map.insert(
        bs58::decode("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")
            .into_vec()
            .unwrap(),
        "pump_amm",
    );
    map.insert(
        bs58::decode("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
            .into_vec()
            .unwrap(),
        "pump_fun",
    );
    map
}

fn flatten_instructions<'a>(
    instructions: &'a [InstructionUpdate],
    out: &mut Vec<&'a InstructionUpdate>,
) {
    for ix in instructions {
        out.push(ix);
        flatten_instructions(&ix.inner, out);
    }
}

pub fn parse_token_launches(
    instructions: &[InstructionUpdate],
    parser_map: &HashMap<Vec<u8>, &'static str>,
    received_at: u64,
) -> Vec<crate::events::TokenLaunch> {
    let mut flat = Vec::new();
    flatten_instructions(instructions, &mut flat);
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    // 1. CreateEvent — scan tx logs for createEvent (primary; most robust, no Geyser instruction dependency).
    // Use transaction-level logs from any instruction (all share the same log_messages) so we don't miss
    // creates when no pump_fun instruction is present in the flattened list (e.g. subscription filter).
    if let Some(any_ix) = flat.first() {
        if let Some(launch) = protocols::pump_fun::parse_create_from_tx_logs(
            &any_ix.shared.log_messages,
            &any_ix.shared.signature,
            any_ix.shared.slot,
            received_at,
        ) {
            if seen.insert((launch.signature.clone(), launch.mint.clone())) {
                out.push(launch);
            }
        }
    }

    // 2. Create/CreateV2 instruction + 3. Fallback for logs — per-instruction (enriches abc/g when available).
    for ix in flat {
        let program_bytes: &[u8] = ix.program.as_ref();
        if parser_map.get(program_bytes) == Some(&"pump_fun") {
            if let Some(launch) = protocols::pump_fun::parse_create(ix, received_at) {
                if seen.insert((launch.signature.clone(), launch.mint.clone())) {
                    out.push(launch);
                }
            }
        }
    }

    out
}

/// Trades: 1) TradeEvent (pump_fun) / buyEvent/sellEvent (pump_amm) from logs, 2) Geyser ix data, 3) fallback logs.
pub fn parse_trade_events(
    instructions: &[InstructionUpdate],
    parser_map: &HashMap<Vec<u8>, &'static str>,
    received_at: u64,
) -> Vec<crate::events::Trade> {
    let mut flat = Vec::new();
    flatten_instructions(instructions, &mut flat);
    let mut out = Vec::new();
    for ix in flat {
        let program_bytes: &[u8] = ix.program.as_ref();
        if parser_map.get(program_bytes) == Some(&"pump_fun") {
            if protocols::pump_fun::has_trade_in_logs(ix) {
                out.extend(protocols::pump_fun::parse_trade_events(ix, received_at));
            }
            continue;
        }
        if parser_map.get(program_bytes) == Some(&"pump_amm") {
            if protocols::pump_amm::has_buy_in_logs(ix) || protocols::pump_amm::has_sell_in_logs(ix) {
                out.extend(protocols::pump_amm::parse_amm_trade_events(ix, received_at));
            }
        }
    }
    out
}

pub fn parse_migration_events(
    instructions: &[InstructionUpdate],
    parser_map: &HashMap<Vec<u8>, &'static str>,
    received_at: u64,
) -> Vec<crate::events::Migration> {
    let mut flat = Vec::new();
    flatten_instructions(instructions, &mut flat);
    let mut out = Vec::new();
    let mut seen_mints = HashSet::new();
    for ix in flat {
        let program_bytes: &[u8] = ix.program.as_ref();
        // 1. pump_fun: Migrate event (logs)
        if parser_map.get(program_bytes) == Some(&"pump_fun") {
            if protocols::pump_fun::has_migrate_in_logs(ix) {
                for m in protocols::pump_fun::parse_migration_events(ix, received_at) {
                    if seen_mints.insert(m.mint.clone()) {
                        out.push(m);
                    }
                }
            }
            continue;
        }
        // 2. pump_amm: Create Pool event + Geyser instruction fallback (same tx may have both)
        if parser_map.get(program_bytes) == Some(&"pump_amm") {
            if protocols::pump_amm::has_create_pool_in_logs(ix) {
                for m in protocols::pump_amm::parse_migration_events_from_amm(ix, received_at) {
                    if seen_mints.insert(m.mint.clone()) {
                        out.push(m);
                    }
                }
            }
        }
    }
    out
}

pub fn enrich_token_launches(
    instructions: &[InstructionUpdate],
    parser_map: &HashMap<Vec<u8>, &'static str>,
    launches: &mut [crate::events::TokenLaunch],
    trades: &[crate::events::Trade],
) {
    let mut flat = Vec::new();
    flatten_instructions(instructions, &mut flat);

    for launch in launches.iter_mut() {
        if launch.pump_accounts.fee_recipient.is_none() {
            for ix in &flat {
                let ix_sig = bs58::encode(ix.shared.signature.as_slice()).into_string();
                if ix_sig != launch.signature {
                    continue;
                }
                let program_bytes: &[u8] = ix.program.as_ref();
                if parser_map.get(program_bytes) != Some(&"pump_fun") {
                    continue;
                }
                if !protocols::pump_fun::has_trade_in_logs(ix) {
                    continue;
                }
                let ix_mint = ix
                    .accounts
                    .get(2)
                    .map(|p| bs58::encode(&p.0).into_string());
                if ix_mint.as_ref() != Some(&launch.mint) {
                    continue;
                }
                if let Some(fee_recipient) = ix.accounts.get(1).map(|p| bs58::encode(&p.0).into_string()) {
                    launch.pump_accounts.fee_recipient = Some(fee_recipient);
                }
                break;
            }
        }

        if launch.initial_bonding_curve_state.is_none() {
            if let Some(t) = trades.iter().find(|t| t.signature == launch.signature && t.mint == launch.mint) {
                launch.initial_bonding_curve_state = Some(crate::events::BondingCurveState {
                    virtual_sol_reserves: t.virtual_sol_reserves,
                    virtual_token_reserves: t.virtual_token_reserves,
                    real_sol_reserves: t.real_sol_reserves,
                    real_token_reserves: t.real_token_reserves,
                });
            }
        }
    }
}