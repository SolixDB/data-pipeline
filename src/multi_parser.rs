use std::collections::{HashMap, HashSet};
use yellowstone_vixen_core::instruction::InstructionUpdate;

/// tradeEvent layout (after 8-byte discriminator): mint(32) + sol(8) + token(8) + is_buy(1) + user(32) + timestamp(8) + ...
const TRADE_EVENT_TIMESTAMP_OFFSET: usize = 8 + 32 + 8 + 8 + 1 + 32;
/// Min size so we only accept actual tradeEvent logs (same as grpc-data-stream TRADE_EVENT_MIN_SIZE)
const TRADE_EVENT_MIN_SIZE: usize = 8 + 32 + 8 + 8 + 1 + 32 + 8 + 8 + 8 + 8 + 8 + 32 + 8 + 8;

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

fn has_create_in_logs(ix: &InstructionUpdate) -> bool {
    ix.shared.log_messages.iter().any(|l| {
        l.contains("Instruction: Create")
            && !l.contains("CreateToken")
            && !l.contains("CreateIdempotent")
    })
}

/// Extract on-chain txn_timestamp (ms) from the first tradeEvent in "Program data: " logs.
/// Same as grpc-data-stream: Create tx usually has an initial buy trade event with real timestamp.
fn txn_timestamp_from_create_logs(ix: &InstructionUpdate) -> Option<u64> {
    use base64::Engine;
    let decoder = base64::engine::general_purpose::STANDARD;
    for log in &ix.shared.log_messages {
        let data_str = log.strip_prefix("Program data: ")?;
        let data = decoder.decode(data_str.trim()).ok()?;
        if data.len() < TRADE_EVENT_MIN_SIZE {
            continue;
        }
        let txn_timestamp_sec =
            u64::from_le_bytes(data[TRADE_EVENT_TIMESTAMP_OFFSET..TRADE_EVENT_TIMESTAMP_OFFSET + 8].try_into().ok()?);
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

fn parse_pump_fun_create(
    ix: &InstructionUpdate,
    received_at: u64,
) -> Option<crate::events::TokenLaunch> {
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
    let signature = bs58::encode(ix.shared.signature.as_slice()).into_string();
    let txn_timestamp = txn_timestamp_from_create_logs(ix).unwrap_or(received_at / 1000);
    Some(crate::events::TokenLaunch {
        signature,
        slot: ix.shared.slot,
        mint,
        name,
        symbol,
        uri,
        creator,
        txn_timestamp,
        received_at,
        sent_at: 0,
    })
}

pub fn parse_token_launches(
    instructions: &[InstructionUpdate],
    parser_map: &HashMap<Vec<u8>, &'static str>,
    received_at: u64,
) -> Vec<crate::events::TokenLaunch> {
    let mut flat = Vec::new();
    flatten_instructions(instructions, &mut flat);
    let mut out = Vec::new();
    for ix in flat {
        let program_bytes: &[u8] = ix.program.as_ref();
        if let Some(&"pump_fun") = parser_map.get(program_bytes) {
            if let Some(launch) = parse_pump_fun_create(ix, received_at) {
                out.push(launch);
            }
        }
    }
    let mut seen = HashSet::new();
    out.into_iter()
        .filter(|l| seen.insert((l.signature.clone(), l.mint.clone())))
        .collect()
}
