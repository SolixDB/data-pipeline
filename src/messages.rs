use serde::{Deserialize, Serialize};

/// Token launch payload sent over WebSocket. Includes all data needed to build Create/Buy/Sell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenLaunchPayload {
    pub signature: String,
    pub slot: u64,
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: String,
    /// Pump.fun PDAs and fee_recipient (bonding curve). See docs/pumpfun_accounts.md.
    #[serde(default)]
    pub pump_accounts: crate::events::PumpFunBondingCurveAccounts,
    /// Token decimals (6 for Pump.fun). For amounts and tx building.
    #[serde(default)]
    pub token_decimals: u8,
    /// Initial curve state after create (from first trade in same tx). For price/slippage.
    pub initial_bonding_curve_state: Option<crate::events::BondingCurveState>,
    /// Program IDs and constants so frontend does not hardcode.
    #[serde(default)]
    pub pump_constants: crate::events::PumpProtocolConstants,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum ServerMessage {
    Connected(ConnectedPayload),
    Error(ErrorPayload),
    SlotUpdate(u64),
    ParsedInstruction(ParsedInstructionPayload),
    TokenLaunch(TokenLaunchPayload),
    Trade(crate::events::Trade),
    Migration(crate::events::Migration),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedInstructionPayload {
    pub parser_name: String,
    pub slot: u64,
    pub parsed: String,
    pub program_logs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectedPayload {
    pub client_id: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub code: String,
    pub message: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum ClientMessage {
    Ping(PingPayload),
    Subscribe(SubscribePayload),
    Unsubscribe(UnsubscribePayload),
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingPayload {
    pub timestamp: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribePayload {
    pub channel: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubscribePayload {
    pub channel: String,
}
