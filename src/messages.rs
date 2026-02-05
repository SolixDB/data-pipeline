use serde::{Deserialize, Serialize};

/// Minimal token launch payload sent over WebSocket (no timing fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenLaunchPayload {
    pub signature: String,
    pub slot: u64,
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedInstructionPayload {
    pub parser_name: String,
    pub slot: u64,
    pub parsed: String,
    /// Program execution logs from the transaction (e.g. "Program log: ...").
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
