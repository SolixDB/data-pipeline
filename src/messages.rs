use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenLaunchPayload {
    pub signature: String,
    pub slot: u64,
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: String,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub pump_accounts: crate::events::PumpFunBondingCurveAccounts,
    #[serde(default)]
    pub token_decimals: u8,
    pub initial_bonding_curve_state: Option<crate::events::BondingCurveState>,
    #[serde(default)]
    pub pump_constants: crate::events::PumpProtocolConstants,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub market_cap: f64,
    #[serde(default)]
    pub liquidity_sol: f64,
    #[serde(default)]
    pub txn_timestamp: u64,
    #[serde(default)]
    pub is_token_2022: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_total_supply: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_program: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_mayhem_mode: Option<bool>,
}

/// Envelope for every WebSocket message so clients can detect gaps (seq jumps).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerMessageEnvelope {
    #[serde(flatten)]
    pub message: ServerMessage,
    /// Monotonic sequence number per connection; if it jumps by more than 1, client should treat as gap and recover (reconnect + snapshot or backfill).
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum ServerMessage {
    Connected(ConnectedPayload),
    /// Sent once before snapshot_* on connect; client can store as_of_slot for gap detection.
    SnapshotInfo(SnapshotInfoPayload),
    TokenLaunch(TokenLaunchPayload),
    Trade(crate::events::Trade),
    Migration(crate::events::Migration),
    SnapshotMigrated(Vec<crate::storage::MigratedEntry>),
    SnapshotSoonMigrating(Vec<crate::storage::SoonMigratingEntry>),
    SnapshotLaunches(Vec<TokenLaunchPayload>),
    SnapshotTrades(Vec<crate::events::Trade>),
    SnapshotTickers(Vec<TokenTicker>),
    Ohclv(crate::events::Ohclv),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfoPayload {
    /// Snapshot state is as of this slot; any later message with slot > as_of_slot is a delta.
    pub as_of_slot: u64,
    /// Sequence number of this snapshot batch (first seq in the snapshot stream).
    pub snapshot_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenTicker {
    pub mint: String,
    pub price: f64,
    pub market_cap: f64,
    pub liquidity_sol: f64,
    #[serde(flatten)]
    pub volume_metrics: crate::storage::VolumeMetrics,
    #[serde(default)]
    pub total_fees_sol: f64,
    pub last_slot: u64,
    pub migrated_to_pool: Option<String>,
}

impl From<&crate::storage::TokenState> for TokenTicker {
    fn from(s: &crate::storage::TokenState) -> Self {
        Self {
            mint: s.mint.clone(),
            price: s.price,
            market_cap: s.market_cap,
            liquidity_sol: s.liquidity_sol,
            volume_metrics: s.volume_metrics.clone(),
            total_fees_sol: s.total_fees_sol,
            last_slot: s.last_slot,
            migrated_to_pool: s.migrated_to_pool.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectedPayload {
    pub client_id: u64,
    /// Recommended topics for list view: subscribe to these to get launches and tickers without full trade firehose (reduces drop risk).
    #[serde(default)]
    pub recommended_topics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
#[serde(rename_all = "snake_case")]
pub enum ClientMessage {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    /// Cumulative ACK: client has received all messages up to and including this seq. Server uses this for selective repeat (retransmit only to sockets that didn't ACK).
    Ack(u64),
}
