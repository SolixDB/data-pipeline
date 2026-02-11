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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<crate::security::MintSecurity>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum ServerMessage {
    Connected(ConnectedPayload),
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
#[serde(rename_all = "snake_case")]
pub enum ClientMessage {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}
