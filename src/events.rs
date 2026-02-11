use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BondingCurveState {
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpProtocolConstants {
    pub token_decimals: u8,
    pub program_ids: PumpProgramIds,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpProgramIds {
    pub pump_fun: String,
    pub pump_fun_fee_program: String,
    pub pump_amm: String,
    pub associated_token_program: String,
    pub token_program: String,
    pub metaplex_metadata: String,
    pub system_program: String,
    pub wsol_mint: String,
}

impl Default for PumpProtocolConstants {
    fn default() -> Self {
        Self {
            token_decimals: 6,
            program_ids: PumpProgramIds {
                pump_fun: "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P".to_string(),
                pump_fun_fee_program: "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ".to_string(),
                pump_amm: "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_string(),
                associated_token_program: "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL".to_string(),
                token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
                metaplex_metadata: "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s".to_string(),
                system_program: "11111111111111111111111111111111".to_string(),
                wsol_mint: "So11111111111111111111111111111111111111112".to_string(),
            },
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PumpFunBondingCurveAccounts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bonding_curve: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub associated_bonding_curve: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub global: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_recipient: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator_vault: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_config: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_authority: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenLaunch {
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
    pub pump_accounts: PumpFunBondingCurveAccounts,
    #[serde(default = "default_token_decimals")]
    pub token_decimals: u8,
    pub initial_bonding_curve_state: Option<BondingCurveState>,
    #[serde(default)]
    pub pump_constants: PumpProtocolConstants,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<crate::security::MintSecurity>,
    #[serde(default)]
    pub is_token_2022: bool,
    pub txn_timestamp: u64,
    pub received_at: u64,
    pub sent_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_total_supply: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_program: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_mayhem_mode: Option<bool>,
}

fn default_token_decimals() -> u8 {
    6
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TradeDirection {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub signature: String,
    pub slot: u64,
    pub mint: String,
    pub trader: String,
    pub direction: TradeDirection,
    pub token_amount: u64,
    pub sol_amount: u64,
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub price: f64,
    pub market_cap: f64,
    pub liquidity_sol: f64,
    pub fee: u64,
    pub is_create: bool,
    #[serde(default)]
    pub is_dev_buy: bool,
    #[serde(default = "default_true")]
    pub is_bonding_curve: bool,
    #[serde(default)]
    pub pool: Option<String>,
    #[serde(default = "default_compute_unit_limit")]
    pub suggested_compute_unit_limit: u32,
    #[serde(default = "default_token_decimals")]
    pub token_decimals: u8,
    pub txn_timestamp: u64,
    pub volume_sol: f64,
    pub volume_5m: f64,
    pub volume_1h: f64,
    pub volume_6h: f64,
    pub tx_count: u64,
    pub tx_count_5m: u64,
    pub tx_count_1h: u64,
    pub tx_count_6h: u64,
    #[serde(default)]
    pub price_change_5m: f64,
    #[serde(default)]
    pub price_change_1h: f64,
    #[serde(default)]
    pub price_change_6h: f64,
    #[serde(default)]
    pub price_change_24h: f64,
    pub total_fees_sol: f64,
    pub received_at: u64,
    pub sent_at: u64,
}

fn default_compute_unit_limit() -> u32 {
    300_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    pub signature: String,
    pub slot: u64,
    pub mint: String,
    pub user: String,
    pub bonding_curve: String,
    pub pool: String,
    pub token_amount: u64,
    pub sol_amount: u64,
    pub fee: u64,
    pub txn_timestamp: u64,
    pub received_at: u64,
    pub sent_at: u64,
    /// Final bonding curve state at migration (curve drained: virtual_* = 0, real_* from event amounts). Lets frontend show 100% complete.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bonding_curve_state: Option<BondingCurveState>,
    /// From pump_amm createPoolEvent when present: quote mint (e.g. WSOL), lp_mint, coin creator, mayhem flag, pool amounts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_mint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lp_mint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coin_creator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_mayhem_mode: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_base_amount: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_quote_amount: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_liquidity: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ohclv {
    pub mint: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub start_time: u64,
    pub duration: u64,
}
