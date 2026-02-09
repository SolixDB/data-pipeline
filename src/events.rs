use serde::{Deserialize, Serialize};

/// Bonding curve reserve state (virtual and real). Affects price and slippage for buy/sell.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BondingCurveState {
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,
}

/// Protocol constants so frontend/backend can build tx without hardcoding. Sent with token_launch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpProtocolConstants {
    /// Token decimals for Pump.fun tokens (always 6).
    pub token_decimals: u8,
    /// Program IDs (base58).
    pub program_ids: PumpProgramIds,
}

/// Program IDs used in Pump.fun Create/Buy/Sell and AMM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpProgramIds {
    pub pump_fun: String,
    pub pump_fun_fee_program: String,
    pub pump_amm: String,
    pub associated_token_program: String,
    pub token_program: String,
    pub metaplex_metadata: String,
    pub system_program: String,
    /// Default quote mint (wSOL) for AMM.
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
    /// Bonding curve PDA: seeds ["bonding-curve", mint], program = Pump.fun
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bonding_curve: Option<String>,
    /// Associated bonding curve (token account) for the bonding curve PDA.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub associated_bonding_curve: Option<String>,
    /// Global config PDA: seeds ["global"], program = Pump.fun.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub global: Option<String>,
    /// Fee recipient (from Global account). Filled when same-tx Buy instruction is present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_recipient: Option<String>,
    /// Metaplex metadata PDA for this mint (for Create flow).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    /// Creator vault PDA: seeds ["creator-vault", creator], program = Pump.fun
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator_vault: Option<String>,
    /// Fee config PDA: seeds ["fee_config", pump_fun_program_id], program = Pump fee program
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_config: Option<String>,
    /// Event authority PDA: seeds ["__event_authority"], program = Pump.fun.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_authority: Option<String>,
}

/// Token launch event (pump.fun Create).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenLaunch {
    pub signature: String,
    pub slot: u64,
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: String,
    /// PDAs/accounts for this token (bonding curve). Enables tx building without extra RPC.
    #[serde(default)]
    pub pump_accounts: PumpFunBondingCurveAccounts,
    /// Token decimals (Pump.fun uses 6). Needed for amount display and tx building.
    #[serde(default = "default_token_decimals")]
    pub token_decimals: u8,
    /// Initial curve state after create (from first trade in same tx). Enables price/slippage and follow-up buys.
    pub initial_bonding_curve_state: Option<BondingCurveState>,
    /// Protocol constants (program IDs, decimals). So frontend does not need to hardcode.
    #[serde(default)]
    pub pump_constants: PumpProtocolConstants,
    /// On-chain transaction timestamp (ms since epoch)
    pub txn_timestamp: u64,
    /// Server received from gRPC timestamp (μs since epoch)
    pub received_at: u64,
    /// Server sent to WebSocket timestamp (μs since epoch)
    pub sent_at: u64,
}

fn default_token_decimals() -> u8 {
    6
}

fn default_true() -> bool {
    true
}

/// Trade direction.
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
    /// Internal flag to distinguish bonding curve trades from AMM trades
    #[serde(default = "default_true")]
    pub is_bonding_curve: bool,
    /// Set for AMM (pump_amm) trades after migration; None for bonding-curve trades.
    #[serde(default)]
    pub pool: Option<String>,
    /// Suggested compute unit limit for a follow-up buy/sell (Pump.fun typically 300_000).
    #[serde(default = "default_compute_unit_limit")]
    pub suggested_compute_unit_limit: u32,
    /// Token decimals (6 for Pump.fun). For amount display and tx building.
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
}
