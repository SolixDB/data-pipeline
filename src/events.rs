//! Event types for the data stream.
//!
//! These are the canonical events emitted and sent to clients.
//! Format matches grpc-data-stream.

use serde::{Deserialize, Serialize};

/// Token launch event - emitted when a new token is created
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenLaunch {
    pub signature: String,
    pub slot: u64,
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: String,
    /// On-chain transaction timestamp (ms since epoch)
    pub txn_timestamp: u64,
    /// Server received from gRPC timestamp (μs since epoch)
    pub received_at: u64,
    /// Server sent to WebSocket timestamp (μs since epoch)
    pub sent_at: u64,
}
