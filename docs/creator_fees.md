# creator_fees — Cumulative Fee Tracking

**Added in:** Latest update  
**Purpose:** Track total fees earned by token creators from all trades

---

## What It Is

A new field `total_fees_sol` in `TokenState` that accumulates all fees from both pump.fun bonding curve and pump_amm trades.

---

## Backend Implementation

### TokenState (SnapshotTickers)

```rust
pub struct TokenState {
    pub mint: String,
    pub price: f64,
    pub market_cap: f64,
    pub liquidity_sol: f64,
    
    // Time-windowed volumes (5m, 1h, 6h, 24h)
    pub volume_5m: f64,
    pub volume_1h: f64,
    pub volume_6h: f64,
    pub volume_24h: f64,
    pub tx_count_5m: u64,
    pub tx_count_1h: u64,
    pub tx_count_6h: u64,
    pub tx_count_24h: u64,
    
    // NEW: Cumulative creator fees (all-time)
    pub total_fees_sol: f64,  // ← Use this
    
    pub last_slot: u64,
}
```

---

## Frontend Integration

### TypeScript Interface

```typescript
interface TokenState {
  mint: string;
  price: number;
  marketCap: number;
  liquiditySol: number;
  
  // Time-windowed volumes
  volume_5m: number;
  volume_1h: number;
  volume_6h: number;
  volume_24h: number;
  
  // Time-windowed tx counts
  tx_count_5m: number;
  tx_count_1h: number;
  tx_count_6h: number;
  tx_count_24h: number;
  
  // Cumulative creator fees (all-time)
  total_fees_sol: number;  // ← New field
  
  lastSlot: number;
}
```

### Display Example

```tsx
// Show creator earnings
<div className="creator-fees">
  <label>Creator Fees:</label>
  <span>{token.total_fees_sol.toFixed(2)} SOL</span>
  <span className="text-muted">
    ${(token.total_fees_sol * solPrice).toFixed(2)}
  </span>
</div>
```

---

## Key Characteristics

| Property | Value |
|----------|-------|
| **Type** | `f64` (SOL, not lamports) |
| **Scope** | All-time cumulative |
| **Sources** | pump.fun + pump_amm |
| **Resets** | Never (keeps accumulating) |
| **Persisted** | Yes (Redis) |

---

## Comparison: Volume vs Fees

| Metric | Volume | Fees |
|--------|--------|------|
| **What** | Trading activity | Creator earnings |
| **Windows** | 5m, 1h, 6h, 24h | All-time |
| **Resets** | Yes (time-based) | No |
| **Use Case** | Trading signals | Revenue tracking |

---

## Use Cases

1. **Token Card**: Display total creator earnings
2. **Creator Profile**: Sum fees across all their tokens
3. **Leaderboard**: Top earning tokens/creators
4. **Analytics**: Fee rate (fees / volume ratio)

---

## Example

```typescript
// Top earning tokens
const topTokens = tokens
  .sort((a, b) => b.total_fees_sol - a.total_fees_sol)
  .slice(0, 10);

// Creator total earnings
const creatorEarnings = tokens
  .filter(t => t.creator === creatorAddress)
  .reduce((sum, t) => sum + t.total_fees_sol, 0);

// Fee efficiency
const feeRate = token.total_fees_sol / token.volume_24h;
```

---

## Notes

- Separate from volume tracking (volume doesn't include fees)
- Starts at 0 for new tokens
- Persists across server restarts
- Available in `SnapshotTickers` message on WebSocket connect
