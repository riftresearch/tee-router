# EVM Token Indexer Client

A strongly-typed Rust client for interacting with the EVM Token Indexer API.

## Usage

```rust
use alloy::primitives::address;
use evm_token_indexer_client::TokenIndexerClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client
    let client = TokenIndexerClient::new("http://localhost:42069")?;
    
    let address = address!("0xbAB0A8E7e9d002394Ac0AF1492d68cAF87cF910E");
    let transfers = client.get_transfers_to(address, None, Some(1), None).await?;
    
    Ok(())
}
```

## API Methods

- `sync_watches(watches)` - Replace the active ERC-20 watch mirror.
- `materialize_candidates()` - Backfill candidates from raw transfers and active watches.
- `pending_candidates(limit)` - Read durable pending deposit candidates.
- `mark_candidate_submitted(candidate_id)` - Ack a candidate after router hint submission.
- `release_candidate(candidate_id, error)` - Record a retryable candidate submission failure.
- `get_transfers_to(address, token, page, min_amount)` - Diagnostic transfer lookup.

## Running the Example

```bash
cargo run --example basic_usage
```
