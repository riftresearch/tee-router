# EVM Token Indexer Client

A strongly-typed Rust client for interacting with the EVM Token Indexer API.

## Usage

```rust
use alloy::primitives::address;
use evm_token_indexer_client::TokenIndexerClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client. Production indexers require the shared bearer key.
    let client = TokenIndexerClient::new_with_api_key(
        "http://localhost:42069",
        Some(std::env::var("TOKEN_INDEXER_API_KEY")?),
    )?;
    
    let address = address!("0xbAB0A8E7e9d002394Ac0AF1492d68cAF87cF910E");
    let transfers = client.get_transfers_to(address, None, None, None).await?;
    
    Ok(())
}
```

## API Methods

- `sync_watches(watches)` - Replace the active ERC-20 watch mirror.
- `materialize_candidates()` - Backfill candidates from raw transfers and active watches.
- `pending_candidates(limit)` - Read durable pending deposit candidates.
- `mark_candidate_submitted(candidate_id)` - Ack a candidate after router hint submission.
- `release_candidate(candidate_id, error)` - Record a retryable candidate submission failure.
- `get_transfers_to(address, token, min_amount, limit)` - Diagnostic latest-transfer lookup.

## Running the Example

```bash
cargo run --example basic_usage
```
