# EVM Indexer Client

Typed Rust client for the pure ERC-20 transfer indexer primitive.

```rust
use alloy::primitives::{Address, U256};
use evm_indexer_client::{EvmIndexerClient, SubscribeFilter, TransferQuery};

# async fn example() -> evm_indexer_client::Result<()> {
let client = EvmIndexerClient::new_with_api_key(
    "http://localhost:4001",
    Some("local-token-indexer-api-key-000000000".to_string()),
)?;

let recipient = Address::repeat_byte(0x11);
let token = Address::repeat_byte(0x22);

let page = client
    .transfers(TransferQuery {
        to: recipient,
        token: Some(token),
        from_block: Some(0),
        min_amount: Some(U256::from(1_u64)),
        max_amount: None,
        limit: Some(100),
        cursor: None,
    })
    .await?;

let stream = client
    .subscribe(SubscribeFilter {
        token_addresses: vec![token],
        recipient_addresses: vec![recipient],
        min_amount: None,
        max_amount: None,
    })
    .await?;
# Ok(())
# }
```

The ignored live integration test uses `EVM_INDEXER_CLIENT_TEST_URL` and optional
`EVM_INDEXER_CLIENT_TEST_API_KEY` to hit a running indexer.
