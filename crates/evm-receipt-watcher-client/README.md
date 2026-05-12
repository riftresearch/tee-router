# EVM Receipt Watcher Client

Typed Rust client for the per-chain `evm-receipt-watcher` service.

```rust
use alloy::primitives::TxHash;
use evm_receipt_watcher_client::{ByIdLookup, EvmReceiptWatcherClient};

# async fn example(tx_hash: TxHash) -> evm_receipt_watcher_client::Result<()> {
let client = EvmReceiptWatcherClient::new("http://localhost:8081", "ethereum")?;
client.watch(tx_hash, "operation-123").await?;
let receipt = client.lookup_by_id(tx_hash).await?;
# Ok(())
# }
```

The `ByIdLookup` implementation uses `Id = TxHash` and
`Receipt = (alloy::rpc::types::TransactionReceipt, Vec<alloy::rpc::types::Log>)`.
