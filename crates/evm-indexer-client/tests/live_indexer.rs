use alloy::primitives::{Address, U256};
use evm_indexer_client::{EvmIndexerClient, SubscribeFilter, TransferQuery};
use tokio::time::{timeout, Duration};

#[tokio::test]
#[ignore = "integration: spawns devnet stack"]
async fn live_indexer_rest_and_ws_subscription() {
    let Ok(base_url) = std::env::var("EVM_INDEXER_CLIENT_TEST_URL") else {
        eprintln!("EVM_INDEXER_CLIENT_TEST_URL not set; skipping live indexer check");
        return;
    };
    let api_key = std::env::var("EVM_INDEXER_CLIENT_TEST_API_KEY").ok();
    let client = EvmIndexerClient::new_with_api_key(base_url, api_key).expect("client");

    let token = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x11);
    let page = client
        .transfers(TransferQuery {
            to: recipient,
            token: Some(token),
            from_block: Some(0),
            min_amount: Some(U256::ZERO),
            max_amount: None,
            limit: Some(1),
            cursor: None,
        })
        .await
        .expect("transfer page");
    assert!(page.transfers.len() <= 1);

    let mut stream = client
        .subscribe(SubscribeFilter {
            token_addresses: vec![token],
            recipient_addresses: vec![recipient],
            min_amount: Some(U256::ZERO),
            max_amount: None,
        })
        .await
        .expect("subscribe");

    let maybe_event = timeout(Duration::from_millis(250), stream.recv()).await;
    assert!(
        maybe_event.is_err() || matches!(maybe_event, Ok(None) | Ok(Some(Ok(_)))),
        "subscription stream returned an immediate error: {maybe_event:?}"
    );
}
