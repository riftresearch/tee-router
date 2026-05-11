use alloy::primitives::Address;
use hyperliquid_client::HyperliquidInfoClient;

#[tokio::test]
#[ignore = "explicit smoke: hits Hyperliquid testnet over the network"]
async fn decodes_hyperliquid_testnet_info_for_one_user() {
    let user = Address::repeat_byte(0x42);
    let client = HyperliquidInfoClient::new("https://api.hyperliquid-testnet.xyz")
        .expect("testnet info client");

    let meta = client.fetch_perp_meta().await.expect("perp meta");
    assert!(!meta.universe.is_empty());

    let fills = client
        .user_fills_by_time(user, 0, None, false)
        .await
        .expect("testnet userFillsByTime decodes");
    assert!(fills.len() <= 2_000);

    let ledger = client
        .user_non_funding_ledger_updates(user, 0, None)
        .await
        .expect("testnet userNonFundingLedgerUpdates decodes");
    assert!(ledger.len() <= 2_000);

    let funding = client
        .user_funding(user, 0, None)
        .await
        .expect("testnet userFunding decodes");
    assert!(funding.len() <= 2_000);

    let rate_limit = client
        .user_rate_limit(user)
        .await
        .expect("rate limit decodes");
    assert!(rate_limit.n_requests_cap > 0);
}
