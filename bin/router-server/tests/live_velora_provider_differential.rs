mod support;

#[tokio::test]
#[ignore = "read-only live-vs-mock Velora quote contract; run with LIVE_PROVIDER_TESTS=1"]
async fn live_vs_mock_velora_quote_contract() -> support::LiveTestResult<()> {
    support::live_provider_differential::live_vs_mock_velora_quote_contract().await
}
