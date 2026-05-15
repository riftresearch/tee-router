mod support;

#[tokio::test]
#[ignore = "read-only live-vs-mock CCTP quote/Iris contract; run with LIVE_PROVIDER_TESTS=1"]
async fn live_vs_mock_cctp_quote_and_iris_contract() -> support::LiveTestResult<()> {
    support::live_provider_differential::live_vs_mock_cctp_quote_contract().await
}
