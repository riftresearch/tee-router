mod support;

#[tokio::test]
#[ignore = "read-only live-vs-mock Velora quote contract; run with LIVE_PROVIDER_TESTS=1"]
async fn live_vs_mock_velora_quote_contract() -> support::LiveTestResult<()> {
    support::live_provider_differential::live_vs_mock_velora_quote_contract().await
}

// Exercises the production quote-time shape: sender_address is None (the
// deposit vault doesn't exist yet) but recipient_address is set. Without
// the action_providers.rs userAddress fallback, real Velora 400s with
// "If receiver is defined userAddress should also be defined" and the
// mock silently succeeds — exactly the skew that hit prod.
#[tokio::test]
#[ignore = "read-only live-vs-mock Velora quote contract (no sender); run with LIVE_PROVIDER_TESTS=1"]
async fn live_vs_mock_velora_quote_contract_no_sender() -> support::LiveTestResult<()> {
    support::live_provider_differential::live_vs_mock_velora_quote_contract_no_sender().await
}
