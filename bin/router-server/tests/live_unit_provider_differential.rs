mod support;

#[tokio::test]
#[ignore = "read-only live-vs-mock Unit /gen and /operations contract; run with LIVE_PROVIDER_TESTS=1"]
async fn live_vs_mock_unit_generate_address_and_operations_contract() -> support::LiveTestResult<()>
{
    support::live_provider_differential::live_vs_mock_hyperunit_generate_address_contract().await
}
