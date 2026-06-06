#![allow(clippy::too_many_arguments)]

alloy::sol! {
    #[derive(Debug)]
    #[sol(rpc)]
    MockSpokePool,
    "src/mock_integrators/across/contract/out/MockSpokePool.sol/MockSpokePool.json",
}
