#![allow(clippy::too_many_arguments)]

alloy::sol! {
    #[derive(Debug)]
    #[sol(rpc)]
    MockCctpTokenMessengerV2,
    "src/mock_integrators/cctp/contract/out/MockCctpTokenMessengerV2.sol/MockCctpTokenMessengerV2.json",
}

alloy::sol! {
    #[derive(Debug)]
    #[sol(rpc)]
    MockCctpMessageTransmitterV2,
    "src/mock_integrators/cctp/contract/out/MockCctpMessageTransmitterV2.sol/MockCctpMessageTransmitterV2.json",
}
