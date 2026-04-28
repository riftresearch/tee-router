#![allow(clippy::too_many_arguments)]

alloy::sol! {
    #[derive(Debug)]
    #[sol(rpc)]
    MockCctpTokenMessengerV2,
    "src/cctp_mock/contract/out/MockCctpTokenMessengerV2.sol/MockCctpTokenMessengerV2.json",
}

alloy::sol! {
    #[derive(Debug)]
    #[sol(rpc)]
    MockCctpMessageTransmitterV2,
    "src/cctp_mock/contract/out/MockCctpMessageTransmitterV2.sol/MockCctpMessageTransmitterV2.json",
}
