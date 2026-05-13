//! End-to-end tests for `hyperliquid-client` against the devnet mock
//! integrator. Each test spins up `MockIntegratorServer`, points a real
//! `HyperliquidClient` at it, and drives a spot-trading lifecycle. The mock
//! speaks the real `/info` + `/exchange` wire shapes, so these tests
//! exercise the same code path the router uses against live Hyperliquid.
//!
//! The mock is deliberately scoped: a single configurable external price per
//! pair, immediate fills for marketable orders, and lightweight resting-order
//! semantics (`openOrders`, `hold`, cancel, and fill-on-price-move) without a
//! full matching engine.

use alloy::{
    hex,
    network::EthereumWallet,
    node_bindings::{Anvil, AnvilInstance},
    primitives::{Address, U256},
    providers::{DynProvider, Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
};
use devnet::hyperliquid_bridge_mock::MockHyperliquidBridge2::MockHyperliquidBridge2Instance;
use devnet::mock_integrators::{MockIntegratorConfig, MockIntegratorServer};
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use hyperliquid_client::{
    client::Network, CancelRequest, ClearinghouseState, HyperliquidClient,
    HyperliquidExchangeClient, HyperliquidInfoClient, Limit, Order, OrderRequest,
    SpotClearinghouseState, UserFill, UserFunding, UserNonFundingLedgerDelta,
    UserNonFundingLedgerUpdate, MINIMUM_BRIDGE_DEPOSIT_USDC,
};
use std::time::Duration;
use url::Url;

const UBTC_USDC: &str = "UBTC/USDC";
const UETH_USDC: &str = "UETH/USDC";

/// Spin up a fresh mock + a testnet-mode client whose wallet has the given
/// initial balances pre-credited.
async fn fixture(balances: &[(&str, f64)]) -> (MockIntegratorServer, HyperliquidClient, Address) {
    let server = MockIntegratorServer::spawn()
        .await
        .expect("spawn mock integrator");
    let wallet = PrivateKeySigner::random();
    let address = wallet.address();
    for (coin, amount) in balances {
        server
            .credit_hyperliquid_balance(address, coin, *amount)
            .await;
    }
    let mut client = HyperliquidClient::new(server.base_url(), wallet, None, Network::Testnet)
        .expect("client construction");
    client.refresh_spot_meta().await.expect("refresh spot meta");
    (server, client, address)
}

async fn bridge_fixture(
    minted_usdc_raw: U256,
) -> (
    AnvilInstance,
    MockIntegratorServer,
    HyperliquidClient,
    Address,
    GenericEIP3009ERC20Instance<DynProvider>,
    Address,
) {
    bridge_fixture_with_config(minted_usdc_raw, |config| config).await
}

async fn bridge_fixture_with_config(
    minted_usdc_raw: U256,
    configure: impl FnOnce(MockIntegratorConfig) -> MockIntegratorConfig,
) -> (
    AnvilInstance,
    MockIntegratorServer,
    HyperliquidClient,
    Address,
    GenericEIP3009ERC20Instance<DynProvider>,
    Address,
) {
    let anvil = Anvil::new()
        .prague()
        .block_time(1)
        .try_spawn()
        .expect("anvil spawn");
    let rpc_url: Url = anvil.endpoint_url();
    let private_key: [u8; 32] = anvil.keys()[0].clone().to_bytes().into();
    let signer = format!("0x{}", hex::encode(private_key))
        .parse::<PrivateKeySigner>()
        .expect("provider signer");
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(signer))
        .connect_http(rpc_url.clone());
    let provider: DynProvider = provider.erased();

    let user = anvil.addresses()[0];
    let token = GenericEIP3009ERC20Instance::deploy(provider.clone())
        .await
        .expect("deploy token");
    token
        .mint(user, minted_usdc_raw)
        .send()
        .await
        .expect("mint send")
        .get_receipt()
        .await
        .expect("mint receipt");

    let bridge = MockHyperliquidBridge2Instance::deploy(provider.clone(), *token.address())
        .await
        .expect("deploy bridge");
    let bridge_address = *bridge.address();

    let config = MockIntegratorConfig::default()
        .with_hyperliquid_bridge_address(format!("{bridge_address:#x}"))
        .with_hyperliquid_evm_rpc_url(rpc_url.to_string())
        .with_hyperliquid_usdc_token_address(format!("{:#x}", token.address()))
        .with_mock_service_evm_chain(anvil.chain_id(), rpc_url.to_string());
    let server = MockIntegratorServer::spawn_with_config(configure(config))
        .await
        .expect("spawn mock integrator");

    let wallet = format!("0x{}", hex::encode(private_key))
        .parse::<PrivateKeySigner>()
        .expect("client wallet");
    let client = HyperliquidClient::new(server.base_url(), wallet, None, Network::Testnet)
        .expect("client construction");

    (anvil, server, client, user, token, bridge_address)
}

fn parse_total(state: &SpotClearinghouseState, coin: &str) -> f64 {
    state
        .balance_of(coin)
        .parse::<f64>()
        .expect("balance parses as f64")
}

fn parse_withdrawable(state: &ClearinghouseState) -> f64 {
    state
        .withdrawable
        .parse::<f64>()
        .expect("withdrawable parses as f64")
}

fn sample_indexer_fill(time: u64) -> UserFill {
    UserFill {
        coin: "UBTC/USDC".to_string(),
        px: "60000".to_string(),
        sz: "0.001".to_string(),
        side: "B".to_string(),
        time,
        start_position: "0".to_string(),
        dir: "Buy".to_string(),
        closed_pnl: "0".to_string(),
        hash: format!("0x{}", "ab".repeat(32)),
        oid: 9001,
        crossed: true,
        fee: "0.00001".to_string(),
        tid: 7001,
        fee_token: "UBTC".to_string(),
    }
}

#[tokio::test]
async fn split_info_and_exchange_clients_work_against_mock() {
    let server = MockIntegratorServer::spawn()
        .await
        .expect("spawn mock integrator");
    let wallet = PrivateKeySigner::random();
    let user = wallet.address();
    server
        .credit_hyperliquid_balance(user, "USDC", 10_000.0)
        .await;
    server.set_hyperliquid_rate("UBTC", "USDC", 30_000.0).await;

    let mut info = HyperliquidInfoClient::new(server.base_url()).expect("info client");
    let exchange =
        HyperliquidExchangeClient::new(server.base_url(), wallet, None, Network::Testnet)
            .expect("exchange client");
    let meta = info.refresh_spot_meta().await.expect("refresh spot meta");
    assert!(meta.base_token_for(UBTC_USDC).is_some());
    let asset = info.asset_index(UBTC_USDC).expect("asset index");

    let response = exchange
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "31000".to_string(),
                sz: "0.1".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Ioc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place order");
    let oid = response["response"]["data"]["statuses"][0]["filled"]["oid"]
        .as_u64()
        .expect("filled oid");

    let status = info.order_status(user, oid).await.expect("order status");
    assert_eq!(status.status, "order");
    assert_eq!(status.order.expect("filled envelope").status, "filled");
}

#[tokio::test]
async fn info_client_decodes_indexer_info_endpoints_against_mock() {
    let server = MockIntegratorServer::spawn()
        .await
        .expect("spawn mock integrator");
    let wallet = PrivateKeySigner::random();
    let user = wallet.address();
    server
        .record_hyperliquid_fill(user, sample_indexer_fill(1_700_000_000_000))
        .await;
    server
        .record_hyperliquid_ledger_update(
            user,
            UserNonFundingLedgerUpdate {
                time: 1_700_000_000_100,
                hash: format!("0x{}", "cd".repeat(32)),
                delta: UserNonFundingLedgerDelta::SpotTransfer {
                    token: "UBTC:0x11111111111111111111111111111111".to_string(),
                    amount: "0.001".to_string(),
                    usdc_value: "60".to_string(),
                    user: format!("{user:#x}"),
                    destination: "0x2222222222222222222222222222222222222222".to_string(),
                    fee: "0".to_string(),
                    native_token_fee: "0.00001".to_string(),
                    nonce: 11,
                },
            },
        )
        .await;
    server
        .record_hyperliquid_funding(
            user,
            UserFunding {
                time: 1_700_000_000_200,
                coin: "ETH".to_string(),
                usdc: "-0.0123".to_string(),
                szi: "1.25".to_string(),
                funding_rate: "0.0000125".to_string(),
            },
        )
        .await;

    let info = HyperliquidInfoClient::new(server.base_url()).expect("info client");
    let meta = info.fetch_perp_meta().await.expect("perp meta");
    assert!(meta.universe.iter().any(|asset| asset.name == "BTC"));

    let fills = info
        .user_fills_by_time(user, 1_699_999_999_999, None, false)
        .await
        .expect("fills by time");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].tid, 7001);

    let ledger = info
        .user_non_funding_ledger_updates(user, 1_700_000_000_000, None)
        .await
        .expect("ledger updates");
    assert_eq!(ledger.len(), 1);
    assert!(matches!(
        ledger[0].delta,
        UserNonFundingLedgerDelta::SpotTransfer { .. }
    ));

    let funding = info
        .user_funding(user, 1_700_000_000_000, None)
        .await
        .expect("funding");
    assert_eq!(funding.len(), 1);
    assert_eq!(funding[0].funding_rate, "0.0000125");

    let rate_limit = info.user_rate_limit(user).await.expect("rate limit");
    assert_eq!(rate_limit.n_requests_cap, 1200);
}

async fn wait_for_hyperliquid_clearinghouse_balance(
    server: &MockIntegratorServer,
    user: Address,
    coin: &str,
    expected: f64,
) -> f64 {
    let mut observed = 0.0;
    for _ in 0..50 {
        observed = server
            .hyperliquid_clearinghouse_balance_of(user, coin)
            .await;
        if (observed - expected).abs() < 1e-9 {
            return observed;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    observed
}

async fn wait_for_token_balance(
    token: &GenericEIP3009ERC20Instance<DynProvider>,
    user: Address,
    expected: U256,
) -> U256 {
    let mut observed = U256::ZERO;
    for _ in 0..50 {
        observed = token.balanceOf(user).call().await.expect("balanceOf call");
        if observed == expected {
            return observed;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    observed
}

#[tokio::test]
async fn spot_meta_resolves_asset_indexes() {
    let (_server, client, _addr) = fixture(&[]).await;
    assert_eq!(client.asset_index("UBTC/USDC").unwrap(), 10_140);
    assert_eq!(client.asset_index("UETH/USDC").unwrap(), 10_141);
    assert_eq!(client.asset_index("@140").unwrap(), 10_140);
}

#[tokio::test]
async fn l2_book_reflects_configured_rate() {
    let (server, client, _addr) = fixture(&[]).await;
    server.set_hyperliquid_rate("UBTC", "USDC", 30_000.0).await;
    let book = client.l2_book(UBTC_USDC).await.expect("fetch l2 book");
    // One-level synthetic book at the configured rate on both sides.
    let best_bid = book.best_bid().expect("has a bid");
    let best_ask = book.best_ask().expect("has an ask");
    assert_eq!(best_bid.px, "30000");
    assert_eq!(best_ask.px, "30000");
    assert_eq!(book.bids().len(), 1);
    assert_eq!(book.asks().len(), 1);
}

#[tokio::test]
async fn ioc_buy_debits_quote_and_credits_base_at_rate() {
    // Rate: 30 000 USDC/UBTC. Buy 0.1 UBTC at limit 31 000 -> fills at 30 000,
    // debits 3 000 USDC, credits 0.1 UBTC.
    let (server, client, user) = fixture(&[("USDC", 10_000.0)]).await;
    server.set_hyperliquid_rate("UBTC", "USDC", 30_000.0).await;
    let asset = client.asset_index(UBTC_USDC).unwrap();
    let response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "31000".to_string(),
                sz: "0.1".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Ioc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place buy");
    assert_eq!(response["status"], "ok");
    let statuses = &response["response"]["data"]["statuses"];
    assert!(statuses[0].get("filled").is_some(), "status: {statuses:#?}");
    assert_eq!(statuses[0]["filled"]["totalSz"], "0.1");
    assert_eq!(statuses[0]["filled"]["avgPx"], "30000");

    let state = client
        .spot_clearinghouse_state(user)
        .await
        .expect("clearinghouse");
    assert!((parse_total(&state, "USDC") - 7_000.0).abs() < 1e-6);
    assert!((parse_total(&state, "UBTC") - 0.1).abs() < 1e-9);
}

#[tokio::test]
async fn ioc_sell_debits_base_and_credits_quote_at_rate() {
    // Rate: 32 000 USDC/UBTC. Sell 0.3 UBTC at limit 31 000 -> fills at
    // 32 000, debits 0.3 UBTC, credits 9 600 USDC.
    let (server, client, user) = fixture(&[("UBTC", 1.0)]).await;
    server.set_hyperliquid_rate("UBTC", "USDC", 32_000.0).await;
    let asset = client.asset_index(UBTC_USDC).unwrap();
    let response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: false,
                limit_px: "31000".to_string(),
                sz: "0.3".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Ioc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place sell");
    assert_eq!(response["status"], "ok");
    assert_eq!(
        response["response"]["data"]["statuses"][0]["filled"]["totalSz"],
        "0.3"
    );
    let state = client
        .spot_clearinghouse_state(user)
        .await
        .expect("clearinghouse");
    assert!((parse_total(&state, "UBTC") - 0.7).abs() < 1e-9);
    assert!((parse_total(&state, "USDC") - 9_600.0).abs() < 1e-6);
}

#[tokio::test]
async fn order_status_reflects_filled_state() {
    let (server, client, user) = fixture(&[("USDC", 5_000.0)]).await;
    server.set_hyperliquid_rate("UBTC", "USDC", 28_000.0).await;
    let asset = client.asset_index(UBTC_USDC).unwrap();
    let response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "29000".to_string(),
                sz: "0.05".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Ioc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place buy");
    let oid = response["response"]["data"]["statuses"][0]["filled"]["oid"]
        .as_u64()
        .expect("filled oid");

    let status = client.order_status(user, oid).await.expect("order status");
    assert_eq!(status.status, "order");
    let envelope = status.order.expect("filled order envelope");
    assert_eq!(envelope.status, "filled");
    assert_eq!(envelope.order.oid, oid);
    assert_eq!(envelope.order.orig_sz, "0.05");
}

#[tokio::test]
async fn ioc_buy_partially_fills_when_book_depth_is_smaller_than_order_size() {
    let (server, client, user) = fixture(&[("USDC", 10_000.0)]).await;
    server.set_hyperliquid_rate("UBTC", "USDC", 30_000.0).await;
    server
        .set_hyperliquid_book_depth("UBTC", "USDC", 0.13)
        .await;
    let asset = client.asset_index(UBTC_USDC).unwrap();

    let response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "31000".to_string(),
                sz: "0.14".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Ioc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place partial ioc buy");
    let filled = &response["response"]["data"]["statuses"][0]["filled"];
    assert_eq!(filled["totalSz"], "0.13");
    let oid = filled["oid"].as_u64().expect("filled oid");

    let open_orders = client.open_orders(user).await.expect("open orders");
    assert!(open_orders.is_empty());

    let status = client.order_status(user, oid).await.expect("order status");
    let envelope = status.order.expect("terminal filled order envelope");
    assert_eq!(envelope.status, "filled");
    assert_eq!(envelope.order.orig_sz, "0.14");
    assert_eq!(envelope.order.sz, "0.01");

    let state = client
        .spot_clearinghouse_state(user)
        .await
        .expect("spot state");
    assert!((parse_total(&state, "USDC") - 6_100.0).abs() < 1e-6);
    assert!((parse_total(&state, "UBTC") - 0.13).abs() < 1e-9);
}

#[tokio::test]
async fn marketable_gtc_buy_partially_fills_and_rests_remainder() {
    let (server, client, user) = fixture(&[("USDC", 10_000.0)]).await;
    server.set_hyperliquid_rate("UBTC", "USDC", 30_000.0).await;
    server
        .set_hyperliquid_book_depth("UBTC", "USDC", 0.05)
        .await;
    let asset = client.asset_index(UBTC_USDC).unwrap();

    let response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "31000".to_string(),
                sz: "0.1".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Gtc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place partial gtc buy");
    let oid = response["response"]["data"]["statuses"][0]["resting"]["oid"]
        .as_u64()
        .expect("resting oid");

    let open_orders = client.open_orders(user).await.expect("open orders");
    let order = open_orders
        .iter()
        .find(|order| order.oid == oid)
        .expect("partially filled resting order");
    assert_eq!(order.orig_sz, "0.1");
    assert_eq!(order.sz, "0.05");

    let state = client
        .spot_clearinghouse_state(user)
        .await
        .expect("spot state");
    let usdc = state
        .balances
        .iter()
        .find(|balance| balance.coin == "USDC")
        .expect("usdc balance");
    let ubtc = state
        .balances
        .iter()
        .find(|balance| balance.coin == "UBTC")
        .expect("ubtc balance");
    assert_eq!(usdc.total, "8500");
    assert_eq!(usdc.hold, "1550");
    assert_eq!(ubtc.total, "0.05");

    let fills = client.user_fills(user).await.expect("user fills");
    assert!(fills
        .iter()
        .any(|fill| fill.oid == oid && fill.sz == "0.05"));
}

#[tokio::test]
async fn resting_order_partially_fills_when_rate_moves_through_limited_book_depth() {
    let (server, client, user) = fixture(&[("USDC", 10_000.0)]).await;
    let asset = client.asset_index(UBTC_USDC).unwrap();
    server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;
    server
        .set_hyperliquid_book_depth("UBTC", "USDC", 0.01)
        .await;

    let place = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "30000".to_string(),
                sz: "0.03".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Gtc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place resting order");
    let oid = place["response"]["data"]["statuses"][0]["resting"]["oid"]
        .as_u64()
        .expect("resting oid");

    server.set_hyperliquid_rate("UBTC", "USDC", 29_000.0).await;

    let open_orders = client.open_orders(user).await.expect("open orders");
    let order = open_orders
        .iter()
        .find(|order| order.oid == oid)
        .expect("partially filled resting order");
    assert_eq!(order.orig_sz, "0.03");
    assert_eq!(order.sz, "0.02");

    let state = client
        .spot_clearinghouse_state(user)
        .await
        .expect("spot state");
    let usdc = state
        .balances
        .iter()
        .find(|balance| balance.coin == "USDC")
        .expect("usdc balance");
    let ubtc = state
        .balances
        .iter()
        .find(|balance| balance.coin == "UBTC")
        .expect("ubtc balance");
    assert_eq!(usdc.total, "9710");
    assert_eq!(usdc.hold, "600");
    assert_eq!(ubtc.total, "0.01");

    let fills = client.user_fills(user).await.expect("user fills");
    assert!(fills
        .iter()
        .any(|fill| fill.oid == oid && fill.sz == "0.01"));
}

#[tokio::test]
async fn user_fills_reports_completed_trades() {
    let (server, client, user) = fixture(&[("USDC", 10_000.0)]).await;
    server.set_hyperliquid_rate("UBTC", "USDC", 30_000.0).await;
    let asset = client.asset_index(UBTC_USDC).unwrap();
    client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "30500".to_string(),
                sz: "0.1".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Ioc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place buy");

    let fills = client.user_fills(user).await.expect("user fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].coin, "@140");
    assert_eq!(fills[0].side, "B");
    assert_eq!(fills[0].px, "30000");
    assert_eq!(fills[0].sz, "0.1");
    assert_eq!(fills[0].dir, "Buy");
}

#[tokio::test]
async fn spot_send_transfers_balance_between_users() {
    let (server, client, sender) = fixture(&[("UBTC", 0.5)]).await;
    let recipient = Address::from([0xAB; 20]);

    let response = client
        .spot_send(
            format!("{recipient:?}"),
            "UBTC:0x1111111111111111111111111111111111111111".to_string(),
            "0.2".to_string(),
            1_700_000_000_000,
        )
        .await
        .expect("spot send");
    assert_eq!(response["status"], "ok");

    let sender_balance = server.hyperliquid_balance_of(sender, "UBTC").await;
    let recipient_balance = server.hyperliquid_balance_of(recipient, "UBTC").await;
    assert!((sender_balance - 0.3).abs() < 1e-9);
    assert!((recipient_balance - 0.2).abs() < 1e-9);
}

#[tokio::test]
async fn native_bridge_deposit_transfer_credits_sender_withdrawable_usdc() {
    let amount_raw = U256::from(6_000_000u64);
    let (_anvil, server, client, user, token, bridge_address) = bridge_fixture(amount_raw).await;

    let deposit = client
        .build_usdc_bridge_deposit_to(bridge_address, *token.address(), amount_raw)
        .expect("build bridge deposit");
    assert_eq!(deposit.bridge, bridge_address);

    let pending = token
        .transfer(deposit.bridge, amount_raw)
        .send()
        .await
        .expect("send deposit tx");
    pending.get_receipt().await.expect("deposit receipt");

    let observed = wait_for_hyperliquid_clearinghouse_balance(&server, user, "USDC", 6.0).await;
    assert!(
        (observed - 6.0).abs() < 1e-9,
        "observed withdrawable USDC balance {observed}"
    );

    let clearinghouse = client
        .clearinghouse_state(user)
        .await
        .expect("clearinghouse state");
    assert!((parse_withdrawable(&clearinghouse) - 6.0).abs() < 1e-9);

    let spot = client
        .spot_clearinghouse_state(user)
        .await
        .expect("spot clearinghouse");
    assert!(parse_total(&spot, "USDC").abs() < 1e-9);
    assert!(server.hyperliquid_exchange_submissions().await.is_empty());
}

#[tokio::test]
async fn native_bridge_deposit_credit_can_be_delayed_by_mock() {
    let amount_raw = U256::from(6_000_000u64);
    let (_anvil, server, client, user, token, bridge_address) =
        bridge_fixture_with_config(amount_raw, |config| {
            config.with_hyperliquid_bridge_deposit_latency(Duration::from_millis(750))
        })
        .await;

    let deposit = client
        .build_usdc_bridge_deposit_to(bridge_address, *token.address(), amount_raw)
        .expect("build bridge deposit");
    token
        .transfer(deposit.bridge, amount_raw)
        .send()
        .await
        .expect("send deposit tx")
        .get_receipt()
        .await
        .expect("deposit receipt");

    tokio::time::sleep(Duration::from_millis(200)).await;
    let early = server
        .hyperliquid_clearinghouse_balance_of(user, "USDC")
        .await;
    assert!(
        early.abs() < 1e-9,
        "bridge deposit credited before configured latency: {early}"
    );

    let observed = wait_for_hyperliquid_clearinghouse_balance(&server, user, "USDC", 6.0).await;
    assert!(
        (observed - 6.0).abs() < 1e-9,
        "observed withdrawable USDC balance {observed}"
    );
}

#[tokio::test]
#[ignore = "integration: spawns devnet stack"]
async fn native_bridge_deposit_credit_can_fail_deterministically() {
    let amount_raw = U256::from(6_000_000u64);
    let (_anvil, server, client, user, token, bridge_address) =
        bridge_fixture_with_config(amount_raw, |config| {
            config.with_hyperliquid_bridge_deposit_failure_probability_bps(10_000)
        })
        .await;

    let deposit = client
        .build_usdc_bridge_deposit_to(bridge_address, *token.address(), amount_raw)
        .expect("build bridge deposit");
    token
        .transfer(deposit.bridge, amount_raw)
        .send()
        .await
        .expect("send deposit tx")
        .get_receipt()
        .await
        .expect("deposit receipt");

    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let observed = server
        .hyperliquid_clearinghouse_balance_of(user, "USDC")
        .await;
    assert!(
        observed.abs() < 1e-9,
        "configured failed bridge deposit credited unexpectedly: {observed}"
    );
}

#[tokio::test]
async fn usd_class_transfer_moves_withdrawable_usdc_into_spot() {
    let (server, client, user) = fixture(&[]).await;
    server
        .credit_hyperliquid_clearinghouse_balance(user, "USDC", 12.5)
        .await;

    let response = client
        .usd_class_transfer("12.5".to_string(), false, 1_700_000_000_000)
        .await
        .expect("usd class transfer");
    assert_eq!(response["status"], "ok");

    let clearinghouse = client
        .clearinghouse_state(user)
        .await
        .expect("clearinghouse state");
    let spot = client
        .spot_clearinghouse_state(user)
        .await
        .expect("spot clearinghouse");
    assert!(parse_withdrawable(&clearinghouse).abs() < 1e-9);
    assert!((parse_total(&spot, "USDC") - 12.5).abs() < 1e-9);
}

#[tokio::test]
#[ignore = "integration: spawns devnet stack"]
async fn withdraw3_deducts_gross_amount_and_releases_net_usdc() {
    let minted_raw = U256::from(12_000_000u64);
    let deposit_raw = U256::from(6_000_000u64);
    let (_anvil, server, client, user, token, bridge_address) = bridge_fixture(minted_raw).await;

    let deposit = client
        .build_usdc_bridge_deposit_to(bridge_address, *token.address(), deposit_raw)
        .expect("build bridge deposit");
    token
        .transfer(deposit.bridge, deposit_raw)
        .send()
        .await
        .expect("send deposit tx")
        .get_receipt()
        .await
        .expect("deposit receipt");

    let observed = wait_for_hyperliquid_clearinghouse_balance(&server, user, "USDC", 6.0).await;
    assert!(
        (observed - 6.0).abs() < 1e-9,
        "observed withdrawable USDC balance {observed}"
    );

    let before_wallet_balance = token
        .balanceOf(user)
        .call()
        .await
        .expect("balance before withdraw");
    assert_eq!(before_wallet_balance, U256::from(6_000_000u64));

    let response = client
        .withdraw_to_bridge(format!("{user:#x}"), "5".to_string(), 1_700_000_000_000)
        .await
        .expect("withdraw3");
    assert_eq!(response["status"], "ok");

    let clearinghouse = client
        .clearinghouse_state(user)
        .await
        .expect("clearinghouse state");
    assert!((parse_withdrawable(&clearinghouse) - 1.0).abs() < 1e-9);

    let after_wallet_balance = wait_for_token_balance(
        &token,
        user,
        before_wallet_balance + U256::from(4_000_000u64),
    )
    .await;
    assert_eq!(after_wallet_balance, U256::from(10_000_000u64));
}

#[tokio::test]
async fn subminimum_native_bridge_transfer_is_ignored_by_mock_crediting() {
    let amount_raw = U256::from(MINIMUM_BRIDGE_DEPOSIT_USDC - 1);
    let (_anvil, server, _client, user, token, bridge_address) = bridge_fixture(amount_raw).await;

    token
        .transfer(bridge_address, amount_raw)
        .send()
        .await
        .expect("transfer send")
        .get_receipt()
        .await
        .expect("transfer receipt");

    tokio::time::sleep(Duration::from_millis(500)).await;
    let observed = server.hyperliquid_balance_of(user, "USDC").await;
    assert!(observed.abs() < 1e-9, "observed USDC balance {observed}");
}

#[tokio::test]
async fn ioc_below_rate_returns_error_and_leaves_balances_untouched() {
    // Rate: 30 000. Buy limit 10 000 can't cross -> error status, USDC
    // balance unchanged.
    let (server, client, user) = fixture(&[("USDC", 5_000.0)]).await;
    server.set_hyperliquid_rate("UBTC", "USDC", 30_000.0).await;
    let asset = client.asset_index(UBTC_USDC).unwrap();
    let response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "10000".to_string(),
                sz: "0.01".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Ioc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place ioc");
    let status = &response["response"]["data"]["statuses"][0];
    let err = status["error"]
        .as_str()
        .unwrap_or_else(|| panic!("expected error, got {status:#?}"));
    assert!(err.contains("does not cross"), "error: {err}");

    // Balance untouched because the order was rejected.
    assert!((server.hyperliquid_balance_of(user, "USDC").await - 5_000.0).abs() < 1e-9);
}

#[tokio::test]
async fn gtc_non_marketable_orders_rest_and_reserve_hold() {
    let (server, client, user) = fixture(&[("USDC", 5_000.0)]).await;
    let asset = client.asset_index(UBTC_USDC).unwrap();
    server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;
    let response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "30000".to_string(),
                sz: "0.01".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Gtc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place order");
    let oid = response["response"]["data"]["statuses"][0]["resting"]["oid"]
        .as_u64()
        .expect("resting oid");

    let open = client.open_orders(user).await.expect("open orders");
    assert!(open.iter().any(|order| order.oid == oid));

    let spot = client
        .spot_clearinghouse_state(user)
        .await
        .expect("spot clearinghouse");
    let usdc = spot
        .balances
        .iter()
        .find(|balance| balance.coin == "USDC")
        .expect("usdc balance");
    assert_eq!(usdc.total, "5000");
    assert_eq!(usdc.hold, "300");
}

#[tokio::test]
async fn alo_marketable_orders_are_rejected() {
    let (server, client, _user) = fixture(&[("USDC", 5_000.0)]).await;
    let asset = client.asset_index(UBTC_USDC).unwrap();
    server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;

    let response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "60000".to_string(),
                sz: "0.01".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Alo".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place order");
    let status = &response["response"]["data"]["statuses"][0];
    let err = status["error"]
        .as_str()
        .unwrap_or_else(|| panic!("expected error, got {status:#?}"));
    assert!(err.contains("add-liquidity-only"), "error: {err}");
}

#[tokio::test]
async fn insufficient_balance_rejects_order() {
    // Account with zero USDC can't place even a tiny buy.
    let (server, client, _user) = fixture(&[]).await;
    server.set_hyperliquid_rate("UBTC", "USDC", 30_000.0).await;
    let asset = client.asset_index(UBTC_USDC).unwrap();
    let response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "30000".to_string(),
                sz: "0.01".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Ioc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place order");
    let status = &response["response"]["data"]["statuses"][0];
    let err = status["error"]
        .as_str()
        .unwrap_or_else(|| panic!("expected error, got {status:#?}"));
    assert!(err.contains("insufficient"), "error: {err}");
}

#[tokio::test]
async fn cancel_orders_remove_resting_orders() {
    let (server, client, user) = fixture(&[("USDC", 1_000.0)]).await;
    let asset = client.asset_index(UBTC_USDC).unwrap();
    server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;
    let place = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "30000".to_string(),
                sz: "0.01".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Gtc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place resting order");
    let oid = place["response"]["data"]["statuses"][0]["resting"]["oid"]
        .as_u64()
        .expect("resting oid");

    let resp = client
        .cancel_orders(vec![CancelRequest { asset, oid }])
        .await
        .expect("cancel orders");
    assert_eq!(resp["status"], "ok");
    assert_eq!(resp["response"]["data"]["statuses"][0], "success");

    let open = client.open_orders(user).await.expect("open orders");
    assert!(open.iter().all(|order| order.oid != oid));

    let status = client.order_status(user, oid).await.expect("order status");
    assert_eq!(status.status, "order");
    assert_eq!(status.order.expect("order envelope").status, "canceled");
}

#[tokio::test]
async fn schedule_cancel_cancels_resting_orders_after_deadline() {
    let (server, client, user) = fixture(&[("USDC", 1_000.0)]).await;
    let asset = client.asset_index(UBTC_USDC).unwrap();
    server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;

    let place = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "30000".to_string(),
                sz: "0.01".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Gtc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place resting order");
    let oid = place["response"]["data"]["statuses"][0]["resting"]["oid"]
        .as_u64()
        .expect("resting oid");

    let deadline = chrono::Utc::now().timestamp_millis().max(0) as u64 + 25;
    let schedule = client
        .schedule_cancel(Some(deadline))
        .await
        .expect("schedule cancel");
    assert_eq!(schedule["status"], "ok");
    assert_eq!(schedule["response"]["type"], "scheduleCancel");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let open = client.open_orders(user).await.expect("open orders");
    assert!(open.iter().all(|order| order.oid != oid));

    let status = client.order_status(user, oid).await.expect("order status");
    assert_eq!(status.status, "order");
    assert_eq!(
        status.order.expect("order envelope").status,
        "scheduledCancel"
    );
}

#[tokio::test]
async fn resting_orders_fill_when_rate_moves_through_them() {
    let (server, client, user) = fixture(&[("USDC", 1_000.0)]).await;
    let asset = client.asset_index(UBTC_USDC).unwrap();
    server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;

    let place = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: "30000".to_string(),
                sz: "0.01".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Gtc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("place resting order");
    let oid = place["response"]["data"]["statuses"][0]["resting"]["oid"]
        .as_u64()
        .expect("resting oid");

    server.set_hyperliquid_rate("UBTC", "USDC", 29_000.0).await;

    let open = client.open_orders(user).await.expect("open orders");
    assert!(open.iter().all(|order| order.oid != oid));

    let status = client.order_status(user, oid).await.expect("order status");
    let envelope = status.order.expect("order envelope");
    assert_eq!(envelope.status, "filled");

    let spot = client
        .spot_clearinghouse_state(user)
        .await
        .expect("spot clearinghouse");
    let usdc = spot
        .balances
        .iter()
        .find(|balance| balance.coin == "USDC")
        .expect("usdc balance");
    let ubtc = spot
        .balances
        .iter()
        .find(|balance| balance.coin == "UBTC")
        .expect("ubtc balance");
    assert_eq!(usdc.total, "710");
    assert_eq!(usdc.hold, "0");
    assert_eq!(ubtc.total, "0.01");

    let fills = client.user_fills(user).await.expect("user fills");
    assert!(fills
        .iter()
        .any(|fill| fill.oid == oid && fill.px == "29000"));
}

#[tokio::test]
async fn ubtc_to_ueth_round_trip_swaps_balances() {
    // The scenario the mock exists for: route UBTC -> USDC -> UETH via two
    // IoC legs, simulating HL's lack of a direct UBTC/UETH pair. Rates:
    // 60 000 USDC/UBTC and 3 000 USDC/UETH. Selling 0.5 UBTC yields 30 000
    // USDC; buying UETH with 30 000 USDC at 3 000 yields 10 UETH.
    let (server, client, user) = fixture(&[("UBTC", 0.5)]).await;
    server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;
    server.set_hyperliquid_rate("UETH", "USDC", 3_000.0).await;

    let ubtc_asset = client.asset_index(UBTC_USDC).unwrap();
    let sell_resp = client
        .place_orders(
            vec![OrderRequest {
                asset: ubtc_asset,
                is_buy: false,
                limit_px: "59000".to_string(),
                sz: "0.5".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Ioc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("sell UBTC leg");
    assert!(sell_resp["response"]["data"]["statuses"][0]
        .get("filled")
        .is_some());

    assert!((server.hyperliquid_balance_of(user, "UBTC").await).abs() < 1e-9);
    assert!((server.hyperliquid_balance_of(user, "USDC").await - 30_000.0).abs() < 1e-6);

    let ueth_asset = client.asset_index(UETH_USDC).unwrap();
    let buy_resp = client
        .place_orders(
            vec![OrderRequest {
                asset: ueth_asset,
                is_buy: true,
                limit_px: "3100".to_string(),
                sz: "10".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Ioc".to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await
        .expect("buy UETH leg");
    assert!(buy_resp["response"]["data"]["statuses"][0]
        .get("filled")
        .is_some());

    assert!((server.hyperliquid_balance_of(user, "USDC").await).abs() < 1e-6);
    assert!((server.hyperliquid_balance_of(user, "UETH").await - 10.0).abs() < 1e-9);
}
