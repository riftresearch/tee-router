use std::{env, error::Error, str::FromStr};

use alloy::primitives::U256;
use router_core::{
    models::ProviderOrderKind,
    protocol::{AssetId, ChainId, DepositAsset},
    services::action_providers::{ExchangeProvider, ExchangeQuoteRequest},
    services::KyberswapProvider,
};

mod support;
use support::assert_raw_amount_string;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_PROVIDER_TESTS: &str = "LIVE_PROVIDER_TESTS";
const KYBERSWAP_API_URL: &str = "KYBERSWAP_API_URL";
const PROBE_ADDRESS: &str = "0x2222222222222222222222222222222222222222";

const ETHEREUM_USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const BASE_USDC: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const ARBITRUM_USDC: &str = "0xaf88d065e77c8cc2239327c5edb3a432268e5831";

#[tokio::test]
#[ignore = "live KyberSwap API quote probe; run with LIVE_PROVIDER_TESTS=1"]
async fn live_kyberswap_usdc_native_eth_quote_matrix() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }

    let provider = KyberswapProvider::new(env::var(KYBERSWAP_API_URL)?)?;
    let cases = [
        ("ethereum", "evm:1", ETHEREUM_USDC),
        ("base", "evm:8453", BASE_USDC),
        ("arbitrum", "evm:42161", ARBITRUM_USDC),
    ];
    let usdc_amounts = ["1000000", "100000000", "1000000000000"];
    let native_amounts = ["100000000000000", "1000000000000000", "1000000000000000000"];

    for (label, chain, usdc) in cases {
        let usdc_asset = asset(chain, usdc)?;
        let native_asset = asset(chain, "native")?;
        assert_direction_has_route(
            &provider,
            label,
            usdc_asset.clone(),
            native_asset.clone(),
            &usdc_amounts,
        )
        .await?;
        assert_direction_has_route(&provider, label, native_asset, usdc_asset, &native_amounts)
            .await?;
    }

    Ok(())
}

async fn assert_direction_has_route(
    provider: &KyberswapProvider,
    label: &str,
    input_asset: DepositAsset,
    output_asset: DepositAsset,
    amounts: &[&str],
) -> TestResult<()> {
    let mut successful = 0usize;
    for amount in amounts {
        let quote = provider
            .quote_trade(ExchangeQuoteRequest {
                input_asset: input_asset.clone(),
                output_asset: output_asset.clone(),
                input_decimals: Some(decimals(&input_asset)),
                output_decimals: Some(decimals(&output_asset)),
                order_kind: ProviderOrderKind::ExactIn {
                    amount_in: (*amount).to_string(),
                    min_amount_out: Some("1".to_string()),
                },
                sender_address: Some(PROBE_ADDRESS.to_string()),
                recipient_address: PROBE_ADDRESS.to_string(),
            })
            .await?;
        if let Some(quote) = quote {
            assert_raw_amount_string("kyberswap.amount_in", &quote.amount_in);
            assert_raw_amount_string("kyberswap.amount_out", &quote.amount_out);
            let amount_out = U256::from_str(&quote.amount_out)?;
            assert!(!amount_out.is_zero(), "{label} quote returned zero output");
            assert_eq!(quote.provider_id, "kyberswap");
            assert_eq!(quote.provider_quote["kind"], "universal_router_swap");
            successful += 1;
        }
    }
    assert!(
        successful > 0,
        "KyberSwap returned no route for {label} {} -> {} across amounts {:?}",
        input_asset.asset.as_str(),
        output_asset.asset.as_str(),
        amounts
    );
    Ok(())
}

fn asset(chain: &str, asset: &str) -> TestResult<DepositAsset> {
    Ok(DepositAsset {
        chain: ChainId::parse(chain)?,
        asset: AssetId::parse(asset)?,
    })
}

fn decimals(asset: &DepositAsset) -> u8 {
    match asset.asset.as_str() {
        "native" => 18,
        _ => 6,
    }
}

fn live_enabled() -> bool {
    matches!(
        env::var(LIVE_PROVIDER_TESTS).as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
}
