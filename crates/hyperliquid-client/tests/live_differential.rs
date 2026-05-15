use std::{
    env,
    error::Error,
    fs,
    future::Future,
    path::PathBuf,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy::{
    network::EthereumWallet,
    primitives::{address, Address, B256, U256},
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    sol,
};
use hyperliquid_client::{client::Network, HyperliquidClient, Limit, Order, OrderRequest, Tif};
use serde_json::{json, Value};
use url::Url;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_PROVIDER_TESTS: &str = "LIVE_PROVIDER_TESTS";
const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const HYPERLIQUID_BASE_URL: &str = "HYPERLIQUID_LIVE_BASE_URL";
const HYPERLIQUID_NETWORK: &str = "HYPERLIQUID_LIVE_NETWORK";
const HYPERLIQUID_PRIVATE_KEY: &str = "HYPERLIQUID_LIVE_PRIVATE_KEY";
const HYPERLIQUID_USER: &str = "HYPERLIQUID_LIVE_USER";
const HYPERLIQUID_PAIR: &str = "HYPERLIQUID_LIVE_PAIR";
const HYPERLIQUID_IS_BUY: &str = "HYPERLIQUID_LIVE_IS_BUY";
const HYPERLIQUID_LIMIT_PX: &str = "HYPERLIQUID_LIVE_LIMIT_PX";
const HYPERLIQUID_SZ: &str = "HYPERLIQUID_LIVE_SZ";
const HYPERLIQUID_SPEND_CONFIRMATION: &str = "HYPERLIQUID_LIVE_I_UNDERSTAND_THIS_TRADES_REAL_FUNDS";
const HYPERLIQUID_MOVE_CONFIRMATION: &str = "HYPERLIQUID_LIVE_I_UNDERSTAND_THIS_MOVES_REAL_FUNDS";
const HYPERLIQUID_ARBITRUM_RPC_URL: &str = "HYPERLIQUID_LIVE_ARBITRUM_RPC_URL";
const HYPERLIQUID_ARBITRUM_USDC: &str = "HYPERLIQUID_LIVE_ARBITRUM_USDC";
const HYPERLIQUID_BRIDGE_DEPOSIT_AMOUNT: &str = "HYPERLIQUID_LIVE_BRIDGE_DEPOSIT_AMOUNT";
const HYPERLIQUID_CLASS_TRANSFER_USDC: &str = "HYPERLIQUID_LIVE_CLASS_TRANSFER_USDC";
const HYPERLIQUID_MARKET_BUY_USDC: &str = "HYPERLIQUID_LIVE_MARKET_BUY_USDC";
const HYPERLIQUID_WITHDRAW_USDC: &str = "HYPERLIQUID_LIVE_WITHDRAW_USDC";
const LIVE_RECOVERY_DIR: &str = "ROUTER_LIVE_RECOVERY_DIR";
const DEFAULT_UBTC_TOKEN: &str = "UBTC";
const DEFAULT_USDC_TOKEN: &str = "USDC";
const DEFAULT_ARBITRUM_USDC: Address = address!("af88d065e77c8cc2239327c5edb3a432268e5831");
const HYPERLIQUID_BALANCE_POLL_ATTEMPTS: usize = 120;
const HYPERLIQUID_BALANCE_POLL_INTERVAL: Duration = Duration::from_secs(5);
const RPC_RETRY_ATTEMPTS: usize = 6;
const RECEIPT_POLL_ATTEMPTS: usize = 90;
const RECEIPT_POLL_INTERVAL: Duration = Duration::from_secs(2);
const HYPERLIQUID_MIN_ORDER_NOTIONAL_USDC: f64 = 10.1;
const HYPERLIQUID_BRIDGE_WITHDRAW_FEE_USDC: f64 = 1.0;
const DEFAULT_HYPERLIQUID_CLASS_TRANSFER_USDC: f64 = 10.5;
const DEFAULT_HYPERLIQUID_MARKET_BUY_USDC: f64 = 10.5;
const DEFAULT_HYPERLIQUID_WITHDRAW_USDC: f64 = 5.0;

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }
}

#[tokio::test]
#[ignore = "live Hyperliquid API observer test; run with LIVE_PROVIDER_TESTS=1"]
async fn live_hyperliquid_readonly_observer_transcript() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }

    let wallet = private_key_env()
        .ok()
        .map(|raw| raw.parse::<PrivateKeySigner>())
        .transpose()?
        .unwrap_or_else(PrivateKeySigner::random);
    let user = env::var(HYPERLIQUID_USER)
        .ok()
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or_else(|| wallet.address());
    let mut client = live_client(wallet, None)?;

    let meta = client.refresh_spot_meta().await?;
    let pair = env::var(HYPERLIQUID_PAIR)
        .ok()
        .or_else(|| meta.universe.first().map(|pair| pair.name.clone()))
        .ok_or("live Hyperliquid spotMeta returned no spot pairs")?;
    let asset_index = client.asset_index(&pair)?;

    let book = client.l2_book(&pair).await?;
    let balances = client.spot_clearinghouse_state(user).await?;
    let open_orders = client.open_orders(user).await?;
    let fills = client.user_fills(user).await?;

    emit_transcript(
        "hyperliquid.readonly",
        json!({
            "base_url": client.http().base_url(),
            "network": format!("{:?}", client.network()),
            "user": format!("{user:?}"),
            "pair": pair,
            "asset_index": asset_index,
            "spot_meta_counts": {
                "tokens": meta.tokens.len(),
                "universe": meta.universe.len()
            },
            "l2_book": book,
            "spot_clearinghouse_state": balances,
            "open_orders": open_orders,
            "user_fills_sample": fills.into_iter().take(10).collect::<Vec<_>>()
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "SPENDS REAL FUNDS by placing a live Hyperliquid spot order"]
async fn live_hyperliquid_ioc_spot_order_transcript_spends_funds() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        HYPERLIQUID_SPEND_CONFIRMATION,
        "YES",
        "refusing to place a live Hyperliquid order",
    )?;

    let wallet = required_private_key()?.parse::<PrivateKeySigner>()?;
    let user = env::var(HYPERLIQUID_USER)
        .ok()
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or_else(|| wallet.address());
    let mut client = live_client(wallet, None)?;
    client.refresh_spot_meta().await?;

    let pair = required_env(HYPERLIQUID_PAIR)?;
    let asset = client.asset_index(&pair)?;
    let is_buy = parse_bool(&required_env(HYPERLIQUID_IS_BUY)?)?;
    let limit_px = required_env(HYPERLIQUID_LIMIT_PX)?;
    let sz = required_env(HYPERLIQUID_SZ)?;

    let before_balances = client.spot_clearinghouse_state(user).await?;
    let before_fills = client.user_fills(user).await?;
    let order = OrderRequest {
        asset,
        is_buy,
        limit_px: limit_px.clone(),
        sz: sz.clone(),
        reduce_only: false,
        order_type: Order::Limit(Limit {
            tif: Tif::Ioc.as_wire().to_string(),
        }),
        cloid: None,
    };
    let exchange_response = client.place_orders(vec![order], "na").await?;
    let status_entry = exchange_response
        .pointer("/response/data/statuses/0")
        .ok_or("Hyperliquid order response missing first status entry")?;
    if let Some(error) = status_entry.get("error") {
        emit_transcript(
            "hyperliquid.order.error",
            json!({
                "request": {
                    "pair": pair,
                    "asset": asset,
                    "is_buy": is_buy,
                    "limit_px": limit_px,
                    "sz": sz,
                    "tif": "Ioc",
                },
                "before_balances": before_balances,
                "exchange_response": exchange_response,
            }),
        );
        return Err(format!("Hyperliquid returned order error: {error}").into());
    }
    let oid = extract_oid(status_entry).ok_or("Hyperliquid response did not include oid")?;

    let order_status = poll_order_status(&client, user, oid).await?;
    let after_balances = client.spot_clearinghouse_state(user).await?;
    let after_fills = client.user_fills(user).await?;

    emit_transcript(
        "hyperliquid.order.lifecycle",
        json!({
            "base_url": client.http().base_url(),
            "network": format!("{:?}", client.network()),
            "user": format!("{user:?}"),
            "request": {
                "pair": pair,
                "asset": asset,
                "is_buy": is_buy,
                "limit_px": limit_px,
                "sz": sz,
                "tif": "Ioc",
            },
            "before_balances": before_balances,
            "before_fills_len": before_fills.len(),
            "exchange_response": exchange_response,
            "polled_order_status": order_status,
            "after_balances": after_balances,
            "after_fills_sample": after_fills.into_iter().take(10).collect::<Vec<_>>(),
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "SPENDS REAL FUNDS by depositing Arbitrum USDC into live Hyperliquid Bridge2"]
async fn live_hyperliquid_bridge_deposit_transcript_spends_funds() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        HYPERLIQUID_MOVE_CONFIRMATION,
        "YES",
        "refusing to submit a live Hyperliquid bridge deposit",
    )?;

    let wallet = required_private_key()?.parse::<PrivateKeySigner>()?;
    let user = env::var(HYPERLIQUID_USER)
        .ok()
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or_else(|| wallet.address());
    let client = live_client(wallet.clone(), None)?;
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(wallet))
        .connect_http(required_env(HYPERLIQUID_ARBITRUM_RPC_URL)?.parse::<Url>()?);
    let usdc = env::var(HYPERLIQUID_ARBITRUM_USDC)
        .ok()
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or(DEFAULT_ARBITRUM_USDC);
    let amount = U256::from_str_radix(&required_env(HYPERLIQUID_BRIDGE_DEPOSIT_AMOUNT)?, 10)?;

    let before_clearinghouse = client.clearinghouse_state(user).await?;
    let before_spot = client.spot_clearinghouse_state(user).await?;
    let before_spot_usdc = parse_balance(&before_spot, DEFAULT_USDC_TOKEN);
    let deposit = client.build_usdc_bridge_deposit(usdc, amount)?;
    let pending = provider.send_transaction(deposit.tx).await?;
    let tx_hash = *pending.tx_hash();
    wait_for_successful_receipt(&provider, tx_hash, "Hyperliquid bridge deposit").await?;

    let expected_after = before_spot_usdc + raw_usdc_to_natural(amount)?;
    let after_spot = poll_spot_balance(&client, user, DEFAULT_USDC_TOKEN, expected_after).await?;
    let after_clearinghouse = client.clearinghouse_state(user).await?;

    emit_transcript(
        "hyperliquid.bridge_deposit",
        json!({
            "user": format!("{user:#x}"),
            "bridge": format!("{:#x}", deposit.bridge),
            "usdc_token": format!("{:#x}", usdc),
            "amount_raw": amount.to_string(),
            "tx_hash": format!("{tx_hash:#x}"),
            "before_spot": before_spot,
            "before_clearinghouse": before_clearinghouse,
            "after_spot": after_spot,
            "after_clearinghouse": after_clearinghouse,
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "MOVES REAL FUNDS by transferring withdrawable Hyperliquid USDC into spot"]
async fn live_hyperliquid_usd_class_transfer_to_spot_transcript_moves_funds() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        HYPERLIQUID_MOVE_CONFIRMATION,
        "YES",
        "refusing to move live Hyperliquid funds between ledgers",
    )?;

    let wallet = required_private_key()?.parse::<PrivateKeySigner>()?;
    let user = env::var(HYPERLIQUID_USER)
        .ok()
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or_else(|| wallet.address());
    let mut client = live_client(wallet, None)?;
    client.refresh_spot_meta().await?;

    let before_spot = client.spot_clearinghouse_state(user).await?;
    let before_clearinghouse = client.clearinghouse_state(user).await?;
    let before_spot_usdc = parse_balance(&before_spot, DEFAULT_USDC_TOKEN);
    let before_withdrawable = parse_decimal_balance(&before_clearinghouse.withdrawable);
    if before_withdrawable <= 0.0 {
        return Err(
            "no withdrawable Hyperliquid USDC available; run the bridge deposit differential first"
                .into(),
        );
    }

    let requested = env::var(HYPERLIQUID_CLASS_TRANSFER_USDC)
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(DEFAULT_HYPERLIQUID_CLASS_TRANSFER_USDC);
    let transfer_amount = requested.min(before_withdrawable);
    let transfer_amount_wire = format_decimal_ceil(transfer_amount, 6);
    let response = client
        .usd_class_transfer(transfer_amount_wire.clone(), false, current_time_ms())
        .await?;
    let expected_spot_total = before_spot_usdc + transfer_amount;
    let after_spot =
        poll_spot_balance(&client, user, DEFAULT_USDC_TOKEN, expected_spot_total).await?;
    let after_clearinghouse = client.clearinghouse_state(user).await?;

    emit_transcript(
        "hyperliquid.usd_class_transfer",
        json!({
            "user": format!("{user:#x}"),
            "requested_amount": requested,
            "applied_amount": transfer_amount_wire,
            "to_perp": false,
            "before_spot": before_spot,
            "before_clearinghouse": before_clearinghouse,
            "exchange_response": response,
            "after_spot": after_spot,
            "after_clearinghouse": after_clearinghouse,
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "MOVES REAL FUNDS by withdrawing live Hyperliquid USDC to Arbitrum"]
async fn live_hyperliquid_bridge_withdrawal_transcript_moves_funds() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        HYPERLIQUID_MOVE_CONFIRMATION,
        "YES",
        "refusing to withdraw live Hyperliquid funds to Arbitrum",
    )?;

    let wallet = required_private_key()?.parse::<PrivateKeySigner>()?;
    let user = env::var(HYPERLIQUID_USER)
        .ok()
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or_else(|| wallet.address());
    let client = live_client(wallet, None)?;
    let provider = ProviderBuilder::new()
        .connect_http(required_env(HYPERLIQUID_ARBITRUM_RPC_URL)?.parse::<Url>()?);
    let usdc = env::var(HYPERLIQUID_ARBITRUM_USDC)
        .ok()
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or(DEFAULT_ARBITRUM_USDC);
    let requested_amount = env::var(HYPERLIQUID_WITHDRAW_USDC)
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(DEFAULT_HYPERLIQUID_WITHDRAW_USDC);
    if requested_amount <= HYPERLIQUID_BRIDGE_WITHDRAW_FEE_USDC {
        return Err(format!(
            "withdraw amount must exceed the {HYPERLIQUID_BRIDGE_WITHDRAW_FEE_USDC} USDC bridge fee"
        )
        .into());
    }
    let amount_wire = format_decimal_ceil(requested_amount, 6);
    let amount_raw = usdc_decimal_to_raw(&amount_wire)?;
    let expected_arbitrum_credit_raw = amount_raw - U256::from(1_000_000u64);
    let funding = ensure_withdrawable_usdc_balance(&client, user, requested_amount).await?;
    let before_spot = client.spot_clearinghouse_state(user).await?;
    let before_clearinghouse = client.clearinghouse_state(user).await?;
    let before_withdrawable = parse_decimal_balance(&before_clearinghouse.withdrawable);
    let before_arbitrum_balance = read_erc20_balance(&provider, usdc, user).await?;

    let response = client
        .withdraw_to_bridge(format!("{user:#x}"), amount_wire.clone(), current_time_ms())
        .await?;
    let expected_after_withdrawable = before_withdrawable - requested_amount;
    let after_clearinghouse =
        poll_withdrawable_balance_at_most(&client, user, expected_after_withdrawable).await?;
    let after_spot = client.spot_clearinghouse_state(user).await?;
    let after_arbitrum_balance = poll_erc20_balance(
        &provider,
        usdc,
        user,
        before_arbitrum_balance + expected_arbitrum_credit_raw,
    )
    .await?;

    emit_transcript(
        "hyperliquid.withdraw3",
        json!({
            "user": format!("{user:#x}"),
            "destination": format!("{user:#x}"),
            "usdc_token": format!("{usdc:#x}"),
            "requested_amount": amount_wire,
            "expected_arbitrum_credit_raw": expected_arbitrum_credit_raw.to_string(),
            "funding": funding,
            "before_spot": before_spot,
            "before_clearinghouse": before_clearinghouse,
            "before_arbitrum_balance_raw": before_arbitrum_balance.to_string(),
            "exchange_response": response,
            "after_spot": after_spot,
            "after_clearinghouse": after_clearinghouse,
            "after_arbitrum_balance_raw": after_arbitrum_balance.to_string(),
            "arbitrum_delta_raw": (after_arbitrum_balance - before_arbitrum_balance).to_string(),
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "SPENDS REAL FUNDS by placing a marketable live Hyperliquid UBTC/USDC buy"]
async fn live_hyperliquid_marketable_ubtc_buy_transcript_spends_funds() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        HYPERLIQUID_SPEND_CONFIRMATION,
        "YES",
        "refusing to place a live Hyperliquid marketable buy",
    )?;

    let wallet = required_private_key()?.parse::<PrivateKeySigner>()?;
    let user = env::var(HYPERLIQUID_USER)
        .ok()
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or_else(|| wallet.address());
    let mut client = live_client(wallet, None)?;
    let meta = client.refresh_spot_meta().await?;
    let pair = env::var(HYPERLIQUID_PAIR).unwrap_or_else(|_| default_ubtc_usdc_pair(&meta));
    let asset = client.asset_index(&pair)?;
    let base_decimals = meta
        .base_token_for(&pair)
        .ok_or_else(|| format!("pair {pair} missing base token metadata"))?
        .sz_decimals as usize;
    let spend_usdc = env::var(HYPERLIQUID_MARKET_BUY_USDC)
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(DEFAULT_HYPERLIQUID_MARKET_BUY_USDC);
    let book = client.l2_book(&pair).await?;
    let best_ask_level = book
        .best_ask()
        .ok_or_else(|| format!("pair {pair} has no ask levels"))?;
    let best_ask = best_ask_level.px.parse::<f64>()?;
    let price_decimals = visible_decimal_places(&best_ask_level.px);
    let target_spend_usdc = spend_usdc.max(HYPERLIQUID_MIN_ORDER_NOTIONAL_USDC);
    let sz = format_decimal_ceil(target_spend_usdc / best_ask, base_decimals);
    let limit_px = format_decimal_ceil(best_ask * 1.01, price_decimals);

    let spot_funding = ensure_spot_usdc_balance(&client, user, target_spend_usdc).await?;
    let before_balances = client.spot_clearinghouse_state(user).await?;
    let before_clearinghouse = client.clearinghouse_state(user).await?;
    let before_fills = client.user_fills(user).await?;
    let response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: true,
                limit_px: limit_px.clone(),
                sz: sz.clone(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: Tif::Ioc.as_wire().to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await?;
    let status_entry = response
        .pointer("/response/data/statuses/0")
        .ok_or("missing first Hyperliquid status entry")?;
    if let Some(error) = status_entry.get("error") {
        return Err(format!("Hyperliquid returned order error: {error}").into());
    }
    let oid = extract_oid(status_entry).ok_or("Hyperliquid response did not include oid")?;
    let order_status = poll_order_status(&client, user, oid).await?;
    let after_balances = client.spot_clearinghouse_state(user).await?;
    let after_clearinghouse = client.clearinghouse_state(user).await?;
    let after_fills = client.user_fills(user).await?;

    emit_transcript(
        "hyperliquid.marketable_buy",
        json!({
            "user": format!("{user:#x}"),
            "pair": pair,
            "asset": asset,
            "requested_spend_usdc": spend_usdc,
            "derived_limit_px": limit_px,
            "derived_sz": sz,
            "spot_funding": spot_funding,
            "before_balances": before_balances,
            "before_clearinghouse": before_clearinghouse,
            "before_fills_len": before_fills.len(),
            "exchange_response": response,
            "polled_order_status": order_status,
            "after_balances": after_balances,
            "after_clearinghouse": after_clearinghouse,
            "after_fills_sample": after_fills.into_iter().take(10).collect::<Vec<_>>(),
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "SPENDS REAL FUNDS by placing and cancelling a live Hyperliquid resting order"]
async fn live_hyperliquid_resting_order_cancel_transcript_spends_funds() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        HYPERLIQUID_SPEND_CONFIRMATION,
        "YES",
        "refusing to place and cancel a live Hyperliquid order",
    )?;

    let wallet = required_private_key()?.parse::<PrivateKeySigner>()?;
    let user = env::var(HYPERLIQUID_USER)
        .ok()
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or_else(|| wallet.address());
    let mut client = live_client(wallet, None)?;
    let meta = client.refresh_spot_meta().await?;
    let pair = env::var(HYPERLIQUID_PAIR).unwrap_or_else(|_| default_ubtc_usdc_pair(&meta));
    let asset = client.asset_index(&pair)?;
    let plan = plan_resting_order(
        &client,
        user,
        &meta,
        &pair,
        HYPERLIQUID_MIN_ORDER_NOTIONAL_USDC,
    )
    .await?;
    let before_open_orders = client.open_orders(user).await?;
    let place_response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: plan.is_buy,
                limit_px: plan.limit_px.clone(),
                sz: plan.sz.clone(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: Tif::Gtc.as_wire().to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await?;
    let status_entry = place_response
        .pointer("/response/data/statuses/0")
        .ok_or("missing first Hyperliquid status entry")?;
    if let Some(error) = status_entry.get("error") {
        return Err(format!("Hyperliquid returned order error: {error}").into());
    }
    let oid = extract_oid(status_entry).ok_or("Hyperliquid response did not include oid")?;
    let open_orders_after_place = poll_open_order_presence(&client, user, oid, true).await?;
    let cancel_response = client
        .cancel_orders(vec![hyperliquid_client::CancelRequest { asset, oid }])
        .await?;
    let order_status_after_cancel = poll_order_status(&client, user, oid).await?;
    let open_orders_after_cancel = poll_open_order_presence(&client, user, oid, false).await?;

    emit_transcript(
        "hyperliquid.cancel",
        json!({
            "user": format!("{user:#x}"),
            "pair": pair,
            "asset": asset,
            "resting_side": if plan.is_buy { "buy" } else { "sell" },
            "resting_limit_px": plan.limit_px,
            "resting_sz": plan.sz,
            "inventory_strategy": plan.inventory_strategy,
            "before_open_orders_len": before_open_orders.len(),
            "place_response": place_response,
            "open_orders_after_place": open_orders_after_place,
            "cancel_response": cancel_response,
            "order_status_after_cancel": order_status_after_cancel,
            "open_orders_after_cancel": open_orders_after_cancel,
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "SPENDS REAL FUNDS by placing a live Hyperliquid resting order and waiting for scheduleCancel"]
async fn live_hyperliquid_schedule_cancel_transcript_spends_funds() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        HYPERLIQUID_SPEND_CONFIRMATION,
        "YES",
        "refusing to place a live Hyperliquid order with scheduleCancel",
    )?;

    let wallet = required_private_key()?.parse::<PrivateKeySigner>()?;
    let user = env::var(HYPERLIQUID_USER)
        .ok()
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or_else(|| wallet.address());
    let mut client = live_client(wallet, None)?;
    let meta = client.refresh_spot_meta().await?;
    let pair = env::var(HYPERLIQUID_PAIR).unwrap_or_else(|_| default_ubtc_usdc_pair(&meta));
    let cancel_time_ms = current_time_ms().saturating_add(15_000);
    let schedule_response = client.schedule_cancel(Some(cancel_time_ms)).await?;
    if schedule_response["status"] == "err" {
        emit_transcript(
            "hyperliquid.schedule_cancel_rejected",
            json!({
                "user": format!("{user:#x}"),
                "pair": pair,
                "schedule_cancel_time_ms": cancel_time_ms,
                "schedule_response": schedule_response,
            }),
        );
        return Ok(());
    }

    let asset = client.asset_index(&pair)?;
    let plan = plan_resting_order(
        &client,
        user,
        &meta,
        &pair,
        HYPERLIQUID_MIN_ORDER_NOTIONAL_USDC,
    )
    .await?;
    let before_open_orders = client.open_orders(user).await?;
    let place_response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: plan.is_buy,
                limit_px: plan.limit_px.clone(),
                sz: plan.sz.clone(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: Tif::Gtc.as_wire().to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await?;
    let status_entry = place_response
        .pointer("/response/data/statuses/0")
        .ok_or("missing first Hyperliquid status entry")?;
    if let Some(error) = status_entry.get("error") {
        return Err(format!("Hyperliquid returned order error: {error}").into());
    }
    let oid = extract_oid(status_entry).ok_or("Hyperliquid response did not include oid")?;
    let open_orders_after_place = poll_open_order_presence(&client, user, oid, true).await?;
    let order_status_after_schedule = match poll_order_status(&client, user, oid).await {
        Ok(status) => status,
        Err(err) => {
            let cleanup = client
                .cancel_orders(vec![hyperliquid_client::CancelRequest { asset, oid }])
                .await
                .ok();
            return Err(format!(
                "scheduleCancel did not cancel oid {oid}; schedule_response={schedule_response}; cleanup={cleanup:?}; {err}"
            )
            .into());
        }
    };
    let open_orders_after_schedule = poll_open_order_presence(&client, user, oid, false).await?;
    let terminal_status = order_status_after_schedule
        .pointer("/order/status")
        .and_then(Value::as_str)
        .ok_or("orderStatus response missing /order/status")?;
    if terminal_status != "scheduledCancel" {
        return Err(format!(
            "expected scheduledCancel terminal status, got {terminal_status}; schedule_response={schedule_response}"
        )
        .into());
    }

    emit_transcript(
        "hyperliquid.schedule_cancel",
        json!({
            "user": format!("{user:#x}"),
            "pair": pair,
            "asset": asset,
            "resting_side": if plan.is_buy { "buy" } else { "sell" },
            "resting_limit_px": plan.limit_px,
            "resting_sz": plan.sz,
            "inventory_strategy": plan.inventory_strategy,
            "before_open_orders_len": before_open_orders.len(),
            "place_response": place_response,
            "open_orders_after_place": open_orders_after_place,
            "schedule_cancel_time_ms": cancel_time_ms,
            "schedule_response": schedule_response,
            "order_status_after_schedule": order_status_after_schedule,
            "open_orders_after_schedule": open_orders_after_schedule,
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "Cancels all live Hyperliquid open orders for the configured wallet"]
async fn live_hyperliquid_cancel_all_open_orders_cleanup() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        HYPERLIQUID_SPEND_CONFIRMATION,
        "YES",
        "refusing to cancel live Hyperliquid open orders",
    )?;

    let wallet = required_private_key()?.parse::<PrivateKeySigner>()?;
    let user = env::var(HYPERLIQUID_USER)
        .ok()
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or_else(|| wallet.address());
    let mut client = live_client(wallet, None)?;
    client.refresh_spot_meta().await?;

    let open_orders = client.open_orders(user).await?;
    let mut canceled = Vec::new();
    for order in &open_orders {
        let asset = client.asset_index(&order.coin)?;
        let response = client
            .cancel_orders(vec![hyperliquid_client::CancelRequest {
                asset,
                oid: order.oid,
            }])
            .await?;
        canceled.push(json!({
            "coin": order.coin,
            "asset": asset,
            "oid": order.oid,
            "response": response,
        }));
    }

    emit_transcript(
        "hyperliquid.cancel_all_open_orders",
        json!({
            "user": format!("{user:#x}"),
            "open_orders_before": open_orders,
            "cancel_results": canceled,
            "open_orders_after": client.open_orders(user).await?,
        }),
    );

    Ok(())
}

struct RestingOrderPlan {
    is_buy: bool,
    limit_px: String,
    sz: String,
    inventory_strategy: Value,
}

async fn plan_resting_order(
    client: &HyperliquidClient,
    user: Address,
    meta: &hyperliquid_client::SpotMeta,
    pair: &str,
    minimum_notional_usdc: f64,
) -> TestResult<RestingOrderPlan> {
    let base_token = meta
        .base_token_for(pair)
        .ok_or_else(|| format!("pair {pair} missing base token metadata"))?;
    let base_decimals = base_token.sz_decimals as usize;
    let base_coin = base_token.name.clone();
    let book = client.l2_book(pair).await?;
    let best_bid_level = book
        .best_bid()
        .ok_or_else(|| format!("pair {pair} has no bid levels"))?;
    let best_ask_level = book
        .best_ask()
        .ok_or_else(|| format!("pair {pair} has no ask levels"))?;
    let best_bid = best_bid_level.px.parse::<f64>()?;
    let best_ask = best_ask_level.px.parse::<f64>()?;
    let price_decimals =
        visible_decimal_places(&best_bid_level.px).max(visible_decimal_places(&best_ask_level.px));

    let buy_limit_px = format_decimal_floor(best_bid * 0.50, price_decimals);
    let buy_limit_px_value = buy_limit_px.parse::<f64>()?;
    let buy_sz = format_decimal_ceil(minimum_notional_usdc / buy_limit_px_value, base_decimals);
    let buy_sz_value = buy_sz.parse::<f64>()?;
    let required_quote_balance = buy_limit_px_value * buy_sz_value;
    if let Ok(spot_funding) = ensure_spot_usdc_balance(client, user, required_quote_balance).await {
        return Ok(RestingOrderPlan {
            is_buy: true,
            limit_px: buy_limit_px,
            sz: buy_sz,
            inventory_strategy: json!({
                "mode": "buy_with_usdc",
                "spot_funding": spot_funding,
            }),
        });
    }

    let spot_state = client.spot_clearinghouse_state(user).await?;
    let available_base = parse_balance(&spot_state, &base_coin);
    let sell_limit_px = format_decimal_ceil(best_ask * 2.0, price_decimals);
    let sell_sz = format_decimal_ceil(minimum_notional_usdc / best_bid, base_decimals);
    let sell_sz_value = sell_sz.parse::<f64>()?;
    if available_base + 1e-9 < sell_sz_value {
        return Err(format!(
            "insufficient inventory to seed resting order on {pair}: need either {minimum_notional_usdc} USDC-equivalent or {sell_sz} {base_coin}, have {available_base} {base_coin}"
        )
        .into());
    }

    Ok(RestingOrderPlan {
        is_buy: false,
        limit_px: sell_limit_px,
        sz: sell_sz,
        inventory_strategy: json!({
            "mode": "sell_base_inventory",
            "spot_state": spot_state,
            "base_coin": base_coin,
            "available_base": available_base,
        }),
    })
}

fn live_client(
    wallet: PrivateKeySigner,
    vault_address: Option<Address>,
) -> TestResult<HyperliquidClient> {
    let base_url =
        env::var(HYPERLIQUID_BASE_URL).unwrap_or_else(|_| "https://api.hyperliquid.xyz".into());
    let network = match env::var(HYPERLIQUID_NETWORK)
        .unwrap_or_else(|_| "mainnet".into())
        .to_ascii_lowercase()
        .as_str()
    {
        "mainnet" => Network::Mainnet,
        "testnet" => Network::Testnet,
        other => return Err(format!("unsupported {HYPERLIQUID_NETWORK}={other}").into()),
    };
    Ok(HyperliquidClient::new(
        &base_url,
        wallet,
        vault_address,
        network,
    )?)
}

async fn poll_order_status(
    client: &HyperliquidClient,
    user: Address,
    oid: u64,
) -> TestResult<Value> {
    let mut last = None;
    for _ in 0..30 {
        let status = client.order_status(user, oid).await?;
        let value = serde_json::to_value(status)?;
        let terminal = value
            .pointer("/order/status")
            .and_then(Value::as_str)
            .is_some_and(|status| status != "open" && status != "resting");
        last = Some(value);
        if terminal {
            return Ok(last.expect("last status exists"));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(format!("timed out waiting for Hyperliquid oid {oid}; last={last:#?}").into())
}

async fn poll_open_order_presence(
    client: &HyperliquidClient,
    user: Address,
    oid: u64,
    should_exist: bool,
) -> TestResult<Vec<Value>> {
    let mut samples = Vec::new();
    for _ in 0..30 {
        let open_orders = client.open_orders(user).await?;
        let present = open_orders.iter().any(|order| order.oid == oid);
        samples.push(serde_json::to_value(&open_orders)?);
        if present == should_exist {
            return Ok(samples);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(format!("timed out waiting for oid {oid} presence={should_exist} in open orders").into())
}

async fn poll_spot_balance(
    client: &HyperliquidClient,
    user: Address,
    coin: &str,
    minimum_expected_total: f64,
) -> TestResult<Value> {
    let mut last = None;
    for _ in 0..HYPERLIQUID_BALANCE_POLL_ATTEMPTS {
        let state = client.spot_clearinghouse_state(user).await?;
        let total = parse_balance(&state, coin);
        let value = serde_json::to_value(&state)?;
        last = Some(value);
        if total + 1e-9 >= minimum_expected_total {
            return Ok(last.expect("spot state exists"));
        }
        tokio::time::sleep(HYPERLIQUID_BALANCE_POLL_INTERVAL).await;
    }
    Err(format!(
        "timed out waiting for {coin} balance to reach at least {minimum_expected_total}; last={last:#?}"
    )
    .into())
}

async fn ensure_spot_usdc_balance(
    client: &HyperliquidClient,
    user: Address,
    minimum_expected_total: f64,
) -> TestResult<Value> {
    let before_spot = client.spot_clearinghouse_state(user).await?;
    let before_spot_total = parse_balance(&before_spot, DEFAULT_USDC_TOKEN);
    let before_clearinghouse = client.clearinghouse_state(user).await?;
    let before_withdrawable = parse_decimal_balance(&before_clearinghouse.withdrawable);
    if before_spot_total + 1e-9 >= minimum_expected_total {
        return Ok(json!({
            "required_spot_total": minimum_expected_total,
            "transfer_needed": false,
            "before_spot": before_spot,
            "before_clearinghouse": before_clearinghouse,
        }));
    }

    let needed = minimum_expected_total - before_spot_total;
    if before_withdrawable + 1e-9 < needed {
        return Err(format!(
            "insufficient Hyperliquid USDC to fund spot wallet: need {needed}, have spot {before_spot_total} and withdrawable {before_withdrawable}"
        )
        .into());
    }

    let amount = format_decimal_ceil(needed, 6);
    let response = client
        .usd_class_transfer(amount.clone(), false, current_time_ms())
        .await?;
    let after_spot =
        poll_spot_balance(client, user, DEFAULT_USDC_TOKEN, minimum_expected_total).await?;
    let after_clearinghouse = client.clearinghouse_state(user).await?;

    Ok(json!({
        "required_spot_total": minimum_expected_total,
        "transfer_needed": true,
        "transfer_amount": amount,
        "before_spot": before_spot,
        "before_clearinghouse": before_clearinghouse,
        "exchange_response": response,
        "after_spot": after_spot,
        "after_clearinghouse": after_clearinghouse,
    }))
}

async fn ensure_withdrawable_usdc_balance(
    client: &HyperliquidClient,
    user: Address,
    minimum_expected_total: f64,
) -> TestResult<Value> {
    let before_spot = client.spot_clearinghouse_state(user).await?;
    let before_spot_total = parse_balance(&before_spot, DEFAULT_USDC_TOKEN);
    let before_clearinghouse = client.clearinghouse_state(user).await?;
    let before_withdrawable = parse_decimal_balance(&before_clearinghouse.withdrawable);
    if before_withdrawable + 1e-9 >= minimum_expected_total {
        return Ok(json!({
            "required_withdrawable_total": minimum_expected_total,
            "transfer_needed": false,
            "before_spot": before_spot,
            "before_clearinghouse": before_clearinghouse,
        }));
    }

    let needed = minimum_expected_total - before_withdrawable;
    if before_spot_total + 1e-9 < needed {
        return Err(format!(
            "insufficient Hyperliquid USDC to fund withdrawable balance: need {needed}, have spot {before_spot_total} and withdrawable {before_withdrawable}"
        )
        .into());
    }

    let amount = format_decimal_ceil(needed, 6);
    let response = client
        .usd_class_transfer(amount.clone(), true, current_time_ms())
        .await?;
    let after_clearinghouse =
        poll_withdrawable_balance(client, user, minimum_expected_total).await?;
    let after_spot = client.spot_clearinghouse_state(user).await?;

    Ok(json!({
        "required_withdrawable_total": minimum_expected_total,
        "transfer_needed": true,
        "transfer_amount": amount,
        "before_spot": before_spot,
        "before_clearinghouse": before_clearinghouse,
        "exchange_response": response,
        "after_spot": after_spot,
        "after_clearinghouse": after_clearinghouse,
    }))
}

async fn poll_withdrawable_balance(
    client: &HyperliquidClient,
    user: Address,
    minimum_expected_total: f64,
) -> TestResult<Value> {
    let mut last = None;
    for _ in 0..HYPERLIQUID_BALANCE_POLL_ATTEMPTS {
        let state = client.clearinghouse_state(user).await?;
        let withdrawable = parse_decimal_balance(&state.withdrawable);
        let value = serde_json::to_value(&state)?;
        last = Some(value);
        if withdrawable + 1e-9 >= minimum_expected_total {
            return Ok(last.expect("clearinghouse state exists"));
        }
        tokio::time::sleep(HYPERLIQUID_BALANCE_POLL_INTERVAL).await;
    }
    Err(format!(
        "timed out waiting for withdrawable balance to reach at least {minimum_expected_total}; last={last:#?}"
    )
    .into())
}

async fn poll_withdrawable_balance_at_most(
    client: &HyperliquidClient,
    user: Address,
    maximum_expected_total: f64,
) -> TestResult<Value> {
    let mut last = None;
    for _ in 0..HYPERLIQUID_BALANCE_POLL_ATTEMPTS {
        let state = client.clearinghouse_state(user).await?;
        let withdrawable = parse_decimal_balance(&state.withdrawable);
        let value = serde_json::to_value(&state)?;
        last = Some(value);
        if withdrawable <= maximum_expected_total + 1e-9 {
            return Ok(last.expect("clearinghouse state exists"));
        }
        tokio::time::sleep(HYPERLIQUID_BALANCE_POLL_INTERVAL).await;
    }
    Err(format!(
        "timed out waiting for withdrawable balance to fall to at most {maximum_expected_total}; last={last:#?}"
    )
    .into())
}

async fn poll_erc20_balance<P>(
    provider: &P,
    token: Address,
    owner: Address,
    minimum_expected_balance: U256,
) -> TestResult<U256>
where
    P: Provider + Clone,
{
    let mut last = U256::ZERO;
    for _ in 0..HYPERLIQUID_BALANCE_POLL_ATTEMPTS {
        let balance = read_erc20_balance(provider, token, owner).await?;
        last = balance;
        if balance >= minimum_expected_balance {
            return Ok(balance);
        }
        tokio::time::sleep(HYPERLIQUID_BALANCE_POLL_INTERVAL).await;
    }
    Err(format!(
        "timed out waiting for ERC20 balance to reach at least {}; last={last}",
        minimum_expected_balance
    )
    .into())
}

async fn read_erc20_balance<P>(provider: &P, token: Address, owner: Address) -> TestResult<U256>
where
    P: Provider + Clone,
{
    retry_rpc("erc20 balanceOf", || async {
        IERC20::new(token, provider.clone())
            .balanceOf(owner)
            .call()
            .await
            .map_err(box_error)
    })
    .await
}

async fn retry_rpc<T, Fut, Op>(label: &str, mut op: Op) -> TestResult<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = TestResult<T>>,
{
    let mut delay = Duration::from_millis(500);
    for attempt in 1..=RPC_RETRY_ATTEMPTS {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt < RPC_RETRY_ATTEMPTS && is_retryable_rpc_error(err.as_ref()) => {
                eprintln!(
                    "retrying RPC {label} after attempt {attempt}/{RPC_RETRY_ATTEMPTS}: {err}"
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("retry loop always returns before exhausting attempts")
}

async fn wait_for_successful_receipt<P>(provider: &P, tx_hash: B256, label: &str) -> TestResult<()>
where
    P: Provider,
{
    for _ in 0..RECEIPT_POLL_ATTEMPTS {
        let receipt = retry_rpc(&format!("{label} receipt"), || async {
            provider
                .get_transaction_receipt(tx_hash)
                .await
                .map_err(box_error)
        })
        .await?;
        if let Some(receipt) = receipt {
            if !receipt.status() {
                return Err(format!("{label} transaction {} reverted", tx_hash).into());
            }
            return Ok(());
        }
        tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
    }
    Err(format!(
        "{label} transaction {} was not mined before timeout",
        tx_hash
    )
    .into())
}

fn is_retryable_rpc_error(err: &(dyn Error + Send + Sync + 'static)) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("429")
        || message.contains("rate limit")
        || message.contains("rate-limited")
        || message.contains("too many requests")
        || message.contains("timeout")
        || message.contains("timed out")
        || message.contains("temporarily unavailable")
        || message.contains("connection reset")
        || message.contains("connection refused")
        || message.contains("connection closed")
        || message.contains("502")
        || message.contains("503")
        || message.contains("504")
}

fn box_error<E>(err: E) -> Box<dyn Error + Send + Sync>
where
    E: Error + Send + Sync + 'static,
{
    Box::new(err)
}

fn extract_oid(status_entry: &Value) -> Option<u64> {
    status_entry
        .pointer("/filled/oid")
        .or_else(|| status_entry.pointer("/resting/oid"))
        .and_then(Value::as_u64)
}

fn live_enabled() -> bool {
    if env::var(LIVE_PROVIDER_TESTS).as_deref() == Ok("1") {
        true
    } else {
        eprintln!("skipping live provider test; set {LIVE_PROVIDER_TESTS}=1 to enable");
        false
    }
}

fn required_env(key: &str) -> TestResult<String> {
    env::var(key).map_err(|_| format!("missing required env var {key}").into())
}

fn private_key_env() -> Result<String, env::VarError> {
    env::var(LIVE_TEST_PRIVATE_KEY).or_else(|_| env::var(HYPERLIQUID_PRIVATE_KEY))
}

fn required_private_key() -> TestResult<String> {
    private_key_env().map_err(|_| {
        format!(
            "missing required env var {} or {}",
            LIVE_TEST_PRIVATE_KEY, HYPERLIQUID_PRIVATE_KEY
        )
        .into()
    })
}

fn require_confirmation(key: &str, expected: &str, context: &str) -> TestResult<()> {
    let actual = required_env(key)?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{context}; expected {key}={expected}, got {actual:?}").into())
    }
}

fn parse_bool(raw: &str) -> TestResult<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "buy" => Ok(true),
        "0" | "false" | "no" | "sell" => Ok(false),
        other => Err(format!("invalid bool value {other:?}").into()),
    }
}

fn parse_balance(state: &hyperliquid_client::SpotClearinghouseState, coin: &str) -> f64 {
    state.balance_of(coin).parse::<f64>().unwrap_or_default()
}

fn parse_decimal_balance(raw: &str) -> f64 {
    raw.parse::<f64>().unwrap_or_default()
}

fn default_ubtc_usdc_pair(meta: &hyperliquid_client::meta::SpotMeta) -> String {
    meta.universe
        .iter()
        .find(|pair| {
            let Some(base) = meta
                .tokens
                .iter()
                .find(|token| token.index == pair.tokens[0])
            else {
                return false;
            };
            let Some(quote) = meta
                .tokens
                .iter()
                .find(|token| token.index == pair.tokens[1])
            else {
                return false;
            };
            base.name == DEFAULT_UBTC_TOKEN && quote.name == DEFAULT_USDC_TOKEN
        })
        .map(|pair| pair.name.clone())
        .unwrap_or_else(|| "UBTC/USDC".to_string())
}

fn raw_usdc_to_natural(amount: U256) -> TestResult<f64> {
    let raw: u128 = amount
        .try_into()
        .map_err(|_| format!("bridge deposit amount {} does not fit into u128", amount))?;
    Ok(raw as f64 / 1_000_000.0)
}

fn usdc_decimal_to_raw(amount: &str) -> TestResult<U256> {
    let (whole, fractional) = match amount.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None => (amount, ""),
    };
    if fractional.len() > 6 {
        return Err(format!("USDC amount {amount:?} has more than 6 decimal places").into());
    }
    let whole = if whole.is_empty() { "0" } else { whole };
    let fractional = format!("{fractional:0<6}");
    let raw = format!("{whole}{fractional}");
    Ok(U256::from_str_radix(&raw, 10)?)
}

fn format_decimal_floor(value: f64, decimals: usize) -> String {
    let scale = 10f64.powi(decimals as i32);
    let floored = (value * scale).floor() / scale;
    trim_decimal_string(format!("{floored:.decimals$}"))
}

fn format_decimal_ceil(value: f64, decimals: usize) -> String {
    let scale = 10f64.powi(decimals as i32);
    let ceiled = (value * scale).ceil() / scale;
    trim_decimal_string(format!("{ceiled:.decimals$}"))
}

fn trim_decimal_string(mut value: String) -> String {
    while value.contains('.') && value.ends_with('0') {
        value.pop();
    }
    if value.ends_with('.') {
        value.pop();
    }
    value
}

fn visible_decimal_places(raw: &str) -> usize {
    let trimmed = raw.trim_end_matches('0').trim_end_matches('.');
    trimmed
        .split_once('.')
        .map_or(0, |(_, fractional)| fractional.len())
}

fn current_time_ms() -> u64 {
    utc::now().timestamp_millis().max(0) as u64
}

fn emit_transcript(label: &str, value: Value) {
    println!(
        "LIVE_PROVIDER_TRANSCRIPT {label}\n{}",
        serde_json::to_string_pretty(&value).expect("transcript serializes")
    );
    if let Err(error) = write_transcript_artifact(label, &value) {
        eprintln!("failed to write live provider transcript artifact: {error}");
    }
}

fn write_transcript_artifact(label: &str, value: &Value) -> TestResult<()> {
    let created_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock before unix epoch: {error}"))?
        .as_millis();
    let dir = env::var(LIVE_RECOVERY_DIR)
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/live-recovery")
        })
        .join("provider-transcripts");
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!(
        "{}-{}.json",
        created_at_ms,
        sanitize_artifact_label(label)
    ));
    let payload = json!({
        "schema_version": 1,
        "kind": "live_provider_transcript",
        "provider": "hyperliquid",
        "label": label,
        "created_at_ms": created_at_ms,
        "private_key_env_candidates": [LIVE_TEST_PRIVATE_KEY, HYPERLIQUID_PRIVATE_KEY],
        "transcript": value,
    });
    fs::write(&path, serde_json::to_vec_pretty(&payload)?)?;
    eprintln!(
        "live provider transcript artifact written to {}",
        path.display()
    );
    Ok(())
}

fn sanitize_artifact_label(label: &str) -> String {
    label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}
