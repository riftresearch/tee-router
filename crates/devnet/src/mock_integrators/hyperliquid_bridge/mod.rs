//! The Hyperliquid Bridge2 venue: the on-chain bridge contract, the ERC-20
//! deposit indexer (credits the shared clearinghouse), and the withdrawal
//! release path (mints USDC via the bridge paymaster and calls `release`).
//!
//! This venue has **no HTTP endpoints** — it is entirely on-chain plus the
//! background indexer. The `/exchange` `withdraw3` handler (in the
//! [`crate::mock_integrators::hyperliquid_spot`] venue) debits the
//! clearinghouse and then calls [`schedule_mock_hyperliquid_withdrawal_release`]
//! here to perform the on-chain release.

use alloy::{
    primitives::{Address, Bytes, B256, U256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::types::Filter,
    sol_types::{SolCall, SolEvent},
};
use chrono::Utc;
use hyperliquid_client::{
    UserNonFundingLedgerDelta, UserNonFundingLedgerUpdate, MINIMUM_BRIDGE_DEPOSIT_USDC,
};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::task::JoinHandle;

use eip7702_paymaster::Execution;

pub(crate) mod contract;

use crate::hyperliquid_core::format_hl_amount;
use crate::mock_integrators::hyperliquid_bridge::contract::MockHyperliquidBridge2;
use crate::mock_integrators::{
    deterministic_bps, mock_evm_indexer_initial_last_scanned, mock_mint_erc20_on_anvil,
    MockIntegratorConfig, MockService, IERC20,
};

/// Per-venue config/state for the Hyperliquid Bridge2 mock. Holds the Bridge2
/// deposit wiring/tunables plus the cross-cutting `Arc`s the deposit indexer
/// (credits the clearinghouse) and the withdrawal release (mints USDC via the
/// bridge paymaster) need. There is no router — the bridge is on-chain plus the
/// background indexer only.
pub(crate) struct HyperliquidBridgeMockState {
    pub(crate) bridge_address: Option<String>,
    pub(crate) evm_rpc_url: Option<String>,
    pub(crate) usdc_token_address: Option<String>,
    pub(crate) bridge_deposit_latency: Duration,
    pub(crate) bridge_deposit_failure_probability_bps: u16,
    /// The Hyperliquid devnet "chain" ledger, shared by `Arc`. The deposit
    /// indexer credits the clearinghouse here; the same instance is held by
    /// every venue and by [`crate::mock_integrators::MockIntegratorState`].
    pub(crate) core: Arc<crate::hyperliquid_core::HyperliquidCore>,
    /// Cross-cutting infra the Bridge2 withdrawal release needs (mock-service
    /// paymasters / RPC URLs). Shared `Arc` instance.
    pub(crate) shared: Arc<crate::mock_integrators::SharedMockState>,
}

pub(crate) async fn maybe_spawn_hyperliquid_bridge_indexer(
    config: &MockIntegratorConfig,
    state: Arc<HyperliquidBridgeMockState>,
) -> eyre::Result<(
    Option<tokio::sync::oneshot::Sender<()>>,
    Option<JoinHandle<()>>,
)> {
    let (Some(rpc_url), Some(bridge_str), Some(token_str)) = (
        config.hyperliquid_evm_rpc_url.as_deref(),
        config.hyperliquid_bridge_address.as_deref(),
        config.hyperliquid_usdc_token_address.as_deref(),
    ) else {
        return Ok((None, None));
    };
    let bridge_address: Address = bridge_str.parse().map_err(|err| {
        eyre::eyre!("mock hyperliquid bridge indexer: invalid bridge address {bridge_str}: {err}")
    })?;
    let token_address: Address = token_str.parse().map_err(|err| {
        eyre::eyre!("mock hyperliquid bridge indexer: invalid token address {token_str}: {err}")
    })?;
    let provider: DynProvider = ProviderBuilder::new().connect(rpc_url).await?.erased();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(run_hyperliquid_bridge_indexer(
        provider,
        token_address,
        bridge_address,
        state,
        shutdown_rx,
    ));
    Ok((Some(shutdown_tx), Some(handle)))
}

async fn run_hyperliquid_bridge_indexer(
    provider: DynProvider,
    token_address: Address,
    bridge_address: Address,
    state: Arc<HyperliquidBridgeMockState>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let signature: B256 = IERC20::Transfer::SIGNATURE_HASH;
    let poll = Duration::from_millis(100);
    let mut last_scanned: u64 = mock_evm_indexer_initial_last_scanned();
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(poll) => {}
        }
        let head = match provider.get_block_number().await {
            Ok(head) => head,
            Err(err) => {
                tracing::warn!(%err, "mock hyperliquid bridge indexer: get_block_number failed");
                continue;
            }
        };
        if head <= last_scanned {
            continue;
        }
        let filter = Filter::new()
            .address(token_address)
            .event_signature(signature)
            .from_block(last_scanned + 1)
            .to_block(head);
        let logs = match provider.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(err) => {
                tracing::warn!(%err, "mock hyperliquid bridge indexer: get_logs failed");
                continue;
            }
        };
        for log in logs {
            let decoded = match IERC20::Transfer::decode_log(&log.inner) {
                Ok(event) => event,
                Err(err) => {
                    tracing::warn!(%err, "mock hyperliquid bridge indexer: decode Transfer failed");
                    continue;
                }
            };
            let event = decoded.data;
            if event.to != bridge_address {
                continue;
            }
            if event.value < U256::from(MINIMUM_BRIDGE_DEPOSIT_USDC) {
                tracing::debug!(
                    from = %event.from,
                    amount = %event.value,
                    "mock hyperliquid bridge indexer: ignoring sub-minimum bridge deposit"
                );
                continue;
            }
            let Some(amount) = raw_usdc_to_natural_f64(event.value) else {
                tracing::warn!(
                    from = %event.from,
                    amount = %event.value,
                    "mock hyperliquid bridge indexer: failed to convert transfer value"
                );
                continue;
            };
            let Some(tx_hash) = log.transaction_hash.map(|hash| format!("{hash:#x}")) else {
                tracing::warn!(
                    from = %event.from,
                    amount = %event.value,
                    "mock hyperliquid bridge indexer: Transfer log missing transaction hash"
                );
                continue;
            };
            if mock_hyperliquid_bridge_deposit_should_fail(
                &state,
                event.from,
                event.value,
                &tx_hash,
            ) {
                tracing::warn!(
                    from = %event.from,
                    amount = %event.value,
                    tx_hash,
                    "mock hyperliquid bridge indexer: configured deposit failure"
                );
                continue;
            }
            if state.bridge_deposit_latency > Duration::ZERO {
                schedule_mock_hyperliquid_bridge_deposit_credit(
                    &state, event.from, amount, tx_hash,
                );
            } else {
                credit_mock_hyperliquid_bridge_deposit(&state, event.from, amount, &tx_hash).await;
            }
        }
        last_scanned = head;
    }
}

fn mock_hyperliquid_bridge_deposit_should_fail(
    state: &HyperliquidBridgeMockState,
    user: Address,
    amount_raw: U256,
    tx_hash: &str,
) -> bool {
    let probability_bps = state.bridge_deposit_failure_probability_bps;
    deterministic_bps(
        &format!("hyperliquid-bridge-deposit:{user}:{tx_hash}:{amount_raw}"),
        9_999,
    ) < probability_bps
}

fn schedule_mock_hyperliquid_bridge_deposit_credit(
    state: &Arc<HyperliquidBridgeMockState>,
    user: Address,
    amount: f64,
    tx_hash: String,
) {
    let state = Arc::clone(state);
    let latency = state.bridge_deposit_latency;
    tokio::spawn(async move {
        tokio::time::sleep(latency).await;
        credit_mock_hyperliquid_bridge_deposit(&state, user, amount, &tx_hash).await;
    });
}

async fn credit_mock_hyperliquid_bridge_deposit(
    state: &Arc<HyperliquidBridgeMockState>,
    user: Address,
    amount: f64,
    tx_hash: &str,
) {
    let mut hl = state.core.lock().await;
    hl.credit_clearinghouse(user, "USDC", amount);
    hl.record_ledger_update(
        user,
        UserNonFundingLedgerUpdate {
            time: Utc::now().timestamp_millis().max(0) as u64,
            hash: tx_hash.to_string(),
            delta: UserNonFundingLedgerDelta::Deposit {
                usdc: format_hl_amount(amount),
            },
        },
    );
    tracing::debug!(
        %user,
        amount,
        tx_hash,
        "mock hyperliquid bridge indexer: credited clearinghouse deposit"
    );
}

fn raw_usdc_to_natural_f64(value: U256) -> Option<f64> {
    let raw: u128 = value.try_into().ok()?;
    Some(raw as f64 / 1_000_000.0)
}

/// Schedules the on-chain bridge release for a withdrawal. Called by the spot
/// `withdraw3` handler **after** it has debited the clearinghouse: the spot
/// venue owns the withdrawal latency (its tunable), so it is passed in here.
/// No-ops if the bridge isn't wired (no RPC/bridge/token configured).
pub(crate) fn schedule_mock_hyperliquid_withdrawal_release(
    state: &Arc<HyperliquidBridgeMockState>,
    destination: Address,
    payout_raw: u64,
    latency: Duration,
) {
    let Some(rpc_url) = state.evm_rpc_url.clone() else {
        return;
    };
    let Some(bridge_address) = state.bridge_address.clone() else {
        return;
    };
    let Some(usdc_token_address) = state.usdc_token_address.clone() else {
        return;
    };
    let state = state.clone();
    tokio::spawn(async move {
        if latency > Duration::ZERO {
            tokio::time::sleep(latency).await;
        }
        if let Err(err) = send_mock_hyperliquid_withdrawal_release(
            state,
            &rpc_url,
            &bridge_address,
            &usdc_token_address,
            destination,
            payout_raw,
        )
        .await
        {
            tracing::warn!(%err, %destination, payout_raw, "mock hyperliquid withdraw release failed");
        }
    });
}

async fn send_mock_hyperliquid_withdrawal_release(
    state: Arc<HyperliquidBridgeMockState>,
    rpc_url: &str,
    bridge_address: &str,
    usdc_token_address: &str,
    destination: Address,
    payout_raw: u64,
) -> Result<(), String> {
    let rpc_endpoint = rpc_url;
    let bridge_address = Address::from_str(bridge_address)
        .map_err(|err| format!("invalid hyperliquid bridge address {bridge_address:?}: {err}"))?;
    let usdc_token_address = Address::from_str(usdc_token_address).map_err(|err| {
        format!("invalid hyperliquid USDC token address {usdc_token_address:?}: {err}")
    })?;
    let provider = ProviderBuilder::new()
        .connect(rpc_endpoint)
        .await
        .map_err(|err| format!("hyperliquid withdrawal release RPC init failed: {err}"))?;
    let chain_id = provider
        .get_chain_id()
        .await
        .map_err(|err| format!("hyperliquid withdrawal release get_chain_id failed: {err}"))?;
    mock_mint_erc20_on_anvil(
        &state.shared,
        chain_id,
        MockService::HyperliquidBridge,
        usdc_token_address,
        bridge_address,
        U256::from(payout_raw),
    )
    .await?;
    let calldata = Bytes::from(
        MockHyperliquidBridge2::releaseCall {
            user: destination,
            usd: payout_raw,
        }
        .abi_encode(),
    );
    let handle = state
        .shared
        .mock_service_paymaster(MockService::HyperliquidBridge, chain_id)?;
    let execution = Execution {
        target: bridge_address,
        value: U256::ZERO,
        callData: calldata,
    };
    handle
        .submit(execution)
        .await
        .map(|_| ())
        .map_err(|err| format!("bridge release submit failed: {err}"))
}
