use alloy::{
    hex,
    primitives::{Address, Bytes, B256, U256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::types::Filter,
    sol_types::{SolCall, SolEvent},
};
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use hyperliquid_client::{
    recover_l1_signer, recover_typed_signer, Actions, SendAsset, SpotSend, UsdClassTransfer,
    UserNonFundingLedgerDelta, UserNonFundingLedgerUpdate, Withdraw3, MINIMUM_BRIDGE_DEPOSIT_USDC,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::task::JoinHandle;

use eip7702_paymaster::Execution;

use crate::hyperliquid_bridge_mock::MockHyperliquidBridge2;
use crate::hyperliquid_core::{format_hl_amount, hyperliquid_has_sufficient_amount};
use crate::mock_integrators::{
    complete_unit_withdrawal_after_hyperliquid_transfer, deterministic_bps, error_response,
    mock_evm_indexer_initial_last_scanned, mock_mint_erc20_on_anvil, MockIntegratorConfig,
    MockIntegratorErrorResponse, MockIntegratorState, MockService, IERC20,
};

pub(crate) async fn maybe_spawn_hyperliquid_bridge_indexer(
    config: &MockIntegratorConfig,
    state: Arc<MockIntegratorState>,
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
    state: Arc<MockIntegratorState>,
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
            if state.hyperliquid_bridge_deposit_latency > Duration::ZERO {
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
    state: &MockIntegratorState,
    user: Address,
    amount_raw: U256,
    tx_hash: &str,
) -> bool {
    let probability_bps = state.hyperliquid_bridge_deposit_failure_probability_bps;
    deterministic_bps(
        &format!("hyperliquid-bridge-deposit:{user}:{tx_hash}:{amount_raw}"),
        9_999,
    ) < probability_bps
}

fn schedule_mock_hyperliquid_bridge_deposit_credit(
    state: &Arc<MockIntegratorState>,
    user: Address,
    amount: f64,
    tx_hash: String,
) {
    let state = Arc::clone(state);
    let latency = state.hyperliquid_bridge_deposit_latency;
    tokio::spawn(async move {
        tokio::time::sleep(latency).await;
        credit_mock_hyperliquid_bridge_deposit(&state, user, amount, &tx_hash).await;
    });
}

async fn credit_mock_hyperliquid_bridge_deposit(
    state: &Arc<MockIntegratorState>,
    user: Address,
    amount: f64,
    tx_hash: &str,
) {
    let mut hl = state.hyperliquid_core.lock().await;
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


fn required_hyperliquid_user(request: &Value, endpoint: &str) -> Result<Address, String> {
    request
        .get("user")
        .and_then(Value::as_str)
        .and_then(parse_user_address)
        .ok_or_else(|| format!("{endpoint} requires a 0x-prefixed `user` field"))
}

fn required_hyperliquid_u64(request: &Value, endpoint: &str, field: &str) -> Result<u64, String> {
    request
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("{endpoint} requires a numeric `{field}` field"))
}

fn hyperliquid_info_validation_error(message: String) -> axum::response::Response {
    error_response(StatusCode::UNPROCESSABLE_ENTITY, message)
}

fn optional_hyperliquid_u64(request: &Value, field: &str) -> Option<u64> {
    request.get(field).and_then(Value::as_u64)
}

/// Mocks Hyperliquid's `POST /info` read-only endpoint. Dispatches on the
/// `type` discriminator — every supported response is round-trippable into
/// the matching `hyperliquid_client` type so clients exercising this mock
/// speak the real wire format.
pub(crate) async fn mock_hyperliquid_info(
    State(state): State<Arc<MockIntegratorState>>,
    Json(request): Json<Value>,
) -> impl IntoResponse {
    let Some(kind) = request.get("type").and_then(Value::as_str) else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "mock Hyperliquid /info requires a `type` field",
        );
    };
    let mut hl = state.hyperliquid_core.lock().await;
    hl.run_due_scheduled_cancels();
    match kind {
        "spotMeta" => match hl.spot_meta_value() {
            Ok(value) => Json(value).into_response(),
            Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        "meta" => Json(hl.perp_meta()).into_response(),
        "l2Book" => {
            let Some(coin) = request.get("coin").and_then(Value::as_str) else {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "l2Book requires a `coin` field",
                );
            };
            Json(hl.l2_book_snapshot(coin)).into_response()
        }
        "orderStatus" => {
            let Some(user) = request
                .get("user")
                .and_then(Value::as_str)
                .and_then(parse_user_address)
            else {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "orderStatus requires a 0x-prefixed `user` field",
                );
            };
            let Some(oid) = request.get("oid").and_then(Value::as_u64) else {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "orderStatus requires a numeric `oid` field",
                );
            };
            Json(hl.order_status_snapshot(user, oid)).into_response()
        }
        "spotClearinghouseState" => {
            let Some(user) = request
                .get("user")
                .and_then(Value::as_str)
                .and_then(parse_user_address)
            else {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "spotClearinghouseState requires a 0x-prefixed `user` field",
                );
            };
            Json(hl.spot_clearinghouse_snapshot(user)).into_response()
        }
        "clearinghouseState" => {
            let Some(user) = request
                .get("user")
                .and_then(Value::as_str)
                .and_then(parse_user_address)
            else {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "clearinghouseState requires a 0x-prefixed `user` field",
                );
            };
            Json(hl.clearinghouse_snapshot(user)).into_response()
        }
        "openOrders" => {
            let Some(user) = request
                .get("user")
                .and_then(Value::as_str)
                .and_then(parse_user_address)
            else {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "openOrders requires a 0x-prefixed `user` field",
                );
            };
            Json(hl.open_orders_for(user)).into_response()
        }
        "userFills" => {
            let user = match required_hyperliquid_user(&request, "userFills") {
                Ok(user) => user,
                Err(message) => return hyperliquid_info_validation_error(message),
            };
            Json(hl.fills_for(user)).into_response()
        }
        "userFillsByTime" => {
            let user = match required_hyperliquid_user(&request, "userFillsByTime") {
                Ok(user) => user,
                Err(message) => return hyperliquid_info_validation_error(message),
            };
            let start_time =
                match required_hyperliquid_u64(&request, "userFillsByTime", "startTime") {
                    Ok(start_time) => start_time,
                    Err(message) => return hyperliquid_info_validation_error(message),
                };
            let end_time = optional_hyperliquid_u64(&request, "endTime");
            let aggregate_by_time = request
                .get("aggregateByTime")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Json(hl.fills_by_time(user, start_time, end_time, aggregate_by_time)).into_response()
        }
        "userNonFundingLedgerUpdates" => {
            let user = match required_hyperliquid_user(&request, "userNonFundingLedgerUpdates") {
                Ok(user) => user,
                Err(message) => return hyperliquid_info_validation_error(message),
            };
            let start_time = match required_hyperliquid_u64(
                &request,
                "userNonFundingLedgerUpdates",
                "startTime",
            ) {
                Ok(start_time) => start_time,
                Err(message) => return hyperliquid_info_validation_error(message),
            };
            let end_time = optional_hyperliquid_u64(&request, "endTime");
            Json(hl.ledger_updates_by_time(user, start_time, end_time)).into_response()
        }
        "userFunding" => {
            let user = match required_hyperliquid_user(&request, "userFunding") {
                Ok(user) => user,
                Err(message) => return hyperliquid_info_validation_error(message),
            };
            let start_time = match required_hyperliquid_u64(&request, "userFunding", "startTime") {
                Ok(start_time) => start_time,
                Err(message) => return hyperliquid_info_validation_error(message),
            };
            let end_time = optional_hyperliquid_u64(&request, "endTime");
            Json(hl.fundings_by_time(user, start_time, end_time)).into_response()
        }
        "userRateLimit" => {
            let user = match required_hyperliquid_user(&request, "userRateLimit") {
                Ok(user) => user,
                Err(message) => return hyperliquid_info_validation_error(message),
            };
            Json(hl.user_rate_limit(user)).into_response()
        }
        other => error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("mock Hyperliquid /info does not support type {other}"),
        ),
    }
}

/// Mocks Hyperliquid's `POST /exchange` signed-action endpoint. Parses the
/// action, recovers the signing address, and mutates the shared state —
/// fills against the configured L2 book, rests remainders in the user's
/// open-orders list, cancels, or moves spot balances between users — then
/// returns the real API's response shape.
pub(crate) async fn mock_hyperliquid_exchange(
    State(state): State<Arc<MockIntegratorState>>,
    Json(request): Json<Value>,
) -> impl IntoResponse {
    if let Some(error) = state.next_hyperliquid_exchange_error.lock().await.take() {
        return (
            StatusCode::BAD_GATEWAY,
            Json(MockIntegratorErrorResponse { error }),
        )
            .into_response();
    }

    state
        .hyperliquid_exchange_submissions
        .lock()
        .await
        .push(request.clone());

    let action_type = request
        .get("action")
        .and_then(|a| a.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    match action_type.as_str() {
        "order" | "cancel" | "scheduleCancel" => {
            handle_l1_action(&state, &request, &action_type).await
        }
        "spotSend" => handle_spot_send(&state, &request).await,
        "sendAsset" => handle_send_asset(&state, &request).await,
        "usdClassTransfer" => handle_usd_class_transfer(&state, &request).await,
        "withdraw3" => handle_withdraw3(&state, &request).await,
        other => error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("mock Hyperliquid /exchange does not support action type {other}"),
        ),
    }
}

async fn handle_l1_action(
    state: &Arc<MockIntegratorState>,
    request: &Value,
    action_type: &str,
) -> axum::response::Response {
    let Some(action_value) = request.get("action").cloned() else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "/exchange body is missing `action`",
        );
    };
    let action: Actions = match serde_json::from_value(action_value.clone()) {
        Ok(a) => a,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("failed to deserialize {action_type} action: {err}"),
            );
        }
    };
    let nonce = match request.get("nonce").and_then(Value::as_u64) {
        Some(n) => n,
        None => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "/exchange body is missing `nonce`",
            );
        }
    };
    let vault_address = request
        .get("vaultAddress")
        .and_then(Value::as_str)
        .and_then(|s| Address::from_str(s).ok());
    let signature = match parse_signature(request.get("signature")) {
        Ok(sig) => sig,
        Err(msg) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, msg),
    };
    let connection_id = match action.hash(nonce, vault_address) {
        Ok(id) => id,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("failed to hash {action_type} action: {err}"),
            );
        }
    };
    let signer = match recover_l1_signer(connection_id, state.mainnet_hyperliquid, &signature) {
        Ok(addr) => addr,
        Err(err) => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                format!("signature recovery failed: {err}"),
            );
        }
    };
    let user = vault_address.unwrap_or(signer);

    let mut hl = state.hyperliquid_core.lock().await;
    hl.run_due_scheduled_cancels();
    match action {
        Actions::Order(bulk) => {
            let statuses = hl.place_spot_orders(user, &bulk);
            Json(json!({
                "status": "ok",
                "response": {
                    "type": "order",
                    "data": { "statuses": statuses }
                }
            }))
            .into_response()
        }
        Actions::Cancel(bulk) => {
            let statuses = hl.cancel_spot_orders(user, &bulk);
            Json(json!({
                "status": "ok",
                "response": {
                    "type": "cancel",
                    "data": { "statuses": statuses }
                }
            }))
            .into_response()
        }
        Actions::ScheduleCancel(schedule) => {
            hl.set_schedule_cancel(user, schedule.time);
            Json(json!({
                "status": "ok",
                "response": {
                    "type": "scheduleCancel"
                }
            }))
            .into_response()
        }
    }
}

async fn handle_spot_send(
    state: &Arc<MockIntegratorState>,
    request: &Value,
) -> axum::response::Response {
    let Some(action_value) = request.get("action").cloned() else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "/exchange body is missing `action`",
        );
    };
    // The wire envelope carries `type: "spotSend"`; SpotSend itself doesn't
    // serialize a type tag, so strip it before deserializing.
    let mut stripped = action_value.clone();
    if let Some(map) = stripped.as_object_mut() {
        map.remove("type");
    }
    let payload: SpotSend = match serde_json::from_value(stripped) {
        Ok(p) => p,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("failed to deserialize spotSend: {err}"),
            );
        }
    };
    let signature = match parse_signature(request.get("signature")) {
        Ok(sig) => sig,
        Err(msg) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, msg),
    };
    let signer = match recover_typed_signer(&payload, &signature) {
        Ok(addr) => addr,
        Err(err) => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                format!("spotSend signature recovery failed: {err}"),
            );
        }
    };
    let nonce = request.get("nonce").and_then(Value::as_u64);

    // The token wire form is "SYMBOL:0x..."; the plain symbol is what we
    // key balances by.
    let token_symbol = payload
        .token
        .split(':')
        .next()
        .unwrap_or(&payload.token)
        .to_string();
    let amount = match payload.amount.parse::<f64>() {
        Ok(v) if v > 0.0 => v,
        _ => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "spotSend amount must be a positive decimal, got {}",
                    payload.amount
                ),
            );
        }
    };

    {
        let mut hl = state.hyperliquid_core.lock().await;
        let available = hl.available_spot(signer, &token_symbol);
        if !hyperliquid_has_sufficient_amount(available, amount) {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("spotSend: {signer:?} has {available} {token_symbol}, needs {amount}"),
            );
        }
        hl.debit_spot_total(signer, &token_symbol, amount);
        if let Ok(dst) = Address::from_str(&payload.destination) {
            hl.credit_spot(dst, &token_symbol, amount);
        }
    }

    let source_tx_hash = match nonce {
        Some(nonce) => format!("{signer:#x}:{nonce}"),
        None => format!("{signer:#x}:0"),
    };
    if let Err(response) = complete_unit_withdrawal_after_hyperliquid_transfer(
        state,
        &payload.destination,
        format!("{signer:#x}"),
        &payload.amount,
        source_tx_hash,
    )
    .await
    {
        return response;
    }

    Json(json!({
        "status": "ok",
        "response": { "type": "default" }
    }))
    .into_response()
}

async fn handle_send_asset(
    state: &Arc<MockIntegratorState>,
    request: &Value,
) -> axum::response::Response {
    let Some(action_value) = request.get("action").cloned() else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "/exchange body is missing `action`",
        );
    };
    let mut stripped = action_value.clone();
    if let Some(map) = stripped.as_object_mut() {
        map.remove("type");
    }
    let payload: SendAsset = match serde_json::from_value(stripped) {
        Ok(p) => p,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("failed to deserialize sendAsset: {err}"),
            );
        }
    };
    let signature = match parse_signature(request.get("signature")) {
        Ok(sig) => sig,
        Err(msg) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, msg),
    };
    let signer = match recover_typed_signer(&payload, &signature) {
        Ok(addr) => addr,
        Err(err) => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                format!("sendAsset signature recovery failed: {err}"),
            );
        }
    };
    if payload.source_dex != "spot" || payload.destination_dex != "spot" {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "mock sendAsset only supports spot->spot transfers, got {}->{}",
                payload.source_dex, payload.destination_dex
            ),
        );
    }
    let source_user = if payload.from_sub_account.trim().is_empty() {
        signer
    } else {
        match Address::from_str(&payload.from_sub_account) {
            Ok(address) => address,
            Err(err) => {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    format!("sendAsset fromSubAccount is not an address: {err}"),
                );
            }
        }
    };
    let token_symbol = payload
        .token
        .split(':')
        .next()
        .unwrap_or(&payload.token)
        .to_string();
    let amount = match payload.amount.parse::<f64>() {
        Ok(v) if v > 0.0 => v,
        _ => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "sendAsset amount must be a positive decimal, got {}",
                    payload.amount
                ),
            );
        }
    };

    {
        let mut hl = state.hyperliquid_core.lock().await;
        let available = hl.available_spot(source_user, &token_symbol);
        if !hyperliquid_has_sufficient_amount(available, amount) {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "sendAsset: {source_user:?} has {available} {token_symbol}, needs {amount}"
                ),
            );
        }
        hl.debit_spot_total(source_user, &token_symbol, amount);
        if let Ok(dst) = Address::from_str(&payload.destination) {
            hl.credit_spot(dst, &token_symbol, amount);
        }
    }

    let source_tx_hash = format!("{source_user:#x}:{}", payload.nonce);
    if let Err(response) = complete_unit_withdrawal_after_hyperliquid_transfer(
        state,
        &payload.destination,
        format!("{source_user:#x}"),
        &payload.amount,
        source_tx_hash,
    )
    .await
    {
        return response;
    }

    Json(json!({
        "status": "ok",
        "response": { "type": "default" }
    }))
    .into_response()
}

async fn handle_usd_class_transfer(
    state: &Arc<MockIntegratorState>,
    request: &Value,
) -> axum::response::Response {
    let Some(action_value) = request.get("action") else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "/exchange body is missing `action`",
        );
    };
    let amount_with_target = match action_value.get("amount").and_then(Value::as_str) {
        Some(amount) => amount.to_string(),
        None => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "usdClassTransfer requires string field `amount`",
            );
        }
    };
    let to_perp = match action_value.get("toPerp").and_then(Value::as_bool) {
        Some(to_perp) => to_perp,
        None => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "usdClassTransfer requires bool field `toPerp`",
            );
        }
    };
    let nonce = match action_value.get("nonce").and_then(Value::as_u64) {
        Some(nonce) => nonce,
        None => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "usdClassTransfer requires u64 field `nonce`",
            );
        }
    };
    let signature = match parse_signature(request.get("signature")) {
        Ok(sig) => sig,
        Err(msg) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, msg),
    };
    let payload = UsdClassTransfer {
        signature_chain_id: if state.mainnet_hyperliquid {
            42_161
        } else {
            421_614
        },
        hyperliquid_chain: if state.mainnet_hyperliquid {
            "Mainnet".to_string()
        } else {
            "Testnet".to_string()
        },
        amount: amount_with_target.clone(),
        to_perp,
        nonce,
    };
    let signer = match recover_typed_signer(&payload, &signature) {
        Ok(addr) => addr,
        Err(err) => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                format!("usdClassTransfer signature recovery failed: {err}"),
            );
        }
    };
    let (amount, maybe_subaccount) = match parse_usd_class_transfer_amount(&amount_with_target) {
        Ok(parsed) => parsed,
        Err(msg) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, msg),
    };
    let user = maybe_subaccount.unwrap_or(signer);

    let mut hl = state.hyperliquid_core.lock().await;
    let available = if to_perp {
        hl.available_spot(user, "USDC")
    } else {
        hl.clearinghouse_total(user, "USDC")
    };
    if !hyperliquid_has_sufficient_amount(available, amount) {
        let source = if to_perp { "spot" } else { "clearinghouse" };
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("usdClassTransfer: {user:?} has {available} USDC in {source}, needs {amount}"),
        );
    }

    if to_perp {
        hl.debit_spot_total(user, "USDC", amount);
        hl.credit_clearinghouse(user, "USDC", amount);
    } else {
        hl.debit_clearinghouse_total(user, "USDC", amount);
        hl.credit_spot(user, "USDC", amount);
    }

    Json(json!({
        "status": "ok",
        "response": { "type": "default" }
    }))
    .into_response()
}

async fn handle_withdraw3(
    state: &Arc<MockIntegratorState>,
    request: &Value,
) -> axum::response::Response {
    let Some(action_value) = request.get("action").cloned() else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "/exchange body is missing `action`",
        );
    };
    let mut stripped = action_value;
    if let Some(map) = stripped.as_object_mut() {
        map.remove("type");
    }
    let payload: Withdraw3 = match serde_json::from_value(stripped) {
        Ok(p) => p,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("failed to deserialize withdraw3: {err}"),
            );
        }
    };
    let signature = match parse_signature(request.get("signature")) {
        Ok(sig) => sig,
        Err(msg) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, msg),
    };
    let signer = match recover_typed_signer(&payload, &signature) {
        Ok(addr) => addr,
        Err(err) => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                format!("withdraw3 signature recovery failed: {err}"),
            );
        }
    };
    let user = request
        .get("vaultAddress")
        .and_then(Value::as_str)
        .and_then(|raw| Address::from_str(raw).ok())
        .unwrap_or(signer);
    let destination = match Address::from_str(&payload.destination) {
        Ok(destination) => destination,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("withdraw3 destination must be a 0x-prefixed address: {err}"),
            );
        }
    };
    let amount_raw = match parse_usdc_decimal_to_raw_u64(&payload.amount) {
        Ok(amount_raw) => amount_raw,
        Err(err) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, err),
    };
    if amount_raw <= HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "withdraw3 amount must exceed the {} raw USDC fee",
                HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW
            ),
        );
    }
    let gross_amount = amount_raw as f64 / 1_000_000.0;
    {
        let mut hl = state.hyperliquid_core.lock().await;
        let available = hl.clearinghouse_total(user, "USDC");
        if !hyperliquid_has_sufficient_amount(available, gross_amount) {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "withdraw3: {user:?} has {available} USDC withdrawable, needs {gross_amount}"
                ),
            );
        }
        hl.debit_clearinghouse_total(user, "USDC", gross_amount);
        hl.record_ledger_update(
            user,
            UserNonFundingLedgerUpdate {
                time: payload.time,
                hash: format!(
                    "0x{}",
                    hex::encode(Sha256::digest(format!(
                        "mock-hl-withdraw:{user}:{destination}:{amount_raw}:{}",
                        payload.time
                    )))
                ),
                delta: UserNonFundingLedgerDelta::Withdraw {
                    usdc: payload.amount.clone(),
                    nonce: payload.time,
                    fee: "1".to_string(),
                },
            },
        );
    }
    schedule_mock_hyperliquid_withdrawal_release(
        state,
        destination,
        amount_raw - HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW,
    );
    Json(json!({
        "status": "ok",
        "response": { "type": "default" }
    }))
    .into_response()
}

const HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW: u64 = 1_000_000;

fn parse_user_address(value: &str) -> Option<Address> {
    value
        .starts_with("0x")
        .then(|| Address::from_str(value).ok())
        .flatten()
}

fn parse_usd_class_transfer_amount(value: &str) -> Result<(f64, Option<Address>), String> {
    let (amount, maybe_subaccount) = match value.split_once(" subaccount:") {
        Some((amount, subaccount)) => (
            amount,
            Some(Address::from_str(subaccount).map_err(|err| {
                format!("invalid usdClassTransfer subaccount {subaccount:?}: {err}")
            })?),
        ),
        None => (value, None),
    };
    let amount = amount
        .parse::<f64>()
        .map_err(|_| format!("usdClassTransfer amount must be a positive decimal, got {value}"))?;
    if amount <= 0.0 {
        return Err(format!(
            "usdClassTransfer amount must be a positive decimal, got {value}"
        ));
    }
    Ok((amount, maybe_subaccount))
}

fn parse_usdc_decimal_to_raw_u64(value: &str) -> Result<u64, String> {
    let (whole, fractional) = match value.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None => (value, ""),
    };
    if fractional.len() > 6 {
        return Err(format!(
            "USDC amount {value:?} has more than 6 decimal places"
        ));
    }
    let whole = if whole.is_empty() { "0" } else { whole };
    let padded_fractional = format!("{fractional:0<6}");
    let whole: u64 = whole
        .parse()
        .map_err(|err| format!("invalid USDC whole amount {whole:?}: {err}"))?;
    let fractional: u64 = padded_fractional
        .parse()
        .map_err(|err| format!("invalid USDC fractional amount {fractional:?}: {err}"))?;
    whole
        .checked_mul(HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW)
        .and_then(|shifted| shifted.checked_add(fractional))
        .ok_or_else(|| format!("USDC amount {value:?} overflows u64 raw units"))
}

fn schedule_mock_hyperliquid_withdrawal_release(
    state: &Arc<MockIntegratorState>,
    destination: Address,
    payout_raw: u64,
) {
    let Some(rpc_url) = state.hyperliquid_evm_rpc_url.clone() else {
        return;
    };
    let Some(bridge_address) = state.hyperliquid_bridge_address.clone() else {
        return;
    };
    let Some(usdc_token_address) = state.hyperliquid_usdc_token_address.clone() else {
        return;
    };
    let latency = state.hyperliquid_withdrawal_latency;
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
    state: Arc<MockIntegratorState>,
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
        &state,
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
    let handle = state.mock_service_paymaster(MockService::HyperliquidBridge, chain_id)?;
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

fn parse_signature(value: Option<&Value>) -> Result<alloy::primitives::Signature, String> {
    let obj = value
        .and_then(Value::as_object)
        .ok_or_else(|| "/exchange body is missing `signature`".to_string())?;
    let r = obj
        .get("r")
        .and_then(Value::as_str)
        .ok_or_else(|| "signature.r missing".to_string())?;
    let s = obj
        .get("s")
        .and_then(Value::as_str)
        .ok_or_else(|| "signature.s missing".to_string())?;
    let v = obj
        .get("v")
        .and_then(Value::as_u64)
        .ok_or_else(|| "signature.v missing".to_string())?;
    let r = U256::from_str_radix(r.trim_start_matches("0x"), 16)
        .map_err(|err| format!("signature.r not hex: {err}"))?;
    let s = U256::from_str_radix(s.trim_start_matches("0x"), 16)
        .map_err(|err| format!("signature.s not hex: {err}"))?;
    let parity = v.saturating_sub(27) != 0;
    Ok(alloy::primitives::Signature::new(r, s, parity))
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_integrators::{MockIntegratorConfig, MockIntegratorServer, MockServicePaymaster};
    use hyperliquid_client::UserFill;
    use hyperunit_client::{UnitChain, UnitGenerateAddressResponse};
    use std::collections::{BTreeMap, BTreeSet};

    /// The mock `POST /info` + `POST /exchange` endpoints must return JSON
    /// that deserializes back into the real `hyperliquid_client` response
    /// types. This guards the "speak the real API shape byte-for-byte"
    /// invariant: production code paths expecting HL's wire format will also
    /// work against the mock.
    fn hyperliquid_shape_test_user() -> Address {
        "0x1111111111111111111111111111111111111111"
            .parse()
            .expect("static address parses")
    }

    fn sample_hyperliquid_user_fill(time: u64, tid: u64) -> UserFill {
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
            oid: 42,
            crossed: true,
            fee: "0.00001".to_string(),
            tid,
            fee_token: "UBTC".to_string(),
        }
    }

    async fn assert_hyperliquid_info_error(
        server: &MockIntegratorServer,
        request: Value,
        expected_error: &str,
    ) {
        let response = reqwest::Client::new()
            .post(format!("{}/info", server.hyperliquid_url()))
            .json(&request)
            .send()
            .await
            .expect("http");
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body: MockIntegratorErrorResponse = response.json().await.expect("error body");
        assert_eq!(body.error, expected_error);
    }

    #[tokio::test]
    async fn hyperliquid_info_spot_meta_matches_real_shape() {
        use hyperliquid_client::meta::SpotMeta;

        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let body: SpotMeta = reqwest::Client::new()
            .post(format!("{}/info", server.hyperliquid_url()))
            .json(&json!({ "type": "spotMeta" }))
            .send()
            .await
            .expect("http")
            .json()
            .await
            .expect("deserialize SpotMeta");

        let map = body
            .coin_to_asset_map()
            .expect("mock spotMeta should produce valid wire asset ids");
        assert_eq!(map.get("UBTC/USDC"), Some(&10_140));
        assert_eq!(map.get("@140"), Some(&10_140));
    }

    #[tokio::test]
    async fn hyperliquid_info_meta_matches_real_shape() {
        use hyperliquid_client::info::PerpMeta;

        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let raw: Value = reqwest::Client::new()
            .post(format!("{}/info", server.hyperliquid_url()))
            .json(&json!({ "type": "meta" }))
            .send()
            .await
            .expect("http")
            .json()
            .await
            .expect("json");
        assert!(raw["universe"].is_array());
        assert!(raw["universe"][0]["szDecimals"].is_number());
        assert!(raw["universe"][0]["maxLeverage"].is_number());

        let body: PerpMeta = serde_json::from_value(raw).expect("deserialize PerpMeta");
        assert!(body.universe.iter().any(|asset| asset.name == "BTC"));
        assert!(body.universe.iter().any(|asset| asset.name == "ETH"));
        assert!(body.universe.iter().any(|asset| asset.name == "HYPE"));
    }

    #[tokio::test]
    async fn hyperliquid_info_user_fills_by_time_matches_real_shape() {
        use hyperliquid_client::info::UserFill;

        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let user = hyperliquid_shape_test_user();
        server
            .record_hyperliquid_fill(user, sample_hyperliquid_user_fill(1_700_000_000_000, 1))
            .await;
        server
            .record_hyperliquid_fill(user, sample_hyperliquid_user_fill(1_700_000_010_000, 2))
            .await;
        server
            .record_hyperliquid_fill(user, sample_hyperliquid_user_fill(1_700_000_020_000, 3))
            .await;

        let raw: Value = reqwest::Client::new()
            .post(format!("{}/info", server.hyperliquid_url()))
            .json(&json!({
                "type": "userFillsByTime",
                "user": format!("{user:#x}"),
                "startTime": 1_700_000_005_000_u64,
                "endTime": 1_700_000_020_000_u64,
                "aggregateByTime": false
            }))
            .send()
            .await
            .expect("http")
            .json()
            .await
            .expect("json");
        assert_eq!(raw.as_array().expect("array").len(), 2);
        assert!(raw[0]["startPosition"].is_string());
        assert!(raw[0]["closedPnl"].is_string());
        assert!(raw[0]["feeToken"].is_string());
        assert!(raw[0]["time"].is_number());
        assert!(raw[0]["px"].is_string());

        let body: Vec<UserFill> = serde_json::from_value(raw).expect("deserialize UserFill");
        assert_eq!(
            body.iter().map(|fill| fill.tid).collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[tokio::test]
    async fn hyperliquid_info_user_fills_by_time_rejects_missing_start_time_and_bad_user() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        assert_hyperliquid_info_error(
            &server,
            json!({
                "type": "userFillsByTime",
                "user": "0x1111111111111111111111111111111111111111"
            }),
            "userFillsByTime requires a numeric `startTime` field",
        )
        .await;
        assert_hyperliquid_info_error(
            &server,
            json!({
                "type": "userFillsByTime",
                "user": "1111111111111111111111111111111111111111",
                "startTime": 1_u64
            }),
            "userFillsByTime requires a 0x-prefixed `user` field",
        )
        .await;
    }

    #[tokio::test]
    async fn hyperliquid_info_user_non_funding_ledger_updates_matches_real_shape() {
        use hyperliquid_client::info::{UserNonFundingLedgerDelta, UserNonFundingLedgerUpdate};

        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let user = hyperliquid_shape_test_user();
        let hash = format!("0x{}", "cd".repeat(32));
        for (time, delta) in [
            (
                1_700_000_000_000,
                UserNonFundingLedgerDelta::Deposit {
                    usdc: "10.5".to_string(),
                },
            ),
            (
                1_700_000_001_000,
                UserNonFundingLedgerDelta::Withdraw {
                    usdc: "1.5".to_string(),
                    nonce: 7,
                    fee: "1".to_string(),
                },
            ),
            (
                1_700_000_002_000,
                UserNonFundingLedgerDelta::InternalTransfer {
                    usdc: "2".to_string(),
                    user: format!("{user:#x}"),
                    destination: "0x2222222222222222222222222222222222222222".to_string(),
                    fee: "0".to_string(),
                },
            ),
            (
                1_700_000_003_000,
                UserNonFundingLedgerDelta::AccountClassTransfer {
                    usdc: "3".to_string(),
                    to_perp: false,
                },
            ),
            (
                1_700_000_004_000,
                UserNonFundingLedgerDelta::SpotTransfer {
                    token: "UBTC:0x11111111111111111111111111111111".to_string(),
                    amount: "0.001".to_string(),
                    usdc_value: "60".to_string(),
                    user: format!("{user:#x}"),
                    destination: "0x2222222222222222222222222222222222222222".to_string(),
                    fee: "0".to_string(),
                    native_token_fee: "0.00001".to_string(),
                    nonce: 8,
                },
            ),
        ] {
            server
                .record_hyperliquid_ledger_update(
                    user,
                    UserNonFundingLedgerUpdate {
                        time,
                        hash: hash.clone(),
                        delta,
                    },
                )
                .await;
        }

        let raw: Value = reqwest::Client::new()
            .post(format!("{}/info", server.hyperliquid_url()))
            .json(&json!({
                "type": "userNonFundingLedgerUpdates",
                "user": format!("{user:#x}"),
                "startTime": 1_700_000_001_000_u64,
                "endTime": 1_700_000_004_000_u64
            }))
            .send()
            .await
            .expect("http")
            .json()
            .await
            .expect("json");
        assert_eq!(raw.as_array().expect("array").len(), 4);
        assert_eq!(raw[0]["delta"]["type"], "withdraw");
        assert!(raw[2]["delta"]["toPerp"].is_boolean());
        assert!(raw[3]["delta"]["usdcValue"].is_string());
        assert!(raw[3]["delta"]["nativeTokenFee"].is_string());

        let body: Vec<UserNonFundingLedgerUpdate> =
            serde_json::from_value(raw).expect("deserialize ledger updates");
        assert_eq!(body.len(), 4);
    }

    #[tokio::test]
    async fn hyperliquid_info_user_non_funding_ledger_updates_rejects_missing_start_time_and_bad_user(
    ) {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        assert_hyperliquid_info_error(
            &server,
            json!({
                "type": "userNonFundingLedgerUpdates",
                "user": "0x1111111111111111111111111111111111111111"
            }),
            "userNonFundingLedgerUpdates requires a numeric `startTime` field",
        )
        .await;
        assert_hyperliquid_info_error(
            &server,
            json!({
                "type": "userNonFundingLedgerUpdates",
                "user": "1111111111111111111111111111111111111111",
                "startTime": 1_u64
            }),
            "userNonFundingLedgerUpdates requires a 0x-prefixed `user` field",
        )
        .await;
    }

    #[tokio::test]
    async fn hyperliquid_info_user_funding_matches_real_shape() {
        use hyperliquid_client::info::UserFunding;

        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let user = hyperliquid_shape_test_user();
        server
            .record_hyperliquid_funding(
                user,
                UserFunding {
                    time: 1_700_000_000_000,
                    coin: "ETH".to_string(),
                    usdc: "-0.0123".to_string(),
                    szi: "1.25".to_string(),
                    funding_rate: "0.0000125".to_string(),
                },
            )
            .await;

        let raw: Value = reqwest::Client::new()
            .post(format!("{}/info", server.hyperliquid_url()))
            .json(&json!({
                "type": "userFunding",
                "user": format!("{user:#x}"),
                "startTime": 1_699_999_999_999_u64
            }))
            .send()
            .await
            .expect("http")
            .json()
            .await
            .expect("json");
        assert!(raw[0]["fundingRate"].is_string());
        assert!(raw[0]["usdc"].is_string());
        assert!(raw[0]["szi"].is_string());
        assert!(raw[0]["time"].is_number());

        let body: Vec<UserFunding> = serde_json::from_value(raw).expect("deserialize funding");
        assert_eq!(body[0].usdc, "-0.0123");
    }

    #[tokio::test]
    async fn hyperliquid_info_user_funding_rejects_missing_start_time_and_bad_user() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        assert_hyperliquid_info_error(
            &server,
            json!({
                "type": "userFunding",
                "user": "0x1111111111111111111111111111111111111111"
            }),
            "userFunding requires a numeric `startTime` field",
        )
        .await;
        assert_hyperliquid_info_error(
            &server,
            json!({
                "type": "userFunding",
                "user": "1111111111111111111111111111111111111111",
                "startTime": 1_u64
            }),
            "userFunding requires a 0x-prefixed `user` field",
        )
        .await;
    }

    #[tokio::test]
    async fn hyperliquid_info_user_rate_limit_matches_real_shape() {
        use hyperliquid_client::info::UserRateLimit;

        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let user = hyperliquid_shape_test_user();
        let raw: Value = reqwest::Client::new()
            .post(format!("{}/info", server.hyperliquid_url()))
            .json(&json!({
                "type": "userRateLimit",
                "user": format!("{user:#x}")
            }))
            .send()
            .await
            .expect("http")
            .json()
            .await
            .expect("json");
        assert!(raw["cumVlm"].is_string());
        assert!(raw["nRequestsUsed"].is_number());
        assert!(raw["nRequestsCap"].is_number());
        assert!(raw["nRequestsSurplus"].is_number());

        let body: UserRateLimit = serde_json::from_value(raw).expect("deserialize rate limit");
        assert_eq!(body.n_requests_cap, 1200);
    }

    #[tokio::test]
    async fn hyperliquid_info_user_rate_limit_rejects_bad_user() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        assert_hyperliquid_info_error(
            &server,
            json!({ "type": "userRateLimit" }),
            "userRateLimit requires a 0x-prefixed `user` field",
        )
        .await;
        assert_hyperliquid_info_error(
            &server,
            json!({
                "type": "userRateLimit",
                "user": "1111111111111111111111111111111111111111"
            }),
            "userRateLimit requires a 0x-prefixed `user` field",
        )
        .await;
    }

    #[tokio::test]
    async fn hyperliquid_info_l2_book_matches_real_shape() {
        use hyperliquid_client::info::L2BookSnapshot;

        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let body: L2BookSnapshot = reqwest::Client::new()
            .post(format!("{}/info", server.hyperliquid_url()))
            .json(&json!({ "type": "l2Book", "coin": "UBTC/USDC" }))
            .send()
            .await
            .expect("http")
            .json()
            .await
            .expect("deserialize L2BookSnapshot");

        assert_eq!(body.coin, "UBTC/USDC");
        assert!(body.best_bid().is_some());
        assert!(body.best_ask().is_some());
    }

    #[tokio::test]
    async fn hyperliquid_info_l2_book_rejects_missing_coin() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let response = reqwest::Client::new()
            .post(format!("{}/info", server.hyperliquid_url()))
            .json(&json!({ "type": "l2Book" }))
            .send()
            .await
            .expect("http");

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body: MockIntegratorErrorResponse = response.json().await.expect("error body");
        assert_eq!(body.error, "l2Book requires a `coin` field");
    }

    #[tokio::test]
    async fn hyperliquid_info_order_status_matches_real_shape() {
        use hyperliquid_client::info::OrderStatusResponse;

        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let body: OrderStatusResponse = reqwest::Client::new()
            .post(format!("{}/info", server.hyperliquid_url()))
            .json(&json!({
                "type": "orderStatus",
                "user": "0x0000000000000000000000000000000000000000",
                "oid": 42
            }))
            .send()
            .await
            .expect("http")
            .json()
            .await
            .expect("deserialize OrderStatusResponse");

        assert_eq!(body.status, "unknownOid");
        assert!(body.order.is_none());
    }

    #[tokio::test]
    async fn hyperliquid_info_order_status_rejects_missing_oid() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let response = reqwest::Client::new()
            .post(format!("{}/info", server.hyperliquid_url()))
            .json(&json!({
                "type": "orderStatus",
                "user": "0x0000000000000000000000000000000000000000"
            }))
            .send()
            .await
            .expect("http");

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body: MockIntegratorErrorResponse = response.json().await.expect("error body");
        assert_eq!(body.error, "orderStatus requires a numeric `oid` field");
    }

    #[tokio::test]
    async fn hyperliquid_info_order_status_rejects_missing_user() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let response = reqwest::Client::new()
            .post(format!("{}/info", server.hyperliquid_url()))
            .json(&json!({ "type": "orderStatus", "oid": 42 }))
            .send()
            .await
            .expect("http");

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body: MockIntegratorErrorResponse = response.json().await.expect("error body");
        assert_eq!(
            body.error,
            "orderStatus requires a 0x-prefixed `user` field"
        );
    }

    #[tokio::test]
    async fn hyperliquid_exchange_order_persists_submission() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        // Raw JSON submission — signature is invalid but the exchange handler
        // still persists the raw envelope before attempting recovery, which is
        // what this test actually asserts.
        let submission = json!({
            "action": {
                "type": "order",
                "orders": [{
                    "a": 10_140,
                    "b": true,
                    "p": "68000",
                    "s": "0.001",
                    "r": false,
                    "t": { "limit": { "tif": "Ioc" } }
                }],
                "grouping": "na"
            },
            "nonce": 1_700_000_000_000_u64,
            "signature": { "r": "0x0", "s": "0x0", "v": 27 }
        });
        let _ = reqwest::Client::new()
            .post(format!("{}/exchange", server.hyperliquid_url()))
            .json(&submission)
            .send()
            .await
            .expect("http");

        let submissions = server.hyperliquid_exchange_submissions().await;
        assert_eq!(submissions.len(), 1);
        assert_eq!(submissions[0]["action"]["type"], "order");
    }

    // Driving a real `HyperliquidClient` against the mock server exercises
    // signing, msgpack hashing, serialization, and mock dispatch end-to-end.
    // If either side's wire format drifts, these round-trip tests break before
    // production does.
    mod hyperliquid_client_round_trip {
        use super::*;
        use alloy::signers::local::PrivateKeySigner;
        use hyperliquid_client::{
            client::Network, CancelRequest, HyperliquidClient, Limit, Order, OrderRequest,
        };

        const TEST_WALLET: &str =
            "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e";

        fn new_client(base_url: &str) -> HyperliquidClient {
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            HyperliquidClient::new(base_url, wallet, None, Network::Testnet).expect("client")
        }

        #[tokio::test]
        async fn client_refresh_spot_meta_populates_asset_index() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let mut client = new_client(&server.hyperliquid_url());

            let meta = client.refresh_spot_meta().await.expect("spot meta");

            assert!(meta.tokens.iter().any(|t| t.name == "UBTC"));
            assert_eq!(client.asset_index("UBTC/USDC").expect("asset"), 10_140);
            assert_eq!(client.asset_index("@140").expect("asset alias"), 10_140);
        }

        #[tokio::test]
        async fn client_l2_book_returns_bid_and_ask_levels() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            server.set_hyperliquid_rate("UBTC", "USDC", 68_000.0).await;
            let client = new_client(&server.hyperliquid_url());

            let book = client.l2_book("UBTC/USDC").await.expect("l2 book");

            assert_eq!(book.coin, "UBTC/USDC");
            assert_eq!(book.best_bid().expect("bid").px, "68000");
            assert_eq!(book.best_ask().expect("ask").px, "68000");
            assert_eq!(book.bids().len(), 1);
            assert_eq!(book.asks().len(), 1);
        }

        #[tokio::test]
        async fn client_order_status_returns_filled_envelope_for_oid() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let mut client = new_client(&server.hyperliquid_url());
            client.refresh_spot_meta().await.expect("spot meta");

            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();
            server
                .credit_hyperliquid_balance(user, "USDC", 10_000.0)
                .await;
            server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;

            let place = client
                .place_orders(
                    vec![OrderRequest {
                        asset: client.asset_index("UBTC/USDC").expect("asset"),
                        is_buy: true,
                        limit_px: "60000".to_string(),
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
                .expect("place orders");
            let oid = place["response"]["data"]["statuses"][0]["filled"]["oid"]
                .as_u64()
                .expect("oid");

            let status = client.order_status(user, oid).await.expect("order status");

            assert_eq!(status.status, "order");
            let envelope = status.order.expect("order envelope");
            assert_eq!(envelope.order.oid, oid);
            assert_eq!(envelope.status, "filled");
        }

        #[tokio::test]
        async fn client_order_status_is_scoped_to_order_user() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let mut client = new_client(&server.hyperliquid_url());
            client.refresh_spot_meta().await.expect("spot meta");

            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();
            server
                .credit_hyperliquid_balance(user, "USDC", 10_000.0)
                .await;
            server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;

            let place = client
                .place_orders(
                    vec![OrderRequest {
                        asset: client.asset_index("UBTC/USDC").expect("asset"),
                        is_buy: true,
                        limit_px: "60000".to_string(),
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
                .expect("place orders");
            let oid = place["response"]["data"]["statuses"][0]["filled"]["oid"]
                .as_u64()
                .expect("oid");
            let other_user = Address::repeat_byte(0x77);

            let response: hyperliquid_client::info::OrderStatusResponse = reqwest::Client::new()
                .post(format!("{}/info", server.hyperliquid_url()))
                .json(&json!({
                    "type": "orderStatus",
                    "user": format!("{other_user:#x}"),
                    "oid": oid
                }))
                .send()
                .await
                .expect("http")
                .json()
                .await
                .expect("order status");

            assert_eq!(response.status, "unknownOid");
            assert!(response.order.is_none());
        }

        #[tokio::test]
        async fn client_place_orders_signs_and_mock_persists_submission() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let mut client = new_client(&server.hyperliquid_url());
            client.refresh_spot_meta().await.expect("spot meta");

            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();
            server.credit_hyperliquid_balance(user, "USDC", 100.0).await;
            server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;

            let order = OrderRequest {
                asset: client.asset_index("UBTC/USDC").expect("asset"),
                is_buy: true,
                limit_px: "60000".to_string(),
                sz: "0.001".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Ioc".to_string(),
                }),
                cloid: None,
            };
            let resp = client
                .place_orders(vec![order], "na")
                .await
                .expect("place orders");

            assert_eq!(resp["status"], "ok");
            assert_eq!(resp["response"]["type"], "order");
            let oid = resp["response"]["data"]["statuses"][0]["filled"]["oid"]
                .as_u64()
                .expect("oid");
            assert_eq!(oid, 1000);
            assert_eq!(
                resp["response"]["data"]["statuses"][0]["filled"]["avgPx"],
                "60000"
            );

            // Balances moved: 60 USDC debited, 0.001 UBTC credited.
            let ubtc = server.hyperliquid_balance_of(user, "UBTC").await;
            let usdc = server.hyperliquid_balance_of(user, "USDC").await;
            assert!((ubtc - 0.001).abs() < 1e-9, "UBTC balance {ubtc}");
            assert!((usdc - 40.0).abs() < 1e-9, "USDC balance {usdc}");

            let submissions = server.hyperliquid_exchange_submissions().await;
            assert_eq!(submissions.len(), 1);
            let submitted = &submissions[0];
            assert_eq!(submitted["action"]["type"], "order");
            assert_eq!(submitted["action"]["grouping"], "na");
            assert_eq!(submitted["action"]["orders"][0]["a"], 10_140);
            assert_eq!(submitted["action"]["orders"][0]["b"], true);
            assert!(submitted["signature"]["r"].is_string());
            assert!(submitted["signature"]["s"].is_string());
            assert!(submitted["nonce"].is_u64());
        }

        #[tokio::test]
        async fn client_place_orders_fills_near_market_gtc_buy() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let mut client = new_client(&server.hyperliquid_url());
            client.refresh_spot_meta().await.expect("spot meta");

            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();
            server
                .credit_hyperliquid_balance(user, "USDC", 100_000.0)
                .await;
            server.set_hyperliquid_rate("UETH", "USDC", 3_000.0).await;

            let resp = client
                .place_orders(
                    vec![OrderRequest {
                        asset: client.asset_index("UETH/USDC").expect("asset"),
                        is_buy: true,
                        limit_px: "2999.9".to_string(),
                        sz: "30.3148".to_string(),
                        reduce_only: false,
                        order_type: Order::Limit(Limit {
                            tif: "Gtc".to_string(),
                        }),
                        cloid: None,
                    }],
                    "na",
                )
                .await
                .expect("place orders");

            assert_eq!(resp["status"], "ok");
            let filled = &resp["response"]["data"]["statuses"][0]["filled"];
            assert_eq!(filled["totalSz"], "30.3148");
            assert_eq!(filled["avgPx"], "2999.9");
            assert!((server.hyperliquid_balance_of(user, "UETH").await - 30.3148).abs() < 1e-9);
        }

        #[tokio::test]
        async fn chained_sell_then_buy_credits_both_legs() {
            let server = MockIntegratorServer::spawn_with_config(MockIntegratorConfig::default())
                .await
                .expect("spawn");
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let vault = Address::repeat_byte(0xc2);
            let mut client =
                HyperliquidClient::new(&server.hyperliquid_url(), wallet, Some(vault), Network::Testnet)
                    .expect("client");
            client.refresh_spot_meta().await.expect("spot meta");

            server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;
            server.set_hyperliquid_rate("UETH", "USDC", 3_000.0).await;

            let rejected_buy = client
                .place_orders(
                    vec![OrderRequest {
                        asset: client.asset_index("UETH/USDC").expect("ueth asset"),
                        is_buy: true,
                        limit_px: "3000".to_string(),
                        sz: "1".to_string(),
                        reduce_only: false,
                        order_type: Order::Limit(Limit {
                            tif: "Ioc".to_string(),
                        }),
                        cloid: None,
                    }],
                    "na",
                )
                .await
                .expect("prefundless buy");
            let rejected_error = rejected_buy["response"]["data"]["statuses"][0]["error"]
                .as_str()
                .expect("insufficient balance error");
            assert!(
                rejected_error.contains("insufficient USDC balance"),
                "unexpected rejection: {rejected_error}"
            );
            let rejected_status = client
                .order_status(vault, 1000)
                .await
                .expect("rejected order status");
            assert_eq!(
                rejected_status.order.expect("rejected order").status,
                "rejected"
            );
            assert!(client
                .user_fills(vault)
                .await
                .expect("fills after rejected buy")
                .is_empty());

            server.credit_hyperliquid_balance(vault, "UBTC", 1.0).await;

            let sell = client
                .place_orders(
                    vec![OrderRequest {
                        asset: client.asset_index("UBTC/USDC").expect("ubtc asset"),
                        is_buy: false,
                        limit_px: "60000".to_string(),
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
                .expect("sell UBTC");
            let sell_oid = sell["response"]["data"]["statuses"][0]["filled"]["oid"]
                .as_u64()
                .expect("sell oid");
            let sell_status = client
                .order_status(vault, sell_oid)
                .await
                .expect("sell order status");
            assert_eq!(sell_status.order.expect("sell order").status, "filled");

            let state = client
                .spot_clearinghouse_state(vault)
                .await
                .expect("post-sell spot state");
            assert_eq!(state.balance_of("USDC"), "6000");
            assert_eq!(state.balance_of("UBTC"), "0.9");

            let buy = client
                .place_orders(
                    vec![OrderRequest {
                        asset: client.asset_index("UETH/USDC").expect("ueth asset"),
                        is_buy: true,
                        limit_px: "3000".to_string(),
                        sz: "1".to_string(),
                        reduce_only: false,
                        order_type: Order::Limit(Limit {
                            tif: "Ioc".to_string(),
                        }),
                        cloid: None,
                    }],
                    "na",
                )
                .await
                .expect("buy UETH");
            let buy_oid = buy["response"]["data"]["statuses"][0]["filled"]["oid"]
                .as_u64()
                .expect("buy oid");
            let buy_status = client
                .order_status(vault, buy_oid)
                .await
                .expect("buy order status");
            assert_eq!(buy_status.order.expect("buy order").status, "filled");

            let state = client
                .spot_clearinghouse_state(vault)
                .await
                .expect("post-buy spot state");
            assert_eq!(state.balance_of("USDC"), "3000");
            assert_eq!(state.balance_of("UETH"), "1");
            assert_eq!(state.balance_of("UBTC"), "0.9");
        }

        #[tokio::test]
        async fn client_cancel_orders_succeeds_for_resting_order() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let mut client = new_client(&server.hyperliquid_url());
            client.refresh_spot_meta().await.expect("spot meta");
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();
            server.credit_hyperliquid_balance(user, "USDC", 100.0).await;
            server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;

            let place = client
                .place_orders(
                    vec![OrderRequest {
                        asset: client.asset_index("UBTC/USDC").expect("asset"),
                        is_buy: true,
                        limit_px: "30000".to_string(),
                        sz: "0.001".to_string(),
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
                .cancel_orders(vec![CancelRequest { asset: 10_140, oid }])
                .await
                .expect("cancel orders");

            assert_eq!(resp["status"], "ok");
            assert_eq!(resp["response"]["type"], "cancel");
            assert_eq!(resp["response"]["data"]["statuses"][0], json!("success"));

            let open_orders = client.open_orders(user).await.expect("open orders");
            assert!(open_orders.iter().all(|order| order.oid != oid));

            let status = client.order_status(user, oid).await.expect("order status");
            assert_eq!(status.status, "order");
            assert_eq!(status.order.expect("order envelope").status, "canceled");

            let submissions = server.hyperliquid_exchange_submissions().await;
            assert_eq!(submissions.len(), 2);
            assert_eq!(submissions[1]["action"]["type"], "cancel");
            assert_eq!(submissions[1]["action"]["cancels"][0]["o"], oid);
        }

        #[tokio::test]
        async fn client_withdraw_to_bridge_signs_and_posts_withdraw3() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let client = new_client(&server.hyperliquid_url());
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            server
                .credit_hyperliquid_clearinghouse_balance(wallet.address(), "USDC", 20.0)
                .await;
            let destination = "0x000000000000000000000000000000000000dead".to_string();

            let resp = client
                .withdraw_to_bridge(destination.clone(), "12.34".to_string(), 1_700_000_000_000)
                .await
                .expect("withdraw");

            assert_eq!(resp["status"], "ok");
            assert_eq!(resp["response"]["type"], "default");

            let submissions = server.hyperliquid_exchange_submissions().await;
            assert_eq!(submissions.len(), 1);
            let submitted = &submissions[0];
            assert_eq!(submitted["action"]["type"], "withdraw3");
            assert_eq!(submitted["action"]["destination"], destination);
            assert_eq!(submitted["action"]["amount"], "12.34");
            assert_eq!(submitted["action"]["hyperliquidChain"], "Testnet");
            assert_eq!(submitted["action"]["signatureChainId"], "0x66eee");
            assert_eq!(submitted["nonce"], 1_700_000_000_000_u64);
        }

        #[tokio::test]
        async fn client_send_asset_signs_and_moves_spot_balance() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let client = new_client(&server.hyperliquid_url());
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();
            let destination = Address::repeat_byte(0x55);
            server
                .credit_hyperliquid_balance(user, "UBTC", 0.0004)
                .await;

            let resp = client
                .send_asset(
                    format!("{destination:#x}"),
                    "spot".to_string(),
                    "spot".to_string(),
                    "UBTC:0x8f49bc64b02C5B7793D4fD4b74b9D643cF5e9059".to_string(),
                    "0.0003".to_string(),
                    1_700_000_000_000,
                )
                .await
                .expect("send asset");

            assert_eq!(resp["status"], "ok");
            assert_eq!(resp["response"]["type"], "default");
            assert!((server.hyperliquid_balance_of(user, "UBTC").await - 0.0001).abs() < 1e-12);
            assert!(
                (server.hyperliquid_balance_of(destination, "UBTC").await - 0.0003).abs() < 1e-12
            );
            let submissions = server.hyperliquid_exchange_submissions().await;
            assert_eq!(submissions.len(), 1);
            let submitted = &submissions[0];
            assert_eq!(submitted["action"]["type"], "sendAsset");
            assert_eq!(
                submitted["action"]["destination"],
                format!("{destination:#x}")
            );
            assert_eq!(submitted["action"]["sourceDex"], "spot");
            assert_eq!(submitted["action"]["destinationDex"], "spot");
            assert_eq!(submitted["action"]["amount"], "0.0003");
            assert_eq!(submitted["action"]["nonce"], 1_700_000_000_000_u64);
            assert_eq!(submitted["action"]["hyperliquidChain"], "Testnet");
            assert_eq!(submitted["action"]["signatureChainId"], "0x66eee");
        }

        #[tokio::test]
        async fn concurrent_send_asset_handlers_return_while_unit_withdrawals_complete() {
            use alloy::node_bindings::Anvil;

            let anvil = Anvil::new().prague().try_spawn().expect("anvil spawn");
            let chain_id = anvil.chain_id();
            let mut paymasters = BTreeMap::new();
            paymasters.insert(
                (MockService::Hyperunit, chain_id),
                MockServicePaymaster::successful(
                    MockService::Hyperunit,
                    chain_id,
                    B256::repeat_byte(0x42),
                ),
            );
            let server = MockIntegratorServer::spawn_with_config(
                MockIntegratorConfig::default()
                    .with_unit_evm_rpc_url(UnitChain::Base, anvil.endpoint())
                    .with_unit_withdrawal_release_timeout(Duration::from_secs(30))
                    .with_mock_service_evm_chain(chain_id, anvil.endpoint())
                    .with_mock_service_paymasters(paymasters),
            )
            .await
            .expect("spawn mock integrator");
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            server
                .credit_hyperliquid_balance(wallet.address(), "UETH", 1.0)
                .await;

            let request_count = 200usize;
            let mut protocol_addresses = Vec::with_capacity(request_count);
            for index in 0..request_count {
                let recipient = Address::repeat_byte(index as u8 + 1);
                let generated: UnitGenerateAddressResponse = reqwest::get(format!(
                    "{}/gen/hyperliquid/base/eth/{recipient:#x}",
                    server.hyperunit_url()
                ))
                .await
                .expect("gen request")
                .error_for_status()
                .expect("gen 200")
                .json()
                .await
                .expect("gen json");
                protocol_addresses.push(generated.address);
            }

            let base_url = server.hyperliquid_url();
            let mut send_tasks = tokio::task::JoinSet::new();
            for (index, protocol_address) in protocol_addresses.iter().cloned().enumerate() {
                let base_url = base_url.clone();
                send_tasks.spawn(async move {
                    let client = new_client(&base_url);
                    client
                        .send_asset(
                            protocol_address,
                            "spot".to_string(),
                            "spot".to_string(),
                            "UETH:0x0000000000000000000000000000000000000000".to_string(),
                            "0.000000000000000001".to_string(),
                            1_700_000_000_000_u64 + index as u64,
                        )
                        .await
                });
            }

            tokio::time::timeout(Duration::from_secs(30), async {
                while let Some(result) = send_tasks.join_next().await {
                    let response = result
                        .expect("sendAsset task join")
                        .expect("sendAsset response");
                    assert_eq!(response["status"], "ok");
                    assert_eq!(response["response"]["type"], "default");
                }
            })
            .await
            .expect("sendAsset handlers timed out");

            let expected_protocol_addresses =
                protocol_addresses.iter().cloned().collect::<BTreeSet<_>>();
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let operations = server.unit_operations().await;
                    let matching = operations
                        .iter()
                        .filter(|operation| {
                            expected_protocol_addresses.contains(&operation.protocol_address)
                        })
                        .collect::<Vec<_>>();
                    assert!(
                        matching
                            .iter()
                            .all(|operation| operation.state != "failure"),
                        "a concurrent Unit withdrawal failed"
                    );
                    if matching.len() == request_count
                        && matching.iter().all(|operation| {
                            operation.state == "done" && operation.destination_tx_hash.is_some()
                        })
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("Unit withdrawal completions timed out");
        }

        #[tokio::test]
        async fn client_clearinghouse_state_round_trips_withdrawable_balance() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let client = new_client(&server.hyperliquid_url());
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();

            server
                .credit_hyperliquid_clearinghouse_balance(user, "USDC", 7.25)
                .await;

            let state = client
                .clearinghouse_state(user)
                .await
                .expect("clearinghouse state");

            assert_eq!(state.withdrawable, "7.25");
            assert_eq!(state.margin_summary.account_value, "7.25");
            assert_eq!(state.cross_margin_summary.account_value, "7.25");
        }

        #[tokio::test]
        async fn client_usd_class_transfer_signs_and_posts_action() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let client = new_client(&server.hyperliquid_url());
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();

            server
                .credit_hyperliquid_clearinghouse_balance(user, "USDC", 12.34)
                .await;

            let resp = client
                .usd_class_transfer("12.34".to_string(), false, 1_700_000_000_000)
                .await
                .expect("usd class transfer");

            assert_eq!(resp["status"], "ok");
            assert_eq!(resp["response"]["type"], "default");
            assert!((server.hyperliquid_balance_of(user, "USDC").await - 12.34).abs() < 1e-9);
            assert!(
                server
                    .hyperliquid_clearinghouse_balance_of(user, "USDC")
                    .await
                    .abs()
                    < 1e-9
            );

            let submissions = server.hyperliquid_exchange_submissions().await;
            assert_eq!(submissions.len(), 1);
            let submitted = &submissions[0];
            assert_eq!(submitted["action"]["type"], "usdClassTransfer");
            assert_eq!(submitted["action"]["amount"], "12.34");
            assert_eq!(submitted["action"]["toPerp"], false);
            assert_eq!(submitted["action"]["nonce"], 1_700_000_000_000_u64);
            assert_eq!(submitted["action"]["hyperliquidChain"], "Testnet");
            assert_eq!(submitted["action"]["signatureChainId"], "0x66eee");
            assert!(submitted.get("vaultAddress").is_none());
        }

        #[tokio::test]
        async fn client_place_orders_surfaces_mock_exchange_error() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            server
                .fail_next_hyperliquid_exchange("simulated HL outage")
                .await;
            let mut client = new_client(&server.hyperliquid_url());
            client.refresh_spot_meta().await.expect("spot meta");

            let err = client
                .place_orders(
                    vec![OrderRequest {
                        asset: client.asset_index("UBTC/USDC").expect("asset"),
                        is_buy: true,
                        limit_px: "68000".to_string(),
                        sz: "0.001".to_string(),
                        reduce_only: false,
                        order_type: Order::Limit(Limit {
                            tif: "Ioc".to_string(),
                        }),
                        cloid: None,
                    }],
                    "na",
                )
                .await
                .expect_err("enqueued error should surface");
            let rendered = format!("{err:?}");
            assert!(
                rendered.contains("simulated HL outage") || rendered.contains("502"),
                "expected mock-injected error to propagate, got {rendered}"
            );
        }
    }
}
