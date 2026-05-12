use super::*;

#[derive(Clone, Default)]
pub struct ProviderObservationActivities {
    deps: Option<Arc<OrderActivityDeps>>,
}

impl ProviderObservationActivities {
    #[must_use]
    pub(crate) fn from_order_activities(order_activities: &OrderActivities) -> Self {
        Self {
            deps: order_activities.shared_deps(),
        }
    }

    fn deps(&self) -> Result<Arc<OrderActivityDeps>, OrderActivityError> {
        self.deps.clone().ok_or_else(|| {
            OrderActivityError::missing_configuration("provider observation activities")
        })
    }
}

#[activities]
impl ProviderObservationActivities {
    /// Scar tissue: §10 provider operation hint flow and verifier dispatch.
    #[activity]
    pub async fn verify_provider_operation_hint(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: VerifyProviderOperationHintInput,
    ) -> Result<ProviderOperationHintVerified, ActivityError> {
        record_activity("verify_provider_operation_hint", async move {
            let deps = self.deps()?;
            if input.signal.execution_step_id != input.step_id {
                return Ok(ProviderOperationHintVerified {
                    provider_operation_id: input.signal.provider_operation_id,
                    decision: ProviderOperationHintDecision::Reject,
                    reason: Some(format!(
                        "hint targets step {}, not {}",
                        input.signal.execution_step_id, input.step_id
                    )),
                });
            }
            deps.db
                .orders()
                .record_execution_step_hint_arrival(input.step_id.inner(), Utc::now())
                .await
                .map_err(OrderActivityError::db_query)?;
            match input.signal.hint_kind {
                ProviderHintKind::AcrossFill | ProviderHintKind::AcrossDestinationFilled => {
                    verify_across_fill_hint(&deps, input).await
                }
                ProviderHintKind::CctpAttestation => {
                    verify_cctp_attestation_hint(&deps, input).await
                }
                ProviderHintKind::CctpReceiveObserved => {
                    verify_cctp_receive_observed_hint(&deps, input).await
                }
                ProviderHintKind::UnitDeposit => verify_unit_deposit_hint(&deps, input).await,
                ProviderHintKind::ProviderObservation => {
                    verify_provider_observation_hint(&deps, input).await
                }
                ProviderHintKind::VeloraSwapSettled => {
                    verify_velora_swap_settled_hint(&deps, input).await
                }
                ProviderHintKind::HyperliquidTrade => {
                    verify_hyperliquid_trade_hint(&deps, input).await
                }
                ProviderHintKind::HlTradeFilled => verify_hl_trade_filled_hint(&deps, input).await,
                ProviderHintKind::HlTradeCanceled => {
                    verify_hl_trade_canceled_hint(&deps, input).await
                }
                ProviderHintKind::HlBridgeDepositObserved => {
                    verify_hl_bridge_deposit_observed_hint(&deps, input).await
                }
                ProviderHintKind::HlBridgeDepositCredited => {
                    verify_hl_bridge_deposit_credited_hint(&deps, input).await
                }
                ProviderHintKind::HlWithdrawalAcknowledged => {
                    verify_hl_withdrawal_acknowledged_hint(&deps, input).await
                }
                ProviderHintKind::HlWithdrawalSettled => {
                    verify_hl_withdrawal_settled_hint(&deps, input).await
                }
            }
        })
        .await
    }

    /// Scar tissue: §11 Across on-chain log recovery.
    #[activity]
    pub async fn recover_across_onchain_log(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: RecoverAcrossOnchainLogInput,
    ) -> Result<AcrossOnchainLogRecovered, ActivityError> {
        record_activity("recover_across_onchain_log", async move {
            let deps = self.deps()?;
            let operation = deps
                .db
                .orders()
                .get_provider_operation(input.provider_operation_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            if operation.order_id != input.order_id.inner() {
                return Err(lost_intent_recovery_error(
                    "validating provider operation order",
                    format!(
                        "provider operation {} belongs to order {}, not {}",
                        operation.id,
                        operation.order_id,
                        input.order_id.inner()
                    ),
                ));
            }

            let recovered = recover_across_operation_from_checkpoint(&deps, operation).await?;
            if recovered
                .as_ref()
                .and_then(|operation| operation.provider_ref.as_deref())
                .is_some_and(is_decimal_u256)
            {
                Ok(AcrossOnchainLogRecovered {
                    provider_operation_id: input.provider_operation_id,
                })
            } else {
                Err(lost_intent_recovery_error(
                    "recovering Across deposit log",
                    format!(
                        "Across provider operation {} has no recoverable deposit log yet",
                        input.provider_operation_id.inner()
                    ),
                ))
            }
        })
        .await
    }
}

#[derive(Debug, Clone)]
pub(super) struct RecoveredAcrossDeposit {
    deposit_id: String,
    deposit_tx_hash: String,
}

pub(super) async fn recover_across_operation_from_checkpoint(
    deps: &OrderActivityDeps,
    operation: OrderProviderOperation,
) -> Result<Option<OrderProviderOperation>, OrderActivityError> {
    if operation.operation_type != ProviderOperationType::AcrossBridge {
        return Ok(Some(operation));
    }
    if operation
        .provider_ref
        .as_deref()
        .is_some_and(is_decimal_u256)
    {
        return Ok(Some(operation));
    }
    if operation.response.get("kind").and_then(Value::as_str) != Some("provider_receipt_checkpoint")
    {
        return Ok(None);
    }

    let Some(step_id) = operation.execution_step_id else {
        return Ok(None);
    };
    let step = deps
        .db
        .orders()
        .get_execution_step(step_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    let Some(recovered) = recover_across_deposit_from_origin_logs(deps, &operation, &step).await?
    else {
        return Ok(None);
    };

    let mut observed_state = json!({
        "source": "temporal_across_deposit_log_recovery",
        "deposit_id": &recovered.deposit_id,
        "deposit_tx_hash": &recovered.deposit_tx_hash,
        "router_recovery": {
            "kind": "across_deposit_log_recovery",
            "deposit_tx_hash": &recovered.deposit_tx_hash,
            "source_provider_ref": &operation.provider_ref,
        },
    });
    if !is_empty_json_object(&operation.observed_state) {
        observed_state["previous_observed_state"] = operation.observed_state.clone();
    }

    let (updated, _) = deps
        .db
        .orders()
        .update_provider_operation_status(
            operation.id,
            operation.status,
            Some(recovered.deposit_id),
            observed_state,
            None,
            Utc::now(),
        )
        .await
        .map_err(OrderActivityError::db_query)?;
    tracing::info!(
        order_id = %updated.order_id,
        provider_operation_id = %updated.id,
        step_id = ?updated.execution_step_id,
        event_name = "provider_operation.across_onchain_log_recovered",
        "provider_operation.across_onchain_log_recovered"
    );
    Ok(Some(updated))
}

pub(super) async fn recover_across_deposit_from_origin_logs(
    deps: &OrderActivityDeps,
    operation: &OrderProviderOperation,
    step: &OrderExecutionStep,
) -> Result<Option<RecoveredAcrossDeposit>, OrderActivityError> {
    let provider_response = operation
        .response
        .get("provider_response")
        .unwrap_or(&operation.response);
    let swap_tx = provider_response.get("swapTx").ok_or_else(|| {
        lost_intent_recovery_error(
            "reading Across provider checkpoint",
            format!(
                "Across checkpoint recovery missing provider_response.swapTx for operation {}",
                operation.id
            ),
        )
    })?;
    let spoke_pool_address = json_str_field(swap_tx, "to").ok_or_else(|| {
        lost_intent_recovery_error(
            "reading Across provider checkpoint",
            format!(
                "Across checkpoint recovery missing swapTx.to for operation {}",
                operation.id
            ),
        )
    })?;
    let spoke_pool_address = Address::from_str(spoke_pool_address).map_err(|err| {
        lost_intent_recovery_error(
            "parsing Across spoke pool address",
            format!(
                "Across checkpoint recovery invalid swapTx.to for operation {}: {err}",
                operation.id
            ),
        )
    })?;

    let origin_chain_id =
        json_u64_field(&operation.request, "origin_chain_id").ok_or_else(|| {
            lost_intent_recovery_error(
                "reading Across operation request",
                format!(
                    "Across checkpoint recovery missing origin_chain_id for operation {}",
                    operation.id
                ),
            )
        })?;
    let destination_chain_id = json_u64_field(&operation.request, "destination_chain_id")
        .ok_or_else(|| {
            lost_intent_recovery_error(
                "reading Across operation request",
                format!(
                    "Across checkpoint recovery missing destination_chain_id for operation {}",
                    operation.id
                ),
            )
        })?;
    let origin_chain = ChainId::parse(format!("evm:{origin_chain_id}")).map_err(|err| {
        lost_intent_recovery_error(
            "parsing Across origin chain",
            format!(
                "Across checkpoint recovery invalid origin chain for operation {}: {err}",
                operation.id
            ),
        )
    })?;
    let backend_chain = backend_chain_for_id(&origin_chain).ok_or_else(|| {
        lost_intent_recovery_error(
            "resolving Across origin backend chain",
            format!("Across checkpoint recovery unsupported origin chain {origin_chain}"),
        )
    })?;
    let evm_chain = deps.chain_registry.get_evm(&backend_chain).ok_or_else(|| {
        lost_intent_recovery_error(
            "loading Across origin EVM chain",
            format!("Across checkpoint recovery origin chain {origin_chain} is not configured"),
        )
    })?;

    let BridgeExecutionRequest::Across(step_request) =
        BridgeExecutionRequest::across_from_value(&step.request).map_err(|err| {
            lost_intent_recovery_error(
                "decoding Across step request",
                format!(
                "Across checkpoint recovery could not decode step request for operation {}: {err}",
                operation.id
                ),
            )
        })?
    else {
        return Err(lost_intent_recovery_error(
            "validating Across step request",
            format!(
                "Across checkpoint recovery step {} is not an Across request",
                step.id
            ),
        ));
    };
    let input_token =
        recovery_step_asset_address(&step_request.input_asset, "input_asset", operation)?;
    let output_token =
        recovery_step_asset_address(&step_request.output_asset, "output_asset", operation)?;
    let depositor = recovery_step_address(
        &step_request.depositor_address,
        "depositor_address",
        operation,
    )?;
    let recipient = recovery_step_address(&step_request.recipient, "recipient", operation)?;
    let amount = U256::from_str_radix(&step_request.amount, 10).map_err(|err| {
        lost_intent_recovery_error(
            "parsing Across request amount",
            format!(
                "Across checkpoint recovery invalid request.amount for operation {}: {err}",
                operation.id
            ),
        )
    })?;
    let expected_output = json_u256_string_field(provider_response, "expectedOutputAmount")
        .or_else(|| json_u256_string_field(provider_response, "minOutputAmount"));

    let Some(checkpoint_tx_hash) = persisted_provider_operation_tx_hash(operation) else {
        return Ok(None);
    };
    let Some(checkpoint_receipt) = evm_chain
        .transaction_receipt(&checkpoint_tx_hash)
        .await
        .map_err(|source| {
            lost_intent_recovery_error("reading Across checkpoint transaction receipt", source)
        })?
    else {
        return Ok(None);
    };
    let Some(from_block) = checkpoint_receipt.block_number else {
        return Ok(None);
    };

    let logs = evm_chain
        .logs(
            &Filter::new()
                .address(spoke_pool_address)
                .event_signature(FundsDeposited::SIGNATURE_HASH)
                .from_block(from_block),
        )
        .await
        .map_err(|source| {
            lost_intent_recovery_error("scanning Across FundsDeposited logs", source)
        })?;

    let input_token = evm_address_to_bytes32(input_token);
    let output_token = evm_address_to_bytes32(output_token);
    let depositor = evm_address_to_bytes32(depositor);
    let recipient = evm_address_to_bytes32(recipient);
    let destination_chain_id = U256::from(destination_chain_id);

    for log in logs {
        let Ok(decoded) = FundsDeposited::decode_log(&log.inner) else {
            continue;
        };
        let event = decoded.data;
        if event.destinationChainId != destination_chain_id
            || event.inputToken != input_token
            || event.outputToken != output_token
            || event.inputAmount != amount
            || expected_output
                .as_ref()
                .is_some_and(|expected| event.outputAmount != *expected)
            || event.depositor != depositor
            || event.recipient != recipient
        {
            continue;
        }
        let Some(deposit_tx_hash) = log.transaction_hash.map(|hash| format!("{hash:#x}")) else {
            continue;
        };
        return Ok(Some(RecoveredAcrossDeposit {
            deposit_id: event.depositId.to_string(),
            deposit_tx_hash,
        }));
    }

    Ok(None)
}

pub(super) async fn verify_across_fill_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(provider_operation_id) = input.signal.provider_operation_id else {
        return Ok(provider_hint_deferred(
            None,
            "AcrossFill hint missing provider_operation_id",
        ));
    };
    let provider_operation_id = provider_operation_id.inner();
    let operation = deps
        .db
        .orders()
        .get_provider_operation(provider_operation_id)
        .await
        .map_err(OrderActivityError::db_query)?;

    if operation.order_id != input.order_id.inner() {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to order {}, not {}",
                operation.id,
                operation.order_id,
                input.order_id.inner()
            ),
        ));
    }
    if operation.execution_step_id != Some(input.step_id.inner()) {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to step {:?}, not {}",
                operation.id,
                operation.execution_step_id,
                input.step_id.inner()
            ),
        ));
    }
    if operation.operation_type != ProviderOperationType::AcrossBridge {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "AcrossFill hint cannot verify {} operation",
                operation.operation_type.to_db_string()
            ),
        ));
    }
    let Some(operation) = recover_across_operation_from_checkpoint(deps, operation).await? else {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            "Across provider operation has no deposit-id provider_ref and no recoverable deposit log yet",
        ));
    };

    let provider = deps
        .action_providers
        .bridge(&operation.provider)
        .ok_or_else(|| provider_observe_not_configured(&operation.provider))?;
    let observation = provider
        .observe_bridge_operation(ProviderOperationObservationRequest {
            operation_id: operation.id,
            operation_type: operation.operation_type,
            provider_ref: operation.provider_ref.clone(),
            request: operation.request.clone(),
            response: operation.response.clone(),
            observed_state: operation.observed_state.clone(),
            hint_evidence: provider_hint_signal_evidence(&input.signal),
        })
        .await
        .map_err(|source| provider_observe_error(&operation.provider, source))?;

    let Some(observation) = observation else {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            "Across provider returned no observation",
        ));
    };
    if let Some(reason) = provider_observation_ref_reject_reason(&operation, &observation) {
        return Ok(provider_hint_rejected(Some(provider_operation_id), reason));
    }

    match observation.status {
        ProviderOperationStatus::Completed => {
            let provider_ref = observation
                .provider_ref
                .clone()
                .or_else(|| operation.provider_ref.clone());
            let observed_state = provider_hint_observed_state(&operation, &input, &observation);
            let (updated, _) = deps
                .db
                .orders()
                .update_provider_operation_status(
                    operation.id,
                    ProviderOperationStatus::Completed,
                    provider_ref,
                    observed_state,
                    observation.response.clone(),
                    Utc::now(),
                )
                .await
                .map_err(OrderActivityError::db_query)?;
            tracing::info!(
                order_id = %updated.order_id,
                provider_operation_id = %updated.id,
                step_id = ?updated.execution_step_id,
                event_name = "provider_operation.completed",
                "provider_operation.completed"
            );
            Ok(ProviderOperationHintVerified {
                provider_operation_id: Some(updated.id.into()),
                decision: ProviderOperationHintDecision::Accept,
                reason: None,
            })
        }
        ProviderOperationStatus::Failed | ProviderOperationStatus::Expired => {
            Ok(provider_hint_rejected(
                Some(provider_operation_id),
                format!(
                    "Across fill observation returned {}",
                    observation.status.to_db_string()
                ),
            ))
        }
        ProviderOperationStatus::Planned
        | ProviderOperationStatus::Submitted
        | ProviderOperationStatus::WaitingExternal => Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "Across fill observation returned {}",
                observation.status.to_db_string()
            ),
        )),
    }
}

pub(super) async fn verify_cctp_attestation_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(provider_operation_id) = input.signal.provider_operation_id else {
        return Ok(provider_hint_deferred(
            None,
            "CctpAttestation hint missing provider_operation_id",
        ));
    };
    let provider_operation_id = provider_operation_id.inner();
    let operation = deps
        .db
        .orders()
        .get_provider_operation(provider_operation_id)
        .await
        .map_err(OrderActivityError::db_query)?;

    if operation.order_id != input.order_id.inner() {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to order {}, not {}",
                operation.id,
                operation.order_id,
                input.order_id.inner()
            ),
        ));
    }
    if operation.execution_step_id != Some(input.step_id.inner()) {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to step {:?}, not {}",
                operation.id,
                operation.execution_step_id,
                input.step_id.inner()
            ),
        ));
    }
    if operation.operation_type != ProviderOperationType::CctpBridge {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "CctpAttestation hint cannot verify {} operation",
                operation.operation_type.to_db_string()
            ),
        ));
    }
    if matches!(
        operation.status,
        ProviderOperationStatus::Completed
            | ProviderOperationStatus::Failed
            | ProviderOperationStatus::Expired
    ) {
        return Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(operation.id.into()),
            decision: if operation.status == ProviderOperationStatus::Completed {
                ProviderOperationHintDecision::Accept
            } else {
                ProviderOperationHintDecision::Reject
            },
            reason: Some(format!(
                "provider operation already terminal: {}",
                operation.status.to_db_string()
            )),
        });
    }

    let provider = deps
        .action_providers
        .bridge(&operation.provider)
        .ok_or_else(|| provider_observe_not_configured(&operation.provider))?;
    let observation = provider
        .observe_bridge_operation(ProviderOperationObservationRequest {
            operation_id: operation.id,
            operation_type: operation.operation_type,
            provider_ref: operation.provider_ref.clone(),
            request: operation.request.clone(),
            response: operation.response.clone(),
            observed_state: operation.observed_state.clone(),
            hint_evidence: provider_hint_signal_evidence(&input.signal),
        })
        .await
        .map_err(|source| provider_observe_error(&operation.provider, source))?;

    let Some(observation) = observation else {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            "CCTP provider returned no attestation observation",
        ));
    };
    if let Some(reason) = provider_observation_ref_reject_reason(&operation, &observation) {
        return Ok(provider_hint_rejected(Some(provider_operation_id), reason));
    }

    let provider_ref = observation
        .provider_ref
        .clone()
        .or_else(|| operation.provider_ref.clone())
        .or_else(|| observation.tx_hash.clone());
    let observed_state = provider_hint_observed_state(&operation, &input, &observation);
    let (updated, _) = deps
        .db
        .orders()
        .update_provider_operation_status(
            operation.id,
            observation.status,
            provider_ref,
            observed_state,
            observation.response.clone(),
            Utc::now(),
        )
        .await
        .map_err(OrderActivityError::db_query)?;

    match updated.status {
        ProviderOperationStatus::Completed => Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(updated.id.into()),
            decision: ProviderOperationHintDecision::Accept,
            reason: None,
        }),
        ProviderOperationStatus::Failed | ProviderOperationStatus::Expired => {
            Ok(provider_hint_rejected(
                Some(updated.id),
                format!(
                    "CCTP attestation observation returned {}",
                    updated.status.to_db_string()
                ),
            ))
        }
        ProviderOperationStatus::Planned
        | ProviderOperationStatus::Submitted
        | ProviderOperationStatus::WaitingExternal => Ok(provider_hint_deferred(
            Some(updated.id),
            format!(
                "CCTP attestation observation returned {}",
                updated.status.to_db_string()
            ),
        )),
    }
}

pub(super) async fn verify_hyperliquid_trade_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(provider_operation_id) = input.signal.provider_operation_id else {
        return Ok(provider_hint_deferred(
            None,
            "HyperliquidTrade hint missing provider_operation_id",
        ));
    };
    let provider_operation_id = provider_operation_id.inner();
    let operation = deps
        .db
        .orders()
        .get_provider_operation(provider_operation_id)
        .await
        .map_err(OrderActivityError::db_query)?;

    if operation.order_id != input.order_id.inner() {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to order {}, not {}",
                operation.id,
                operation.order_id,
                input.order_id.inner()
            ),
        ));
    }
    if operation.execution_step_id != Some(input.step_id.inner()) {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to step {:?}, not {}",
                operation.id,
                operation.execution_step_id,
                input.step_id.inner()
            ),
        ));
    }
    if !matches!(
        operation.operation_type,
        ProviderOperationType::HyperliquidTrade | ProviderOperationType::HyperliquidLimitOrder
    ) {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "HyperliquidTrade hint cannot verify {} operation",
                operation.operation_type.to_db_string()
            ),
        ));
    }
    if matches!(
        operation.status,
        ProviderOperationStatus::Completed
            | ProviderOperationStatus::Failed
            | ProviderOperationStatus::Expired
    ) {
        return Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(operation.id.into()),
            decision: if operation.status == ProviderOperationStatus::Completed {
                ProviderOperationHintDecision::Accept
            } else {
                ProviderOperationHintDecision::Reject
            },
            reason: Some(format!(
                "provider operation already terminal: {}",
                operation.status.to_db_string()
            )),
        });
    }

    Ok(provider_hint_deferred(
        Some(provider_operation_id),
        "Hyperliquid trade operations require typed Sauron HL trade evidence",
    ))
}

pub(super) async fn verify_hl_trade_filled_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(ProviderOperationHintEvidence::HlTradeFilled(evidence)) =
        input.signal.evidence.as_ref()
    else {
        return Ok(provider_hint_rejected(
            input.signal.provider_operation_id.map(|id| id.inner()),
            "HlTradeFilled hint missing typed evidence",
        ));
    };
    let operation = load_typed_hint_operation(
        deps,
        &input,
        "HlTradeFilled",
        &[
            ProviderOperationType::HyperliquidTrade,
            ProviderOperationType::HyperliquidLimitOrder,
        ],
    )
    .await?;
    if let Some(decision) = terminal_hint_decision(&operation) {
        return Ok(decision);
    }
    validate_hl_trade_user(&operation, &evidence.user)?;
    validate_hl_trade_oid(&operation, evidence.oid)?;
    validate_hl_trade_size(&operation, &evidence.sz)?;
    complete_provider_operation_from_typed_hint(
        deps,
        &input,
        operation,
        TypedProviderOperationUpdate {
            status: ProviderOperationStatus::Completed,
            provider_ref: input
                .signal
                .provider_ref
                .clone()
                .or_else(|| Some(evidence.hash.clone())),
            observed_state: typed_evidence_state("hl_trade_filled", evidence)?,
            response: None,
            tx_hash: Some(evidence.hash.clone()),
        },
    )
    .await
}

async fn load_typed_hint_operation(
    deps: &OrderActivityDeps,
    input: &VerifyProviderOperationHintInput,
    label: &'static str,
    allowed_types: &[ProviderOperationType],
) -> Result<OrderProviderOperation, OrderActivityError> {
    let provider_operation_id = input
        .signal
        .provider_operation_id
        .ok_or_else(|| {
            OrderActivityError::hint_verification(label, "missing provider_operation_id")
        })?
        .inner();
    let operation = deps
        .db
        .orders()
        .get_provider_operation(provider_operation_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    if operation.order_id != input.order_id.inner() {
        return Err(OrderActivityError::hint_verification(
            label,
            format!(
                "provider operation {} belongs to order {}, not {}",
                operation.id,
                operation.order_id,
                input.order_id.inner()
            ),
        ));
    }
    if operation.execution_step_id != Some(input.step_id.inner()) {
        return Err(OrderActivityError::hint_verification(
            label,
            format!(
                "provider operation {} belongs to step {:?}, not {}",
                operation.id,
                operation.execution_step_id,
                input.step_id.inner()
            ),
        ));
    }
    if !allowed_types.contains(&operation.operation_type) {
        return Err(OrderActivityError::hint_verification(
            label,
            format!(
                "{label} cannot verify {} operation",
                operation.operation_type.to_db_string()
            ),
        ));
    }
    Ok(operation)
}

async fn complete_provider_operation_from_typed_hint(
    deps: &OrderActivityDeps,
    input: &VerifyProviderOperationHintInput,
    operation: OrderProviderOperation,
    update: TypedProviderOperationUpdate,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let mut observed_state = update.observed_state;
    if let Some(tx_hash) = &update.tx_hash {
        observed_state["tx_hash"] = json!(tx_hash);
    }
    if !is_empty_json_object(&operation.observed_state) {
        observed_state["previous_observed_state"] = operation.observed_state.clone();
    }
    let (updated, _) = deps
        .db
        .orders()
        .update_provider_operation_status(
            operation.id,
            update.status,
            update.provider_ref,
            observed_state,
            update.response,
            Utc::now(),
        )
        .await
        .map_err(OrderActivityError::db_query)?;
    let decision = match updated.status {
        ProviderOperationStatus::Completed => ProviderOperationHintDecision::Accept,
        ProviderOperationStatus::Failed | ProviderOperationStatus::Expired => {
            ProviderOperationHintDecision::Reject
        }
        ProviderOperationStatus::Planned
        | ProviderOperationStatus::Submitted
        | ProviderOperationStatus::WaitingExternal => ProviderOperationHintDecision::Defer,
    };
    Ok(ProviderOperationHintVerified {
        provider_operation_id: Some(updated.id.into()),
        decision,
        reason: if decision == ProviderOperationHintDecision::Defer {
            Some(format!(
                "{:?} typed hint kept provider operation in {}",
                input.signal.hint_kind,
                updated.status.to_db_string()
            ))
        } else {
            None
        },
    })
}

struct TypedProviderOperationUpdate {
    status: ProviderOperationStatus,
    provider_ref: Option<String>,
    observed_state: Value,
    response: Option<Value>,
    tx_hash: Option<String>,
}

fn typed_evidence_state<T: serde::Serialize>(
    source: &'static str,
    evidence: &T,
) -> Result<Value, OrderActivityError> {
    Ok(json!({
        "source": source,
        "evidence": serde_json::to_value(evidence)
            .map_err(|err| OrderActivityError::serialization("serializing typed hint evidence", err))?,
    }))
}

pub(super) async fn verify_velora_swap_settled_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(ProviderOperationHintEvidence::VeloraSwapSettled(evidence)) =
        input.signal.evidence.as_ref()
    else {
        return Ok(provider_hint_rejected(
            input.signal.provider_operation_id.map(|id| id.inner()),
            "VeloraSwapSettled hint missing typed evidence",
        ));
    };
    let operation = load_typed_hint_operation(
        deps,
        &input,
        "VeloraSwapSettled",
        &[ProviderOperationType::UniversalRouterSwap],
    )
    .await?;
    if let Some(decision) = terminal_hint_decision(&operation) {
        return Ok(decision);
    }
    validate_provider_ref_matches("VeloraSwapSettled", &operation, &evidence.tx_hash)?;
    validate_request_address_match(
        "VeloraSwapSettled recipient",
        operation
            .request
            .get("recipient_address")
            .and_then(Value::as_str),
        &evidence.recipient,
    )?;
    if let Some(token_address) = evidence.token_address.as_deref() {
        validate_expected_asset_token(
            "VeloraSwapSettled output token",
            operation
                .request
                .pointer("/output_asset/chain_id")
                .and_then(Value::as_str),
            operation
                .request
                .pointer("/output_asset/asset")
                .and_then(Value::as_str),
            token_address,
        )?;
    }
    let expected_amount_out = operation
        .request
        .get("min_amount_out")
        .and_then(Value::as_str)
        .or_else(|| operation.request.get("amount_out").and_then(Value::as_str));
    if let Some(expected_amount_out) = expected_amount_out {
        validate_decimal_gte(
            "VeloraSwapSettled amount_out",
            &evidence.amount_out,
            expected_amount_out,
        )?;
    }

    complete_provider_operation_from_typed_hint(
        deps,
        &input,
        operation,
        TypedProviderOperationUpdate {
            status: ProviderOperationStatus::Completed,
            provider_ref: Some(evidence.tx_hash.clone()),
            observed_state: typed_evidence_state("velora_swap_settled", evidence)?,
            response: None,
            tx_hash: Some(evidence.tx_hash.clone()),
        },
    )
    .await
}

pub(super) async fn verify_cctp_receive_observed_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(ProviderOperationHintEvidence::CctpReceiveObserved(evidence)) =
        input.signal.evidence.as_ref()
    else {
        return Ok(provider_hint_rejected(
            input.signal.provider_operation_id.map(|id| id.inner()),
            "CctpReceiveObserved hint missing typed evidence",
        ));
    };
    let operation = load_typed_hint_operation(
        deps,
        &input,
        "CctpReceiveObserved",
        &[ProviderOperationType::CctpReceive],
    )
    .await?;
    if let Some(decision) = terminal_hint_decision(&operation) {
        return Ok(decision);
    }
    validate_provider_ref_matches("CctpReceiveObserved", &operation, &evidence.tx_hash)?;
    validate_request_address_match(
        "CctpReceiveObserved recipient",
        operation
            .request
            .get("recipient_address")
            .and_then(Value::as_str),
        &evidence.recipient,
    )?;
    validate_expected_asset_token(
        "CctpReceiveObserved token",
        operation
            .request
            .get("destination_chain_id")
            .and_then(Value::as_str),
        operation
            .request
            .get("output_asset")
            .and_then(Value::as_str),
        &evidence.token,
    )?;
    let Some(expected_amount) = operation.request.get("amount").and_then(Value::as_str) else {
        return Err(OrderActivityError::hint_verification(
            "CctpReceiveObserved",
            format!("provider operation {} request missing amount", operation.id),
        ));
    };
    validate_decimal_equal(
        "CctpReceiveObserved amount",
        &evidence.amount,
        expected_amount,
    )?;

    complete_provider_operation_from_typed_hint(
        deps,
        &input,
        operation,
        TypedProviderOperationUpdate {
            status: ProviderOperationStatus::Completed,
            provider_ref: Some(evidence.tx_hash.clone()),
            observed_state: typed_evidence_state("cctp_receive_observed", evidence)?,
            response: None,
            tx_hash: Some(evidence.tx_hash.clone()),
        },
    )
    .await
}

fn validate_provider_ref_matches(
    label: &'static str,
    operation: &OrderProviderOperation,
    observed_tx_hash: &str,
) -> Result<(), OrderActivityError> {
    match operation.provider_ref.as_deref() {
        Some(expected) if expected.eq_ignore_ascii_case(observed_tx_hash) => Ok(()),
        Some(expected) => Err(OrderActivityError::hint_verification(
            label,
            format!("observed tx_hash {observed_tx_hash} does not match operation ref {expected}"),
        )),
        None => Err(OrderActivityError::hint_verification(
            label,
            format!(
                "provider operation {} is missing provider_ref",
                operation.id
            ),
        )),
    }
}

fn validate_request_address_match(
    label: &'static str,
    expected: Option<&str>,
    observed: &str,
) -> Result<(), OrderActivityError> {
    let Some(expected) = expected.filter(|value| !value.trim().is_empty()) else {
        return Ok(());
    };
    if expected.eq_ignore_ascii_case(observed) {
        Ok(())
    } else {
        Err(OrderActivityError::hint_verification(
            label,
            format!("observed address {observed} does not match request address {expected}"),
        ))
    }
}

fn validate_expected_asset_token(
    label: &'static str,
    chain_id: Option<&str>,
    asset: Option<&str>,
    observed_token: &str,
) -> Result<(), OrderActivityError> {
    let Some(expected_token) = normalized_reference_asset(chain_id, asset, label)? else {
        return Ok(());
    };
    if expected_token.eq_ignore_ascii_case(observed_token) {
        Ok(())
    } else {
        Err(OrderActivityError::hint_verification(
            label,
            format!(
                "observed token {observed_token} does not match expected token {expected_token}"
            ),
        ))
    }
}

fn normalized_reference_asset(
    chain_id: Option<&str>,
    asset: Option<&str>,
    label: &'static str,
) -> Result<Option<String>, OrderActivityError> {
    let (Some(chain_id), Some(asset)) = (chain_id, asset) else {
        return Ok(None);
    };
    let deposit_asset = DepositAsset {
        chain: ChainId::parse(chain_id).map_err(|err| {
            OrderActivityError::hint_verification(label, format!("invalid chain_id: {err}"))
        })?,
        asset: AssetId::parse(asset).map_err(|err| {
            OrderActivityError::hint_verification(label, format!("invalid asset: {err}"))
        })?,
    }
    .normalized_asset_identity()
    .map_err(|err| OrderActivityError::hint_verification(label, err))?;
    Ok(match deposit_asset.asset {
        AssetId::Native => None,
        AssetId::Reference(address) => Some(address),
    })
}

fn validate_decimal_gte(
    label: &'static str,
    observed: &str,
    expected_minimum: &str,
) -> Result<(), OrderActivityError> {
    if decimal_string_gte(observed, expected_minimum)? {
        Ok(())
    } else {
        Err(OrderActivityError::hint_verification(
            label,
            format!("observed amount {observed} is below expected minimum {expected_minimum}"),
        ))
    }
}

fn validate_decimal_equal(
    label: &'static str,
    observed: &str,
    expected: &str,
) -> Result<(), OrderActivityError> {
    let observed = normalize_decimal_string(observed)?;
    let expected = normalize_decimal_string(expected)?;
    if observed == expected {
        Ok(())
    } else {
        Err(OrderActivityError::hint_verification(
            label,
            format!("observed amount {observed} does not match expected amount {expected}"),
        ))
    }
}

fn validate_hl_trade_oid(
    operation: &OrderProviderOperation,
    evidence_oid: u64,
) -> Result<(), OrderActivityError> {
    let expected = hl_order_oid_from_observed_state(&operation.observed_state).or_else(|| {
        operation
            .provider_ref
            .as_deref()
            .and_then(|value| value.parse().ok())
    });
    match expected {
        Some(expected) if expected == evidence_oid => Ok(()),
        Some(expected) => Err(OrderActivityError::hint_verification(
            "HyperliquidTrade",
            format!("evidence oid {evidence_oid} does not match operation oid {expected}"),
        )),
        None => Err(OrderActivityError::hint_verification(
            "HyperliquidTrade",
            "operation is missing expected oid",
        )),
    }
}

fn validate_hl_trade_user(
    operation: &OrderProviderOperation,
    evidence_user: &str,
) -> Result<(), OrderActivityError> {
    let expected = operation.request.get("user").and_then(Value::as_str);
    match expected {
        Some(expected) if expected.eq_ignore_ascii_case(evidence_user) => Ok(()),
        Some(expected) => Err(OrderActivityError::hint_verification(
            "HyperliquidTrade",
            format!("evidence user {evidence_user} does not match operation user {expected}"),
        )),
        None => Err(OrderActivityError::hint_verification(
            "HyperliquidTrade",
            "operation request is missing expected user",
        )),
    }
}

fn validate_hl_trade_size(
    operation: &OrderProviderOperation,
    filled_sz: &str,
) -> Result<(), OrderActivityError> {
    let Some(expected_sz) = operation.request.get("sz").and_then(Value::as_str) else {
        return Ok(());
    };
    if decimal_string_gte(filled_sz, expected_sz)? {
        Ok(())
    } else {
        Err(OrderActivityError::hint_verification(
            "HyperliquidTrade",
            format!("filled size {filled_sz} is below expected size {expected_sz}"),
        ))
    }
}

fn validate_hl_bridge_user(
    operation: &OrderProviderOperation,
    evidence_user: &str,
) -> Result<(), OrderActivityError> {
    let expected = operation.provider_ref.as_deref().or_else(|| {
        operation
            .request
            .get("hyperliquid_user")
            .and_then(Value::as_str)
    });
    match expected {
        Some(expected) if expected.eq_ignore_ascii_case(evidence_user) => Ok(()),
        Some(expected) => Err(OrderActivityError::hint_verification(
            "HyperliquidBridge",
            format!("evidence user {evidence_user} does not match operation user {expected}"),
        )),
        None => Err(OrderActivityError::hint_verification(
            "HyperliquidBridge",
            "operation is missing expected Hyperliquid user",
        )),
    }
}

fn validate_hl_withdrawal_user(
    operation: &OrderProviderOperation,
    evidence_user: &str,
) -> Result<(), OrderActivityError> {
    let expected = operation
        .request
        .get("hyperliquid_custody_vault_address")
        .and_then(Value::as_str);
    match expected {
        Some(expected) if expected.eq_ignore_ascii_case(evidence_user) => Ok(()),
        Some(expected) => Err(OrderActivityError::hint_verification(
            "HyperliquidBridgeWithdrawal",
            format!("evidence user {evidence_user} does not match operation user {expected}"),
        )),
        None => Ok(()),
    }
}

fn validate_hl_withdrawal_settlement_nonce(
    operation: &OrderProviderOperation,
    evidence_nonce: u64,
) -> Result<(), OrderActivityError> {
    let acknowledged_nonce = operation
        .observed_state
        .pointer("/evidence/nonce")
        .and_then(Value::as_u64);
    match acknowledged_nonce {
        Some(acknowledged_nonce) if acknowledged_nonce == evidence_nonce => Ok(()),
        Some(acknowledged_nonce) => Err(OrderActivityError::hint_verification(
            "HyperliquidBridgeWithdrawal",
            format!(
                "settlement nonce {evidence_nonce} does not match acknowledged nonce {acknowledged_nonce}"
            ),
        )),
        None => Ok(()),
    }
}

fn validate_hl_bridge_usdc_amount(
    operation: &OrderProviderOperation,
    evidence_usdc: &str,
) -> Result<(), OrderActivityError> {
    let amount = operation
        .request
        .get("amount")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            OrderActivityError::hint_verification(
                "HyperliquidBridgeDeposit",
                "operation request missing amount",
            )
        })?;
    validate_usdc_decimal_matches_raw("HyperliquidBridgeDeposit", evidence_usdc, amount)
}

fn validate_hl_withdrawal_gross_usdc_amount(
    operation: &OrderProviderOperation,
    evidence_usdc: &str,
) -> Result<(), OrderActivityError> {
    let expected = operation
        .request
        .get("amount_decimal")
        .and_then(Value::as_str)
        .or_else(|| operation.request.get("amount").and_then(Value::as_str))
        .ok_or_else(|| {
            OrderActivityError::hint_verification(
                "HyperliquidBridgeWithdrawal",
                "operation request missing amount",
            )
        })?;
    if expected.contains('.') {
        if decimal_strings_equal(evidence_usdc, expected)? {
            Ok(())
        } else {
            Err(OrderActivityError::hint_verification(
                "HyperliquidBridgeWithdrawal",
                format!("evidence usdc {evidence_usdc} does not match operation amount {expected}"),
            ))
        }
    } else {
        validate_usdc_decimal_matches_raw("HyperliquidBridgeWithdrawal", evidence_usdc, expected)
    }
}

fn validate_hl_withdrawal_payout_usdc_amount(
    operation: &OrderProviderOperation,
    evidence_usdc: &str,
) -> Result<(), OrderActivityError> {
    let expected = hl_withdrawal_payout_raw_amount(operation)?;
    validate_usdc_decimal_matches_raw("HyperliquidBridgeWithdrawal", evidence_usdc, &expected)
}

fn hl_withdrawal_payout_raw_amount(
    operation: &OrderProviderOperation,
) -> Result<String, OrderActivityError> {
    let amount = operation
        .request
        .get("amount")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            OrderActivityError::hint_verification(
                "HyperliquidBridgeWithdrawal",
                "operation request missing amount",
            )
        })?;
    let gross = parse_raw_usdc_u256("HyperliquidBridgeWithdrawal amount", amount)?;
    let fee = operation
        .request
        .get("withdraw_fee_raw")
        .and_then(Value::as_str)
        .map(|raw| parse_raw_usdc_u256("HyperliquidBridgeWithdrawal withdraw_fee_raw", raw))
        .transpose()?
        .unwrap_or(U256::ZERO);
    if gross < fee {
        return Err(OrderActivityError::hint_verification(
            "HyperliquidBridgeWithdrawal",
            format!("operation amount {amount} is smaller than withdraw fee {fee}"),
        ));
    }
    Ok((gross - fee).to_string())
}

fn parse_raw_usdc_u256(context: &str, raw: &str) -> Result<U256, OrderActivityError> {
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(OrderActivityError::amount_parse(
            context.to_string(),
            format!("invalid raw USDC amount {raw:?}"),
        ));
    }
    U256::from_str_radix(raw, 10)
        .map_err(|source| OrderActivityError::amount_parse(context.to_string(), source.to_string()))
}

fn validate_usdc_decimal_matches_raw(
    context: &'static str,
    decimal: &str,
    raw: &str,
) -> Result<(), OrderActivityError> {
    let expected = raw_usdc_to_decimal(raw)?;
    if decimal_strings_equal(decimal, &expected)? {
        Ok(())
    } else {
        Err(OrderActivityError::hint_verification(
            context,
            format!("evidence usdc {decimal} does not match raw amount {raw} ({expected})"),
        ))
    }
}

fn raw_usdc_to_decimal(raw: &str) -> Result<String, OrderActivityError> {
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(OrderActivityError::amount_parse(
            "formatting raw USDC hint amount",
            format!("invalid raw amount {raw:?}"),
        ));
    }
    let raw = raw.trim_start_matches('0');
    let raw = if raw.is_empty() { "0" } else { raw };
    if raw.len() <= 6 {
        let mut fraction = "0".repeat(6 - raw.len());
        fraction.push_str(raw);
        let fraction = fraction.trim_end_matches('0');
        return Ok(if fraction.is_empty() {
            "0".to_string()
        } else {
            format!("0.{fraction}")
        });
    }
    let (whole, fraction) = raw.split_at(raw.len() - 6);
    let fraction = fraction.trim_end_matches('0');
    Ok(if fraction.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fraction}")
    })
}

fn decimal_strings_equal(left: &str, right: &str) -> Result<bool, OrderActivityError> {
    Ok(normalize_decimal_string(left)? == normalize_decimal_string(right)?)
}

fn decimal_string_gte(left: &str, right: &str) -> Result<bool, OrderActivityError> {
    let left = normalize_decimal_string(left)?;
    let right = normalize_decimal_string(right)?;
    let (left_whole, left_frac) = split_decimal(&left);
    let (right_whole, right_frac) = split_decimal(&right);
    if left_whole.len() != right_whole.len() {
        return Ok(left_whole.len() > right_whole.len());
    }
    if left_whole != right_whole {
        return Ok(left_whole > right_whole);
    }
    let width = left_frac.len().max(right_frac.len());
    let mut left_frac = left_frac.to_string();
    let mut right_frac = right_frac.to_string();
    left_frac.extend(std::iter::repeat_n('0', width - left_frac.len()));
    right_frac.extend(std::iter::repeat_n('0', width - right_frac.len()));
    Ok(left_frac >= right_frac)
}

fn normalize_decimal_string(value: &str) -> Result<String, OrderActivityError> {
    let Some((whole, fraction)) = value.split_once('.') else {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(OrderActivityError::amount_parse(
                "normalizing decimal hint amount",
                format!("invalid decimal amount {value:?}"),
            ));
        }
        return Ok(nonempty_decimal_digits(value.trim_start_matches('0')));
    };
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(OrderActivityError::amount_parse(
            "normalizing decimal hint amount",
            format!("invalid decimal amount {value:?}"),
        ));
    }
    let whole = nonempty_decimal_digits(whole.trim_start_matches('0'));
    let fraction = fraction.trim_end_matches('0');
    Ok(if fraction.is_empty() {
        whole
    } else {
        format!("{whole}.{fraction}")
    })
}

fn split_decimal(value: &str) -> (&str, &str) {
    value.split_once('.').unwrap_or((value, ""))
}

fn nonempty_decimal_digits(value: &str) -> String {
    if value.is_empty() {
        "0".to_string()
    } else {
        value.to_string()
    }
}

fn hl_order_oid_from_observed_state(value: &Value) -> Option<u64> {
    [
        "/oid",
        "/provider_observed_state/order/order/oid",
        "/previous_observed_state/oid",
        "/previous_observed_state/provider_observed_state/order/order/oid",
    ]
    .iter()
    .find_map(|pointer| value.pointer(pointer).and_then(Value::as_u64))
}

pub(super) async fn verify_hl_trade_canceled_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(ProviderOperationHintEvidence::HlTradeCanceled(evidence)) =
        input.signal.evidence.as_ref()
    else {
        return Ok(provider_hint_rejected(
            input.signal.provider_operation_id.map(|id| id.inner()),
            "HlTradeCanceled hint missing typed evidence",
        ));
    };
    let operation = load_typed_hint_operation(
        deps,
        &input,
        "HlTradeCanceled",
        &[
            ProviderOperationType::HyperliquidTrade,
            ProviderOperationType::HyperliquidLimitOrder,
        ],
    )
    .await?;
    if let Some(decision) = terminal_hint_decision(&operation) {
        return Ok(decision);
    }
    validate_hl_trade_user(&operation, &evidence.user)?;
    validate_hl_trade_oid(&operation, evidence.oid)?;
    complete_provider_operation_from_typed_hint(
        deps,
        &input,
        operation,
        TypedProviderOperationUpdate {
            status: ProviderOperationStatus::Failed,
            provider_ref: input.signal.provider_ref.clone(),
            observed_state: typed_evidence_state("hl_trade_canceled", evidence)?,
            response: None,
            tx_hash: None,
        },
    )
    .await
}

pub(super) async fn verify_hl_bridge_deposit_observed_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(ProviderOperationHintEvidence::HlBridgeDepositObserved(evidence)) =
        input.signal.evidence.as_ref()
    else {
        return Ok(provider_hint_rejected(
            input.signal.provider_operation_id.map(|id| id.inner()),
            "HlBridgeDepositObserved hint missing typed evidence",
        ));
    };
    let operation = load_typed_hint_operation(
        deps,
        &input,
        "HlBridgeDepositObserved",
        &[ProviderOperationType::HyperliquidBridgeDeposit],
    )
    .await?;
    if let Some(decision) = terminal_hint_decision(&operation) {
        return Ok(decision);
    }
    validate_hl_bridge_user(&operation, &evidence.user)?;
    validate_hl_bridge_usdc_amount(&operation, &evidence.usdc)?;
    complete_provider_operation_from_typed_hint(
        deps,
        &input,
        operation,
        TypedProviderOperationUpdate {
            status: ProviderOperationStatus::WaitingExternal,
            provider_ref: Some(evidence.user.clone()),
            observed_state: typed_evidence_state("hl_bridge_deposit_observed", evidence)?,
            response: None,
            tx_hash: Some(evidence.arb_tx_hash.clone()),
        },
    )
    .await
}

pub(super) async fn verify_hl_bridge_deposit_credited_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(ProviderOperationHintEvidence::HlBridgeDepositCredited(evidence)) =
        input.signal.evidence.as_ref()
    else {
        return Ok(provider_hint_rejected(
            input.signal.provider_operation_id.map(|id| id.inner()),
            "HlBridgeDepositCredited hint missing typed evidence",
        ));
    };
    let operation = load_typed_hint_operation(
        deps,
        &input,
        "HlBridgeDepositCredited",
        &[ProviderOperationType::HyperliquidBridgeDeposit],
    )
    .await?;
    if let Some(decision) = terminal_hint_decision(&operation) {
        return Ok(decision);
    }
    validate_hl_bridge_user(&operation, &evidence.user)?;
    validate_hl_bridge_usdc_amount(&operation, &evidence.usdc)?;
    complete_provider_operation_from_typed_hint(
        deps,
        &input,
        operation,
        TypedProviderOperationUpdate {
            status: ProviderOperationStatus::Completed,
            provider_ref: Some(evidence.user.clone()),
            observed_state: typed_evidence_state("hl_bridge_deposit_credited", evidence)?,
            response: None,
            tx_hash: Some(evidence.hl_credit_hash.clone()),
        },
    )
    .await
}

pub(super) async fn verify_hl_withdrawal_acknowledged_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(ProviderOperationHintEvidence::HlWithdrawalAcknowledged(evidence)) =
        input.signal.evidence.as_ref()
    else {
        return Ok(provider_hint_rejected(
            input.signal.provider_operation_id.map(|id| id.inner()),
            "HlWithdrawalAcknowledged hint missing typed evidence",
        ));
    };
    let operation = load_typed_hint_operation(
        deps,
        &input,
        "HlWithdrawalAcknowledged",
        &[ProviderOperationType::HyperliquidBridgeWithdrawal],
    )
    .await?;
    if let Some(decision) = terminal_hint_decision(&operation) {
        return Ok(decision);
    }
    validate_hl_withdrawal_user(&operation, &evidence.user)?;
    validate_hl_withdrawal_gross_usdc_amount(&operation, &evidence.usdc)?;
    complete_provider_operation_from_typed_hint(
        deps,
        &input,
        operation,
        TypedProviderOperationUpdate {
            status: ProviderOperationStatus::WaitingExternal,
            provider_ref: input.signal.provider_ref.clone(),
            observed_state: typed_evidence_state("hl_withdrawal_acknowledged", evidence)?,
            response: None,
            tx_hash: Some(evidence.hl_request_hash.clone()),
        },
    )
    .await
}

pub(super) async fn verify_hl_withdrawal_settled_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(ProviderOperationHintEvidence::HlWithdrawalSettled(evidence)) =
        input.signal.evidence.as_ref()
    else {
        return Ok(provider_hint_rejected(
            input.signal.provider_operation_id.map(|id| id.inner()),
            "HlWithdrawalSettled hint missing typed evidence",
        ));
    };
    let operation = load_typed_hint_operation(
        deps,
        &input,
        "HlWithdrawalSettled",
        &[ProviderOperationType::HyperliquidBridgeWithdrawal],
    )
    .await?;
    if let Some(decision) = terminal_hint_decision(&operation) {
        return Ok(decision);
    }
    validate_hl_withdrawal_user(&operation, &evidence.user)?;
    validate_hl_withdrawal_payout_usdc_amount(&operation, &evidence.usdc)?;
    validate_hl_withdrawal_settlement_nonce(&operation, evidence.time_window_match_to_nonce)?;
    complete_provider_operation_from_typed_hint(
        deps,
        &input,
        operation,
        TypedProviderOperationUpdate {
            status: ProviderOperationStatus::Completed,
            provider_ref: Some(evidence.arb_payout_tx_hash.clone()),
            observed_state: typed_evidence_state("hl_withdrawal_settled", evidence)?,
            response: None,
            tx_hash: Some(evidence.arb_payout_tx_hash.clone()),
        },
    )
    .await
}

pub(super) async fn verify_unit_deposit_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(provider_operation_id) = input.signal.provider_operation_id else {
        return Ok(provider_hint_deferred(
            None,
            "UnitDeposit hint missing provider_operation_id",
        ));
    };
    let provider_operation_id = provider_operation_id.inner();
    let operation = deps
        .db
        .orders()
        .get_provider_operation(provider_operation_id)
        .await
        .map_err(OrderActivityError::db_query)?;

    if let Some(decision) = validate_unit_deposit_operation(&operation, &input) {
        return Ok(decision);
    }

    let Some(ProviderOperationHintEvidence::UnitDeposit(evidence)) = input.signal.evidence.as_ref()
    else {
        return verify_provider_observation_hint(deps, input).await;
    };
    let chain = match verify_chain_evidence(evidence) {
        Ok(chain) => chain,
        Err(reason) => return Ok(provider_hint_rejected(Some(provider_operation_id), reason)),
    };
    let recipient = match verify_recipient_evidence(deps, &operation, &chain).await? {
        Ok(recipient) => recipient,
        Err(reason) => return Ok(provider_hint_rejected(Some(provider_operation_id), reason)),
    };
    let amount = match verify_amount_evidence(
        deps,
        &operation,
        input.step_id.inner(),
        &input,
        &recipient,
        &chain,
    )
    .await?
    {
        Ok(amount) => amount,
        Err(reason) => return Ok(provider_hint_rejected(Some(provider_operation_id), reason)),
    };
    let provider_observation = observe_unit_deposit_provider(deps, &operation, &chain).await?;
    let updated = persist_unit_deposit_hint_observation(
        deps,
        &operation,
        &input,
        &recipient,
        &amount,
        &chain,
        provider_observation,
    )
    .await?;

    Ok(unit_deposit_hint_decision(&updated))
}

fn unit_deposit_hint_decision(updated: &OrderProviderOperation) -> ProviderOperationHintVerified {
    match updated.status {
        ProviderOperationStatus::Completed => ProviderOperationHintVerified {
            provider_operation_id: Some(updated.id.into()),
            decision: ProviderOperationHintDecision::Accept,
            reason: None,
        },
        ProviderOperationStatus::Failed | ProviderOperationStatus::Expired => {
            provider_hint_rejected(
                Some(updated.id),
                format!(
                    "UnitDeposit observation returned {}",
                    updated.status.to_db_string()
                ),
            )
        }
        ProviderOperationStatus::Planned
        | ProviderOperationStatus::Submitted
        | ProviderOperationStatus::WaitingExternal => provider_hint_deferred(
            Some(updated.id),
            format!(
                "UnitDeposit observation returned {}",
                updated.status.to_db_string()
            ),
        ),
    }
}

struct ChainEvidence<'a> {
    evidence: &'a UnitDepositHintEvidence,
}

struct RecipientEvidence {
    provider_address: OrderProviderAddress,
}

struct AmountEvidence {
    expected_amount: U256,
    verified_amount: U256,
}

struct UnitDepositProviderObservation {
    status: ProviderOperationStatus,
    provider_observed_state: Value,
    provider_tx_hash: Option<String>,
    provider_error: Option<Value>,
    provider_response: Option<Value>,
}

fn validate_unit_deposit_operation(
    operation: &OrderProviderOperation,
    input: &VerifyProviderOperationHintInput,
) -> Option<ProviderOperationHintVerified> {
    let provider_operation_id = operation.id;
    if operation.order_id != input.order_id.inner() {
        return Some(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to order {}, not {}",
                operation.id,
                operation.order_id,
                input.order_id.inner()
            ),
        ));
    }
    if operation.execution_step_id != Some(input.step_id.inner()) {
        return Some(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to step {:?}, not {}",
                operation.id,
                operation.execution_step_id,
                input.step_id.inner()
            ),
        ));
    }
    if operation.operation_type != ProviderOperationType::UnitDeposit {
        return Some(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "UnitDeposit hint cannot verify {} operation",
                operation.operation_type.to_db_string()
            ),
        ));
    }
    terminal_hint_decision(operation)
}

fn terminal_hint_decision(
    operation: &OrderProviderOperation,
) -> Option<ProviderOperationHintVerified> {
    if !matches!(
        operation.status,
        ProviderOperationStatus::Completed
            | ProviderOperationStatus::Failed
            | ProviderOperationStatus::Expired
    ) {
        return None;
    }
    Some(ProviderOperationHintVerified {
        provider_operation_id: Some(operation.id.into()),
        decision: if operation.status == ProviderOperationStatus::Completed {
            ProviderOperationHintDecision::Accept
        } else {
            ProviderOperationHintDecision::Reject
        },
        reason: Some(format!(
            "provider operation already terminal: {}",
            operation.status.to_db_string()
        )),
    })
}

fn verify_chain_evidence(evidence: &UnitDepositHintEvidence) -> Result<ChainEvidence<'_>, String> {
    if evidence.tx_hash.trim().is_empty() {
        return Err("UnitDeposit evidence tx_hash is empty".to_string());
    }
    if evidence.address.trim().is_empty() {
        return Err("UnitDeposit evidence address is empty".to_string());
    }
    Ok(ChainEvidence { evidence })
}

async fn verify_recipient_evidence(
    deps: &OrderActivityDeps,
    operation: &OrderProviderOperation,
    chain: &ChainEvidence<'_>,
) -> Result<Result<RecipientEvidence, String>, OrderActivityError> {
    let addresses = deps
        .db
        .orders()
        .get_provider_addresses_by_operation(operation.id)
        .await
        .map_err(OrderActivityError::db_query)?;
    let Some(provider_address) = addresses.into_iter().find(|address| {
        address.role == ProviderAddressRole::UnitDeposit
            && address
                .address
                .eq_ignore_ascii_case(&chain.evidence.address)
    }) else {
        return Ok(Err(format!(
            "evidence address {} does not match a UnitDeposit provider address",
            chain.evidence.address
        )));
    };
    Ok(Ok(RecipientEvidence { provider_address }))
}

async fn verify_amount_evidence(
    deps: &OrderActivityDeps,
    operation: &OrderProviderOperation,
    step_id: Uuid,
    input: &VerifyProviderOperationHintInput,
    recipient: &RecipientEvidence,
    chain: &ChainEvidence<'_>,
) -> Result<Result<AmountEvidence, String>, OrderActivityError> {
    let verified_amount = match verify_unit_deposit_candidate(
        deps,
        &recipient.provider_address,
        chain.evidence,
    )
    .await
    {
        Ok(amount) => amount,
        Err(reason) => return Ok(Err(reason)),
    };
    let step = deps
        .db
        .orders()
        .get_execution_step(step_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    let expected_amount = match expected_provider_operation_amount(
        operation,
        Some(&step),
        input.signal.hint_id.inner(),
    ) {
        Ok(amount) => amount,
        Err(reason) => return Ok(Err(reason)),
    };
    if verified_amount < expected_amount {
        return Ok(Err(format!(
            "observed amount {verified_amount} is below expected amount {expected_amount}"
        )));
    }
    Ok(Ok(AmountEvidence {
        expected_amount,
        verified_amount,
    }))
}

async fn observe_unit_deposit_provider(
    deps: &OrderActivityDeps,
    operation: &OrderProviderOperation,
    chain: &ChainEvidence<'_>,
) -> Result<UnitDepositProviderObservation, OrderActivityError> {
    let provider = deps
        .action_providers
        .unit(&operation.provider)
        .ok_or_else(|| provider_observe_not_configured(&operation.provider))?;
    let observation = provider
        .observe_unit_operation(ProviderOperationObservationRequest {
            operation_id: operation.id,
            operation_type: operation.operation_type,
            provider_ref: operation.provider_ref.clone(),
            request: operation.request.clone(),
            response: operation.response.clone(),
            observed_state: operation.observed_state.clone(),
            hint_evidence: unit_deposit_evidence_json(chain.evidence),
        })
        .await
        .map_err(|source| provider_observe_error(&operation.provider, source))?;
    Ok(match observation {
        Some(observation) => UnitDepositProviderObservation {
            status: observation.status,
            provider_observed_state: json_object_or_wrapped(observation.observed_state),
            provider_tx_hash: observation.tx_hash,
            provider_error: observation.error,
            provider_response: observation.response,
        },
        None => UnitDepositProviderObservation {
            status: ProviderOperationStatus::WaitingExternal,
            provider_observed_state: json!({}),
            provider_tx_hash: None,
            provider_error: None,
            provider_response: None,
        },
    })
}

async fn persist_unit_deposit_hint_observation(
    deps: &OrderActivityDeps,
    operation: &OrderProviderOperation,
    input: &VerifyProviderOperationHintInput,
    recipient: &RecipientEvidence,
    amount: &AmountEvidence,
    chain: &ChainEvidence<'_>,
    observation: UnitDepositProviderObservation,
) -> Result<OrderProviderOperation, OrderActivityError> {
    let tx_hash = observation
        .provider_tx_hash
        .unwrap_or_else(|| chain.evidence.tx_hash.clone());
    let mut observed_state = unit_deposit_observed_state(
        input,
        recipient,
        amount,
        chain,
        &tx_hash,
        observation.provider_observed_state,
    );
    if let Some(error) = observation.provider_error {
        observed_state["provider_error"] = error;
    }
    let response = observation.provider_response.or_else(|| {
        Some(unit_deposit_hint_response(
            operation, input, amount, &tx_hash,
        ))
    });
    let (updated, _) = deps
        .db
        .orders()
        .update_provider_operation_status(
            operation.id,
            observation.status,
            operation.provider_ref.clone(),
            observed_state,
            response,
            Utc::now(),
        )
        .await
        .map_err(OrderActivityError::db_query)?;
    Ok(updated)
}

fn unit_deposit_observed_state(
    input: &VerifyProviderOperationHintInput,
    recipient: &RecipientEvidence,
    amount: &AmountEvidence,
    chain: &ChainEvidence<'_>,
    tx_hash: &str,
    provider_observed_state: Value,
) -> Value {
    json!({
        "source": "temporal_provider_operation_hint_validation",
        "hint_id": input.signal.hint_id,
        "hint_kind": input.signal.hint_kind,
        "evidence": unit_deposit_evidence_json(chain.evidence),
        "validated_provider_address_id": recipient.provider_address.id,
        "expected_amount": amount.expected_amount.to_string(),
        "observed_amount": amount.verified_amount.to_string(),
        "transfer_index": chain.evidence.transfer_index,
        "chain_verified": true,
        "tx_hash": tx_hash,
        "provider_observed_state": provider_observed_state,
    })
}

fn unit_deposit_hint_response(
    operation: &OrderProviderOperation,
    input: &VerifyProviderOperationHintInput,
    amount: &AmountEvidence,
    tx_hash: &str,
) -> Value {
    json!({
        "kind": "provider_operation_hint_validation",
        "provider": &operation.provider,
        "operation_id": operation.id,
        "hint_id": input.signal.hint_id,
        "tx_hash": tx_hash,
        "amount": amount.verified_amount.to_string(),
        "chain_verified": true,
    })
}

pub(super) async fn verify_provider_observation_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, OrderActivityError> {
    let Some(provider_operation_id) = input.signal.provider_operation_id else {
        return Ok(provider_hint_deferred(
            None,
            "ProviderObservation hint missing provider_operation_id",
        ));
    };
    let provider_operation_id = provider_operation_id.inner();
    let operation = deps
        .db
        .orders()
        .get_provider_operation(provider_operation_id)
        .await
        .map_err(OrderActivityError::db_query)?;

    if operation.order_id != input.order_id.inner() {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to order {}, not {}",
                operation.id,
                operation.order_id,
                input.order_id.inner()
            ),
        ));
    }
    if operation.execution_step_id != Some(input.step_id.inner()) {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to step {:?}, not {}",
                operation.id,
                operation.execution_step_id,
                input.step_id.inner()
            ),
        ));
    }
    if matches!(
        operation.status,
        ProviderOperationStatus::Completed
            | ProviderOperationStatus::Failed
            | ProviderOperationStatus::Expired
    ) {
        return Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(operation.id.into()),
            decision: if operation.status == ProviderOperationStatus::Completed {
                ProviderOperationHintDecision::Accept
            } else {
                ProviderOperationHintDecision::Reject
            },
            reason: Some(format!(
                "provider operation already terminal: {}",
                operation.status.to_db_string()
            )),
        });
    }
    let operation = if operation.operation_type == ProviderOperationType::AcrossBridge {
        let Some(operation) = recover_across_operation_from_checkpoint(deps, operation).await?
        else {
            return Ok(provider_hint_deferred(
                Some(provider_operation_id),
                "Across provider operation has no deposit-id provider_ref and no recoverable deposit log yet",
            ));
        };
        operation
    } else {
        operation
    };
    if matches!(
        operation.operation_type,
        ProviderOperationType::HyperliquidBridgeDeposit
            | ProviderOperationType::HyperliquidBridgeWithdrawal
            | ProviderOperationType::HyperliquidTrade
            | ProviderOperationType::HyperliquidLimitOrder
    ) {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            "Hyperliquid operations require typed Sauron HL evidence",
        ));
    }
    if matches!(
        operation.operation_type,
        ProviderOperationType::CctpReceive | ProviderOperationType::UniversalRouterSwap
    ) {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "{} operations require typed Sauron EVM receipt evidence",
                operation.operation_type.to_db_string()
            ),
        ));
    }

    let request = ProviderOperationObservationRequest {
        operation_id: operation.id,
        operation_type: operation.operation_type,
        provider_ref: operation.provider_ref.clone(),
        request: operation.request.clone(),
        response: operation.response.clone(),
        observed_state: operation.observed_state.clone(),
        hint_evidence: provider_hint_signal_evidence(&input.signal),
    };
    let observation = match operation.operation_type {
        ProviderOperationType::AcrossBridge
        | ProviderOperationType::CctpBridge
        | ProviderOperationType::CctpReceive
        | ProviderOperationType::HyperliquidBridgeDeposit
        | ProviderOperationType::HyperliquidBridgeWithdrawal => {
            let provider = deps
                .action_providers
                .bridge(&operation.provider)
                .ok_or_else(|| provider_observe_not_configured(&operation.provider))?;
            provider
                .observe_bridge_operation(request)
                .await
                .map_err(|source| provider_observe_error(&operation.provider, source))?
        }
        ProviderOperationType::UnitDeposit | ProviderOperationType::UnitWithdrawal => {
            let provider = deps
                .action_providers
                .unit(&operation.provider)
                .ok_or_else(|| provider_observe_not_configured(&operation.provider))?;
            provider
                .observe_unit_operation(request)
                .await
                .map_err(|source| provider_observe_error(&operation.provider, source))?
        }
        ProviderOperationType::HyperliquidTrade
        | ProviderOperationType::HyperliquidLimitOrder
        | ProviderOperationType::UniversalRouterSwap => {
            let provider = deps
                .action_providers
                .exchange(&operation.provider)
                .ok_or_else(|| provider_observe_not_configured(&operation.provider))?;
            provider
                .observe_trade_operation(request)
                .await
                .map_err(|source| provider_observe_error(&operation.provider, source))?
        }
    };

    let Some(observation) = observation else {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "provider {} returned no observation for {}",
                operation.provider,
                operation.operation_type.to_db_string()
            ),
        ));
    };
    if let Some(reason) = provider_observation_ref_reject_reason(&operation, &observation) {
        return Ok(provider_hint_rejected(Some(provider_operation_id), reason));
    }

    let provider_ref = observation
        .provider_ref
        .clone()
        .or_else(|| operation.provider_ref.clone())
        .or_else(|| observation.tx_hash.clone());
    let observed_state = provider_hint_observed_state(&operation, &input, &observation);
    let (updated, _) = deps
        .db
        .orders()
        .update_provider_operation_status(
            operation.id,
            observation.status,
            provider_ref,
            observed_state,
            observation.response.clone(),
            Utc::now(),
        )
        .await
        .map_err(OrderActivityError::db_query)?;

    match updated.status {
        ProviderOperationStatus::Completed => Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(updated.id.into()),
            decision: ProviderOperationHintDecision::Accept,
            reason: None,
        }),
        ProviderOperationStatus::Failed | ProviderOperationStatus::Expired => {
            Ok(provider_hint_rejected(
                Some(updated.id),
                format!(
                    "provider observation returned {}",
                    updated.status.to_db_string()
                ),
            ))
        }
        ProviderOperationStatus::Planned
        | ProviderOperationStatus::Submitted
        | ProviderOperationStatus::WaitingExternal => Ok(provider_hint_deferred(
            Some(updated.id),
            format!(
                "provider observation returned {}",
                updated.status.to_db_string()
            ),
        )),
    }
}

pub(super) fn provider_hint_deferred(
    provider_operation_id: Option<Uuid>,
    reason: impl Into<String>,
) -> ProviderOperationHintVerified {
    ProviderOperationHintVerified {
        provider_operation_id: provider_operation_id.map(Into::into),
        decision: ProviderOperationHintDecision::Defer,
        reason: Some(reason.into()),
    }
}

pub(super) fn provider_hint_rejected(
    provider_operation_id: Option<Uuid>,
    reason: impl Into<String>,
) -> ProviderOperationHintVerified {
    ProviderOperationHintVerified {
        provider_operation_id: provider_operation_id.map(Into::into),
        decision: ProviderOperationHintDecision::Reject,
        reason: Some(reason.into()),
    }
}

pub(super) fn provider_observation_ref_reject_reason(
    operation: &OrderProviderOperation,
    observation: &ProviderOperationObservation,
) -> Option<String> {
    let expected = operation.provider_ref.as_deref()?;
    match observation.provider_ref.as_deref() {
        Some(observed) if observed == expected => None,
        Some(observed) => Some(format!(
            "provider observation ref {observed} does not match operation ref {expected}"
        )),
        None => Some(format!(
            "provider observation for {} did not include expected operation ref {expected}",
            operation.operation_type.to_db_string()
        )),
    }
}

pub(super) fn provider_hint_observed_state(
    operation: &OrderProviderOperation,
    input: &VerifyProviderOperationHintInput,
    observation: &ProviderOperationObservation,
) -> Value {
    let mut observed_state = json!({
        "source": "temporal_provider_operation_hint_signal",
        "hint_id": input.signal.hint_id,
        "hint_kind": input.signal.hint_kind,
        "provider_observed_state": &observation.observed_state,
    });
    if let Some(tx_hash) = &observation.tx_hash {
        observed_state["tx_hash"] = json!(tx_hash);
    }
    if let Some(error) = &observation.error {
        observed_state["provider_error"] = error.clone();
    }
    if let Some(recovery) = across_log_recovery_marker(operation) {
        observed_state["router_recovery"] = recovery;
    }
    if !is_empty_json_object(&operation.observed_state) {
        observed_state["previous_observed_state"] = operation.observed_state.clone();
    }
    observed_state
}

pub(super) fn across_log_recovery_marker(operation: &OrderProviderOperation) -> Option<Value> {
    if operation.operation_type != ProviderOperationType::AcrossBridge {
        return None;
    }
    if let Some(recovery) = find_across_log_recovery_marker(&operation.observed_state) {
        return Some(recovery);
    }
    if let Some(deposit_tx_hash) =
        recovered_across_deposit_tx_hash_from_value(&operation.observed_state)
    {
        return Some(json!({
            "kind": "across_deposit_log_recovery",
            "deposit_tx_hash": deposit_tx_hash,
            "source_provider_ref": &operation.provider_ref,
        }));
    }
    if operation.response.get("kind").and_then(Value::as_str) != Some("provider_receipt_checkpoint")
    {
        return None;
    }
    let deposit_tx_hash = provider_operation_tx_hash_from_value(&operation.response)
        .or_else(|| provider_operation_tx_hash_from_value(&operation.observed_state));
    Some(json!({
        "kind": "across_deposit_log_recovery",
        "deposit_tx_hash": deposit_tx_hash,
        "source_provider_ref": &operation.provider_ref,
    }))
}

pub(super) fn find_across_log_recovery_marker(value: &Value) -> Option<Value> {
    let object = value.as_object()?;
    if object
        .get("kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "across_deposit_log_recovery")
    {
        return Some(value.clone());
    }
    object.values().find_map(find_across_log_recovery_marker)
}

pub(super) fn recovered_across_deposit_tx_hash_from_value(value: &Value) -> Option<String> {
    let object = value.as_object()?;
    let has_recovery_shape = object.contains_key("deposit_id")
        || object
            .get("source")
            .and_then(Value::as_str)
            .is_some_and(|source| source == "temporal_across_deposit_log_recovery");
    if has_recovery_shape {
        if let Some(deposit_tx_hash) = object.get("deposit_tx_hash").and_then(Value::as_str) {
            return Some(deposit_tx_hash.to_string());
        }
    }
    object
        .get("previous_observed_state")
        .and_then(recovered_across_deposit_tx_hash_from_value)
}

pub(super) fn provider_hint_signal_evidence(signal: &ProviderOperationHintSignal) -> Value {
    json!({
        "source": "temporal_provider_operation_hint_signal",
        "hint_id": signal.hint_id,
        "provider": signal.provider,
        "hint_kind": signal.hint_kind,
        "provider_ref": &signal.provider_ref,
        "evidence": &signal.evidence,
    })
}

pub(super) fn unit_deposit_evidence_json(evidence: &UnitDepositHintEvidence) -> Value {
    let mut value = json!({
        "tx_hash": &evidence.tx_hash,
        "address": &evidence.address,
        "transfer_index": evidence.transfer_index,
    });
    if let Some(amount) = &evidence.amount {
        value["amount"] = json!(amount);
    }
    value
}

pub(super) async fn verify_unit_deposit_candidate(
    deps: &OrderActivityDeps,
    provider_address: &OrderProviderAddress,
    evidence: &UnitDepositHintEvidence,
) -> Result<U256, String> {
    let backend_chain = backend_chain_for_id(&provider_address.chain).ok_or_else(|| {
        format!(
            "provider address chain {} is not supported by the temporal worker",
            provider_address.chain
        )
    })?;
    let chain = deps.chain_registry.get(&backend_chain).ok_or_else(|| {
        format!(
            "temporal worker has no chain implementation for {}",
            backend_chain.to_db_string()
        )
    })?;
    let asset = provider_address
        .asset
        .as_ref()
        .ok_or_else(|| "provider address does not declare an asset".to_string())?;
    let currency = Currency {
        chain: backend_chain,
        token: token_identifier(asset),
        decimals: currency_decimals(&backend_chain, asset),
    };

    match chain
        .verify_user_deposit_candidate(
            &provider_address.address,
            &currency,
            &evidence.tx_hash,
            evidence.transfer_index,
        )
        .await
    {
        Ok(UserDepositCandidateStatus::Verified(deposit)) => Ok(deposit.amount),
        Ok(UserDepositCandidateStatus::TxNotFound) => Err(format!(
            "candidate transaction {} was not found",
            evidence.tx_hash
        )),
        Ok(UserDepositCandidateStatus::TransferNotFound) => Err(format!(
            "candidate transaction {} does not pay provider address {} at transfer index {}",
            evidence.tx_hash, provider_address.address, evidence.transfer_index
        )),
        Err(err) => Err(format!(
            "failed to verify candidate deposit on chain: {err}"
        )),
    }
}

pub(super) fn expected_provider_operation_amount(
    operation: &OrderProviderOperation,
    step: Option<&OrderExecutionStep>,
    hint_id: Uuid,
) -> Result<U256, String> {
    if let Some(expected) = operation
        .request
        .get("expected_amount")
        .and_then(Value::as_str)
    {
        return U256::from_str(expected)
            .map_err(|err| format!("hint {hint_id}: operation expected_amount is invalid: {err}"));
    }
    if let Some(amount_in) = step.and_then(|step| step.amount_in.as_deref()) {
        return U256::from_str(amount_in)
            .map_err(|err| format!("hint {hint_id}: step amount_in is invalid: {err}"));
    }

    Ok(U256::from(1_u64))
}

pub(super) fn token_identifier(asset: &AssetId) -> TokenIdentifier {
    match asset {
        AssetId::Native => TokenIdentifier::Native,
        AssetId::Reference(value) => TokenIdentifier::address(value.clone()),
    }
}

pub(super) fn currency_decimals(chain: &ChainType, asset: &AssetId) -> u8 {
    match (chain, asset) {
        (ChainType::Bitcoin, AssetId::Native) => 8,
        (_, AssetId::Native) => 18,
        (_, AssetId::Reference(_)) => 8,
    }
}

pub(super) fn is_decimal_u256(value: &str) -> bool {
    !value.is_empty() && U256::from_str_radix(value, 10).is_ok()
}

pub(super) fn json_str_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

pub(super) fn json_u64_field(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64).or_else(|| {
        value
            .get(key)
            .and_then(Value::as_str)
            .and_then(|raw| raw.parse::<u64>().ok())
    })
}

pub(super) fn json_u256_string_field(value: &Value, key: &str) -> Option<U256> {
    value
        .get(key)
        .and_then(Value::as_str)
        .and_then(|raw| U256::from_str_radix(raw, 10).ok())
}

pub(super) fn recovery_step_asset_address(
    raw: &str,
    key: &'static str,
    operation: &OrderProviderOperation,
) -> Result<Address, OrderActivityError> {
    let asset = AssetId::parse(raw).map_err(|err| {
        lost_intent_recovery_error(
            "parsing Across step asset address",
            format!(
                "Across checkpoint recovery invalid request.{key} for operation {}: {err}",
                operation.id
            ),
        )
    })?;
    match asset {
        AssetId::Native => Ok(Address::ZERO),
        AssetId::Reference(address) => Address::from_str(&address).map_err(|err| {
            lost_intent_recovery_error(
                "parsing Across reference asset address",
                format!(
                    "Across checkpoint recovery invalid request.{key} for operation {}: {err}",
                    operation.id
                ),
            )
        }),
    }
}

pub(super) fn recovery_step_address(
    raw: &str,
    key: &'static str,
    operation: &OrderProviderOperation,
) -> Result<Address, OrderActivityError> {
    Address::from_str(raw).map_err(|err| {
        lost_intent_recovery_error(
            "parsing Across request address",
            format!(
                "Across checkpoint recovery invalid request.{key} for operation {}: {err}",
                operation.id
            ),
        )
    })
}

pub(super) fn evm_address_to_bytes32(address: Address) -> FixedBytes<32> {
    let mut bytes = [0_u8; 32];
    bytes[12..].copy_from_slice(address.as_slice());
    FixedBytes::from(bytes)
}

pub(super) fn persisted_provider_operation_tx_hash(
    operation: &OrderProviderOperation,
) -> Option<String> {
    provider_operation_tx_hash_from_value(&operation.response)
        .or_else(|| provider_operation_tx_hash_from_value(&operation.observed_state))
}

pub(super) fn provider_operation_tx_hash_from_value(value: &Value) -> Option<String> {
    value
        .get("tx_hash")
        .or_else(|| value.get("latest_tx_hash"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            value
                .get("tx_hashes")
                .and_then(Value::as_array)
                .and_then(|hashes| hashes.last())
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .or_else(|| {
            value
                .get("receipt")
                .and_then(provider_operation_tx_hash_from_value)
        })
        .or_else(|| {
            value
                .get("response")
                .and_then(provider_operation_tx_hash_from_value)
        })
        .or_else(|| {
            value
                .get("provider_response")
                .and_then(provider_operation_tx_hash_from_value)
        })
        .or_else(|| {
            value
                .get("previous_observed_state")
                .and_then(provider_operation_tx_hash_from_value)
        })
        .or_else(|| {
            value
                .get("provider_observed_state")
                .and_then(provider_operation_tx_hash_from_value)
        })
}

pub(super) fn json_object_or_wrapped(value: Value) -> Value {
    if value.is_object() {
        value
    } else {
        json!({ "value": value })
    }
}
