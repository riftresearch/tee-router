use super::*;

pub(super) async fn hydrate_destination_execution_steps(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    steps: Vec<OrderExecutionStep>,
) -> Result<Vec<OrderExecutionStep>, OrderActivityError> {
    let mut source_deposit_vault: Option<CustodyVault> = None;
    let mut destination_execution_vault: Option<CustodyVault> = None;
    let mut hyperliquid_spot_vault: Option<CustodyVault> = None;
    let mut hydrated = Vec::with_capacity(steps.len());

    for mut step in steps {
        if json_string_equals(
            &step.request,
            "recipient_custody_vault_role",
            CustodyVaultRole::DestinationExecution.to_db_string(),
        ) {
            let asset = step
                .output_asset
                .clone()
                .ok_or(OrderActivityError::MissingHydration {
                    vault_role: "destination_execution_recipient_output_asset",
                    step_id: step.id.into(),
                })?;
            let vault = ensure_destination_execution_vault(
                deps,
                order_id,
                &asset,
                &mut destination_execution_vault,
            )
            .await?;
            if step.request.get("recipient").is_some() {
                set_json_value(&mut step.request, "recipient", json!(vault.address));
            }
            if step.request.get("recipient_address").is_some() {
                set_json_value(&mut step.request, "recipient_address", json!(vault.address));
            }
            set_json_value(
                &mut step.request,
                "recipient_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "destination_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "destination_custody_vault_address",
                json!(vault.address),
            );
        }

        if json_string_equals(
            &step.request,
            "source_custody_vault_role",
            CustodyVaultRole::DestinationExecution.to_db_string(),
        ) {
            let asset = step
                .input_asset
                .clone()
                .ok_or(OrderActivityError::MissingHydration {
                    vault_role: "destination_execution_source_input_asset",
                    step_id: step.id.into(),
                })?;
            let vault = ensure_destination_execution_vault(
                deps,
                order_id,
                &asset,
                &mut destination_execution_vault,
            )
            .await?;
            set_json_value(
                &mut step.request,
                "source_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.request,
                "source_custody_vault_address",
                json!(vault.address),
            );
            set_json_value(
                &mut step.details,
                "source_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "source_custody_vault_address",
                json!(vault.address),
            );
        }

        if json_string_equals(
            &step.request,
            "depositor_custody_vault_role",
            CustodyVaultRole::DestinationExecution.to_db_string(),
        ) {
            let asset = step
                .input_asset
                .clone()
                .ok_or(OrderActivityError::MissingHydration {
                    vault_role: "destination_execution_depositor_input_asset",
                    step_id: step.id.into(),
                })?;
            let vault = ensure_destination_execution_vault(
                deps,
                order_id,
                &asset,
                &mut destination_execution_vault,
            )
            .await?;
            set_json_value(
                &mut step.request,
                "depositor_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(&mut step.request, "depositor_address", json!(vault.address));
            set_json_value(
                &mut step.details,
                "depositor_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "depositor_custody_vault_address",
                json!(vault.address),
            );
        }

        if json_string_equals(
            &step.request,
            "refund_custody_vault_role",
            CustodyVaultRole::DestinationExecution.to_db_string(),
        ) {
            let asset = step
                .input_asset
                .clone()
                .ok_or(OrderActivityError::MissingHydration {
                    vault_role: "destination_execution_refund_input_asset",
                    step_id: step.id.into(),
                })?;
            let vault = ensure_destination_execution_vault(
                deps,
                order_id,
                &asset,
                &mut destination_execution_vault,
            )
            .await?;
            set_json_value(
                &mut step.request,
                "refund_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(&mut step.request, "refund_address", json!(vault.address));
            set_json_value(
                &mut step.details,
                "refund_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "refund_custody_vault_address",
                json!(vault.address),
            );
        }

        if json_string_equals(
            &step.request,
            "hyperliquid_custody_vault_role",
            CustodyVaultRole::SourceDeposit.to_db_string(),
        ) {
            let chain_id = step
                .request
                .get("hyperliquid_custody_vault_chain_id")
                .and_then(Value::as_str)
                .ok_or(OrderActivityError::MissingHydration {
                    vault_role: "source_deposit_hyperliquid_chain",
                    step_id: step.id.into(),
                })?;
            let asset_id = step
                .request
                .get("hyperliquid_custody_vault_asset_id")
                .and_then(Value::as_str)
                .ok_or(OrderActivityError::MissingHydration {
                    vault_role: "source_deposit_hyperliquid_asset",
                    step_id: step.id.into(),
                })?;
            let asset = DepositAsset {
                chain: ChainId::parse(chain_id).map_err(|source| {
                    invariant_error(
                        "hyperliquid_custody_vault_chain_id_valid",
                        format!(
                            "step {} hyperliquid_custody_vault_chain_id is invalid: {source}",
                            step.id
                        ),
                    )
                })?,
                asset: AssetId::parse(asset_id).map_err(|source| {
                    invariant_error(
                        "hyperliquid_custody_vault_asset_id_valid",
                        format!(
                            "step {} hyperliquid_custody_vault_asset_id is invalid: {source}",
                            step.id
                        ),
                    )
                })?,
            };
            let vault =
                ensure_source_deposit_vault(deps, order_id, &asset, &mut source_deposit_vault)
                    .await?;
            set_json_value(
                &mut step.request,
                "hyperliquid_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.request,
                "hyperliquid_custody_vault_address",
                json!(vault.address),
            );
            set_json_value(
                &mut step.details,
                "hyperliquid_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "hyperliquid_custody_vault_address",
                json!(vault.address),
            );
        }

        if json_string_equals(
            &step.request,
            "hyperliquid_custody_vault_role",
            CustodyVaultRole::HyperliquidSpot.to_db_string(),
        ) {
            let vault =
                ensure_hyperliquid_spot_vault(deps, order_id, &mut hyperliquid_spot_vault).await?;
            set_json_value(
                &mut step.request,
                "hyperliquid_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.request,
                "hyperliquid_custody_vault_address",
                json!(vault.address),
            );
            set_json_value(
                &mut step.details,
                "hyperliquid_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "hyperliquid_custody_vault_address",
                json!(vault.address),
            );
        }

        if json_string_equals(
            &step.request,
            "hyperliquid_custody_vault_role",
            CustodyVaultRole::DestinationExecution.to_db_string(),
        ) {
            let chain_id = step
                .request
                .get("hyperliquid_custody_vault_chain_id")
                .and_then(Value::as_str)
                .ok_or(OrderActivityError::MissingHydration {
                    vault_role: "destination_execution_hyperliquid_chain",
                    step_id: step.id.into(),
                })?;
            let asset_id = step
                .request
                .get("hyperliquid_custody_vault_asset_id")
                .and_then(Value::as_str)
                .ok_or(OrderActivityError::MissingHydration {
                    vault_role: "destination_execution_hyperliquid_asset",
                    step_id: step.id.into(),
                })?;
            let asset = DepositAsset {
                chain: ChainId::parse(chain_id).map_err(|source| {
                    invariant_error(
                        "hyperliquid_custody_vault_chain_id_valid",
                        format!(
                            "step {} hyperliquid_custody_vault_chain_id is invalid: {source}",
                            step.id
                        ),
                    )
                })?,
                asset: AssetId::parse(asset_id).map_err(|source| {
                    invariant_error(
                        "hyperliquid_custody_vault_asset_id_valid",
                        format!(
                            "step {} hyperliquid_custody_vault_asset_id is invalid: {source}",
                            step.id
                        ),
                    )
                })?,
            };
            let vault = ensure_destination_execution_vault(
                deps,
                order_id,
                &asset,
                &mut destination_execution_vault,
            )
            .await?;
            set_json_value(
                &mut step.request,
                "hyperliquid_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.request,
                "hyperliquid_custody_vault_address",
                json!(vault.address),
            );
            set_json_value(
                &mut step.details,
                "hyperliquid_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "hyperliquid_custody_vault_address",
                json!(vault.address),
            );
        }

        hydrated.push(step);
    }

    Ok(hydrated)
}

pub(super) async fn ensure_source_deposit_vault(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    asset: &DepositAsset,
    cache: &mut Option<CustodyVault>,
) -> Result<CustodyVault, OrderActivityError> {
    if let Some(vault) = cache.as_ref().filter(|vault| {
        vault.chain == asset.chain
            && vault.asset.as_ref() == Some(&asset.asset)
            && vault.role == CustodyVaultRole::SourceDeposit
    }) {
        return Ok(vault.clone());
    }

    let vault = find_source_deposit_vault(deps, order_id, asset)
        .await?
        .ok_or_else(|| {
            invariant_error(
                "source_deposit_vault_hydrated",
                format!(
                    "source deposit custody vault for order {order_id} and asset {}:{} was not found",
                    asset.chain.as_str(),
                    asset.asset.as_str()
                ),
            )
        })?;
    *cache = Some(vault.clone());
    Ok(vault)
}

pub(super) async fn find_source_deposit_vault(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    asset: &DepositAsset,
) -> Result<Option<CustodyVault>, OrderActivityError> {
    let vaults = deps
        .db
        .orders()
        .get_custody_vaults(order_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    Ok(vaults.into_iter().find(|vault| {
        vault.role == CustodyVaultRole::SourceDeposit
            && vault.chain == asset.chain
            && vault.asset.as_ref() == Some(&asset.asset)
    }))
}

pub(super) async fn ensure_hyperliquid_spot_vault(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    cache: &mut Option<CustodyVault>,
) -> Result<CustodyVault, OrderActivityError> {
    if let Some(vault) = cache.as_ref() {
        return Ok(vault.clone());
    }

    if let Some(vault) = find_hyperliquid_spot_vault(deps, order_id).await? {
        *cache = Some(vault.clone());
        return Ok(vault);
    }

    let created = deps
        .custody_action_executor
        .create_router_derived_vault(
            order_id,
            CustodyVaultRole::HyperliquidSpot,
            CustodyVaultVisibility::Internal,
            ChainId::parse("hyperliquid").map_err(|source| {
                invariant_error("hyperliquid_chain_id_constant_valid", source.to_string())
            })?,
            None,
            json!({ "source": "order_workflow_plan_hydration" }),
        )
        .await;
    let vault = match created {
        Ok(vault) => vault,
        Err(CustodyActionError::Database { source }) if is_unique_violation(&source) => {
            find_hyperliquid_spot_vault(deps, order_id)
                .await?
                .ok_or_else(|| {
                    invariant_error(
                        "hyperliquid_spot_vault_unique_reread",
                        format!(
                        "HyperliquidSpot custody vault unique violation for order {order_id} but re-read returned none"
                    ))
                })?
        }
        Err(source) => {
            return Err(custody_action_error(
                "create hyperliquid spot vault",
                source,
            ))
        }
    };
    *cache = Some(vault.clone());
    Ok(vault)
}

pub(super) async fn find_hyperliquid_spot_vault(
    deps: &OrderActivityDeps,
    order_id: Uuid,
) -> Result<Option<CustodyVault>, OrderActivityError> {
    let vaults = deps
        .db
        .orders()
        .get_custody_vaults(order_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    Ok(vaults.into_iter().find(|vault| {
        vault.role == CustodyVaultRole::HyperliquidSpot
            && vault.chain == ChainId::parse("hyperliquid").expect("valid hyperliquid chain id")
    }))
}

pub(super) async fn ensure_destination_execution_vault(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    asset: &DepositAsset,
    cache: &mut Option<CustodyVault>,
) -> Result<CustodyVault, OrderActivityError> {
    if let Some(vault) = cache.as_ref().filter(|vault| {
        vault.chain == asset.chain
            && vault.asset.as_ref() == Some(&asset.asset)
            && vault.role == CustodyVaultRole::DestinationExecution
    }) {
        return Ok(vault.clone());
    }

    if let Some(vault) = find_destination_execution_vault(deps, order_id, asset).await? {
        *cache = Some(vault.clone());
        return Ok(vault);
    }

    let created = deps
        .custody_action_executor
        .create_router_derived_vault(
            order_id,
            CustodyVaultRole::DestinationExecution,
            CustodyVaultVisibility::Internal,
            asset.chain.clone(),
            Some(asset.asset.clone()),
            json!({ "source": "order_workflow_plan_hydration" }),
        )
        .await;
    let vault = match created {
        Ok(vault) => vault,
        Err(CustodyActionError::Database { source }) if is_unique_violation(&source) => find_destination_execution_vault(deps, order_id, asset)
            .await?
            .ok_or_else(|| {
                invariant_error(
                    "destination_execution_vault_unique_reread",
                    format!(
                        "custody vault unique violation for order {order_id} on {} {} but re-read returned none",
                        asset.chain.as_str(),
                        asset.asset.as_str()
                    ),
                )
            })?,
        Err(source) => {
            return Err(custody_action_error(
                "create destination execution vault",
                source,
            ))
        }
    };
    *cache = Some(vault.clone());
    Ok(vault)
}

pub(super) async fn find_destination_execution_vault(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    asset: &DepositAsset,
) -> Result<Option<CustodyVault>, OrderActivityError> {
    let vaults = deps
        .db
        .orders()
        .get_custody_vaults(order_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    Ok(vaults.into_iter().find(|vault| {
        vault.role == CustodyVaultRole::DestinationExecution
            && vault.chain == asset.chain
            && vault.asset.as_ref() == Some(&asset.asset)
    }))
}

pub(super) fn json_string_equals(value: &Value, key: &'static str, expected: &'static str) -> bool {
    value.get(key).and_then(Value::as_str) == Some(expected)
}

pub(super) fn provider_operation_has_checkpoint(operation: &OrderProviderOperation) -> bool {
    operation.provider_ref.is_some()
        || json_object_non_empty(&operation.response)
        || json_object_non_empty(&operation.observed_state)
}

pub(super) fn manual_context_step_rank(status: OrderExecutionStepStatus) -> u8 {
    match status {
        OrderExecutionStepStatus::Failed => 5,
        OrderExecutionStepStatus::Running | OrderExecutionStepStatus::Waiting => 4,
        OrderExecutionStepStatus::Ready | OrderExecutionStepStatus::Planned => 3,
        OrderExecutionStepStatus::Completed => 2,
        OrderExecutionStepStatus::Cancelled | OrderExecutionStepStatus::Skipped => 1,
        OrderExecutionStepStatus::Superseded => 0,
    }
}

pub(super) fn json_object_non_empty(value: &Value) -> bool {
    value.as_object().is_some_and(|object| !object.is_empty())
}

pub(super) fn is_unique_violation(err: &RouterCoreError) -> bool {
    matches!(
        err,
        RouterCoreError::DatabaseQuery { source }
            if source
                .as_database_error()
                .and_then(|db_err| db_err.code())
                .as_deref()
                == Some("23505")
    )
}

pub(super) fn request_string_field(
    step: &OrderExecutionStep,
    field: &'static str,
) -> Result<String, OrderActivityError> {
    step.request
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            refund_materialization_error(format!(
                "step {} refund request is missing string field {field}",
                step.id
            ))
        })
}

pub(super) fn request_uuid_field(
    step: &OrderExecutionStep,
    field: &'static str,
) -> Result<Uuid, OrderActivityError> {
    let value = request_string_field(step, field)?;
    Uuid::parse_str(&value).map_err(|source| {
        refund_materialization_error(format!(
            "step {} refund request field {field} is not a uuid: {source}",
            step.id
        ))
    })
}

pub(super) async fn hydrate_cctp_receive_request(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
) -> Result<Value, OrderActivityError> {
    let burn_transition_decl_id = step
        .request
        .get("burn_transition_decl_id")
        .and_then(Value::as_str)
        .ok_or(OrderActivityError::MissingHydration {
            vault_role: "cctp_receive_burn_transition",
            step_id: step.id.into(),
        })?;
    let operations = deps
        .db
        .orders()
        .get_provider_operations(step.order_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    let operation = operations.into_iter().rev().find(|operation| {
        operation.operation_type == ProviderOperationType::CctpBridge
            && operation.status == ProviderOperationStatus::Completed
            && operation
                .request
                .get("transition_decl_id")
                .and_then(Value::as_str)
                == Some(burn_transition_decl_id)
    });
    let Some(operation) = operation else {
        return Err(OrderActivityError::MissingHydration {
            vault_role: "cctp_completed_burn_operation",
            step_id: step.id.into(),
        });
    };
    let cctp_state = operation
        .observed_state
        .get("provider_observed_state")
        .unwrap_or(&operation.observed_state);
    if let Some(decoded_amount) = cctp_state
        .get("decoded_message_body")
        .and_then(|body| body.get("amount"))
        .and_then(Value::as_str)
    {
        let planned_amount = step.request.get("amount").and_then(Value::as_str).ok_or(
            OrderActivityError::MissingHydration {
                vault_role: "cctp_receive_amount",
                step_id: step.id.into(),
            },
        )?;
        if decoded_amount != planned_amount {
            return Err(invariant_error(
                "cctp_receive_amount_matches_attestation",
                format!(
                    "CCTP receive amount {planned_amount} does not match attested burn amount {decoded_amount}"
                ),
            ));
        }
    }
    let message = cctp_state.get("message").and_then(Value::as_str).ok_or(
        OrderActivityError::MissingHydration {
            vault_role: "cctp_attested_message",
            step_id: step.id.into(),
        },
    )?;
    let attestation = cctp_state
        .get("attestation")
        .and_then(Value::as_str)
        .ok_or(OrderActivityError::MissingHydration {
            vault_role: "cctp_attestation",
            step_id: step.id.into(),
        })?;
    let mut request = step.request.clone();
    set_json_value(&mut request, "message", json!(message));
    set_json_value(&mut request, "attestation", json!(attestation));
    set_json_value(
        &mut request,
        "burn_provider_operation_id",
        json!(operation.id),
    );
    if let Some(tx_hash) = operation.provider_ref {
        set_json_value(&mut request, "burn_tx_hash", json!(tx_hash));
    }
    Ok(request)
}
