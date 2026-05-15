use super::*;

#[derive(Clone, Default)]
pub struct RefundActivities {
    deps: Option<Arc<OrderActivityDeps>>,
}

impl RefundActivities {
    #[must_use]
    pub(crate) fn from_order_activities(order_activities: &OrderActivities) -> Self {
        Self {
            deps: order_activities.shared_deps(),
        }
    }

    fn deps(&self) -> Result<Arc<OrderActivityDeps>, OrderActivityError> {
        self.deps
            .clone()
            .ok_or_else(|| OrderActivityError::missing_configuration("refund activities"))
    }
}

#[activities]
impl RefundActivities {
    /// Scar tissue: §8 refund position discovery and §16.1 balance reads.
    #[activity]
    pub async fn discover_single_refund_position(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DiscoverSingleRefundPositionInput,
    ) -> Result<SingleRefundPositionDiscovery, ActivityError> {
        record_activity("discover_single_refund_position", async move {
            let deps = self.deps()?;
            discover_single_refund_position_with_deps(&deps, input).await
        })
        .await
    }

    /// Scar tissue: §9 refund tree and §16.1 refund materialisation.
    #[activity]
    pub async fn materialize_refund_plan(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MaterializeRefundPlanInput,
    ) -> Result<RefundPlanShape, ActivityError> {
        record_activity("materialize_refund_plan", async move {
            let deps = self.deps()?;
            if input.position.position_kind == RecoverablePositionKind::ExternalCustody {
                return materialize_external_custody_refund_plan(&deps, input).await;
            }
            if input.position.position_kind == RecoverablePositionKind::HyperliquidSpot {
                return materialize_hyperliquid_spot_refund_plan(&deps, input).await;
            }
            let order = deps
                .db
                .orders()
                .get(input.order_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            let Some(funding_vault_id) = order.funding_vault_id else {
                return Ok(refund_plan_untenable(
                    RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
                ));
            };
            let vault = match deps.db.vaults().get(funding_vault_id).await {
                Ok(vault) => vault,
                Err(RouterCoreError::NotFound) => {
                    return Ok(refund_plan_untenable(
                        RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
                    ));
                }
                Err(source) => return Err(OrderActivityError::db_query(source)),
            };
            let amount = deps
                .custody_action_executor
                .deposit_vault_balance_raw(&vault)
                .await
                .map_err(|source| {
                    custody_action_error("read funding vault refund balance", source)
                })?;
            if !raw_amount_is_positive(&amount, "funding vault refund balance")? {
                return Ok(refund_plan_untenable(
                    RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
                ));
            }
            let record = deps
                .db
                .orders()
                .create_refund_attempt_from_funding_vault(
                    input.order_id.inner(),
                    input.failed_attempt_id.inner(),
                    FundingVaultRefundAttemptPlan {
                        funding_vault_id,
                        amount,
                        failure_reason: json!({
                            "reason": "primary_execution_attempts_exhausted",
                            "trace": "refund_workflow",
                            "failed_attempt_id": input.failed_attempt_id.inner(),
                        }),
                        input_custody_snapshot: json!({
                            "schema_version": 1,
                            "source_kind": "funding_vault",
                            "funding_vault_id": funding_vault_id,
                        }),
                    },
                    Utc::now(),
                )
                .await
                .map_err(OrderActivityError::db_query)?;
            tracing::info!(
                order_id = %record.order.id,
                attempt_id = %record.attempt.id,
                step_count = record.steps.len(),
                event_name = "order.refund_plan_materialized",
                "order.refund_plan_materialized"
            );
            telemetry::record_refund_attempt_materialized("funding_vault", "direct_internal");
            Ok(RefundPlanShape {
                outcome: RefundPlanOutcome::Materialized {
                    refund_attempt_id: record.attempt.id.into(),
                    steps: record
                        .steps
                        .into_iter()
                        .map(|step| WorkflowExecutionStep {
                            step_id: step.id.into(),
                            step_index: step.step_index,
                            step_type: step.step_type,
                        })
                        .collect(),
                },
            })
        })
        .await
    }
}

pub(super) async fn discover_single_refund_position_with_deps(
    deps: &OrderActivityDeps,
    input: DiscoverSingleRefundPositionInput,
) -> Result<SingleRefundPositionDiscovery, OrderActivityError> {
    let failed_attempt = deps
        .db
        .orders()
        .get_execution_attempt(input.failed_attempt_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?;
    if failed_attempt.order_id != input.order_id.inner() {
        return Err(invariant_error(
            "refund_failed_attempt_belongs_to_order",
            format!(
                "refund failed attempt {} does not belong to order {}",
                failed_attempt.id,
                input.order_id.inner()
            ),
        ));
    }
    let order = deps
        .db
        .orders()
        .get(input.order_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?;

    let mut positions = Vec::new();
    if let Some(funding_vault_id) = order.funding_vault_id {
        let vault = match deps.db.vaults().get(funding_vault_id).await {
            Ok(vault) => Some(vault),
            Err(RouterCoreError::NotFound) => None,
            Err(source) => return Err(OrderActivityError::db_query(source)),
        };
        if let Some(vault) = vault {
            let amount = deps
                .custody_action_executor
                .deposit_vault_balance_raw(&vault)
                .await
                .map_err(|source| {
                    custody_action_error("read funding vault refund balance", source)
                })?;
            if raw_amount_is_positive(&amount, "funding vault refund balance")? {
                positions.push(SingleRefundPosition {
                    position_kind: RecoverablePositionKind::FundingVault,
                    owning_step_id: None,
                    funding_vault_id: Some(funding_vault_id.into()),
                    custody_vault_id: None,
                    asset: vault.deposit_asset,
                    amount: RawAmount::new(amount).map_err(|source| {
                        amount_parse_error("funding vault refund balance", source)
                    })?,
                    hyperliquid_coin: None,
                    hyperliquid_canonical: None,
                    requires_clearinghouse_unwrap: false,
                });
            }
        }
    }

    let custody_vaults = deps
        .db
        .orders()
        .get_custody_vaults(order.id)
        .await
        .map_err(OrderActivityError::db_query)?;
    let mut source_deposit_vault = None;
    for vault in custody_vaults {
        match vault.role {
            CustodyVaultRole::SourceDeposit => {
                if source_deposit_vault.is_none() {
                    source_deposit_vault = Some(vault);
                }
            }
            CustodyVaultRole::DestinationExecution => {
                if let Some(asset_id) = vault.asset.clone() {
                    let amount = deps
                        .custody_action_executor
                        .custody_vault_balance_raw(&vault)
                        .await
                        .map_err(|source| {
                            custody_action_error("read external custody refund balance", source)
                        })?;
                    if raw_amount_is_positive(&amount, "external custody refund balance")? {
                        positions.push(SingleRefundPosition {
                            position_kind: RecoverablePositionKind::ExternalCustody,
                            owning_step_id: None,
                            funding_vault_id: None,
                            custody_vault_id: Some(vault.id.into()),
                            asset: DepositAsset {
                                chain: vault.chain.clone(),
                                asset: asset_id,
                            },
                            amount: RawAmount::new(amount).map_err(|source| {
                                amount_parse_error("external custody refund balance", source)
                            })?,
                            hyperliquid_coin: None,
                            hyperliquid_canonical: None,
                            requires_clearinghouse_unwrap: false,
                        });
                    }
                }
                positions.extend(
                    hyperliquid_spot_refund_positions_for_vault(
                        deps,
                        &order,
                        &vault,
                        Some(order.source_asset.clone()),
                    )
                    .await?,
                );
            }
            CustodyVaultRole::HyperliquidSpot => {
                positions.extend(
                    hyperliquid_spot_refund_positions_for_vault(deps, &order, &vault, None).await?,
                );
            }
        }
    }

    if let Some(source_deposit_vault) = source_deposit_vault {
        positions.extend(
            hyperliquid_spot_refund_positions_for_vault(
                deps,
                &order,
                &source_deposit_vault,
                Some(order.source_asset.clone()),
            )
            .await?,
        );
    }

    Ok(refund_position_discovery_from_positions(
        input.order_id.inner(),
        positions,
    ))
}

async fn hyperliquid_spot_refund_positions_for_vault(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    position_asset: Option<DepositAsset>,
) -> Result<Vec<SingleRefundPosition>, OrderActivityError> {
    let balances = deps
        .custody_action_executor
        .inspect_hyperliquid_spot_balances(vault)
        .await
        .map_err(|source| {
            custody_action_error("inspect hyperliquid spot refund balances", source)
        })?;
    let registry = deps.action_providers.asset_registry();
    let mut positions = Vec::new();
    for balance in balances {
        let Some((canonical, registered_asset)) =
            registry.hyperliquid_coin_asset(&balance.coin, Some(&order.source_asset.chain))
        else {
            continue;
        };
        let decimals = registry
            .chain_asset(&registered_asset)
            .ok_or_else(|| {
                refund_discovery_error(format!(
                    "missing registered decimals for Hyperliquid refund asset {} {}",
                    registered_asset.chain, registered_asset.asset
                ))
            })?
            .decimals;
        let Some(amount) =
            hyperliquid_refund_balance_amount_raw(&balance.total, &balance.hold, decimals)?
        else {
            continue;
        };
        positions.push(SingleRefundPosition {
            position_kind: RecoverablePositionKind::HyperliquidSpot,
            owning_step_id: None,
            funding_vault_id: None,
            custody_vault_id: Some(vault.id.into()),
            asset: position_asset.clone().unwrap_or(registered_asset),
            amount: RawAmount::new(amount)
                .map_err(|source| amount_parse_error("hyperliquid spot refund balance", source))?,
            hyperliquid_coin: Some(balance.coin),
            hyperliquid_canonical: Some(canonical),
            requires_clearinghouse_unwrap: false,
        });
    }
    let clearinghouse_amount = deps
        .custody_action_executor
        .inspect_hyperliquid_clearinghouse_balance(vault)
        .await
        .map_err(|source| {
            custody_action_error("inspect hyperliquid clearinghouse refund balance", source)
        })?;
    if !clearinghouse_amount.is_zero() {
        positions.push(SingleRefundPosition {
            position_kind: RecoverablePositionKind::HyperliquidSpot,
            owning_step_id: None,
            funding_vault_id: None,
            custody_vault_id: Some(vault.id.into()),
            asset: position_asset.unwrap_or_else(|| order.source_asset.clone()),
            amount: RawAmount::new(clearinghouse_amount.to_string()).map_err(|source| {
                amount_parse_error("hyperliquid clearinghouse refund balance", source)
            })?,
            hyperliquid_coin: Some("USDC".to_string()),
            hyperliquid_canonical: Some(CanonicalAsset::Usdc),
            requires_clearinghouse_unwrap: true,
        });
    }
    Ok(positions)
}

pub(super) fn refund_position_discovery_from_positions(
    order_id: Uuid,
    mut positions: Vec<SingleRefundPosition>,
) -> SingleRefundPositionDiscovery {
    if positions.is_empty() {
        return refund_single_position_untenable(0, 0);
    }

    let selected_index = positions
        .iter()
        .enumerate()
        .min_by_key(|(_, position)| refund_position_priority(position.position_kind))
        .map(|(index, _)| index)
        .expect("positions is not empty");
    let selected = positions.remove(selected_index);

    for dropped in &positions {
        let dropped_position_kind = recoverable_position_kind_label(dropped.position_kind);
        tracing::warn!(
            order_id = %order_id,
            selected_position_kind = recoverable_position_kind_label(selected.position_kind),
            dropped_position_kind,
            dropped_amount = %dropped.amount,
            event_name = "order.refund_position_dropped",
            "order.refund_position_dropped"
        );
        telemetry::record_refund_positions_dropped(dropped_position_kind);
    }

    SingleRefundPositionDiscovery {
        outcome: SingleRefundPositionOutcome::Position(selected),
    }
}

fn refund_position_priority(position_kind: RecoverablePositionKind) -> u8 {
    match position_kind {
        RecoverablePositionKind::FundingVault => 0,
        RecoverablePositionKind::ExternalCustody => 1,
        RecoverablePositionKind::HyperliquidSpot => 2,
    }
}

pub(super) fn refund_single_position_untenable(
    position_count: usize,
    recoverable_position_count: usize,
) -> SingleRefundPositionDiscovery {
    telemetry::record_refund_position_untenable(
        RefundUntenableReason::RefundRequiresSingleRecoverablePosition {
            position_count,
            recoverable_position_count,
        }
        .reason_str(),
    );
    SingleRefundPositionDiscovery {
        outcome: SingleRefundPositionOutcome::Untenable {
            reason: RefundUntenableReason::RefundRequiresSingleRecoverablePosition {
                position_count,
                recoverable_position_count,
            },
        },
    }
}

pub(super) fn refund_plan_untenable(reason: RefundUntenableReason) -> RefundPlanShape {
    telemetry::record_refund_position_untenable(reason.reason_str());
    RefundPlanShape {
        outcome: RefundPlanOutcome::Untenable { reason },
    }
}

pub(super) fn recoverable_position_kind_label(
    position_kind: RecoverablePositionKind,
) -> &'static str {
    match position_kind {
        RecoverablePositionKind::FundingVault => "funding_vault",
        RecoverablePositionKind::ExternalCustody => "external_custody",
        RecoverablePositionKind::HyperliquidSpot => "hyperliquid_spot",
    }
}

pub(super) fn raw_amount_is_positive(
    value: &str,
    label: &'static str,
) -> Result<bool, OrderActivityError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || !trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(amount_parse_error(
            label,
            format!("{label} is not a raw unsigned integer: {value:?}"),
        ));
    }
    Ok(trimmed.as_bytes().iter().any(|digit| *digit != b'0'))
}

pub(super) async fn materialize_external_custody_refund_plan(
    deps: &OrderActivityDeps,
    input: MaterializeRefundPlanInput,
) -> Result<RefundPlanShape, OrderActivityError> {
    let Some(source) = load_external_custody_refund_source(deps, &input).await? else {
        return Ok(refund_plan_untenable(
            RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
        ));
    };

    let now = Utc::now();
    let maybe_plan = if source.asset == source.order.source_asset {
        Some(external_custody_direct_refund_steps(
            &source.order,
            &source.vault,
            &source.asset,
            &source.amount,
            now,
        ))
    } else {
        external_custody_refund_back_steps(
            deps,
            &source.order,
            &source.vault,
            &source.asset,
            &source.amount,
            now,
        )
        .await?
    };
    let Some((mut legs, mut steps)) = maybe_plan else {
        return Ok(refund_plan_untenable(
            RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
        ));
    };
    let first_transition_kind = legs
        .first()
        .and_then(|leg| leg.transition_decl_id.as_deref())
        .unwrap_or("direct_internal")
        .to_string();
    steps = hydrate_destination_execution_steps(deps, input.order_id.inner(), steps).await?;
    apply_execution_leg_usd_valuations(deps, &mut legs).await;
    let (refund_attempt_id, steps) =
        create_external_custody_refund_attempt(deps, &input, &source, legs, steps, now).await?;
    telemetry::record_refund_attempt_materialized(
        recoverable_position_kind_label(input.position.position_kind),
        &first_transition_kind,
    );
    Ok(materialized_refund_plan_shape(refund_attempt_id, steps))
}

struct ExternalCustodyRefundSource {
    order: RouterOrder,
    vault: CustodyVault,
    asset: DepositAsset,
    amount: String,
}

async fn load_external_custody_refund_source(
    deps: &OrderActivityDeps,
    input: &MaterializeRefundPlanInput,
) -> Result<Option<ExternalCustodyRefundSource>, OrderActivityError> {
    let order = deps
        .db
        .orders()
        .get(input.order_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?;
    let custody_vault_id = input.position.custody_vault_id.ok_or_else(|| {
        refund_materialization_error("external-custody refund position is missing custody_vault_id")
    })?;
    let vault = match deps
        .db
        .orders()
        .get_custody_vault(custody_vault_id.inner())
        .await
    {
        Ok(vault) => vault,
        Err(RouterCoreError::NotFound) => return Ok(None),
        Err(source) => return Err(OrderActivityError::db_query(source)),
    };
    if vault.order_id != Some(input.order_id.inner()) {
        return Err(invariant_error(
            "external_custody_vault_belongs_to_order",
            format!(
                "external-custody vault {} does not belong to order {}",
                vault.id,
                input.order_id.inner()
            ),
        ));
    }
    let Some(asset_id) = vault.asset.clone() else {
        return Ok(None);
    };
    let asset = DepositAsset {
        chain: vault.chain.clone(),
        asset: asset_id,
    };
    let amount = deps
        .custody_action_executor
        .custody_vault_balance_raw(&vault)
        .await
        .map_err(|source| custody_action_error("read external custody refund balance", source))?;
    if !raw_amount_is_positive(&amount, "external custody refund balance")? {
        return Ok(None);
    }
    Ok(Some(ExternalCustodyRefundSource {
        order,
        vault,
        asset,
        amount,
    }))
}

async fn create_external_custody_refund_attempt(
    deps: &OrderActivityDeps,
    input: &MaterializeRefundPlanInput,
    source: &ExternalCustodyRefundSource,
    legs: Vec<OrderExecutionLeg>,
    steps: Vec<OrderExecutionStep>,
    now: chrono::DateTime<Utc>,
) -> Result<(Uuid, Vec<OrderExecutionStep>), OrderActivityError> {
    let record = deps
        .db
        .orders()
        .create_refund_attempt_from_external_custody(
            input.order_id.inner(),
            input.failed_attempt_id.inner(),
            ExternalCustodyRefundAttemptPlan {
                source_custody_vault_id: source.vault.id,
                legs,
                steps,
                failure_reason: json!({
                    "reason": "primary_execution_attempts_exhausted",
                    "trace": "refund_workflow",
                    "failed_attempt_id": input.failed_attempt_id.inner(),
                }),
                input_custody_snapshot: json!({
                    "schema_version": 1,
                    "source_kind": "external_custody",
                    "custody_vault_id": source.vault.id,
                    "custody_vault_role": source.vault.role.to_db_string(),
                    "source_asset": {
                        "chain": source.asset.chain.as_str(),
                        "asset": source.asset.asset.as_str(),
                    },
                    "amount": source.amount,
                }),
            },
            now,
        )
        .await
        .map_err(OrderActivityError::db_query)?;
    tracing::info!(
        order_id = %record.order.id,
        attempt_id = %record.attempt.id,
        step_count = record.steps.len(),
        event_name = "order.refund_plan_materialized",
        "order.refund_plan_materialized"
    );
    Ok((record.attempt.id, record.steps))
}

pub(super) async fn materialize_hyperliquid_spot_refund_plan(
    deps: &OrderActivityDeps,
    input: MaterializeRefundPlanInput,
) -> Result<RefundPlanShape, OrderActivityError> {
    let Some(source) = load_hyperliquid_spot_refund_source(deps, &input).await? else {
        return Ok(refund_plan_untenable(
            RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
        ));
    };

    let now = Utc::now();
    let Some((mut legs, steps)) = hyperliquid_spot_refund_back_steps(
        deps,
        &source.order,
        &source.vault,
        source.canonical,
        &source.amount,
        source.requires_clearinghouse_unwrap,
        now,
    )
    .await?
    else {
        return Ok(refund_plan_untenable(
            RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
        ));
    };
    let first_transition_kind = legs
        .first()
        .and_then(|leg| leg.transition_decl_id.as_deref())
        .unwrap_or("unknown")
        .to_string();
    apply_execution_leg_usd_valuations(deps, &mut legs).await;
    let (refund_attempt_id, steps) =
        create_hyperliquid_spot_refund_attempt(deps, &input, &source, legs, steps, now).await?;
    telemetry::record_refund_attempt_materialized(
        recoverable_position_kind_label(input.position.position_kind),
        &first_transition_kind,
    );
    Ok(materialized_refund_plan_shape(refund_attempt_id, steps))
}

struct HyperliquidSpotRefundSource {
    order: RouterOrder,
    vault: CustodyVault,
    asset: DepositAsset,
    amount: String,
    coin: String,
    canonical: CanonicalAsset,
    requires_clearinghouse_unwrap: bool,
}

async fn load_hyperliquid_spot_refund_source(
    deps: &OrderActivityDeps,
    input: &MaterializeRefundPlanInput,
) -> Result<Option<HyperliquidSpotRefundSource>, OrderActivityError> {
    let order = deps
        .db
        .orders()
        .get(input.order_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?;
    let custody_vault_id = input.position.custody_vault_id.ok_or_else(|| {
        refund_materialization_error("HyperliquidSpot refund position is missing custody_vault_id")
    })?;
    let vault = match deps
        .db
        .orders()
        .get_custody_vault(custody_vault_id.inner())
        .await
    {
        Ok(vault) => vault,
        Err(RouterCoreError::NotFound) => return Ok(None),
        Err(source) => return Err(OrderActivityError::db_query(source)),
    };
    validate_hyperliquid_spot_refund_vault(input, &vault)?;
    let coin = input.position.hyperliquid_coin.clone().ok_or_else(|| {
        refund_materialization_error("HyperliquidSpot refund position is missing coin")
    })?;
    let canonical = input.position.hyperliquid_canonical.ok_or_else(|| {
        refund_materialization_error("HyperliquidSpot refund position is missing canonical asset")
    })?;
    let (current_canonical, asset, amount) = if input.position.requires_clearinghouse_unwrap {
        if canonical != CanonicalAsset::Usdc || !coin.eq_ignore_ascii_case("USDC") {
            return Err(invariant_error(
                "hyperliquid_clearinghouse_refund_usdc_only",
                format!(
                    "Hyperliquid clearinghouse refund position must be USDC, got coin {coin} canonical {}",
                    canonical.as_str()
                ),
            ));
        }
        let Some(amount) = current_hyperliquid_clearinghouse_refund_balance(deps, &vault).await?
        else {
            return Ok(None);
        };
        (CanonicalAsset::Usdc, input.position.asset.clone(), amount)
    } else {
        let Some((current_canonical, asset, amount)) =
            current_hyperliquid_spot_refund_balance(deps, &order, &vault, &coin).await?
        else {
            return Ok(None);
        };
        (current_canonical, asset, amount)
    };
    if current_canonical != canonical {
        return Err(invariant_error(
            "hyperliquid_spot_refund_coin_canonical_stable",
            format!(
                "HyperliquidSpot refund coin {coin} canonical changed from {} to {}",
                canonical.as_str(),
                current_canonical.as_str()
            ),
        ));
    }
    Ok(Some(HyperliquidSpotRefundSource {
        order,
        vault,
        asset,
        amount,
        coin,
        canonical,
        requires_clearinghouse_unwrap: input.position.requires_clearinghouse_unwrap,
    }))
}

fn validate_hyperliquid_spot_refund_vault(
    input: &MaterializeRefundPlanInput,
    vault: &CustodyVault,
) -> Result<(), OrderActivityError> {
    if vault.order_id != Some(input.order_id.inner()) {
        return Err(invariant_error(
            "hyperliquid_spot_vault_belongs_to_order",
            format!(
                "HyperliquidSpot vault {} does not belong to order {}",
                vault.id,
                input.order_id.inner()
            ),
        ));
    }
    if !matches!(
        vault.role,
        CustodyVaultRole::HyperliquidSpot | CustodyVaultRole::SourceDeposit
    ) {
        return Err(invariant_error(
            "hyperliquid_spot_vault_role",
            format!(
                "custody vault {} is {}, not hyperliquid_spot or source_deposit",
                vault.id,
                vault.role.to_db_string()
            ),
        ));
    }
    Ok(())
}

async fn create_hyperliquid_spot_refund_attempt(
    deps: &OrderActivityDeps,
    input: &MaterializeRefundPlanInput,
    source: &HyperliquidSpotRefundSource,
    legs: Vec<OrderExecutionLeg>,
    steps: Vec<OrderExecutionStep>,
    now: chrono::DateTime<Utc>,
) -> Result<(Uuid, Vec<OrderExecutionStep>), OrderActivityError> {
    let record = deps
        .db
        .orders()
        .create_refund_attempt_from_hyperliquid_spot(
            input.order_id.inner(),
            input.failed_attempt_id.inner(),
            HyperliquidSpotRefundAttemptPlan {
                source_custody_vault_id: source.vault.id,
                legs,
                steps,
                failure_reason: json!({
                    "reason": "primary_execution_attempts_exhausted",
                    "trace": "refund_workflow",
                    "failed_attempt_id": input.failed_attempt_id.inner(),
                }),
                input_custody_snapshot: json!({
                    "schema_version": 1,
                    "source_kind": "hyperliquid_spot",
                    "custody_vault_id": source.vault.id,
                    "custody_vault_role": source.vault.role.to_db_string(),
                    "source_asset": {
                        "chain": source.asset.chain.as_str(),
                        "asset": source.asset.asset.as_str(),
                    },
                    "hyperliquid_coin": source.coin,
                    "hyperliquid_canonical": source.canonical.as_str(),
                    "requires_clearinghouse_unwrap": source.requires_clearinghouse_unwrap,
                    "amount": source.amount,
                }),
            },
            now,
        )
        .await
        .map_err(OrderActivityError::db_query)?;
    tracing::info!(
        order_id = %record.order.id,
        attempt_id = %record.attempt.id,
        step_count = record.steps.len(),
        event_name = "order.refund_plan_materialized",
        "order.refund_plan_materialized"
    );
    Ok((record.attempt.id, record.steps))
}

fn materialized_refund_plan_shape(
    refund_attempt_id: Uuid,
    steps: Vec<OrderExecutionStep>,
) -> RefundPlanShape {
    RefundPlanShape {
        outcome: RefundPlanOutcome::Materialized {
            refund_attempt_id: refund_attempt_id.into(),
            steps: steps
                .into_iter()
                .map(|step| WorkflowExecutionStep {
                    step_id: step.id.into(),
                    step_index: step.step_index,
                    step_type: step.step_type,
                })
                .collect(),
        },
    }
}

pub(super) async fn current_hyperliquid_spot_refund_balance(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    coin: &str,
) -> Result<Option<(CanonicalAsset, DepositAsset, String)>, OrderActivityError> {
    let balances = deps
        .custody_action_executor
        .inspect_hyperliquid_spot_balances(vault)
        .await
        .map_err(|source| {
            custody_action_error("inspecting HyperliquidSpot refund balance", source)
        })?;
    let Some(balance) = balances.into_iter().find(|balance| balance.coin == coin) else {
        return Ok(None);
    };
    let registry = deps.action_providers.asset_registry();
    let Some((canonical, asset)) =
        registry.hyperliquid_coin_asset(&balance.coin, Some(&order.source_asset.chain))
    else {
        return Ok(None);
    };
    let decimals = registry
        .chain_asset(&asset)
        .ok_or_else(|| {
            refund_discovery_error(format!(
                "missing registered decimals for Hyperliquid refund asset {} {}",
                asset.chain, asset.asset
            ))
        })?
        .decimals;
    let Some(amount) =
        hyperliquid_refund_balance_amount_raw(&balance.total, &balance.hold, decimals)?
    else {
        return Ok(None);
    };
    Ok(Some((canonical, asset, amount)))
}

pub(super) async fn current_hyperliquid_clearinghouse_refund_balance(
    deps: &OrderActivityDeps,
    vault: &CustodyVault,
) -> Result<Option<String>, OrderActivityError> {
    let amount = deps
        .custody_action_executor
        .inspect_hyperliquid_clearinghouse_balance(vault)
        .await
        .map_err(|source| {
            custody_action_error(
                "inspecting Hyperliquid clearinghouse refund balance",
                source,
            )
        })?;
    if amount.is_zero() {
        return Ok(None);
    }
    Ok(Some(amount.to_string()))
}

pub(super) async fn hyperliquid_spot_refund_back_steps(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    canonical: CanonicalAsset,
    amount: &str,
    requires_clearinghouse_unwrap: bool,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Option<(Vec<OrderExecutionLeg>, Vec<OrderExecutionStep>)>, OrderActivityError> {
    let Some(quoted_path) =
        best_hyperliquid_spot_refund_quote(deps, order, canonical, amount).await?
    else {
        tracing::warn!(
            order_id = %order.id,
            source_asset_chain = %order.source_asset.chain,
            source_asset = %order.source_asset.asset,
            amount,
            event_name = "order.refund_plan_unavailable",
            "no PR6b7a HyperliquidSpot refund path is available"
        );
        return Ok(None);
    };

    materialize_hyperliquid_spot_refund_path(
        order,
        vault,
        &quoted_path,
        amount,
        requires_clearinghouse_unwrap,
        planned_at,
    )
    .map(Some)
}

pub(super) async fn best_hyperliquid_spot_refund_quote(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    canonical: CanonicalAsset,
    amount: &str,
) -> Result<Option<RefundQuotedPath>, OrderActivityError> {
    let start = MarketOrderNode::Venue {
        provider: ProviderId::Hyperliquid,
        canonical,
    };
    let goal = MarketOrderNode::External(order.source_asset.clone());
    let mut paths = deps
        .action_providers
        .asset_registry()
        .select_transition_paths_between(start, goal, REFUND_PATH_MAX_DEPTH)
        .into_iter()
        .filter(|path| {
            refund_path_compatible_with_position(
                RecoverablePositionKind::HyperliquidSpot,
                None,
                path,
            )
        })
        .collect::<Vec<_>>();
    paths.sort_by(|left, right| {
        left.transitions
            .len()
            .cmp(&right.transitions.len())
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut best = None;
    for path in paths {
        let path_id = path.id.clone();
        match quote_hyperliquid_spot_refund_path(deps, order, amount, path).await {
            Ok(Some(quoted)) => {
                best = choose_better_refund_quote(quoted, best)?;
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    order_id = %order.id,
                    path_id,
                    error = ?err,
                    "HyperliquidSpot refund transition-path quote failed"
                );
            }
        }
    }
    Ok(best)
}

pub(super) async fn quote_hyperliquid_spot_refund_path(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    amount: &str,
    path: TransitionPath,
) -> Result<Option<RefundQuotedPath>, OrderActivityError> {
    let mut cursor_amount = amount.to_string();
    let mut legs = Vec::new();

    for transition in &path.transitions {
        let Some(quote) =
            quote_hyperliquid_spot_refund_transition(deps, order, transition, &cursor_amount)
                .await?
        else {
            return Ok(None);
        };
        cursor_amount = quote.amount_out;
        legs.extend(quote.legs);
    }

    Ok(Some(RefundQuotedPath {
        path,
        amount_out: cursor_amount,
        legs,
    }))
}

async fn quote_hyperliquid_spot_refund_transition(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    transition: &TransitionDecl,
    cursor_amount: &str,
) -> Result<Option<RefundTransitionQuote>, OrderActivityError> {
    match transition.kind {
        MarketOrderTransitionKind::HyperliquidTrade => {
            refund_hyperliquid_trade_quote_transition(deps, order, transition, cursor_amount).await
        }
        MarketOrderTransitionKind::UnitWithdrawal => {
            refund_unit_withdrawal_quote_transition(deps, order, transition, cursor_amount)
        }
        MarketOrderTransitionKind::AcrossBridge
        | MarketOrderTransitionKind::CctpBridge
        | MarketOrderTransitionKind::HyperliquidBridgeDeposit
        | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
            refund_bridge_quote_transition(
                deps,
                order,
                transition,
                cursor_amount,
                &order.refund_address,
            )
            .await
        }
        MarketOrderTransitionKind::UniversalRouterSwap => {
            refund_universal_router_quote_transition(deps, order, transition, cursor_amount, None)
                .await
        }
        MarketOrderTransitionKind::UnitDeposit => {
            refund_unit_deposit_quote_transition(deps, transition, cursor_amount)
        }
    }
}

pub(super) fn refund_path_compatible_with_position(
    position_kind: RecoverablePositionKind,
    external_vault_role: Option<CustodyVaultRole>,
    path: &TransitionPath,
) -> bool {
    let Some(first) = path.transitions.first() else {
        return false;
    };
    match position_kind {
        RecoverablePositionKind::FundingVault => false,
        RecoverablePositionKind::ExternalCustody => match first.kind {
            MarketOrderTransitionKind::AcrossBridge | MarketOrderTransitionKind::CctpBridge => true,
            MarketOrderTransitionKind::UniversalRouterSwap => {
                external_custody_universal_router_refund_is_supported_first_hop(path)
            }
            MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
                external_vault_role == Some(CustodyVaultRole::DestinationExecution)
            }
            MarketOrderTransitionKind::UnitDeposit => true,
            MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => false,
            _ => false,
        },
        RecoverablePositionKind::HyperliquidSpot => matches!(
            first.kind,
            MarketOrderTransitionKind::HyperliquidTrade
                | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal
                | MarketOrderTransitionKind::UnitWithdrawal
        ),
    }
}

pub(super) fn external_custody_universal_router_refund_is_supported_first_hop(
    path: &TransitionPath,
) -> bool {
    let Some(transition) = path.transitions.first() else {
        return false;
    };
    matches!(transition.from, MarketOrderNode::External(_))
        && matches!(transition.to, MarketOrderNode::External(_))
        && transition.input.asset.chain == transition.output.asset.chain
        && transition.input.asset.asset != transition.output.asset.asset
        && transition.input.asset.chain.evm_chain_id().is_some()
}

pub(super) fn materialize_hyperliquid_spot_refund_path(
    order: &RouterOrder,
    vault: &CustodyVault,
    quoted_path: &RefundQuotedPath,
    source_amount: &str,
    requires_clearinghouse_unwrap: bool,
    planned_at: chrono::DateTime<Utc>,
) -> Result<(Vec<OrderExecutionLeg>, Vec<OrderExecutionStep>), OrderActivityError> {
    let mut execution_legs = Vec::new();
    let mut steps = Vec::new();
    let mut step_index = 0_i32;
    let mut leg_index = 0_i32;

    if requires_clearinghouse_unwrap {
        let (leg, step) =
            hyperliquid_clearinghouse_to_spot_refund_prep(order, vault, source_amount, planned_at)?;
        execution_legs.push(leg);
        steps.push(step);
        step_index = 1;
        leg_index = 1;
    }

    for (transition_index, transition) in quoted_path.path.transitions.iter().enumerate() {
        let is_final = transition_index + 1 == quoted_path.path.transitions.len();
        let transition_legs = refund_legs_for_transition(&quoted_path.legs, transition);
        if transition_legs.is_empty() {
            return Err(refund_materialization_error(format!(
                "quoted HyperliquidSpot refund path is missing legs for transition {}",
                transition.id
            )));
        }
        let transition_steps = materialize_hyperliquid_spot_refund_transition(
            order,
            vault,
            quoted_path,
            transition,
            step_index,
            is_final,
            planned_at,
        )?;
        append_refund_transition_plan(
            order,
            transition,
            transition_legs,
            transition_steps,
            leg_index,
            planned_at,
            &mut execution_legs,
            &mut steps,
        )?;
        leg_index = leg_index
            .checked_add(1)
            .ok_or_else(|| refund_materialization_error("refund leg_index overflow"))?;
        step_index = i32::try_from(steps.len()).map_err(|err| {
            refund_materialization_error(format!("refund step_index overflow: {err}"))
        })?;
    }

    Ok((execution_legs, steps))
}

fn hyperliquid_clearinghouse_to_spot_refund_prep(
    order: &RouterOrder,
    vault: &CustodyVault,
    amount: &str,
    planned_at: chrono::DateTime<Utc>,
) -> Result<(OrderExecutionLeg, OrderExecutionStep), OrderActivityError> {
    let hyperliquid_usdc = DepositAsset {
        chain: ChainId::parse("hyperliquid").map_err(refund_materialization_error)?,
        asset: AssetId::Native,
    };
    let leg_id = Uuid::now_v7();
    let leg = OrderExecutionLeg {
        id: leg_id,
        order_id: order.id,
        execution_attempt_id: None,
        transition_decl_id: None,
        leg_index: 0,
        leg_type: OrderExecutionStepType::HyperliquidClearinghouseToSpot
            .to_db_string()
            .to_string(),
        provider: ProviderId::Hyperliquid.as_str().to_string(),
        status: OrderExecutionStepStatus::Planned,
        input_asset: hyperliquid_usdc.clone(),
        output_asset: hyperliquid_usdc.clone(),
        amount_in: amount.to_string(),
        expected_amount_out: amount.to_string(),
        min_amount_out: Some(amount.to_string()),
        actual_amount_in: None,
        actual_amount_out: None,
        started_at: None,
        completed_at: None,
        provider_quote_expires_at: None,
        details: json!({
            "schema_version": 1,
            "refund_kind": "hyperliquid_clearinghouse_unwrap",
            "action_step_types": [OrderExecutionStepType::HyperliquidClearinghouseToSpot.to_db_string()],
        }),
        usd_valuation: json!({}),
        created_at: planned_at,
        updated_at: planned_at,
    };
    let step = OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id: order.id,
        execution_attempt_id: None,
        execution_leg_id: Some(leg_id),
        transition_decl_id: None,
        step_index: 0,
        step_type: OrderExecutionStepType::HyperliquidClearinghouseToSpot,
        provider: ProviderId::Hyperliquid.as_str().to_string(),
        status: OrderExecutionStepStatus::Planned,
        input_asset: Some(hyperliquid_usdc.clone()),
        output_asset: Some(hyperliquid_usdc),
        amount_in: Some(amount.to_string()),
        min_amount_out: Some(amount.to_string()),
        tx_hash: None,
        provider_ref: None,
        idempotency_key: None,
        attempt_count: 0,
        next_attempt_at: None,
        started_at: None,
        completed_at: None,
        request: json!({
            "order_id": order.id,
            "amount": amount,
            "hyperliquid_custody_vault_id": vault.id,
            "hyperliquid_custody_vault_address": vault.address,
        }),
        details: json!({
            "schema_version": 1,
            "refund_kind": "hyperliquid_clearinghouse_unwrap",
            "hyperliquid_custody_vault_id": vault.id,
            "hyperliquid_custody_vault_role": vault.role.to_db_string(),
            "hyperliquid_custody_vault_status": "bound",
        }),
        response: json!({}),
        error: json!({}),
        usd_valuation: json!({}),
        created_at: planned_at,
        updated_at: planned_at,
    };
    Ok((leg, step))
}

fn materialize_hyperliquid_spot_refund_transition(
    order: &RouterOrder,
    vault: &CustodyVault,
    quoted_path: &RefundQuotedPath,
    transition: &TransitionDecl,
    step_index: i32,
    is_final: bool,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Vec<OrderExecutionStep>, OrderActivityError> {
    match transition.kind {
        MarketOrderTransitionKind::HyperliquidTrade => materialize_hyperliquid_spot_trade_steps(
            order,
            vault,
            quoted_path,
            transition,
            step_index,
            planned_at,
        ),
        MarketOrderTransitionKind::UnitWithdrawal => {
            let leg = hyperliquid_spot_refund_leg(
                quoted_path,
                transition,
                OrderExecutionStepType::UnitWithdrawal,
            )?;
            let custody = explicit_hyperliquid_spot_refund_binding(vault);
            Ok(vec![refund_transition_unit_withdrawal_step(
                order, transition, &leg, &custody, is_final, step_index, planned_at,
            )?])
        }
        MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
            let leg = hyperliquid_spot_refund_leg(
                quoted_path,
                transition,
                OrderExecutionStepType::HyperliquidBridgeWithdrawal,
            )?;
            let custody = explicit_hyperliquid_spot_refund_binding(vault);
            Ok(vec![refund_transition_hyperliquid_bridge_withdrawal_step(
                order, transition, &leg, &custody, is_final, step_index, planned_at,
            )?])
        }
        MarketOrderTransitionKind::CctpBridge => materialize_derived_cctp_refund_transition(
            order,
            quoted_path,
            transition,
            step_index,
            is_final,
            planned_at,
        ),
        MarketOrderTransitionKind::AcrossBridge
        | MarketOrderTransitionKind::HyperliquidBridgeDeposit
        | MarketOrderTransitionKind::UniversalRouterSwap
        | MarketOrderTransitionKind::UnitDeposit => materialize_derived_external_refund_transition(
            order,
            quoted_path,
            transition,
            step_index,
            is_final,
            planned_at,
        ),
    }
}

fn materialize_hyperliquid_spot_trade_steps(
    order: &RouterOrder,
    vault: &CustodyVault,
    quoted_path: &RefundQuotedPath,
    transition: &TransitionDecl,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Vec<OrderExecutionStep>, OrderActivityError> {
    let transition_legs = refund_legs_for_transition(&quoted_path.legs, transition);
    if transition_legs.is_empty() {
        return Err(refund_materialization_error(format!(
            "quoted refund path is missing HyperliquidTrade legs for transition {}",
            transition.id
        )));
    }
    let custody = explicit_hyperliquid_spot_refund_binding(vault);
    materialize_refund_hyperliquid_trade_steps(
        order,
        transition,
        transition_legs,
        &custody,
        false,
        step_index,
        planned_at,
    )
}

fn materialize_derived_external_refund_transition(
    order: &RouterOrder,
    quoted_path: &RefundQuotedPath,
    transition: &TransitionDecl,
    step_index: i32,
    is_final: bool,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Vec<OrderExecutionStep>, OrderActivityError> {
    let source = RefundExternalSourceBinding::DerivedDestinationExecution;
    let step_type = execution_step_type_for_transition_kind(transition.kind);
    let leg = hyperliquid_spot_refund_leg(quoted_path, transition, step_type)?;
    let step = match transition.kind {
        MarketOrderTransitionKind::AcrossBridge => refund_transition_across_bridge_step(
            order, source, transition, &leg, is_final, step_index, planned_at,
        )?,
        MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
            refund_transition_hyperliquid_bridge_deposit_step(
                order, source, transition, &leg, step_index, planned_at,
            )?
        }
        MarketOrderTransitionKind::UniversalRouterSwap => {
            refund_transition_universal_router_swap_step(
                order, source, transition, &leg, is_final, step_index, planned_at,
            )?
        }
        MarketOrderTransitionKind::UnitDeposit => refund_transition_unit_deposit_step(
            order, source, transition, &leg, step_index, planned_at,
        )?,
        _ => {
            return Err(refund_materialization_error(format!(
                "unsupported derived external refund transition {}",
                transition.kind.as_str()
            )));
        }
    };
    Ok(vec![step])
}

fn materialize_derived_cctp_refund_transition(
    order: &RouterOrder,
    quoted_path: &RefundQuotedPath,
    transition: &TransitionDecl,
    step_index: i32,
    is_final: bool,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Vec<OrderExecutionStep>, OrderActivityError> {
    let burn_leg =
        hyperliquid_spot_refund_leg(quoted_path, transition, OrderExecutionStepType::CctpBurn)?;
    let receive_leg =
        hyperliquid_spot_refund_leg(quoted_path, transition, OrderExecutionStepType::CctpReceive)?;
    refund_transition_cctp_bridge_steps(
        order,
        RefundExternalSourceBinding::DerivedDestinationExecution,
        transition,
        &burn_leg,
        &receive_leg,
        is_final,
        step_index,
        planned_at,
    )
}

fn hyperliquid_spot_refund_leg(
    quoted_path: &RefundQuotedPath,
    transition: &TransitionDecl,
    step_type: OrderExecutionStepType,
) -> Result<QuoteLeg, OrderActivityError> {
    refund_take_one_leg(&quoted_path.legs, transition, step_type)
}

fn explicit_hyperliquid_spot_refund_binding(vault: &CustodyVault) -> RefundHyperliquidBinding {
    RefundHyperliquidBinding::Explicit {
        vault_id: vault.id,
        address: vault.address.clone(),
        role: vault.role,
        asset: None,
    }
}

pub(super) fn refund_unit_deposit_quote_leg(
    transition: &TransitionDecl,
    amount: &str,
    expires_at: chrono::DateTime<Utc>,
) -> QuoteLeg {
    QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: amount,
        amount_out: amount,
        expires_at,
        raw: json!({}),
    })
}

pub(super) fn refund_unit_withdrawal_quote_leg(
    order: &RouterOrder,
    transition: &TransitionDecl,
    amount: &str,
    expires_at: chrono::DateTime<Utc>,
) -> QuoteLeg {
    QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: amount,
        amount_out: amount,
        expires_at,
        raw: json!({
            "recipient_address": order.refund_address,
        }),
    })
}

pub(super) fn refund_unit_withdrawal_amount_meets_minimum(
    transition: &TransitionDecl,
    amount: &str,
) -> Result<bool, OrderActivityError> {
    let minimum_amount = unit_withdrawal_minimum_raw(&transition.output.asset).to_string();
    refund_amount_gte(amount, &minimum_amount)
}

#[derive(Debug, Clone)]
pub(super) enum RefundExternalSourceBinding {
    Explicit {
        vault_id: Uuid,
        address: String,
        role: CustodyVaultRole,
    },
    DerivedDestinationExecution,
}

pub(super) fn refund_external_source_binding(
    vault: &CustodyVault,
    transition_index: usize,
) -> RefundExternalSourceBinding {
    if transition_index == 0 {
        RefundExternalSourceBinding::Explicit {
            vault_id: vault.id,
            address: vault.address.clone(),
            role: vault.role,
        }
    } else {
        RefundExternalSourceBinding::DerivedDestinationExecution
    }
}

pub(super) fn external_refund_source_binding(
    vault: &CustodyVault,
    transition_index: usize,
) -> RefundExternalSourceBinding {
    refund_external_source_binding(vault, transition_index)
}

pub(super) fn external_custody_direct_refund_steps(
    order: &RouterOrder,
    vault: &CustodyVault,
    asset: &DepositAsset,
    amount: &str,
    planned_at: chrono::DateTime<Utc>,
) -> (Vec<OrderExecutionLeg>, Vec<OrderExecutionStep>) {
    let leg = OrderExecutionLeg {
        id: Uuid::now_v7(),
        order_id: order.id,
        execution_attempt_id: None,
        transition_decl_id: None,
        leg_index: 0,
        leg_type: OrderExecutionStepType::Refund.to_db_string().to_string(),
        provider: "internal".to_string(),
        status: OrderExecutionStepStatus::Planned,
        input_asset: asset.clone(),
        output_asset: asset.clone(),
        amount_in: amount.to_string(),
        expected_amount_out: amount.to_string(),
        min_amount_out: None,
        actual_amount_in: None,
        actual_amount_out: None,
        started_at: None,
        completed_at: None,
        provider_quote_expires_at: None,
        details: json!({
            "schema_version": 1,
            "refund_kind": "external_custody_direct_transfer",
            "quote_leg_count": 1,
            "action_step_types": [OrderExecutionStepType::Refund.to_db_string()],
        }),
        usd_valuation: json!({}),
        created_at: planned_at,
        updated_at: planned_at,
    };
    let step = OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id: order.id,
        execution_attempt_id: None,
        execution_leg_id: Some(leg.id),
        transition_decl_id: None,
        step_index: 0,
        step_type: OrderExecutionStepType::Refund,
        provider: "internal".to_string(),
        status: OrderExecutionStepStatus::Planned,
        input_asset: Some(asset.clone()),
        output_asset: Some(asset.clone()),
        amount_in: Some(amount.to_string()),
        min_amount_out: None,
        tx_hash: None,
        provider_ref: None,
        idempotency_key: Some(format!("order:{}:external-refund:0", order.id)),
        attempt_count: 0,
        next_attempt_at: None,
        started_at: None,
        completed_at: None,
        details: json!({
            "schema_version": 1,
            "refund_kind": "external_custody_direct_transfer",
            "source_custody_vault_id": vault.id,
            "source_custody_vault_role": vault.role.to_db_string(),
            "recipient_address": &order.refund_address,
        }),
        request: json!({
            "order_id": order.id,
            "source_custody_vault_id": vault.id,
            "recipient_address": &order.refund_address,
            "amount": amount,
        }),
        response: json!({}),
        error: json!({}),
        usd_valuation: json!({}),
        created_at: planned_at,
        updated_at: planned_at,
    };
    (vec![leg], vec![step])
}
