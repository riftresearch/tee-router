use super::*;

use chains::ChainRegistry;
use router_core::{
    config::Settings,
    models::{
        CustodyVaultControlType, CustodyVaultStatus, MarketOrderAction, RouterOrderAction,
        RouterOrderStatus, RouterOrderType,
    },
    services::{
        asset_registry::{AssetSlot, RequiredCustodyRole},
        custody_action_executor::HyperliquidCallNetwork,
        ActionProviderHttpOptions,
    },
};
use sqlx_postgres::PgPool;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

const POSTGRES_PORT: u16 = 5432;
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "password";
const POSTGRES_DATABASE: &str = "postgres";

struct TestDatabase {
    _container: ContainerAsync<GenericImage>,
    db: Database,
    pool: PgPool,
}

struct SeededRunningStep {
    order_id: Uuid,
    attempt_id: Uuid,
    step_id: Uuid,
}

#[test]
fn same_leg_completed_step_blocks_pre_execution_refresh() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let current_step = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        1,
        OrderExecutionStepStatus::Running,
    );
    let completed_same_leg = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        0,
        OrderExecutionStepStatus::Completed,
    );

    assert!(leg_already_crossed_provider_boundary(
        &current_step,
        &[completed_same_leg, current_step.clone()],
        &[]
    ));
}

#[test]
fn same_leg_provider_operation_blocks_pre_execution_refresh() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let current_step = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        1,
        OrderExecutionStepStatus::Running,
    );
    let submitted_same_leg = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        0,
        OrderExecutionStepStatus::Running,
    );
    let provider_operation = test_provider_operation(order_id, attempt_id, submitted_same_leg.id);

    assert!(leg_already_crossed_provider_boundary(
        &current_step,
        &[submitted_same_leg, current_step.clone()],
        &[provider_operation]
    ));
}

#[test]
fn different_leg_completed_step_does_not_block_pre_execution_refresh() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let current_leg_id = Uuid::now_v7();
    let other_leg_id = Uuid::now_v7();
    let current_step = test_execution_step(
        order_id,
        attempt_id,
        current_leg_id,
        1,
        OrderExecutionStepStatus::Running,
    );
    let completed_other_leg = test_execution_step(
        order_id,
        attempt_id,
        other_leg_id,
        0,
        OrderExecutionStepStatus::Completed,
    );

    assert!(!leg_already_crossed_provider_boundary(
        &current_step,
        &[completed_other_leg, current_step.clone()],
        &[]
    ));
}

#[tokio::test]
async fn classify_stale_running_step_records_all_decisions() {
    let test_db = test_database().await;
    let deps = test_deps(test_db.db.clone());

    let durable = seed_running_step(&test_db.pool, true, true).await;
    let classified = classify_stale_running_step_for_deps(
        &deps,
        ClassifyStaleRunningStepInput {
            order_id: durable.order_id.into(),
            attempt_id: durable.attempt_id.into(),
            step_id: durable.step_id.into(),
        },
    )
    .await
    .expect("classify durable progress");
    assert_eq!(
        classified.decision,
        StaleRunningStepDecision::DurableProviderOperationWaitingExternalProgress
    );
    assert_step_classification(
        &test_db.db,
        durable.step_id,
        "durable_provider_operation_waiting_external_progress",
    )
    .await;

    let ambiguous = seed_running_step(&test_db.pool, true, false).await;
    let classified = classify_stale_running_step_for_deps(
        &deps,
        ClassifyStaleRunningStepInput {
            order_id: ambiguous.order_id.into(),
            attempt_id: ambiguous.attempt_id.into(),
            step_id: ambiguous.step_id.into(),
        },
    )
    .await
    .expect("classify ambiguous external window");
    assert_eq!(
        classified.decision,
        StaleRunningStepDecision::AmbiguousExternalSideEffectWindow
    );
    assert_step_classification(
        &test_db.db,
        ambiguous.step_id,
        "ambiguous_external_side_effect_window",
    )
    .await;

    let missing_checkpoint = seed_running_step(&test_db.pool, false, false).await;
    let classified = classify_stale_running_step_for_deps(
        &deps,
        ClassifyStaleRunningStepInput {
            order_id: missing_checkpoint.order_id.into(),
            attempt_id: missing_checkpoint.attempt_id.into(),
            step_id: missing_checkpoint.step_id.into(),
        },
    )
    .await
    .expect("classify missing checkpoint");
    assert_eq!(
        classified.decision,
        StaleRunningStepDecision::StaleRunningStepWithoutCheckpoint
    );
    assert_step_classification(
        &test_db.db,
        missing_checkpoint.step_id,
        "stale_running_step_without_checkpoint",
    )
    .await;
}

#[tokio::test]
async fn waiting_external_transition_records_latency_timestamp() {
    let test_db = test_database().await;
    let seeded = seed_running_step(&test_db.pool, false, false).await;
    let waiting_at = Utc::now();

    let step = test_db
        .db
        .orders()
        .wait_execution_step(seeded.step_id, json!({"kind": "waiting"}), None, waiting_at)
        .await
        .expect("mark step waiting external");
    assert_eq!(step.status, OrderExecutionStepStatus::Waiting);

    let latency = test_db
        .db
        .orders()
        .get_execution_step_latency_record(seeded.step_id)
        .await
        .expect("load latency record");
    assert!(latency.waiting_external_at.is_some());
    assert_eq!(latency.hint_arrived_at, None);
}

#[tokio::test]
async fn hint_arrival_records_latency_timestamp() {
    let test_db = test_database().await;
    let seeded = seed_running_step(&test_db.pool, false, false).await;
    let arrived_at = Utc::now();

    test_db
        .db
        .orders()
        .record_execution_step_hint_arrival(seeded.step_id, arrived_at)
        .await
        .expect("record hint arrival");

    let latency = test_db
        .db
        .orders()
        .get_execution_step_latency_record(seeded.step_id)
        .await
        .expect("load latency record");
    assert!(latency.hint_arrived_at.is_some());
}

#[tokio::test]
async fn hyperliquid_bridge_quotes_build_refund_quote_leg_shapes() {
    let registry = ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
        across: None,
        cctp: None,
        hyperunit_base_url: None,
        hyperunit_proxy_url: None,
        hyperliquid_base_url: Some("http://127.0.0.1:1".to_string()),
        velora: None,
        hyperliquid_network: HyperliquidCallNetwork::Testnet,
        hyperliquid_order_timeout_ms: 30_000,
    })
    .expect("hyperliquid bridge provider registry");
    let bridge = registry
        .bridge(ProviderId::HyperliquidBridge.as_str())
        .expect("hyperliquid bridge provider");
    let external_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let hl_usdc = test_asset("hyperliquid", "native");
    let deposit = hyperliquid_bridge_deposit_transition(external_usdc.clone(), hl_usdc.clone());
    let withdrawal =
        hyperliquid_bridge_withdrawal_transition(hl_usdc.clone(), external_usdc.clone());

    let deposit_quote = bridge
        .quote_bridge(BridgeQuoteRequest {
            source_asset: external_usdc.clone(),
            destination_asset: hl_usdc.clone(),
            order_kind: MarketOrderKind::ExactIn {
                amount_in: "150000000".to_string(),
                min_amount_out: Some("1".to_string()),
            },
            recipient_address: test_address(1),
            depositor_address: test_address(2),
            partial_fills_enabled: false,
        })
        .await
        .expect("quote HL deposit")
        .expect("HL deposit quote");
    let deposit_legs =
        refund_bridge_quote_legs(&deposit, &deposit_quote).expect("deposit quote legs");
    assert_eq!(deposit_legs.len(), 1);
    assert_eq!(
        deposit_legs[0].execution_step_type,
        OrderExecutionStepType::HyperliquidBridgeDeposit
    );
    assert_eq!(deposit_legs[0].amount_in, "150000000");
    assert_eq!(deposit_legs[0].amount_out, "150000000");

    let withdrawal_quote = bridge
        .quote_bridge(BridgeQuoteRequest {
            source_asset: hl_usdc,
            destination_asset: external_usdc,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: "150000000".to_string(),
                min_amount_out: Some("1".to_string()),
            },
            recipient_address: test_address(1),
            depositor_address: test_address(2),
            partial_fills_enabled: false,
        })
        .await
        .expect("quote HL withdrawal")
        .expect("HL withdrawal quote");
    let withdrawal_legs =
        refund_bridge_quote_legs(&withdrawal, &withdrawal_quote).expect("withdrawal quote legs");
    assert_eq!(withdrawal_legs.len(), 1);
    assert_eq!(
        withdrawal_legs[0].execution_step_type,
        OrderExecutionStepType::HyperliquidBridgeWithdrawal
    );
    assert_eq!(withdrawal_legs[0].amount_in, "150000000");
    assert_eq!(withdrawal_legs[0].amount_out, "149000000");
}

#[test]
fn external_custody_hyperliquid_bridge_path_materializes_steps() {
    let planned_at = Utc::now();
    let external_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let hl_usdc = test_asset("hyperliquid", "native");
    let order = test_order(external_usdc.clone(), planned_at);
    let vault = test_custody_vault(
        &order,
        CustodyVaultRole::DestinationExecution,
        &external_usdc,
    );
    let deposit = hyperliquid_bridge_deposit_transition(external_usdc.clone(), hl_usdc.clone());
    let withdrawal =
        hyperliquid_bridge_withdrawal_transition(hl_usdc.clone(), external_usdc.clone());
    let quoted_path = RefundQuotedPath {
        path: TransitionPath {
            id: "hl-deposit>hl-withdrawal".to_string(),
            transitions: vec![deposit.clone(), withdrawal.clone()],
        },
        amount_out: "149000000".to_string(),
        legs: vec![
            quote_leg_for_transition(&deposit, "150000000", "150000000", planned_at),
            quote_leg_for_transition(&withdrawal, "150000000", "149000000", planned_at),
        ],
    };

    let (legs, steps) =
        materialize_external_custody_refund_path(&order, &vault, &quoted_path, planned_at)
            .expect("materialize HL bridge refund path");

    assert_eq!(legs.len(), 2);
    assert_eq!(steps.len(), 2);
    assert_eq!(
        steps[0].step_type,
        OrderExecutionStepType::HyperliquidBridgeDeposit
    );
    assert_eq!(steps[0].provider, ProviderId::HyperliquidBridge.as_str());
    assert_eq!(steps[0].input_asset, Some(external_usdc.clone()));
    assert_eq!(steps[0].output_asset, Some(hl_usdc.clone()));
    assert_eq!(steps[0].amount_in.as_deref(), Some("150000000"));
    assert_eq!(
        steps[0].request.get("source_custody_vault_id"),
        Some(&json!(vault.id))
    );
    assert_eq!(
        steps[0]
            .request
            .get("source_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::DestinationExecution.to_db_string())
    );

    assert_eq!(
        steps[1].step_type,
        OrderExecutionStepType::HyperliquidBridgeWithdrawal
    );
    assert_eq!(steps[1].provider, ProviderId::HyperliquidBridge.as_str());
    assert_eq!(steps[1].input_asset, Some(hl_usdc));
    assert_eq!(steps[1].output_asset, Some(external_usdc.clone()));
    assert_eq!(steps[1].amount_in.as_deref(), Some("150000000"));
    assert_eq!(steps[1].min_amount_out.as_deref(), Some("149000000"));
    assert_eq!(
        steps[1].request.get("hyperliquid_custody_vault_id"),
        Some(&json!(vault.id))
    );
    assert_eq!(
        steps[1]
            .request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::DestinationExecution.to_db_string())
    );
    assert_eq!(
        steps[1]
            .request
            .get("hyperliquid_custody_vault_chain_id")
            .and_then(Value::as_str),
        Some("evm:42161")
    );
    assert_eq!(
        steps[1]
            .request
            .get("hyperliquid_custody_vault_asset_id")
            .and_then(Value::as_str),
        Some("0xaf88d065e77c8cc2239327c5edb3a432268e5831")
    );
    assert_eq!(
        steps[1]
            .request
            .get("transfer_from_spot")
            .and_then(Value::as_bool),
        Some(false)
    );
}

#[test]
fn external_custody_hyperliquid_bridge_role_gate_requires_destination_execution() {
    let external_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let hl_usdc = test_asset("hyperliquid", "native");
    let deposit = hyperliquid_bridge_deposit_transition(external_usdc.clone(), hl_usdc.clone());
    let withdrawal =
        hyperliquid_bridge_withdrawal_transition(hl_usdc.clone(), external_usdc.clone());
    let path = TransitionPath {
        id: "hl-deposit>hl-withdrawal".to_string(),
        transitions: vec![deposit, withdrawal.clone()],
    };

    assert!(refund_path_compatible_with_position(
        RecoverablePositionKind::ExternalCustody,
        Some(CustodyVaultRole::DestinationExecution),
        &path,
    ));
    assert!(!refund_path_compatible_with_position(
        RecoverablePositionKind::ExternalCustody,
        Some(CustodyVaultRole::SourceDeposit),
        &path,
    ));

    let first_hop_withdrawal = TransitionPath {
        id: "hl-withdrawal".to_string(),
        transitions: vec![withdrawal],
    };
    assert!(!refund_path_compatible_with_position(
        RecoverablePositionKind::ExternalCustody,
        Some(CustodyVaultRole::DestinationExecution),
        &first_hop_withdrawal,
    ));
}

#[test]
fn hyperliquid_bridge_withdrawal_marks_derived_destination_for_hydration() {
    let planned_at = Utc::now();
    let external_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let hl_usdc = test_asset("hyperliquid", "native");
    let order = test_order(external_usdc.clone(), planned_at);
    let transition =
        hyperliquid_bridge_withdrawal_transition(hl_usdc.clone(), external_usdc.clone());
    let leg = quote_leg_for_transition(&transition, "150000000", "149000000", planned_at);
    let custody = RefundHyperliquidBinding::DerivedDestinationExecution {
        asset: external_usdc,
    };

    let step = refund_transition_hyperliquid_bridge_withdrawal_step(
        &order,
        &transition,
        &leg,
        &custody,
        false,
        3,
        planned_at,
    )
    .expect("build derived HL withdrawal step");

    assert_eq!(
        step.request.get("hyperliquid_custody_vault_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        step.request.get("hyperliquid_custody_vault_address"),
        Some(&Value::Null)
    );
    assert_eq!(
        step.request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::DestinationExecution.to_db_string())
    );
    assert_eq!(
        step.details
            .get("hyperliquid_custody_vault_status")
            .and_then(Value::as_str),
        Some("pending_derivation")
    );
    assert_eq!(
        step.request
            .get("recipient_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::DestinationExecution.to_db_string())
    );
}

#[test]
fn unit_refund_quote_legs_are_passthrough_shapes() {
    let planned_at = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let order = test_order(btc.clone(), planned_at);
    let deposit = unit_deposit_transition(btc.clone(), hl_btc.clone(), CanonicalAsset::Btc);
    let withdrawal = unit_withdrawal_transition(hl_btc, btc, CanonicalAsset::Btc);

    let deposit_leg = refund_unit_deposit_quote_leg(&deposit, "30000", planned_at);
    assert_eq!(
        deposit_leg.execution_step_type,
        OrderExecutionStepType::UnitDeposit
    );
    assert_eq!(deposit_leg.amount_in, "30000");
    assert_eq!(deposit_leg.amount_out, "30000");
    assert_eq!(deposit_leg.raw, json!({}));

    let withdrawal_leg = refund_unit_withdrawal_quote_leg(&order, &withdrawal, "30000", planned_at);
    assert_eq!(
        withdrawal_leg.execution_step_type,
        OrderExecutionStepType::UnitWithdrawal
    );
    assert_eq!(withdrawal_leg.amount_in, "30000");
    assert_eq!(withdrawal_leg.amount_out, "30000");
    assert_eq!(
        withdrawal_leg
            .raw
            .get("recipient_address")
            .and_then(Value::as_str),
        Some(order.refund_address.as_str())
    );
}

#[test]
fn external_custody_unit_path_materializes_deposit_and_withdrawal_steps() {
    let planned_at = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let order = test_order(btc.clone(), planned_at);
    let vault = test_custody_vault(&order, CustodyVaultRole::SourceDeposit, &btc);
    let deposit = unit_deposit_transition(btc.clone(), hl_btc.clone(), CanonicalAsset::Btc);
    let withdrawal = unit_withdrawal_transition(hl_btc.clone(), btc.clone(), CanonicalAsset::Btc);
    let quoted_path = RefundQuotedPath {
        path: TransitionPath {
            id: "unit-deposit>unit-withdrawal".to_string(),
            transitions: vec![deposit.clone(), withdrawal.clone()],
        },
        amount_out: "30000".to_string(),
        legs: vec![
            quote_leg_for_transition(&deposit, "30000", "30000", planned_at),
            quote_leg_for_transition(&withdrawal, "30000", "30000", planned_at),
        ],
    };

    let (legs, steps) =
        materialize_external_custody_refund_path(&order, &vault, &quoted_path, planned_at)
            .expect("materialize Unit refund path");

    assert_eq!(legs.len(), 2);
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].step_type, OrderExecutionStepType::UnitDeposit);
    assert_eq!(steps[0].provider, ProviderId::Unit.as_str());
    assert_eq!(steps[0].input_asset, Some(btc.clone()));
    assert_eq!(steps[0].output_asset, None);
    assert_eq!(steps[0].amount_in.as_deref(), Some("30000"));
    assert_eq!(
        steps[0].request.get("source_custody_vault_id"),
        Some(&json!(vault.id))
    );
    assert_eq!(
        steps[0].request.get("revert_custody_vault_id"),
        Some(&json!(vault.id))
    );
    assert_eq!(
        steps[0]
            .request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::HyperliquidSpot.to_db_string())
    );

    assert_eq!(steps[1].step_type, OrderExecutionStepType::UnitWithdrawal);
    assert_eq!(steps[1].provider, ProviderId::Unit.as_str());
    assert_eq!(steps[1].input_asset, Some(hl_btc));
    assert_eq!(steps[1].output_asset, Some(btc));
    assert_eq!(steps[1].amount_in.as_deref(), Some("30000"));
    assert_eq!(steps[1].min_amount_out.as_deref(), Some("0"));
    assert_eq!(
        steps[1].request.get("hyperliquid_custody_vault_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        steps[1]
            .request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::HyperliquidSpot.to_db_string())
    );
    assert_eq!(
        steps[1]
            .details
            .get("hyperliquid_custody_vault_status")
            .and_then(Value::as_str),
        Some("pending_derivation")
    );
    assert_eq!(
        steps[1]
            .request
            .get("recipient_address")
            .and_then(Value::as_str),
        Some(order.refund_address.as_str())
    );
}

#[test]
fn unit_withdrawal_builder_accepts_all_hyperliquid_binding_flavors() {
    let planned_at = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let order = test_order(btc.clone(), planned_at);
    let vault = test_custody_vault(&order, CustodyVaultRole::HyperliquidSpot, &hl_btc);
    let withdrawal = unit_withdrawal_transition(hl_btc.clone(), btc.clone(), CanonicalAsset::Btc);
    let leg = quote_leg_for_transition(&withdrawal, "30000", "30000", planned_at);

    let explicit = refund_transition_unit_withdrawal_step(
        &order,
        &withdrawal,
        &leg,
        &RefundHyperliquidBinding::Explicit {
            vault_id: vault.id,
            address: vault.address.clone(),
            role: CustodyVaultRole::HyperliquidSpot,
            asset: Some(hl_btc),
        },
        true,
        0,
        planned_at,
    )
    .expect("explicit UnitWithdrawal");
    assert_eq!(
        explicit.request.get("hyperliquid_custody_vault_id"),
        Some(&json!(vault.id))
    );
    assert_eq!(
        explicit
            .details
            .get("hyperliquid_custody_vault_status")
            .and_then(Value::as_str),
        Some("bound")
    );

    let derived_spot = refund_transition_unit_withdrawal_step(
        &order,
        &withdrawal,
        &leg,
        &RefundHyperliquidBinding::DerivedSpot,
        false,
        1,
        planned_at,
    )
    .expect("derived spot UnitWithdrawal");
    assert_eq!(
        derived_spot.request.get("hyperliquid_custody_vault_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        derived_spot
            .request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::HyperliquidSpot.to_db_string())
    );
    assert_eq!(
        derived_spot
            .request
            .get("recipient_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::DestinationExecution.to_db_string())
    );

    let derived_destination = refund_transition_unit_withdrawal_step(
        &order,
        &withdrawal,
        &leg,
        &RefundHyperliquidBinding::DerivedDestinationExecution { asset: btc },
        false,
        2,
        planned_at,
    )
    .expect("derived destination UnitWithdrawal");
    assert_eq!(
        derived_destination
            .request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::DestinationExecution.to_db_string())
    );
    assert_eq!(
        derived_destination
            .request
            .get("hyperliquid_custody_vault_chain_id")
            .and_then(Value::as_str),
        Some("bitcoin")
    );
    assert_eq!(
        derived_destination
            .request
            .get("hyperliquid_custody_vault_asset_id")
            .and_then(Value::as_str),
        Some("native")
    );
}

#[test]
fn unit_refund_compat_gate_accepts_external_deposit_and_hyperliquid_withdrawal() {
    let btc = test_asset("bitcoin", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let deposit = unit_deposit_transition(btc.clone(), hl_btc.clone(), CanonicalAsset::Btc);
    let withdrawal = unit_withdrawal_transition(hl_btc, btc, CanonicalAsset::Btc);

    assert!(refund_path_compatible_with_position(
        RecoverablePositionKind::ExternalCustody,
        Some(CustodyVaultRole::SourceDeposit),
        &TransitionPath {
            id: "unit-deposit>unit-withdrawal".to_string(),
            transitions: vec![deposit, withdrawal.clone()],
        },
    ));
    assert!(refund_path_compatible_with_position(
        RecoverablePositionKind::HyperliquidSpot,
        None,
        &TransitionPath {
            id: "unit-withdrawal".to_string(),
            transitions: vec![withdrawal.clone()],
        },
    ));
    assert!(!refund_path_compatible_with_position(
        RecoverablePositionKind::ExternalCustody,
        Some(CustodyVaultRole::SourceDeposit),
        &TransitionPath {
            id: "unit-withdrawal".to_string(),
            transitions: vec![withdrawal],
        },
    ));
}

#[test]
fn refresh_cctp_quote_legs_materialize_burn_and_receive() {
    let expires_at = Utc::now();
    let base_usdc = test_asset("evm:8453", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    let arbitrum_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let transition = cctp_transition(base_usdc.clone(), arbitrum_usdc.clone());
    let quote = BridgeQuote {
        provider_id: "cctp".to_string(),
        amount_in: "1000000".to_string(),
        amount_out: "999000".to_string(),
        provider_quote: json!({ "message_hash": "0xcctp" }),
        expires_at,
    };

    let legs = refresh_cctp_quote_transition_legs(&transition, &quote);

    assert_eq!(legs.len(), 2);
    assert_eq!(legs[0].transition_decl_id, "cctp");
    assert_eq!(legs[0].parent_transition_id(), "cctp");
    assert_eq!(
        legs[0].execution_step_type,
        OrderExecutionStepType::CctpBurn
    );
    assert_eq!(
        legs[0].input_asset,
        QuoteLegAsset::from_deposit_asset(&base_usdc)
    );
    assert_eq!(
        legs[0].output_asset,
        QuoteLegAsset::from_deposit_asset(&arbitrum_usdc)
    );
    assert_eq!(legs[0].amount_in, "1000000");
    assert_eq!(legs[0].amount_out, "999000");
    assert_eq!(
        legs[0]
            .raw
            .get("execution_step_type")
            .and_then(Value::as_str),
        Some(OrderExecutionStepType::CctpBurn.to_db_string())
    );

    assert_eq!(legs[1].transition_decl_id, "cctp:receive");
    assert_eq!(legs[1].parent_transition_id(), "cctp");
    assert_eq!(
        legs[1].execution_step_type,
        OrderExecutionStepType::CctpReceive
    );
    assert_eq!(
        legs[1].input_asset,
        QuoteLegAsset::from_deposit_asset(&arbitrum_usdc)
    );
    assert_eq!(
        legs[1].output_asset,
        QuoteLegAsset::from_deposit_asset(&arbitrum_usdc)
    );
    assert_eq!(legs[1].amount_in, "999000");
    assert_eq!(legs[1].amount_out, "999000");
    assert_eq!(
        legs[1].raw.get("kind").and_then(Value::as_str),
        Some(OrderExecutionStepType::CctpReceive.to_db_string())
    );
    assert_eq!(
        legs[1].raw.get("bridge_kind").and_then(Value::as_str),
        Some("cctp_bridge")
    );
}

#[test]
fn refresh_spot_cross_token_quote_legs_parse_hyperliquid_legs() {
    let expires_at = Utc::now();
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let hl_usdc = test_asset("hyperliquid", "native");
    let hl_eth = test_asset("hyperliquid", "UETH");
    let quote = ExchangeQuote {
        provider_id: "hyperliquid".to_string(),
        amount_in: "30000".to_string(),
        amount_out: "40000000000000000".to_string(),
        min_amount_out: Some("1".to_string()),
        max_amount_in: None,
        provider_quote: json!({
            "kind": "spot_cross_token",
            "legs": [
                {
                    "input_asset": QuoteLegAsset::from_deposit_asset(&hl_btc),
                    "output_asset": QuoteLegAsset::from_deposit_asset(&hl_usdc),
                    "amount_in": "30000",
                    "amount_out": "20000000"
                },
                {
                    "input_asset": QuoteLegAsset::from_deposit_asset(&hl_usdc),
                    "output_asset": QuoteLegAsset::from_deposit_asset(&hl_eth),
                    "amount_in": "20000000",
                    "amount_out": "40000000000000000"
                }
            ]
        }),
        expires_at,
    };

    let legs = refresh_spot_cross_token_quote_transition_legs(
        "hl-trade",
        MarketOrderTransitionKind::HyperliquidTrade,
        ProviderId::Hyperliquid,
        &quote,
    )
    .expect("parse spot_cross_token legs");

    assert_eq!(legs.len(), 2);
    assert_eq!(legs[0].transition_decl_id, "hl-trade:leg:0");
    assert_eq!(legs[0].parent_transition_id(), "hl-trade");
    assert_eq!(
        legs[0].execution_step_type,
        OrderExecutionStepType::HyperliquidTrade
    );
    assert_eq!(
        legs[0].input_asset,
        QuoteLegAsset::from_deposit_asset(&hl_btc)
    );
    assert_eq!(
        legs[0].output_asset,
        QuoteLegAsset::from_deposit_asset(&hl_usdc)
    );
    assert_eq!(legs[0].amount_in, "30000");
    assert_eq!(legs[0].amount_out, "20000000");
    assert_eq!(legs[1].transition_decl_id, "hl-trade:leg:1");
    assert_eq!(legs[1].parent_transition_id(), "hl-trade");
    assert_eq!(
        legs[1].input_asset,
        QuoteLegAsset::from_deposit_asset(&hl_usdc)
    );
    assert_eq!(
        legs[1].output_asset,
        QuoteLegAsset::from_deposit_asset(&hl_eth)
    );
    assert_eq!(legs[1].amount_in, "20000000");
    assert_eq!(legs[1].amount_out, "40000000000000000");
}

#[test]
fn refresh_unit_quote_legs_are_passthrough_shapes() {
    let expires_at = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let deposit = unit_deposit_transition(btc.clone(), hl_btc.clone(), CanonicalAsset::Btc);
    let withdrawal = unit_withdrawal_transition(hl_btc.clone(), btc.clone(), CanonicalAsset::Btc);
    let reserve_quote = refresh_bitcoin_fee_reserve_quote(U256::from(2500_u64));

    let deposit_leg =
        refresh_unit_deposit_quote_leg(&deposit, "30000", expires_at, reserve_quote.clone());
    assert_eq!(
        deposit_leg.execution_step_type,
        OrderExecutionStepType::UnitDeposit
    );
    assert_eq!(
        deposit_leg.input_asset,
        QuoteLegAsset::from_deposit_asset(&btc)
    );
    assert_eq!(
        deposit_leg.output_asset,
        QuoteLegAsset::from_deposit_asset(&hl_btc)
    );
    assert_eq!(deposit_leg.amount_in, "30000");
    assert_eq!(deposit_leg.amount_out, "30000");
    assert_eq!(deposit_leg.raw, reserve_quote);
    assert_eq!(
        deposit_leg
            .raw
            .pointer("/source_fee_reserve/reserve_bps")
            .and_then(Value::as_u64),
        Some(REFRESH_BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS)
    );

    let withdrawal_leg =
        refresh_unit_withdrawal_quote_leg(&withdrawal, "30000", "0xrecipient", expires_at);
    assert_eq!(
        withdrawal_leg.execution_step_type,
        OrderExecutionStepType::UnitWithdrawal
    );
    assert_eq!(
        withdrawal_leg.input_asset,
        QuoteLegAsset::from_deposit_asset(&hl_btc)
    );
    assert_eq!(
        withdrawal_leg.output_asset,
        QuoteLegAsset::from_deposit_asset(&btc)
    );
    assert_eq!(withdrawal_leg.amount_in, "30000");
    assert_eq!(withdrawal_leg.amount_out, "30000");
    assert_eq!(
        withdrawal_leg
            .raw
            .get("recipient_address")
            .and_then(Value::as_str),
        Some("0xrecipient")
    );
    assert_eq!(
        withdrawal_leg
            .raw
            .pointer("/hyperliquid_core_activation_fee/amount_raw")
            .and_then(Value::as_str),
        Some(
            REFRESH_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW
                .to_string()
                .as_str()
        )
    );
}

#[test]
fn refresh_unit_deposit_fee_reserve_reads_original_quote_leg() {
    let now = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let transition = unit_deposit_transition(btc.clone(), hl_btc, CanonicalAsset::Btc);
    let fee_reserve = U256::from(42_000_u64);
    let leg = refresh_unit_deposit_quote_leg(
        &transition,
        "100000",
        now,
        refresh_bitcoin_fee_reserve_quote(fee_reserve),
    );
    let quote = test_market_order_quote(
        btc,
        transition.output.asset.clone(),
        vec![leg],
        now + chrono::Duration::minutes(5),
        now,
    );

    assert_eq!(
        refresh_unit_deposit_fee_reserve(&quote, &transition).expect("read fee reserve"),
        Some(fee_reserve)
    );

    let mut no_reserve_quote = quote.clone();
    no_reserve_quote.provider_quote = json!({ "legs": [] });
    assert_eq!(
        refresh_unit_deposit_fee_reserve(&no_reserve_quote, &transition)
            .expect("missing fee reserve is allowed"),
        None
    );
}

#[test]
fn refresh_hyperliquid_spot_send_reserve_math_matches_legacy() {
    assert_eq!(
        refresh_reserve_hyperliquid_spot_send_quote_gas("amount_in", "1000001")
            .expect("subtract reserve"),
        "1"
    );
    assert_eq!(
        refresh_add_hyperliquid_spot_send_quote_gas_reserve("amount_in", "1").expect("add reserve"),
        "1000001"
    );
    assert!(
        refresh_reserve_hyperliquid_spot_send_quote_gas("amount_in", "1000000").is_err(),
        "amount must strictly exceed the reserve"
    );
    assert!(
        refresh_add_hyperliquid_spot_send_quote_gas_reserve("amount_in", &U256::MAX.to_string(),)
            .is_err(),
        "addition must reject overflow"
    );
}

async fn test_database() -> TestDatabase {
    let image = GenericImage::new("postgres", "18-alpine")
        .with_exposed_port(POSTGRES_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", POSTGRES_USER)
        .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .with_env_var("POSTGRES_DB", POSTGRES_DATABASE)
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=10",
            "-c",
            "max_replication_slots=10",
        ]);

    let container = image.start().await.expect("start Postgres testcontainer");
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT.tcp())
        .await
        .expect("read Postgres testcontainer port");
    let database_url = format!(
        "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{port}/{POSTGRES_DATABASE}"
    );
    let db = Database::connect(&database_url, 4, 1)
        .await
        .expect("connect migrated test database");
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect raw test pool");

    TestDatabase {
        _container: container,
        db,
        pool,
    }
}

fn test_deps(db: Database) -> OrderActivityDeps {
    let settings = Arc::new(test_settings());
    let chain_registry = Arc::new(ChainRegistry::new());
    let action_providers = Arc::new(
        ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
            across: None,
            cctp: None,
            hyperunit_base_url: None,
            hyperunit_proxy_url: None,
            hyperliquid_base_url: None,
            velora: None,
            hyperliquid_network: HyperliquidCallNetwork::Testnet,
            hyperliquid_order_timeout_ms: 30_000,
        })
        .expect("empty action provider registry"),
    );
    let custody_executor = Arc::new(CustodyActionExecutor::new(
        db.clone(),
        settings,
        chain_registry.clone(),
    ));
    OrderActivityDeps::new(db, action_providers, custody_executor, chain_registry)
}

fn test_settings() -> Settings {
    let dir = tempfile::tempdir().expect("settings tempdir");
    let path = dir.path().join("router-master-key.hex");
    std::fs::write(&path, alloy::hex::encode([0x42_u8; 64])).expect("write router master key");
    Settings::load(&path).expect("load test settings")
}

fn test_asset(chain: &str, asset: &str) -> DepositAsset {
    DepositAsset {
        chain: ChainId::parse(chain).expect("valid test chain"),
        asset: AssetId::parse(asset).expect("valid test asset"),
    }
}

fn test_address(byte: u8) -> String {
    format!("0x{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}")
}

fn test_order(source_asset: DepositAsset, now: chrono::DateTime<Utc>) -> RouterOrder {
    RouterOrder {
        id: Uuid::now_v7(),
        order_type: RouterOrderType::MarketOrder,
        status: RouterOrderStatus::Refunding,
        funding_vault_id: None,
        source_asset: source_asset.clone(),
        destination_asset: source_asset,
        recipient_address: test_address(9),
        refund_address: test_address(8),
        action: RouterOrderAction::MarketOrder(MarketOrderAction {
            order_kind: MarketOrderKind::ExactIn {
                amount_in: "150000000".to_string(),
                min_amount_out: Some("1".to_string()),
            },
            slippage_bps: Some(100),
        }),
        action_timeout_at: now + chrono::Duration::hours(1),
        idempotency_key: None,
        workflow_trace_id: Uuid::now_v7().simple().to_string(),
        workflow_parent_span_id: "0000000000000000".to_string(),
        created_at: now,
        updated_at: now,
    }
}

fn test_custody_vault(
    order: &RouterOrder,
    role: CustodyVaultRole,
    asset: &DepositAsset,
) -> CustodyVault {
    CustodyVault {
        id: Uuid::now_v7(),
        order_id: Some(order.id),
        role,
        visibility: CustodyVaultVisibility::Internal,
        chain: asset.chain.clone(),
        asset: Some(asset.asset.clone()),
        address: test_address(7),
        control_type: CustodyVaultControlType::RouterDerivedKey,
        derivation_salt: None,
        signer_ref: None,
        status: CustodyVaultStatus::Active,
        metadata: json!({}),
        created_at: order.created_at,
        updated_at: order.created_at,
    }
}

fn hyperliquid_bridge_deposit_transition(
    input: DepositAsset,
    output: DepositAsset,
) -> TransitionDecl {
    TransitionDecl {
        id: "hl-deposit".to_string(),
        kind: MarketOrderTransitionKind::HyperliquidBridgeDeposit,
        provider: ProviderId::HyperliquidBridge,
        input: AssetSlot {
            asset: input.clone(),
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        output: AssetSlot {
            asset: output.clone(),
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        from: MarketOrderNode::External(input),
        to: MarketOrderNode::Venue {
            provider: ProviderId::Hyperliquid,
            canonical: CanonicalAsset::Usdc,
        },
    }
}

fn hyperliquid_bridge_withdrawal_transition(
    input: DepositAsset,
    output: DepositAsset,
) -> TransitionDecl {
    TransitionDecl {
        id: "hl-withdrawal".to_string(),
        kind: MarketOrderTransitionKind::HyperliquidBridgeWithdrawal,
        provider: ProviderId::HyperliquidBridge,
        input: AssetSlot {
            asset: input.clone(),
            required_custody_role: RequiredCustodyRole::HyperliquidSpot,
        },
        output: AssetSlot {
            asset: output.clone(),
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        from: MarketOrderNode::Venue {
            provider: ProviderId::Hyperliquid,
            canonical: CanonicalAsset::Usdc,
        },
        to: MarketOrderNode::External(output),
    }
}

fn unit_deposit_transition(
    input: DepositAsset,
    output: DepositAsset,
    canonical: CanonicalAsset,
) -> TransitionDecl {
    TransitionDecl {
        id: "unit-deposit".to_string(),
        kind: MarketOrderTransitionKind::UnitDeposit,
        provider: ProviderId::Unit,
        input: AssetSlot {
            asset: input.clone(),
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        output: AssetSlot {
            asset: output,
            required_custody_role: RequiredCustodyRole::HyperliquidSpot,
        },
        from: MarketOrderNode::External(input),
        to: MarketOrderNode::Venue {
            provider: ProviderId::Hyperliquid,
            canonical,
        },
    }
}

fn unit_withdrawal_transition(
    input: DepositAsset,
    output: DepositAsset,
    canonical: CanonicalAsset,
) -> TransitionDecl {
    TransitionDecl {
        id: "unit-withdrawal".to_string(),
        kind: MarketOrderTransitionKind::UnitWithdrawal,
        provider: ProviderId::Unit,
        input: AssetSlot {
            asset: input,
            required_custody_role: RequiredCustodyRole::HyperliquidSpot,
        },
        output: AssetSlot {
            asset: output.clone(),
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        from: MarketOrderNode::Venue {
            provider: ProviderId::Hyperliquid,
            canonical,
        },
        to: MarketOrderNode::External(output),
    }
}

fn cctp_transition(input: DepositAsset, output: DepositAsset) -> TransitionDecl {
    TransitionDecl {
        id: "cctp".to_string(),
        kind: MarketOrderTransitionKind::CctpBridge,
        provider: ProviderId::Cctp,
        input: AssetSlot {
            asset: input.clone(),
            required_custody_role: RequiredCustodyRole::SourceOrIntermediate,
        },
        output: AssetSlot {
            asset: output.clone(),
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        from: MarketOrderNode::External(input),
        to: MarketOrderNode::External(output),
    }
}

fn quote_leg_for_transition(
    transition: &TransitionDecl,
    amount_in: &str,
    amount_out: &str,
    expires_at: chrono::DateTime<Utc>,
) -> QuoteLeg {
    QuoteLeg::new(QuoteLegSpec {
            transition_decl_id: &transition.id,
            transition_kind: transition.kind,
            provider: transition.provider,
            input_asset: &transition.input.asset,
            output_asset: &transition.output.asset,
            amount_in,
            amount_out,
            expires_at,
            raw: json!({
                "kind": match transition.kind {
                    MarketOrderTransitionKind::HyperliquidBridgeDeposit => "hyperliquid_native_bridge",
                    MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => "hyperliquid_bridge_withdrawal",
                    _ => transition.kind.as_str(),
                },
            }),
        })
        .with_execution_step_type(execution_step_type_for_transition_kind(
            transition.kind,
        ))
}

fn test_market_order_quote(
    source_asset: DepositAsset,
    destination_asset: DepositAsset,
    legs: Vec<QuoteLeg>,
    expires_at: chrono::DateTime<Utc>,
    created_at: chrono::DateTime<Utc>,
) -> MarketOrderQuote {
    MarketOrderQuote {
        id: Uuid::now_v7(),
        order_id: None,
        source_asset,
        destination_asset,
        recipient_address: test_address(1),
        provider_id: "path:test".to_string(),
        order_kind: MarketOrderKindType::ExactIn,
        amount_in: "100000".to_string(),
        amount_out: "100000".to_string(),
        min_amount_out: Some("1".to_string()),
        max_amount_in: None,
        slippage_bps: Some(100),
        provider_quote: json!({ "legs": legs }),
        usd_valuation: json!({}),
        expires_at,
        created_at,
    }
}

fn test_execution_step(
    order_id: Uuid,
    attempt_id: Uuid,
    leg_id: Uuid,
    step_index: i32,
    status: OrderExecutionStepStatus,
) -> OrderExecutionStep {
    let now = Utc::now();
    OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id,
        execution_attempt_id: Some(attempt_id),
        execution_leg_id: Some(leg_id),
        transition_decl_id: Some(format!("test-transition-{step_index}")),
        step_index,
        step_type: OrderExecutionStepType::UniversalRouterSwap,
        provider: ProviderId::Velora.as_str().to_string(),
        status,
        input_asset: None,
        output_asset: None,
        amount_in: Some("1000".to_string()),
        min_amount_out: Some("1".to_string()),
        tx_hash: None,
        provider_ref: None,
        idempotency_key: Some(format!("test-step-{step_index}")),
        attempt_count: 0,
        next_attempt_at: None,
        started_at: Some(now),
        completed_at: if status == OrderExecutionStepStatus::Completed {
            Some(now)
        } else {
            None
        },
        details: json!({}),
        request: json!({}),
        response: json!({}),
        error: json!({}),
        usd_valuation: json!({}),
        created_at: now,
        updated_at: now,
    }
}

fn test_provider_operation(
    order_id: Uuid,
    attempt_id: Uuid,
    step_id: Uuid,
) -> OrderProviderOperation {
    let now = Utc::now();
    OrderProviderOperation {
        id: Uuid::now_v7(),
        order_id,
        execution_attempt_id: Some(attempt_id),
        execution_step_id: Some(step_id),
        provider: ProviderId::Velora.as_str().to_string(),
        operation_type: ProviderOperationType::UniversalRouterSwap,
        provider_ref: Some("test-provider-ref".to_string()),
        status: ProviderOperationStatus::Submitted,
        request: json!({}),
        response: json!({}),
        observed_state: json!({}),
        created_at: now,
        updated_at: now,
    }
}

async fn seed_running_step(
    pool: &PgPool,
    with_checkpoint: bool,
    with_durable_provider_operation: bool,
) -> SeededRunningStep {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let step_id = Uuid::now_v7();
    let now = Utc::now();
    let started_at = now - chrono::Duration::minutes(10);
    let trace_id = order_id.simple().to_string();
    let parent_span_id = trace_id[..16].to_string();
    let details = if with_checkpoint {
        json!({
            "provider_side_effect_checkpoint": {
                "kind": "provider_side_effect_about_to_fire",
                "reason": "about_to_fire_provider_side_effect",
                "recorded_at": started_at.to_rfc3339(),
                "scar_tissue": "§6"
            }
        })
    } else {
        json!({})
    };

    sqlx_core::query::query(
        r#"
            INSERT INTO router_orders (
                id,
                order_type,
                status,
                source_chain_id,
                source_asset_id,
                destination_chain_id,
                destination_asset_id,
                recipient_address,
                refund_address,
                action_timeout_at,
                workflow_trace_id,
                workflow_parent_span_id,
                created_at,
                updated_at
            )
            VALUES (
                $1, 'market_order', 'executing', 'evm:8453', 'native',
                'evm:8453', 'native', '0x0000000000000000000000000000000000000001',
                '0x0000000000000000000000000000000000000002',
                $2, $3, $4, $5, $5
            )
            "#,
    )
    .bind(order_id)
    .bind(now + chrono::Duration::hours(1))
    .bind(trace_id)
    .bind(parent_span_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert router order");

    sqlx_core::query::query(
        r#"
            INSERT INTO market_order_actions (
                order_id,
                order_kind,
                amount_in,
                min_amount_out,
                amount_out,
                max_amount_in,
                created_at,
                updated_at,
                slippage_bps
            )
            VALUES ($1, 'exact_in', '100', '1', NULL, NULL, $2, $2, 100)
            "#,
    )
    .bind(order_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert market order action");

    sqlx_core::query::query(
        r#"
            INSERT INTO order_execution_attempts (
                id,
                order_id,
                attempt_index,
                attempt_kind,
                status,
                failure_reason_json,
                input_custody_snapshot_json,
                created_at,
                updated_at
            )
            VALUES ($1, $2, 1, 'primary_execution', 'active', '{}'::jsonb, '{}'::jsonb, $3, $3)
            "#,
    )
    .bind(attempt_id)
    .bind(order_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert execution attempt");

    sqlx_core::query::query(
        r#"
            INSERT INTO order_execution_legs (
                id,
                order_id,
                execution_attempt_id,
                transition_decl_id,
                leg_index,
                leg_type,
                provider,
                status,
                input_chain_id,
                input_asset_id,
                output_chain_id,
                output_asset_id,
                amount_in,
                expected_amount_out,
                min_amount_out,
                started_at,
                details_json,
                usd_valuation_json,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, 'test-transition', 0, 'swap', 'velora',
                'running', 'evm:8453', 'native', 'evm:8453', 'native',
                '100', '100', '1', $4, '{}'::jsonb, '{}'::jsonb, $5, $5
            )
            "#,
    )
    .bind(leg_id)
    .bind(order_id)
    .bind(attempt_id)
    .bind(started_at)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert execution leg");

    sqlx_core::query::query(
        r#"
            INSERT INTO order_execution_steps (
                id,
                order_id,
                execution_attempt_id,
                execution_leg_id,
                transition_decl_id,
                step_index,
                step_type,
                provider,
                status,
                input_chain_id,
                input_asset_id,
                output_chain_id,
                output_asset_id,
                amount_in,
                min_amount_out,
                idempotency_key,
                attempt_count,
                started_at,
                details_json,
                request_json,
                response_json,
                error_json,
                usd_valuation_json,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, 'test-transition', 0, 'universal_router_swap',
                'velora', 'running', 'evm:8453', 'native', 'evm:8453', 'native',
                '100', '1', $5, 1, $6, $7, '{}'::jsonb, '{}'::jsonb,
                '{}'::jsonb, '{}'::jsonb, $8, $8
            )
            "#,
    )
    .bind(step_id)
    .bind(order_id)
    .bind(attempt_id)
    .bind(leg_id)
    .bind(format!("order:{order_id}:execution:0"))
    .bind(started_at)
    .bind(details)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert execution step");

    if with_durable_provider_operation {
        sqlx_core::query::query(
            r#"
                INSERT INTO order_provider_operations (
                    id,
                    order_id,
                    execution_attempt_id,
                    execution_step_id,
                    provider,
                    operation_type,
                    provider_ref,
                    status,
                    request_json,
                    response_json,
                    observed_state_json,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, 'velora', 'universal_router_swap',
                    'provider-ref', 'waiting_external', '{}'::jsonb,
                    '{"receipt":"recorded"}'::jsonb, '{}'::jsonb, $5, $5
                )
                "#,
        )
        .bind(Uuid::now_v7())
        .bind(order_id)
        .bind(attempt_id)
        .bind(step_id)
        .bind(now)
        .execute(pool)
        .await
        .expect("insert durable provider operation");
    }

    SeededRunningStep {
        order_id,
        attempt_id,
        step_id,
    }
}

async fn assert_step_classification(db: &Database, step_id: Uuid, expected_reason: &str) {
    let step = db
        .orders()
        .get_execution_step(step_id)
        .await
        .expect("load classified step");
    assert_eq!(
        step.details
            .get("stale_running_step_classification")
            .and_then(|classification| classification.get("reason"))
            .and_then(Value::as_str),
        Some(expected_reason)
    );
}
