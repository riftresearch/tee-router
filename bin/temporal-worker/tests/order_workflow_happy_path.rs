use std::{
    net::{SocketAddr, TcpStream},
    process::Command,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, U256},
    providers::{ext::AnvilApi, Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use bitcoin::{Address as BitcoinAddress, Amount as BitcoinAmount, Network as BitcoinNetwork};
use bitcoincore_rpc_async::Auth as BitcoinRpcAuth;
use chains::{
    bitcoin::BitcoinChain,
    evm::{EvmChain, EvmGasSponsorConfig},
    hyperliquid::HyperliquidChain,
    ChainRegistry,
};
use chrono::Utc;
use devnet::{
    evm_devnet::{MOCK_CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS, MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS},
    mock_integrators::{MockIntegratorConfig, MockIntegratorServer},
    RiftDevnet,
};
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use router_core::{
    config::Settings,
    db::Database,
    db::ExecutionAttemptPlan,
    models::{
        CustodyVaultRole, CustodyVaultVisibility, DepositVaultStatus, OrderExecutionAttempt,
        OrderExecutionAttemptKind, OrderExecutionAttemptStatus, OrderExecutionStepStatus,
        OrderExecutionStepType, OrderProviderOperation, ProviderOperationStatus,
        ProviderOperationType, RouterOrderQuote, RouterOrderStatus, VaultAction,
    },
    protocol::{AssetId, ChainId, DepositAsset},
    services::{
        custody_action_executor::{
            CustodyActionExecutor, HyperliquidCallNetwork, HyperliquidRuntimeConfig,
        },
        AcrossHttpProviderConfig, ActionProviderHttpOptions, ActionProviderRegistry,
        CctpHttpProviderConfig, VeloraHttpProviderConfig,
    },
};
use router_primitives::ChainType;
use router_server::{
    api::{CreateOrderRequest, CreateVaultRequest, MarketOrderQuoteKind, MarketOrderQuoteRequest},
    services::{order_manager::OrderManager, vault_manager::VaultManager},
};
use serde_json::{json, Value};
use sqlx_core::connection::Connection;
use sqlx_postgres::{PgConnectOptions, PgConnection, PgPool};
use temporal_worker::{
    order_execution::{
        activities::{OrderActivities, OrderActivityDeps},
        build_worker, order_workflow_id, refund_workflow_id,
        types::{
            AcknowledgeUnrecoverableSignal, ManualReleaseSignal, ManualTriggerRefundSignal,
            OrderTerminalStatus, OrderWorkflowInput, OrderWorkflowOutput, ProviderHintKind,
            ProviderKind, ProviderOperationHintSignal,
        },
        workflow_start_options,
        workflows::{OrderWorkflow, RefundWorkflow},
        DEFAULT_TASK_QUEUE,
    },
    runtime::{connect_client, TemporalConnection},
};
use temporalio_client::{Client, WorkflowGetResultOptions, WorkflowSignalOptions};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

const BASE_USDC_ADDRESS: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const ETHEREUM_USDC_ADDRESS: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const ARBITRUM_USDC_ADDRESS: &str = "0xaf88d065e77c8cc2239327c5edb3a432268e5831";
const POSTGRES_PORT: u16 = 5432;
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "password";
const POSTGRES_DATABASE: &str = "postgres";
const ROUTER_TEST_DATABASE_URL_ENV: &str = "ROUTER_TEST_DATABASE_URL";
const EVM_NATIVE_GAS_BUFFER_WEI: u128 = 1_000_000_000_000_000_000;
const EXECUTE_STEP_TEMPORAL_ATTEMPTS: usize = 3;
const ORDER_WORKFLOW_TEST_TIMEOUT: Duration = Duration::from_secs(240);
const ORDER_AMOUNT_IN_WEI: &str = "60000000000000000";
const ORDER_BTC_AMOUNT_SATS: &str = "1000000";
const ORDER_USDC_AMOUNT_RAW: &str = "150000000";
const TEST_RESIDUAL_SINK_ADDRESS: &str = "0x2222222222222222222222222222222222222222";
const TESTNET_HYPERLIQUID_BRIDGE_ADDRESS: &str = "0x08cfc1b6b2dcf36a1480b99353a354aa8ac56f89";

struct TestPostgres {
    admin_database_url: String,
    _container: Option<ContainerAsync<GenericImage>>,
}

struct WorkflowRun {
    _postgres: TestPostgres,
    db: Database,
    order_id: Uuid,
    output: OrderWorkflowOutput,
    refund_address_balance: U256,
    refund_address_base_usdc_balance: U256,
}

#[derive(Clone, Copy, Default)]
enum WorkflowRoute {
    #[default]
    VeloraBaseEthToBaseUsdc,
    VeloraBaseEthToBaseUsdcExactOut,
    AcrossBaseEthToEthereumUsdc,
    CctpBaseUsdcToArbitrumEth,
    CctpBaseUsdcToBitcoin,
    HyperliquidArbitrumUsdcToBitcoin,
    UnitBitcoinToBaseEth,
}

#[derive(Clone, Copy)]
enum ManualInterventionTestAction {
    Release,
    TriggerRefund,
    AcknowledgeUnrecoverable,
}

#[derive(Default)]
struct WorkflowOptions {
    route: WorkflowRoute,
    velora_transaction_failures: usize,
    velora_stale_quote_failures: usize,
    expire_quote_legs: bool,
    expire_quote_transition_kinds: Vec<&'static str>,
    refreshed_eth_usd_micro: Option<u128>,
    expect_external_custody_across_refund: bool,
    expect_hyperliquid_spot_unit_refund: bool,
    expect_external_custody_cctp_refund: bool,
    expect_external_custody_velora_refund: bool,
    expect_external_custody_across_then_velora_refund: bool,
    suppress_across_hint_signal: bool,
    simulate_across_lost_intent_checkpoint: bool,
    manual_intervention_action: Option<ManualInterventionTestAction>,
}

struct SeededOrder {
    order_id: Uuid,
    funding_vault_address: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_completes_funded_single_step_order() {
    let run = run_order_workflow(WorkflowOptions::default()).await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_manual_intervention_release_resumes_and_completes() {
    let run = run_order_workflow(WorkflowOptions {
        manual_intervention_action: Some(ManualInterventionTestAction::Release),
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed order after manual release");
    assert_eq!(completed.status, RouterOrderStatus::Completed);
    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load attempts after manual release");
    assert!(
        attempts.iter().any(|attempt| attempt.attempt_index == 2),
        "manual release should resume through a retry attempt"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_manual_intervention_trigger_refund_reaches_refunded() {
    let run = run_order_workflow(WorkflowOptions {
        manual_intervention_action: Some(ManualInterventionTestAction::TriggerRefund),
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Refunded);
    let refunded = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load refunded order after manual trigger");
    assert_eq!(refunded.status, RouterOrderStatus::Refunded);
    assert!(
        run.refund_address_balance >= U256::from_str(ORDER_AMOUNT_IN_WEI).expect("amount in wei"),
        "manual trigger refund should return the funding vault balance"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_manual_intervention_acknowledge_unrecoverable_stays_terminal() {
    let run = run_order_workflow(WorkflowOptions {
        manual_intervention_action: Some(ManualInterventionTestAction::AcknowledgeUnrecoverable),
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(
        run.output.terminal_status,
        OrderTerminalStatus::ManualInterventionRequired
    );
    let acknowledged = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load acknowledged manual intervention order");
    assert_eq!(
        acknowledged.status,
        RouterOrderStatus::ManualInterventionRequired
    );
    let attempt = run
        .db
        .orders()
        .get_latest_execution_attempt(run.order_id)
        .await
        .expect("load latest attempt")
        .expect("manual intervention attempt");
    assert!(
        attempt
            .failure_reason
            .get("manual_intervention_terminal_ack")
            .is_some(),
        "acknowledge signal should persist terminal acknowledgement"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_completes_across_step_via_hint() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::AcrossBaseEthToEthereumUsdc,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let across_operation = wait_for_provider_operation_status(
        &run.db,
        run.order_id,
        ProviderOperationType::AcrossBridge,
        ProviderOperationStatus::Completed,
        Duration::from_secs(5),
    )
    .await;
    let step = run
        .db
        .orders()
        .get_execution_step(
            across_operation
                .execution_step_id
                .expect("Across operation should be linked to a step"),
        )
        .await
        .expect("load Across step");
    assert_eq!(step.status, OrderExecutionStepStatus::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_completes_across_step_via_polling_fallback() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::AcrossBaseEthToEthereumUsdc,
        suppress_across_hint_signal: true,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let across_operation =
        provider_operation_by_type(&run.db, run.order_id, ProviderOperationType::AcrossBridge)
            .await;
    assert_eq!(across_operation.status, ProviderOperationStatus::Completed);
    let step = run
        .db
        .orders()
        .get_execution_step(
            across_operation
                .execution_step_id
                .expect("Across operation should be linked to a step"),
        )
        .await
        .expect("load Across step");
    assert_eq!(step.status, OrderExecutionStepStatus::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_recovers_across_lost_intent_from_onchain_log() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::AcrossBaseEthToEthereumUsdc,
        simulate_across_lost_intent_checkpoint: true,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let across_operation =
        provider_operation_by_type(&run.db, run.order_id, ProviderOperationType::AcrossBridge)
            .await;
    assert_eq!(across_operation.status, ProviderOperationStatus::Completed);
    assert!(
        across_operation.provider_ref.is_some(),
        "recovered Across operation should persist deposit id as provider_ref"
    );
    let observed_state = across_operation.observed_state.to_string();
    let response = across_operation.response.to_string();
    assert!(
        observed_state.contains("across_deposit_log_recovery")
            || response.contains("provider_receipt_checkpoint"),
        "completed Across operation should retain the log-recovery marker or checkpoint response: observed_state={}, response={}",
        across_operation.observed_state,
        across_operation.response,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_completes_unit_deposit_via_polling_fallback() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::UnitBitcoinToBaseEth,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed Unit order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let unit_deposit_operation =
        provider_operation_by_type(&run.db, run.order_id, ProviderOperationType::UnitDeposit).await;
    assert_eq!(
        unit_deposit_operation.status,
        ProviderOperationStatus::Completed
    );
    let unit_deposit_step = run
        .db
        .orders()
        .get_execution_step(
            unit_deposit_operation
                .execution_step_id
                .expect("UnitDeposit operation should be linked to a step"),
        )
        .await
        .expect("load UnitDeposit step");
    assert_eq!(
        unit_deposit_step.status,
        OrderExecutionStepStatus::Completed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_retries_failed_step_then_completes() {
    let run = run_order_workflow(WorkflowOptions {
        velora_transaction_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].attempt_index, 1);
    assert_eq!(
        attempts[0].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(attempts[1].attempt_index, 2);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Completed);

    let first_attempt_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[0].id)
        .await
        .expect("load first attempt steps");
    assert!(
        first_attempt_steps
            .iter()
            .any(|step| step.status == OrderExecutionStepStatus::Superseded),
        "first attempt failed suffix should be superseded"
    );
    let retry_attempt_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .expect("load retry attempt steps");
    assert!(
        retry_attempt_steps
            .iter()
            .all(|step| step.status == OrderExecutionStepStatus::Completed),
        "retry attempt should complete every cloned step"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refunds_after_retry_exhaustion() {
    let run = run_order_workflow(WorkflowOptions {
        velora_transaction_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS * 2,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Refunded);
    let refunded = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load refunded order");
    assert_eq!(refunded.status, RouterOrderStatus::Refunded);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 3);
    assert_eq!(attempts[0].attempt_index, 1);
    assert_eq!(
        attempts[0].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(attempts[1].attempt_index, 2);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(attempts[2].attempt_index, 3);
    assert_eq!(
        attempts[2].attempt_kind,
        OrderExecutionAttemptKind::RefundRecovery
    );
    assert_eq!(attempts[2].status, OrderExecutionAttemptStatus::Completed);

    let first_attempt_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[0].id)
        .await
        .expect("load first attempt steps");
    assert!(
        first_attempt_steps
            .iter()
            .any(|step| step.status == OrderExecutionStepStatus::Superseded),
        "first attempt failed suffix should be superseded"
    );
    let second_attempt_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .expect("load second attempt steps");
    assert!(
        second_attempt_steps
            .iter()
            .any(|step| step.status == OrderExecutionStepStatus::Failed),
        "second attempt should retain the terminal failed step"
    );
    let refund_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[2].id)
        .await
        .expect("load refund attempt steps");
    assert_eq!(refund_steps.len(), 1);
    let refund_step = &refund_steps[0];
    assert_eq!(refund_step.step_type, OrderExecutionStepType::Refund);
    assert_eq!(refund_step.provider, "internal");
    assert_eq!(refund_step.status, OrderExecutionStepStatus::Completed);
    assert!(
        run.refund_address_balance >= U256::from_str_radix(ORDER_AMOUNT_IN_WEI, 10).unwrap(),
        "refund address should receive at least the original funded amount"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refunds_external_custody_across_after_retry_exhaustion() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::AcrossBaseEthToEthereumUsdc,
        velora_transaction_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS * 2,
        expect_external_custody_across_refund: true,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Refunded);
    let refunded = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load refunded order");
    assert_eq!(refunded.status, RouterOrderStatus::Refunded);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 3);
    assert_eq!(
        attempts[2].attempt_kind,
        OrderExecutionAttemptKind::RefundRecovery
    );
    assert_eq!(attempts[2].status, OrderExecutionAttemptStatus::Completed);

    let refund_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[2].id)
        .await
        .expect("load external refund attempt steps");
    assert_eq!(refund_steps.len(), 1);
    let refund_step = &refund_steps[0];
    assert_eq!(refund_step.step_type, OrderExecutionStepType::AcrossBridge);
    assert_eq!(refund_step.provider, "across");
    assert_eq!(refund_step.status, OrderExecutionStepStatus::Completed);
    assert!(
        run.refund_address_balance >= U256::from_str_radix(ORDER_AMOUNT_IN_WEI, 10).unwrap(),
        "refund address should receive the bridged external-custody balance"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refunds_external_custody_cctp_after_retry_exhaustion() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::CctpBaseUsdcToArbitrumEth,
        velora_transaction_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS * 2,
        expect_external_custody_cctp_refund: true,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Refunded);
    let refunded = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load CCTP-refunded order");
    assert_eq!(refunded.status, RouterOrderStatus::Refunded);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 3);
    assert_eq!(
        attempts[2].attempt_kind,
        OrderExecutionAttemptKind::RefundRecovery
    );
    assert_eq!(attempts[2].status, OrderExecutionAttemptStatus::Completed);

    let refund_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[2].id)
        .await
        .expect("load CCTP refund attempt steps");
    assert_eq!(
        refund_steps
            .iter()
            .map(|step| step.step_type)
            .collect::<Vec<_>>(),
        vec![
            OrderExecutionStepType::CctpBurn,
            OrderExecutionStepType::CctpReceive
        ]
    );
    let expected_refund_amount = refund_steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::CctpReceive)
        .and_then(|step| step.amount_in.as_deref())
        .and_then(|amount| U256::from_str_radix(amount, 10).ok())
        .expect("CCTP receive step should carry expected refund amount");
    assert!(
        refund_steps
            .iter()
            .all(|step| step.status == OrderExecutionStepStatus::Completed),
        "CCTP burn and receive should both complete"
    );
    assert!(
        run.refund_address_base_usdc_balance >= expected_refund_amount,
        "refund address should receive the CCTP-bridged ExternalCustody USDC balance"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refunds_external_custody_universal_router_after_retry_exhaustion() {
    let run = run_order_workflow(WorkflowOptions {
        velora_transaction_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS * 2,
        expect_external_custody_velora_refund: true,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Refunded);
    let refunded = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load UniversalRouter-refunded order");
    assert_eq!(refunded.status, RouterOrderStatus::Refunded);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 3);
    assert_eq!(
        attempts[2].attempt_kind,
        OrderExecutionAttemptKind::RefundRecovery
    );
    assert_eq!(attempts[2].status, OrderExecutionAttemptStatus::Completed);

    let refund_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[2].id)
        .await
        .expect("load UniversalRouter refund attempt steps");
    assert_eq!(refund_steps.len(), 1);
    let refund_step = &refund_steps[0];
    assert_eq!(
        refund_step.step_type,
        OrderExecutionStepType::UniversalRouterSwap
    );
    assert_eq!(refund_step.provider, "velora");
    assert_eq!(refund_step.status, OrderExecutionStepStatus::Completed);
    let expected_refund_amount = refund_step
        .request
        .get("amount_out")
        .and_then(serde_json::Value::as_str)
        .and_then(|amount| U256::from_str_radix(amount, 10).ok())
        .expect("UniversalRouter refund step should carry amount_out");
    assert!(
        run.refund_address_balance >= expected_refund_amount,
        "refund address should receive the UniversalRouter-swapped ExternalCustody balance"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refunds_external_custody_across_then_universal_router_after_retry_exhaustion(
) {
    let run = run_order_workflow(WorkflowOptions {
        velora_transaction_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS * 2,
        expect_external_custody_across_then_velora_refund: true,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Refunded);
    let refunded = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load multi-hop refunded order");
    assert_eq!(refunded.status, RouterOrderStatus::Refunded);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 3);
    assert_eq!(
        attempts[2].attempt_kind,
        OrderExecutionAttemptKind::RefundRecovery
    );
    assert_eq!(attempts[2].status, OrderExecutionAttemptStatus::Completed);

    let refund_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[2].id)
        .await
        .expect("load multi-hop refund attempt steps");
    assert_eq!(
        refund_steps
            .iter()
            .map(|step| step.step_type)
            .collect::<Vec<_>>(),
        vec![
            OrderExecutionStepType::AcrossBridge,
            OrderExecutionStepType::UniversalRouterSwap
        ]
    );
    assert!(
        refund_steps
            .iter()
            .all(|step| step.status == OrderExecutionStepStatus::Completed),
        "Across and UniversalRouter refund steps should both complete"
    );
    let expected_refund_amount = refund_steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::UniversalRouterSwap)
        .and_then(|step| step.request.get("amount_out"))
        .and_then(serde_json::Value::as_str)
        .and_then(|amount| U256::from_str_radix(amount, 10).ok())
        .expect("UniversalRouter refund step should carry amount_out");
    assert!(
        run.refund_address_balance >= expected_refund_amount,
        "refund address should receive the two-hop ExternalCustody refund output"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refunds_hyperliquid_spot_unit_withdrawal_after_retry_exhaustion() {
    let run = run_order_workflow(WorkflowOptions {
        velora_transaction_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS * 2,
        expect_hyperliquid_spot_unit_refund: true,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Refunded);
    let refunded = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load refunded order");
    assert_eq!(refunded.status, RouterOrderStatus::Refunded);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 3);
    assert_eq!(
        attempts[2].attempt_kind,
        OrderExecutionAttemptKind::RefundRecovery
    );
    assert_eq!(attempts[2].status, OrderExecutionAttemptStatus::Completed);

    let refund_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[2].id)
        .await
        .expect("load HyperliquidSpot refund attempt steps");
    assert_eq!(refund_steps.len(), 1);
    let refund_step = &refund_steps[0];
    assert_eq!(
        refund_step.step_type,
        OrderExecutionStepType::UnitWithdrawal
    );
    assert_eq!(refund_step.provider, "unit");
    assert_eq!(refund_step.status, OrderExecutionStepStatus::Completed);
    assert!(
        run.refund_address_balance >= U256::from_str_radix(ORDER_AMOUNT_IN_WEI, 10).unwrap(),
        "refund address should receive the Unit withdrawal from HyperliquidSpot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refreshes_stale_quote_then_completes() {
    let run = run_order_workflow(WorkflowOptions {
        expire_quote_legs: true,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[0].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::RefreshedExecution
    );
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Completed);

    let stale_attempt_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[0].id)
        .await
        .expect("load stale attempt steps");
    assert!(
        stale_attempt_steps.iter().any(|step| {
            step.status == OrderExecutionStepStatus::Superseded
                && step.error.get("reason").and_then(serde_json::Value::as_str)
                    == Some("superseded_by_stale_provider_quote_refresh")
        }),
        "stale attempt failed suffix should be superseded by quote refresh"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refreshes_exact_out_stale_quote_then_completes() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::VeloraBaseEthToBaseUsdcExactOut,
        expire_quote_legs: true,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed ExactOut order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let original_quote = match run
        .db
        .orders()
        .get_router_order_quote(run.order_id)
        .await
        .expect("load ExactOut order quote")
    {
        RouterOrderQuote::MarketOrder(quote) => quote,
        RouterOrderQuote::LimitOrder(_) => panic!("expected market quote"),
    };
    assert_eq!(original_quote.order_kind.to_db_string(), "exact_out");

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load ExactOut refresh attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::RefreshedExecution
    );
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Completed);

    let refreshed_legs = run
        .db
        .orders()
        .get_execution_legs_for_attempt(attempts[1].id)
        .await
        .expect("load refreshed ExactOut legs");
    assert_eq!(refreshed_legs.len(), 1);
    assert_eq!(
        refreshed_legs[0].expected_amount_out,
        original_quote.amount_out
    );
    assert_eq!(refreshed_legs[0].amount_in, original_quote.amount_in);

    let refreshed_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .expect("load refreshed ExactOut steps");
    assert_eq!(refreshed_steps.len(), 1);
    assert_eq!(
        refreshed_steps[0].step_type,
        OrderExecutionStepType::UniversalRouterSwap
    );
    assert_eq!(
        refreshed_steps[0].status,
        OrderExecutionStepStatus::Completed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refreshes_stale_quote_multi_leg_then_completes() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::AcrossBaseEthToEthereumUsdc,
        expire_quote_legs: true,
        expire_quote_transition_kinds: vec!["universal_router_swap"],
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[0].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::RefreshedExecution
    );
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Completed);

    let stale_attempt_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[0].id)
        .await
        .expect("load stale attempt steps");
    let across_step = stale_attempt_steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::AcrossBridge)
        .expect("stale attempt should contain the completed Across step");
    assert_eq!(across_step.status, OrderExecutionStepStatus::Completed);
    let stale_velora_step = stale_attempt_steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::UniversalRouterSwap)
        .expect("stale attempt should contain the superseded Velora step");
    assert_eq!(
        stale_velora_step.status,
        OrderExecutionStepStatus::Superseded
    );
    assert_eq!(
        stale_velora_step
            .error
            .get("reason")
            .and_then(serde_json::Value::as_str),
        Some("superseded_by_stale_provider_quote_refresh")
    );

    let stale_attempt_legs = run
        .db
        .orders()
        .get_execution_legs_for_attempt(attempts[0].id)
        .await
        .expect("load stale attempt legs");
    let completed_across_leg = stale_attempt_legs
        .iter()
        .find(|leg| leg.leg_type == "across_bridge")
        .expect("stale attempt should contain an Across leg");
    let actual_across_output = completed_across_leg
        .actual_amount_out
        .as_ref()
        .expect("completed Across leg should record actual output");

    let refreshed_attempt_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .expect("load refreshed attempt steps");
    assert_eq!(refreshed_attempt_steps.len(), 1);
    assert_eq!(
        refreshed_attempt_steps[0].step_type,
        OrderExecutionStepType::UniversalRouterSwap
    );
    assert_eq!(
        refreshed_attempt_steps[0].status,
        OrderExecutionStepStatus::Completed
    );
    assert_eq!(
        refreshed_attempt_steps[0].step_index,
        stale_velora_step.step_index
    );

    let refreshed_attempt_legs = run
        .db
        .orders()
        .get_execution_legs_for_attempt(attempts[1].id)
        .await
        .expect("load refreshed attempt legs");
    assert_eq!(refreshed_attempt_legs.len(), 1);
    assert_eq!(refreshed_attempt_legs[0].leg_index, 1);
    assert_eq!(
        refreshed_attempt_legs[0].amount_in.as_str(),
        actual_across_output
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refreshes_stale_cctp_quote_then_completes() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::CctpBaseUsdcToArbitrumEth,
        expire_quote_legs: true,
        expire_quote_transition_kinds: vec!["cctp_bridge"],
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed CCTP refresh order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load CCTP refresh attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::RefreshedExecution
    );
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Completed);

    let refreshed_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .expect("load refreshed CCTP steps");
    assert!(
        refreshed_steps
            .iter()
            .any(|step| step.step_type == OrderExecutionStepType::CctpBurn
                && step.status == OrderExecutionStepStatus::Completed),
        "refreshed CCTP attempt should execute burn step"
    );
    assert!(
        refreshed_steps
            .iter()
            .any(|step| step.step_type == OrderExecutionStepType::CctpReceive
                && step.status == OrderExecutionStepStatus::Completed),
        "refreshed CCTP attempt should execute receive step"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refreshes_unit_deposit_quote_with_fee_reserve_then_completes() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::UnitBitcoinToBaseEth,
        expire_quote_legs: true,
        expire_quote_transition_kinds: vec!["unit_deposit"],
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load Unit refresh attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::RefreshedExecution
    );
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Completed);

    let original_quote = match run
        .db
        .orders()
        .get_router_order_quote(run.order_id)
        .await
        .expect("load Unit order quote")
    {
        RouterOrderQuote::MarketOrder(quote) => quote,
        RouterOrderQuote::LimitOrder(_) => panic!("expected market quote"),
    };
    let original_reserve = original_quote
        .provider_quote
        .get("legs")
        .and_then(serde_json::Value::as_array)
        .and_then(|legs| {
            legs.iter().find_map(|leg| {
                (leg.get("transition_kind")
                    .and_then(serde_json::Value::as_str)
                    == Some("unit_deposit"))
                .then(|| {
                    leg.pointer("/raw/source_fee_reserve/amount")
                        .and_then(serde_json::Value::as_str)
                        .map(ToString::to_string)
                })
                .flatten()
            })
        })
        .expect("original Unit quote should carry a Bitcoin fee reserve");

    let refreshed_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .expect("load refreshed Unit steps");
    let unit_deposit = refreshed_steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::UnitDeposit)
        .expect("refreshed Unit attempt should include UnitDeposit");
    assert_eq!(unit_deposit.status, OrderExecutionStepStatus::Completed);
    assert_eq!(
        unit_deposit
            .request
            .pointer("/source_fee_reserve/amount")
            .and_then(serde_json::Value::as_str),
        Some(original_reserve.as_str()),
        "refreshed UnitDeposit should preserve the original Bitcoin fee reserve"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refreshes_hyperliquid_bridge_quote_then_completes() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::CctpBaseUsdcToBitcoin,
        expire_quote_legs: true,
        expire_quote_transition_kinds: vec!["hyperliquid_bridge_deposit"],
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load Hyperliquid bridge refresh attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::RefreshedExecution
    );
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Completed);

    let primary_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[0].id)
        .await
        .expect("load primary Hyperliquid bridge refresh steps");
    assert!(
        primary_steps
            .iter()
            .any(|step| step.step_type == OrderExecutionStepType::CctpReceive
                && step.status == OrderExecutionStepStatus::Completed),
        "primary attempt should complete CCTP before refreshing the stale HL bridge leg"
    );
    assert!(
        primary_steps.iter().any(|step| step.step_type
            == OrderExecutionStepType::HyperliquidBridgeDeposit
            && step.status == OrderExecutionStepStatus::Superseded),
        "primary HL bridge step should be superseded by stale-quote refresh"
    );

    let refreshed_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .expect("load refreshed Hyperliquid bridge steps");
    for step_type in [
        OrderExecutionStepType::HyperliquidBridgeDeposit,
        OrderExecutionStepType::HyperliquidTrade,
        OrderExecutionStepType::UnitWithdrawal,
    ] {
        assert!(
            refreshed_steps
                .iter()
                .any(|step| step.step_type == step_type
                    && step.status == OrderExecutionStepStatus::Completed),
            "refreshed attempt should complete {step_type:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refreshes_hyperliquid_trade_quote_then_completes() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::UnitBitcoinToBaseEth,
        expire_quote_legs: true,
        expire_quote_transition_kinds: vec!["hyperliquid_trade"],
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed HyperliquidTrade refresh order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load HyperliquidTrade refresh attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[0].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::RefreshedExecution
    );
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Completed);

    let primary_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[0].id)
        .await
        .expect("load primary HyperliquidTrade refresh steps");
    assert!(
        primary_steps
            .iter()
            .any(|step| step.step_type == OrderExecutionStepType::UnitDeposit
                && step.status == OrderExecutionStepStatus::Completed),
        "primary attempt should complete UnitDeposit before refreshing the stale HL trade leg"
    );
    assert!(
        primary_steps.iter().any(
            |step| step.step_type == OrderExecutionStepType::HyperliquidTrade
                && step.status == OrderExecutionStepStatus::Superseded
        ),
        "primary HL trade step should be superseded by stale-quote refresh"
    );

    let primary_legs = run
        .db
        .orders()
        .get_execution_legs_for_attempt(attempts[0].id)
        .await
        .expect("load primary HyperliquidTrade refresh legs");
    let unit_deposit_leg = primary_legs
        .iter()
        .find(|leg| leg.leg_type == "unit_deposit")
        .expect("primary attempt should include the completed UnitDeposit leg");
    let actual_unit_output = unit_deposit_leg
        .actual_amount_out
        .as_ref()
        .expect("completed UnitDeposit leg should record actual output for HL trade refresh");

    let refreshed_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .expect("load refreshed HyperliquidTrade steps");
    for step_type in [
        OrderExecutionStepType::HyperliquidTrade,
        OrderExecutionStepType::UnitWithdrawal,
    ] {
        assert!(
            refreshed_steps
                .iter()
                .any(|step| step.step_type == step_type
                    && step.status == OrderExecutionStepStatus::Completed),
            "refreshed attempt should complete {step_type:?}"
        );
    }

    let refreshed_legs = run
        .db
        .orders()
        .get_execution_legs_for_attempt(attempts[1].id)
        .await
        .expect("load refreshed HyperliquidTrade legs");
    let refreshed_trade = refreshed_legs
        .iter()
        .find(|leg| leg.leg_type == "hyperliquid_trade")
        .expect("refreshed attempt should include the HyperliquidTrade leg");
    assert_eq!(
        refreshed_trade.amount_in.as_str(),
        actual_unit_output,
        "refreshed HL trade should carry the completed UnitDeposit output forward"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_completes_arbitrum_usdc_hyperliquid_trade_path() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::HyperliquidArbitrumUsdcToBitcoin,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed Arbitrum USDC order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load Arbitrum USDC attempts");
    assert_eq!(attempts.len(), 1);
    let steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[0].id)
        .await
        .expect("load Arbitrum USDC steps");
    let bridge_deposit = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::HyperliquidBridgeDeposit)
        .expect("Arbitrum USDC route should bridge into Hyperliquid");
    assert_eq!(
        bridge_deposit.status,
        OrderExecutionStepStatus::Completed,
        "Hyperliquid bridge deposit should complete before the trade"
    );
    let trade = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::HyperliquidTrade)
        .expect("Arbitrum USDC route should include HyperliquidTrade");
    assert_eq!(trade.status, OrderExecutionStepStatus::Completed);
    assert_eq!(
        trade
            .request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::SourceDeposit.to_db_string())
    );
    assert!(
        trade
            .request
            .get("hyperliquid_custody_vault_id")
            .and_then(Value::as_str)
            .is_some(),
        "source-deposit HyperliquidTrade must be hydrated with the funding vault id"
    );
    assert!(
        trade
            .request
            .get("hyperliquid_custody_vault_address")
            .and_then(Value::as_str)
            .is_some(),
        "source-deposit HyperliquidTrade must be hydrated with the funding vault address"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refresh_untenable_routes_to_refund() {
    let run = run_order_workflow(WorkflowOptions {
        expire_quote_legs: true,
        refreshed_eth_usd_micro: Some(1_000_000),
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(
        run.output.terminal_status,
        OrderTerminalStatus::RefundRequired
    );
    let refund_required = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load refund-required order");
    assert_eq!(refund_required.status, RouterOrderStatus::RefundRequired);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refresh_multi_leg_untenable_routes_to_refund() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::AcrossBaseEthToEthereumUsdc,
        expire_quote_legs: true,
        expire_quote_transition_kinds: vec!["universal_router_swap"],
        refreshed_eth_usd_micro: Some(1_000_000),
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(
        run.output.terminal_status,
        OrderTerminalStatus::RefundRequired
    );
    let refund_required = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load refund-required order");
    assert_eq!(refund_required.status, RouterOrderStatus::RefundRequired);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
}

async fn run_order_workflow(options: WorkflowOptions) -> WorkflowRun {
    ensure_temporal_up();

    let postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let db = Database::connect(&database_url, 8, 1)
        .await
        .expect("connect test database");

    let (devnet, _) = RiftDevnet::builder()
        .using_esplora(true)
        .build()
        .await
        .expect("devnet setup failed");
    install_mock_usdc_clone(
        &devnet.base,
        BASE_USDC_ADDRESS.parse().expect("valid Base USDC address"),
    )
    .await;
    let across_enabled = matches!(options.route, WorkflowRoute::AcrossBaseEthToEthereumUsdc)
        || options.expect_external_custody_across_then_velora_refund;
    let cctp_enabled = matches!(
        options.route,
        WorkflowRoute::CctpBaseUsdcToArbitrumEth | WorkflowRoute::CctpBaseUsdcToBitcoin
    );
    let arbitrum_enabled = cctp_enabled
        || matches!(
            options.route,
            WorkflowRoute::HyperliquidArbitrumUsdcToBitcoin
        );
    let unit_enabled = matches!(options.route, WorkflowRoute::UnitBitcoinToBaseEth)
        || matches!(options.route, WorkflowRoute::CctpBaseUsdcToBitcoin)
        || matches!(
            options.route,
            WorkflowRoute::HyperliquidArbitrumUsdcToBitcoin
        )
        || options.expect_hyperliquid_spot_unit_refund;
    let bitcoin_enabled = matches!(
        options.route,
        WorkflowRoute::UnitBitcoinToBaseEth
            | WorkflowRoute::CctpBaseUsdcToBitcoin
            | WorkflowRoute::HyperliquidArbitrumUsdcToBitcoin
    );
    if across_enabled {
        install_mock_usdc_clone(
            &devnet.ethereum,
            ETHEREUM_USDC_ADDRESS
                .parse()
                .expect("valid Ethereum USDC address"),
        )
        .await;
    }
    if arbitrum_enabled {
        install_mock_usdc_clone(
            &devnet.arbitrum,
            ARBITRUM_USDC_ADDRESS
                .parse()
                .expect("valid Arbitrum USDC address"),
        )
        .await;
    }
    let mocks = spawn_mocks(&devnet, &options).await;
    let settings = Arc::new(test_settings());
    let hyperliquid_enabled = unit_enabled;
    let chain_registry = Arc::new(
        test_chain_registry(
            &devnet,
            options.route,
            hyperliquid_enabled,
            across_enabled,
            arbitrum_enabled,
            bitcoin_enabled,
        )
        .await,
    );
    let action_providers = Arc::new(test_action_providers(
        &mocks,
        options.route,
        hyperliquid_enabled,
        across_enabled,
        cctp_enabled,
    ));
    let mut custody_executor =
        CustodyActionExecutor::new(db.clone(), settings.clone(), chain_registry.clone());
    if hyperliquid_enabled {
        custody_executor =
            custody_executor.with_hyperliquid_runtime(Some(HyperliquidRuntimeConfig::new(
                mocks.base_url().to_string(),
                HyperliquidCallNetwork::Testnet,
            )));
    }
    let custody_executor = Arc::new(custody_executor);

    let seeded = match options.route {
        WorkflowRoute::VeloraBaseEthToBaseUsdc => {
            seed_funded_single_step_order(
                &db,
                settings.clone(),
                chain_registry.clone(),
                action_providers.clone(),
                &devnet,
                MarketOrderQuoteKind::ExactIn {
                    amount_in: ORDER_AMOUNT_IN_WEI.to_string(),
                    slippage_bps: Some(100),
                },
            )
            .await
        }
        WorkflowRoute::VeloraBaseEthToBaseUsdcExactOut => {
            seed_funded_single_step_order(
                &db,
                settings.clone(),
                chain_registry.clone(),
                action_providers.clone(),
                &devnet,
                MarketOrderQuoteKind::ExactOut {
                    amount_out: ORDER_USDC_AMOUNT_RAW.to_string(),
                    slippage_bps: Some(100),
                },
            )
            .await
        }
        WorkflowRoute::AcrossBaseEthToEthereumUsdc => {
            seed_funded_across_order(
                &db,
                settings.clone(),
                chain_registry.clone(),
                action_providers.clone(),
                &devnet,
            )
            .await
        }
        WorkflowRoute::CctpBaseUsdcToArbitrumEth => {
            seed_funded_cctp_order(
                &db,
                settings.clone(),
                chain_registry.clone(),
                action_providers.clone(),
                &devnet,
            )
            .await
        }
        WorkflowRoute::CctpBaseUsdcToBitcoin => {
            seed_funded_cctp_hyperliquid_unit_order(
                &db,
                settings.clone(),
                chain_registry.clone(),
                action_providers.clone(),
                &devnet,
            )
            .await
        }
        WorkflowRoute::HyperliquidArbitrumUsdcToBitcoin => {
            seed_funded_arbitrum_hyperliquid_unit_order(
                &db,
                settings.clone(),
                chain_registry.clone(),
                action_providers.clone(),
                &devnet,
            )
            .await
        }
        WorkflowRoute::UnitBitcoinToBaseEth => {
            seed_funded_unit_order(
                &db,
                settings.clone(),
                chain_registry.clone(),
                action_providers.clone(),
                &devnet,
            )
            .await
        }
    };
    let order_id = seeded.order_id;
    if options.expect_hyperliquid_spot_unit_refund {
        seed_hyperliquid_spot_refund_position(
            &custody_executor,
            &mocks,
            &devnet,
            order_id,
            &seeded.funding_vault_address,
        )
        .await;
    }
    if options.expect_external_custody_velora_refund {
        seed_external_custody_universal_router_refund_position(
            &custody_executor,
            &devnet,
            order_id,
            &seeded.funding_vault_address,
        )
        .await;
    }
    if options.expect_external_custody_across_then_velora_refund {
        seed_external_custody_across_then_universal_router_refund_position(
            &custody_executor,
            &devnet,
            order_id,
            &seeded.funding_vault_address,
        )
        .await;
    }
    if options.expire_quote_legs {
        expire_market_order_quote_legs(
            &database_url,
            order_id,
            &options.expire_quote_transition_kinds,
        )
        .await;
    }
    if let Some(usd_micro) = options.refreshed_eth_usd_micro {
        mocks.set_velora_usd_price_micro("ETH", usd_micro).await;
    }

    let task_queue = format!("{DEFAULT_TASK_QUEUE}-{}", Uuid::now_v7());
    let connection = TemporalConnection {
        temporal_address: "http://127.0.0.1:7233".to_string(),
        namespace: "default".to_string(),
    };
    let activity_deps = OrderActivityDeps::new(
        db.clone(),
        action_providers,
        custody_executor,
        chain_registry.clone(),
    );
    if options.manual_intervention_action.is_some() {
        seed_manual_intervention_state(&db, &activity_deps, order_id).await;
    }
    let database_url_for_workflow = database_url.clone();

    let local = tokio::task::LocalSet::new();
    let db_for_workflow = db.clone();
    let settings_for_workflow = settings.clone();
    let chain_registry_for_workflow = chain_registry.clone();
    let devnet_for_workflow = &devnet;
    let output = local
        .run_until(async move {
            let mut built = build_worker(
                &connection,
                &task_queue,
                OrderActivities::new(activity_deps),
            )
            .await
            .expect("build Temporal worker");
            let shutdown_worker = built.worker.shutdown_handle();
            let worker_task = tokio::task::spawn_local(async move { built.worker.run().await });
            sleep(Duration::from_millis(500)).await;

            let client = connect_client(&connection)
                .await
                .expect("connect Temporal client");
            let workflow_id = order_workflow_id(order_id.into());
            let handle = client
                .start_workflow(
                    OrderWorkflow::run,
                    OrderWorkflowInput {
                        order_id: order_id.into(),
                    },
                    workflow_start_options(&task_queue, &workflow_id),
                )
                .await
                .expect("start OrderWorkflow");
            if let Some(action) = options.manual_intervention_action {
                let signal_result = match action {
                    ManualInterventionTestAction::Release => {
                        handle
                            .signal(
                                OrderWorkflow::manual_intervention_release,
                                ManualReleaseSignal {
                                    reason: "operator fixed manual release test".to_string(),
                                    operator_id: Some("temporal-test".to_string()),
                                    requested_at: Utc::now(),
                                },
                                WorkflowSignalOptions::default(),
                            )
                            .await
                    }
                    ManualInterventionTestAction::TriggerRefund => {
                        handle
                            .signal(
                                OrderWorkflow::manual_refund_trigger,
                                ManualTriggerRefundSignal {
                                    reason: "operator requested refund in manual test".to_string(),
                                    operator_id: Some("temporal-test".to_string()),
                                    requested_at: Utc::now(),
                                    refund_kind_hint: None,
                                },
                                WorkflowSignalOptions::default(),
                            )
                            .await
                    }
                    ManualInterventionTestAction::AcknowledgeUnrecoverable => {
                        handle
                            .signal(
                                OrderWorkflow::acknowledge_unrecoverable,
                                AcknowledgeUnrecoverableSignal {
                                    reason: "operator acknowledged unrecoverable test".to_string(),
                                    operator_id: Some("temporal-test".to_string()),
                                    requested_at: Utc::now(),
                                },
                                WorkflowSignalOptions::default(),
                            )
                            .await
                    }
                };
                assert_signal_sent_or_workflow_completed(
                    signal_result,
                    "send manual intervention signal",
                );
            }
            if matches!(options.route, WorkflowRoute::AcrossBaseEthToEthereumUsdc) {
                let handle_before_hint = client.get_workflow_handle::<OrderWorkflow>(&workflow_id);
                let across_operation = tokio::select! {
                    operation = wait_for_provider_operation(
                        &db_for_workflow,
                        order_id,
                        ProviderOperationType::AcrossBridge,
                        Duration::from_secs(90),
                    ) => operation,
                    result = handle_before_hint.get_result(WorkflowGetResultOptions::default()) => {
                        let state = workflow_state_summary(&db_for_workflow, order_id).await;
                        panic!("OrderWorkflow ended before the Across provider operation was persisted: {result:?}\n{state}");
                    }
                };
                let across_operation = if options.simulate_across_lost_intent_checkpoint {
                    let across_operation = wait_for_provider_operation_status(
                        &db_for_workflow,
                        order_id,
                        ProviderOperationType::AcrossBridge,
                        ProviderOperationStatus::WaitingExternal,
                        Duration::from_secs(90),
                    )
                    .await;
                    simulate_across_lost_intent_checkpoint(
                        &database_url_for_workflow,
                        &db_for_workflow,
                        &across_operation,
                    )
                    .await
                } else {
                    across_operation
                };
                wait_for_across_deposit_indexed(
                    &mocks,
                    Address::from_str(&seeded.funding_vault_address)
                        .expect("funding vault EVM address"),
                    Duration::from_secs(90),
                )
                .await;
                fund_destination_execution_vault_from_across(
                    &db_for_workflow,
                    devnet_for_workflow,
                    order_id,
                    &across_operation,
                )
                .await;
                if !options.suppress_across_hint_signal {
                    let signal_result = handle
                        .signal(
                            OrderWorkflow::provider_operation_hint,
                            ProviderOperationHintSignal {
                                order_id: order_id.into(),
                                hint_id: Uuid::now_v7().into(),
                                provider_operation_id: Some(across_operation.id.into()),
                                provider: ProviderKind::Bridge,
                                hint_kind: ProviderHintKind::AcrossFill,
                                provider_ref: across_operation.provider_ref,
                                evidence: None,
                            },
                            WorkflowSignalOptions::default(),
                        )
                        .await;
                    assert_signal_sent_or_workflow_completed(
                        signal_result,
                        "send Across provider-operation hint signal",
                    );
                }
            }
            if matches!(
                options.route,
                WorkflowRoute::CctpBaseUsdcToArbitrumEth | WorkflowRoute::CctpBaseUsdcToBitcoin
            ) {
                let cctp_operation = wait_for_order_cctp_burn(
                    &client,
                    &db_for_workflow,
                    &mocks,
                    &workflow_id,
                    order_id,
                )
                .await;
                if options.expect_external_custody_cctp_refund {
                    drain_base_usdc_funding_vault_residual(
                        &db_for_workflow,
                        &settings_for_workflow,
                        &chain_registry_for_workflow,
                        order_id,
                        &seeded.funding_vault_address,
                    )
                    .await;
                }
                signal_order_cctp_attestation(&client, &workflow_id, order_id, cctp_operation)
                    .await;
            }
            if matches!(
                options.route,
                WorkflowRoute::CctpBaseUsdcToBitcoin
                    | WorkflowRoute::HyperliquidArbitrumUsdcToBitcoin
            ) {
                signal_order_hyperliquid_bridge_deposit_observation(
                    &client,
                    &db_for_workflow,
                    &mocks,
                    &workflow_id,
                    order_id,
                )
                .await;
            }
            if matches!(
                options.route,
                WorkflowRoute::UnitBitcoinToBaseEth
                    | WorkflowRoute::CctpBaseUsdcToBitcoin
                    | WorkflowRoute::HyperliquidArbitrumUsdcToBitcoin
            ) {
                signal_order_unit_withdrawal_observation(
                    &client,
                    &db_for_workflow,
                    &mocks,
                    &workflow_id,
                    order_id,
                )
                .await;
            }
            if options.expect_external_custody_across_refund {
                signal_external_custody_refund_across_fill(
                    &client,
                    &db_for_workflow,
                    &mocks,
                    devnet_for_workflow,
                    &seeded.funding_vault_address,
                    order_id,
                    false,
                )
                .await;
            }
            if options.expect_external_custody_across_then_velora_refund {
                signal_external_custody_refund_across_fill(
                    &client,
                    &db_for_workflow,
                    &mocks,
                    devnet_for_workflow,
                    &seeded.funding_vault_address,
                    order_id,
                    true,
                )
                .await;
            }
            if options.expect_external_custody_cctp_refund {
                signal_external_custody_refund_cctp_attestation(
                    &client,
                    &db_for_workflow,
                    &mocks,
                    &workflow_id,
                    order_id,
                )
                .await;
            }
            if options.expect_hyperliquid_spot_unit_refund {
                signal_hyperliquid_spot_refund_unit_withdrawal(
                    &client,
                    &db_for_workflow,
                    &mocks,
                    order_id,
                )
                .await;
            }
            let output: OrderWorkflowOutput = timeout(
                ORDER_WORKFLOW_TEST_TIMEOUT,
                handle.get_result(WorkflowGetResultOptions::default()),
            )
            .await
            .expect("OrderWorkflow timed out")
            .expect("OrderWorkflow result");

            shutdown_worker();
            timeout(Duration::from_secs(10), worker_task)
                .await
                .expect("worker shutdown timed out")
                .expect("worker task join")
                .expect("worker run");
            output
        })
        .await;
    let refund_address_balance = native_balance(&devnet.base, &valid_evm_address()).await;
    let refund_address_base_usdc_balance = erc20_balance(
        &devnet.base,
        BASE_USDC_ADDRESS.parse().expect("valid Base USDC address"),
        Address::from_str(&valid_evm_address()).expect("valid refund address"),
    )
    .await;

    WorkflowRun {
        _postgres: postgres,
        db,
        order_id,
        output,
        refund_address_balance,
        refund_address_base_usdc_balance,
    }
}

async fn seed_manual_intervention_state(
    db: &Database,
    activity_deps: &OrderActivityDeps,
    order_id: Uuid,
) {
    let order = db.orders().get(order_id).await.expect("load order");
    let funding_vault_id = order.funding_vault_id.expect("order funding vault");
    let source_vault = db
        .vaults()
        .get(funding_vault_id)
        .await
        .expect("load funding vault");
    let quote = db
        .orders()
        .get_router_order_quote(order_id)
        .await
        .expect("load order quote");
    let route = match quote {
        RouterOrderQuote::MarketOrder(quote) => activity_deps
            .planner
            .plan(&order, &source_vault, &quote, Utc::now())
            .expect("plan manual intervention seed"),
        RouterOrderQuote::LimitOrder(_) => panic!("manual intervention seed expects market order"),
    };
    let materialized = db
        .orders()
        .materialize_primary_execution_attempt(
            order_id,
            ExecutionAttemptPlan {
                legs: route.legs,
                steps: route.steps,
            },
            Utc::now(),
        )
        .await
        .expect("materialize manual intervention seed attempt");
    let step = materialized
        .steps
        .first()
        .expect("materialized manual intervention step")
        .clone();
    let now = Utc::now();
    db.orders()
        .persist_execution_step_ready_to_fire(order_id, materialized.attempt.id, step.id, now)
        .await
        .expect("mark manual intervention seed step running");
    db.orders()
        .fail_execution_step(
            step.id,
            json!({
                "error": "seeded manual intervention test failure",
                "reason": "seeded_manual_intervention",
            }),
            now,
        )
        .await
        .expect("fail manual intervention seed step");
    db.orders()
        .mark_execution_attempt_manual_intervention_required(
            materialized.attempt.id,
            json!({
                "reason": "seeded_manual_intervention",
                "step_id": step.id,
            }),
            json!({}),
            now,
        )
        .await
        .expect("mark manual intervention seed attempt");
    db.orders()
        .mark_order_manual_intervention_required(order_id, now)
        .await
        .expect("mark manual intervention seed order");
}

fn ensure_temporal_up() {
    let local_temporal = SocketAddr::from(([127, 0, 0, 1], 7233));
    if TcpStream::connect_timeout(&local_temporal, Duration::from_millis(500)).is_ok() {
        return;
    }

    let status = Command::new("just")
        .arg("temporal-up")
        .status()
        .expect("run `just temporal-up`; install just or start Temporal manually");
    assert!(status.success(), "`just temporal-up` failed with {status}");
}

async fn test_postgres() -> TestPostgres {
    if let Ok(admin_database_url) = std::env::var(ROUTER_TEST_DATABASE_URL_ENV) {
        return TestPostgres {
            admin_database_url,
            _container: None,
        };
    }

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

    let container = image.start().await.unwrap_or_else(|err| {
        panic!(
            "failed to start Postgres testcontainer; ensure Docker is running or set {ROUTER_TEST_DATABASE_URL_ENV}: {err}"
        )
    });
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT.tcp())
        .await
        .expect("read Postgres testcontainer port");
    let admin_database_url = format!(
        "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{port}/{POSTGRES_DATABASE}"
    );

    TestPostgres {
        admin_database_url,
        _container: Some(container),
    }
}

async fn create_test_database(admin_database_url: &str) -> String {
    let connect_options =
        PgConnectOptions::from_str(admin_database_url).expect("parse test database admin URL");
    let mut admin = PgConnection::connect_with(&connect_options)
        .await
        .expect("connect to test database admin URL");
    let database_name = format!("temporal_order_workflow_{}", Uuid::now_v7().simple());

    sqlx_core::query::query(&format!(r#"CREATE DATABASE "{database_name}""#))
        .execute(&mut admin)
        .await
        .expect("create isolated test database");

    let mut database_url =
        url::Url::parse(admin_database_url).expect("parse test database admin URL");
    database_url.set_path(&database_name);
    database_url.to_string()
}

fn test_settings() -> Settings {
    let dir = tempfile::tempdir().expect("settings tempdir");
    let path = dir.path().join("router-master-key.hex");
    std::fs::write(&path, alloy::hex::encode([0x42_u8; 64])).expect("write router master key");
    Settings::load(&path).expect("load test settings")
}

async fn test_chain_registry(
    devnet: &RiftDevnet,
    _route: WorkflowRoute,
    hyperliquid_enabled: bool,
    across_enabled: bool,
    arbitrum_enabled: bool,
    bitcoin_enabled: bool,
) -> ChainRegistry {
    let base_chain = EvmChain::new_with_gas_sponsor(
        &devnet.base.anvil.endpoint(),
        BASE_USDC_ADDRESS,
        ChainType::Base,
        b"temporal-worker-base-wallet",
        1,
        Duration::from_secs(1),
        Some(EvmGasSponsorConfig {
            private_key: format!(
                "0x{}",
                alloy::hex::encode(devnet.base.anvil.keys()[0].to_bytes())
            ),
            vault_gas_buffer_wei: U256::from(EVM_NATIVE_GAS_BUFFER_WEI),
        }),
    )
    .await
    .expect("base chain setup");
    let mut registry = ChainRegistry::new();
    registry.register_evm(ChainType::Base, Arc::new(base_chain));
    if across_enabled {
        let ethereum_chain = EvmChain::new_with_gas_sponsor(
            &devnet.ethereum.anvil.endpoint(),
            ETHEREUM_USDC_ADDRESS,
            ChainType::Ethereum,
            b"temporal-worker-ethereum-wallet",
            1,
            Duration::from_secs(1),
            Some(EvmGasSponsorConfig {
                private_key: format!(
                    "0x{}",
                    alloy::hex::encode(devnet.ethereum.anvil.keys()[0].to_bytes())
                ),
                vault_gas_buffer_wei: U256::from(EVM_NATIVE_GAS_BUFFER_WEI),
            }),
        )
        .await
        .expect("ethereum chain setup");
        registry.register_evm(ChainType::Ethereum, Arc::new(ethereum_chain));
    }
    if arbitrum_enabled {
        let arbitrum_chain = EvmChain::new_with_gas_sponsor(
            &devnet.arbitrum.anvil.endpoint(),
            ARBITRUM_USDC_ADDRESS,
            ChainType::Arbitrum,
            b"temporal-worker-arbitrum-wallet",
            1,
            Duration::from_secs(1),
            Some(EvmGasSponsorConfig {
                private_key: format!(
                    "0x{}",
                    alloy::hex::encode(devnet.arbitrum.anvil.keys()[0].to_bytes())
                ),
                vault_gas_buffer_wei: U256::from(EVM_NATIVE_GAS_BUFFER_WEI),
            }),
        )
        .await
        .expect("arbitrum chain setup");
        registry.register_evm(ChainType::Arbitrum, Arc::new(arbitrum_chain));
    }
    if hyperliquid_enabled {
        registry.register(
            ChainType::Hyperliquid,
            Arc::new(HyperliquidChain::new(
                b"temporal-worker-hyperliquid-wallet",
                1,
                Duration::from_secs(1),
            )),
        );
    }
    if bitcoin_enabled {
        let bitcoin_chain = BitcoinChain::new(
            &devnet.bitcoin.rpc_url,
            BitcoinRpcAuth::CookieFile(devnet.bitcoin.cookie.clone()),
            devnet
                .bitcoin
                .esplora_url
                .as_deref()
                .expect("Bitcoin esplora URL is available"),
            BitcoinNetwork::Regtest,
        )
        .expect("bitcoin chain setup");
        registry.register_bitcoin(ChainType::Bitcoin, Arc::new(bitcoin_chain));
    }
    registry
}

fn test_action_providers(
    mocks: &MockIntegratorServer,
    route: WorkflowRoute,
    hyperliquid_enabled: bool,
    across_enabled: bool,
    cctp_enabled: bool,
) -> ActionProviderRegistry {
    ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
        across: across_enabled.then(|| {
            AcrossHttpProviderConfig::new(mocks.base_url().to_string(), "temporal-worker-across")
        }),
        cctp: cctp_enabled.then(|| CctpHttpProviderConfig::mock(mocks.base_url().to_string())),
        hyperunit_base_url: hyperliquid_enabled.then(|| mocks.base_url().to_string()),
        hyperunit_proxy_url: None,
        hyperliquid_base_url: hyperliquid_enabled.then(|| mocks.base_url().to_string()),
        velora: matches!(
            route,
            WorkflowRoute::VeloraBaseEthToBaseUsdc
                | WorkflowRoute::VeloraBaseEthToBaseUsdcExactOut
                | WorkflowRoute::AcrossBaseEthToEthereumUsdc
                | WorkflowRoute::CctpBaseUsdcToArbitrumEth
        )
        .then(|| VeloraHttpProviderConfig {
            base_url: mocks.base_url().to_string(),
            partner: Some("temporal-worker-e2e".to_string()),
        }),
        hyperliquid_network: HyperliquidCallNetwork::Testnet,
        hyperliquid_order_timeout_ms: 30_000,
    })
    .expect("action providers")
}

async fn spawn_mocks(devnet: &RiftDevnet, options: &WorkflowOptions) -> MockIntegratorServer {
    let mut config = MockIntegratorConfig::default();
    let across_enabled = matches!(options.route, WorkflowRoute::AcrossBaseEthToEthereumUsdc)
        || options.expect_external_custody_across_then_velora_refund;
    let unit_enabled = matches!(options.route, WorkflowRoute::UnitBitcoinToBaseEth)
        || matches!(options.route, WorkflowRoute::CctpBaseUsdcToBitcoin)
        || matches!(
            options.route,
            WorkflowRoute::HyperliquidArbitrumUsdcToBitcoin
        )
        || options.expect_hyperliquid_spot_unit_refund;
    if matches!(
        options.route,
        WorkflowRoute::VeloraBaseEthToBaseUsdc | WorkflowRoute::VeloraBaseEthToBaseUsdcExactOut
    ) {
        config = config.with_velora_swap_contract_address(
            devnet.base.anvil.chain_id(),
            format!("{:#x}", devnet.base.mock_velora_swap_contract.address()),
        );
    }
    if across_enabled {
        config = config
            .with_velora_swap_contract_address(
                devnet.base.anvil.chain_id(),
                format!("{:#x}", devnet.base.mock_velora_swap_contract.address()),
            )
            .with_velora_swap_contract_address(
                devnet.ethereum.anvil.chain_id(),
                format!("{:#x}", devnet.ethereum.mock_velora_swap_contract.address()),
            )
            .with_across_chain(
                devnet.ethereum.anvil.chain_id(),
                format!(
                    "{:#x}",
                    devnet.ethereum.mock_across_spoke_pool_contract.address()
                ),
                devnet.ethereum.anvil.ws_endpoint_url().to_string(),
            )
            .with_across_chain(
                devnet.base.anvil.chain_id(),
                format!(
                    "{:#x}",
                    devnet.base.mock_across_spoke_pool_contract.address()
                ),
                devnet.base.anvil.ws_endpoint_url().to_string(),
            );
    }
    if matches!(
        options.route,
        WorkflowRoute::CctpBaseUsdcToArbitrumEth | WorkflowRoute::CctpBaseUsdcToBitcoin
    ) {
        config = config
            .with_velora_swap_contract_address(
                devnet.arbitrum.anvil.chain_id(),
                format!("{:#x}", devnet.arbitrum.mock_velora_swap_contract.address()),
            )
            .with_cctp_chain(
                devnet.base.anvil.chain_id(),
                MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS,
                devnet.base.anvil.endpoint(),
            )
            .with_cctp_chain(
                devnet.arbitrum.anvil.chain_id(),
                MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS,
                devnet.arbitrum.anvil.endpoint(),
            )
            .with_cctp_destination_token(devnet.base.anvil.chain_id(), BASE_USDC_ADDRESS)
            .with_cctp_destination_token(devnet.arbitrum.anvil.chain_id(), ARBITRUM_USDC_ADDRESS);
    }
    if unit_enabled {
        config = config
            .with_unit_evm_rpc_url(
                hyperunit_client::UnitChain::Base,
                devnet.base.anvil.endpoint_url().to_string(),
            )
            .with_hyperliquid_bridge_address(TESTNET_HYPERLIQUID_BRIDGE_ADDRESS)
            .with_hyperliquid_evm_rpc_url(devnet.arbitrum.anvil.endpoint())
            .with_hyperliquid_usdc_token_address(ARBITRUM_USDC_ADDRESS)
            .with_mainnet_hyperliquid(false);
    }
    if matches!(
        options.route,
        WorkflowRoute::UnitBitcoinToBaseEth | WorkflowRoute::CctpBaseUsdcToBitcoin
    ) {
        config = config.with_unit_bitcoin_rpc(
            devnet.bitcoin.rpc_url.clone(),
            devnet.bitcoin.cookie.clone(),
        );
    }
    config = config
        .with_velora_transaction_fail_next_n(options.velora_transaction_failures)
        .with_velora_transaction_stale_quote_fail_next_n(options.velora_stale_quote_failures);
    MockIntegratorServer::spawn_with_config(config)
        .await
        .expect("spawn mock integrators")
}

async fn expire_market_order_quote_legs(
    database_url: &str,
    order_id: Uuid,
    transition_kinds: &[&'static str],
) {
    let pool = PgPool::connect(database_url)
        .await
        .expect("connect raw pool for quote expiry");
    let expired_at = (Utc::now() - chrono::Duration::minutes(1)).to_rfc3339();
    let transition_kinds = transition_kinds
        .iter()
        .map(|kind| (*kind).to_string())
        .collect::<Vec<_>>();
    sqlx_core::query::query(
        r#"
        UPDATE market_order_quotes
        SET provider_quote = jsonb_set(
            provider_quote,
            '{legs}',
            (
                SELECT jsonb_agg(
                    CASE
                        WHEN cardinality($3::text[]) = 0
                          OR leg.value->>'transition_kind' = ANY($3::text[])
                        THEN jsonb_set(leg.value, '{expires_at}', to_jsonb($2::text), false)
                        ELSE leg.value
                    END
                    ORDER BY leg.ordinality
                )
                FROM jsonb_array_elements(provider_quote->'legs') WITH ORDINALITY AS leg(value, ordinality)
            ),
            false
        )
        WHERE order_id = $1
        "#,
    )
    .bind(order_id)
    .bind(expired_at)
    .bind(transition_kinds)
    .execute(&pool)
    .await
    .expect("expire market quote legs");
}

async fn seed_hyperliquid_spot_refund_position(
    custody_executor: &CustodyActionExecutor,
    mocks: &MockIntegratorServer,
    devnet: &RiftDevnet,
    order_id: Uuid,
    funding_vault_address: &str,
) {
    set_native_balance(
        &devnet.base,
        Address::from_str(funding_vault_address).expect("funding vault EVM address"),
        U256::ZERO,
    )
    .await;
    let vault = custody_executor
        .create_router_derived_vault(
            order_id,
            CustodyVaultRole::HyperliquidSpot,
            CustodyVaultVisibility::Internal,
            ChainId::parse("hyperliquid").expect("hyperliquid chain id"),
            None,
            json!({ "test": "temporal_hyperliquid_spot_refund" }),
        )
        .await
        .expect("create HyperliquidSpot custody vault");
    let user = Address::from_str(&vault.address).expect("Hyperliquid vault address");
    let amount = U256::from_str_radix(ORDER_AMOUNT_IN_WEI, 10).expect("order amount parses");
    let natural_eth = amount
        .to_string()
        .parse::<f64>()
        .expect("amount parses as f64")
        / 1_000_000_000_000_000_000_f64;
    mocks
        .credit_hyperliquid_balance(user, "UETH", natural_eth)
        .await;
}

async fn seed_external_custody_universal_router_refund_position(
    custody_executor: &CustodyActionExecutor,
    devnet: &RiftDevnet,
    order_id: Uuid,
    funding_vault_address: &str,
) {
    set_native_balance(
        &devnet.base,
        Address::from_str(funding_vault_address).expect("funding vault EVM address"),
        U256::ZERO,
    )
    .await;
    let vault = custody_executor
        .create_router_derived_vault(
            order_id,
            CustodyVaultRole::DestinationExecution,
            CustodyVaultVisibility::Internal,
            ChainId::parse("evm:8453").expect("base chain id"),
            Some(AssetId::parse(BASE_USDC_ADDRESS).expect("base USDC asset")),
            json!({ "test": "temporal_external_custody_universal_router_refund" }),
        )
        .await
        .expect("create UniversalRouter ExternalCustody refund vault");
    fund_erc20_source_vault(
        &devnet.base,
        BASE_USDC_ADDRESS.parse().expect("valid Base USDC address"),
        &vault.address,
        ORDER_USDC_AMOUNT_RAW,
    )
    .await;
}

async fn seed_external_custody_across_then_universal_router_refund_position(
    custody_executor: &CustodyActionExecutor,
    devnet: &RiftDevnet,
    order_id: Uuid,
    funding_vault_address: &str,
) {
    set_native_balance(
        &devnet.base,
        Address::from_str(funding_vault_address).expect("funding vault EVM address"),
        U256::ZERO,
    )
    .await;
    let vault = custody_executor
        .create_router_derived_vault(
            order_id,
            CustodyVaultRole::DestinationExecution,
            CustodyVaultVisibility::Internal,
            ChainId::parse("evm:1").expect("ethereum chain id"),
            Some(AssetId::parse(ETHEREUM_USDC_ADDRESS).expect("ethereum USDC asset")),
            json!({ "test": "temporal_external_custody_across_then_universal_router_refund" }),
        )
        .await
        .expect("create Across-to-UniversalRouter ExternalCustody refund vault");
    fund_erc20_source_vault(
        &devnet.ethereum,
        ETHEREUM_USDC_ADDRESS
            .parse()
            .expect("valid Ethereum USDC address"),
        &vault.address,
        ORDER_USDC_AMOUNT_RAW,
    )
    .await;
}

async fn wait_for_provider_operation(
    db: &Database,
    order_id: Uuid,
    operation_type: ProviderOperationType,
    max_wait: Duration,
) -> OrderProviderOperation {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        let operations = db
            .orders()
            .get_provider_operations(order_id)
            .await
            .expect("load provider operations");
        if let Some(operation) = operations
            .into_iter()
            .find(|operation| operation.operation_type == operation_type)
        {
            return operation;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for provider operation {operation_type:?} on order {order_id}"
        );
        sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_provider_operation_status(
    db: &Database,
    order_id: Uuid,
    operation_type: ProviderOperationType,
    status: ProviderOperationStatus,
    max_wait: Duration,
) -> OrderProviderOperation {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        let operations = db
            .orders()
            .get_provider_operations(order_id)
            .await
            .expect("load provider operations");
        if let Some(operation) = operations.into_iter().find(|operation| {
            operation.operation_type == operation_type && operation.status == status
        }) {
            return operation;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for provider operation {operation_type:?} on order {order_id} to reach {status:?}"
        );
        sleep(Duration::from_millis(250)).await;
    }
}

fn assert_signal_sent_or_workflow_completed<T, E: std::fmt::Debug>(
    result: Result<T, E>,
    context: &'static str,
) {
    if let Err(error) = result {
        let debug = format!("{error:?}");
        assert!(
            debug.contains("workflow execution already completed"),
            "{context}: {debug}"
        );
    }
}

async fn wait_for_order_cctp_burn(
    client: &Client,
    db: &Database,
    mocks: &MockIntegratorServer,
    workflow_id: &str,
    order_id: Uuid,
) -> OrderProviderOperation {
    let handle_before_hint = client.get_workflow_handle::<OrderWorkflow>(workflow_id);
    let cctp_operation = tokio::select! {
        operation = wait_for_provider_operation_with_ref(
            db,
            order_id,
            None,
            ProviderOperationType::CctpBridge,
            Duration::from_secs(90),
        ) => operation,
        result = handle_before_hint.get_result(WorkflowGetResultOptions::default()) => {
            let state = workflow_state_summary(db, order_id).await;
            panic!("OrderWorkflow ended before the CCTP provider operation reached a checkpoint: {result:?}\n{state}");
        }
    };
    wait_for_cctp_burn_indexed(
        mocks,
        cctp_operation
            .provider_ref
            .as_deref()
            .expect("CCTP operation should carry burn tx hash"),
        Duration::from_secs(90),
    )
    .await;
    cctp_operation
}

async fn signal_order_cctp_attestation(
    client: &Client,
    workflow_id: &str,
    order_id: Uuid,
    cctp_operation: OrderProviderOperation,
) {
    let handle_before_hint = client.get_workflow_handle::<OrderWorkflow>(workflow_id);
    let signal_result = handle_before_hint
        .signal(
            OrderWorkflow::provider_operation_hint,
            ProviderOperationHintSignal {
                order_id: order_id.into(),
                hint_id: Uuid::now_v7().into(),
                provider_operation_id: Some(cctp_operation.id.into()),
                provider: ProviderKind::Bridge,
                hint_kind: ProviderHintKind::CctpAttestation,
                provider_ref: cctp_operation.provider_ref,
                evidence: None,
            },
            WorkflowSignalOptions::default(),
        )
        .await;
    assert_signal_sent_or_workflow_completed(
        signal_result,
        "send CCTP provider-operation hint signal",
    );
}

async fn signal_order_hyperliquid_bridge_deposit_observation(
    client: &Client,
    db: &Database,
    mocks: &MockIntegratorServer,
    workflow_id: &str,
    order_id: Uuid,
) {
    let handle_before_hint = client.get_workflow_handle::<OrderWorkflow>(workflow_id);
    let operation = tokio::select! {
        operation = wait_for_provider_operation(
            db,
            order_id,
            ProviderOperationType::HyperliquidBridgeDeposit,
            Duration::from_secs(180),
        ) => operation,
        result = handle_before_hint.get_result(WorkflowGetResultOptions::default()) => {
            let state = workflow_state_summary(db, order_id).await;
            panic!("OrderWorkflow ended before the HyperliquidBridgeDeposit provider operation was persisted: {result:?}\n{state}");
        }
    };
    let user = Address::from_str(
        operation
            .provider_ref
            .as_deref()
            .expect("HyperliquidBridgeDeposit operation should carry provider_ref user"),
    )
    .expect("HyperliquidBridgeDeposit provider_ref should be an address");
    let expected_withdrawable_raw = operation
        .request
        .get("expected_withdrawable_raw")
        .and_then(serde_json::Value::as_str)
        .and_then(|raw| raw.parse::<f64>().ok())
        .expect("HyperliquidBridgeDeposit request should carry expected_withdrawable_raw");
    let expected_withdrawable = expected_withdrawable_raw / 1_000_000_f64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let observed = mocks
            .hyperliquid_clearinghouse_balance_of(user, "USDC")
            .await;
        if observed >= expected_withdrawable {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for Hyperliquid bridge deposit credit for {user:#x}; expected {expected_withdrawable}, observed {observed}"
        );
        sleep(Duration::from_millis(250)).await;
    }

    let signal_result = handle_before_hint
        .signal(
            OrderWorkflow::provider_operation_hint,
            ProviderOperationHintSignal {
                order_id: order_id.into(),
                hint_id: Uuid::now_v7().into(),
                provider_operation_id: Some(operation.id.into()),
                provider: ProviderKind::Bridge,
                hint_kind: ProviderHintKind::ProviderObservation,
                provider_ref: operation.provider_ref,
                evidence: None,
            },
            WorkflowSignalOptions::default(),
        )
        .await;
    assert_signal_sent_or_workflow_completed(
        signal_result,
        "send HyperliquidBridgeDeposit provider-observation hint signal",
    );
}

async fn signal_order_unit_withdrawal_observation(
    client: &Client,
    db: &Database,
    mocks: &MockIntegratorServer,
    workflow_id: &str,
    order_id: Uuid,
) {
    let handle_before_hint = client.get_workflow_handle::<OrderWorkflow>(workflow_id);
    let unit_withdrawal_operation = tokio::select! {
        operation = wait_for_provider_operation(
            db,
            order_id,
            ProviderOperationType::UnitWithdrawal,
            Duration::from_secs(180),
        ) => operation,
        result = handle_before_hint.get_result(WorkflowGetResultOptions::default()) => {
            let state = workflow_state_summary(db, order_id).await;
            panic!("OrderWorkflow ended before the UnitWithdrawal provider operation was persisted: {result:?}\n{state}");
        }
    };
    let provider_ref = unit_withdrawal_operation
        .provider_ref
        .as_deref()
        .expect("UnitWithdrawal operation should carry provider_ref");
    let amount = unit_withdrawal_operation
        .request
        .get("amount")
        .and_then(serde_json::Value::as_str)
        .expect("UnitWithdrawal operation request should carry amount");
    mocks
        .complete_unit_operation_with_source_amount(provider_ref, amount)
        .await
        .expect("complete mock Unit withdrawal operation");

    let signal_result = handle_before_hint
        .signal(
            OrderWorkflow::provider_operation_hint,
            ProviderOperationHintSignal {
                order_id: order_id.into(),
                hint_id: Uuid::now_v7().into(),
                provider_operation_id: Some(unit_withdrawal_operation.id.into()),
                provider: ProviderKind::Unit,
                hint_kind: ProviderHintKind::ProviderObservation,
                provider_ref: unit_withdrawal_operation.provider_ref,
                evidence: None,
            },
            WorkflowSignalOptions::default(),
        )
        .await;
    assert_signal_sent_or_workflow_completed(
        signal_result,
        "send UnitWithdrawal provider-operation hint signal",
    );
}

async fn drain_base_usdc_funding_vault_residual(
    db: &Database,
    settings: &Settings,
    chain_registry: &ChainRegistry,
    order_id: Uuid,
    funding_vault_address: &str,
) {
    let evm_chain = chain_registry
        .get_evm(&ChainType::Base)
        .expect("Base chain registered");
    let balance = evm_chain
        .erc20_balance(BASE_USDC_ADDRESS, funding_vault_address)
        .await
        .expect("read Base USDC funding-vault residual");
    if balance.is_zero() {
        return;
    }

    let order = db.orders().get(order_id).await.expect("load CCTP order");
    let funding_vault_id = order
        .funding_vault_id
        .expect("CCTP order should have a funding vault");
    let funding_vault = db
        .vaults()
        .get(funding_vault_id)
        .await
        .expect("load CCTP funding vault");
    let chain = chain_registry
        .get(&ChainType::Base)
        .expect("Base chain operations registered");
    let wallet = chain
        .derive_wallet(
            &settings.master_key_bytes(),
            &funding_vault.deposit_vault_salt,
        )
        .expect("derive CCTP funding-vault wallet");

    evm_chain
        .ensure_native_gas_for_erc20_transfer(
            BASE_USDC_ADDRESS,
            funding_vault_address,
            TEST_RESIDUAL_SINK_ADDRESS,
            balance,
        )
        .await
        .expect("fund CCTP source residual drain gas");
    evm_chain
        .transfer_erc20_amount(
            BASE_USDC_ADDRESS,
            wallet.private_key(),
            TEST_RESIDUAL_SINK_ADDRESS,
            balance,
        )
        .await
        .expect("drain CCTP source residual");

    let remaining = evm_chain
        .erc20_balance(BASE_USDC_ADDRESS, funding_vault_address)
        .await
        .expect("read drained Base USDC funding-vault residual");
    assert_eq!(remaining, U256::ZERO);
}

async fn signal_external_custody_refund_across_fill(
    client: &Client,
    db: &Database,
    mocks: &MockIntegratorServer,
    devnet: &RiftDevnet,
    funding_vault_address: &str,
    order_id: Uuid,
    fund_destination_vault: bool,
) {
    set_native_balance(
        &devnet.base,
        Address::from_str(funding_vault_address).expect("funding vault EVM address"),
        U256::ZERO,
    )
    .await;
    let parent_attempt = wait_for_execution_attempt(
        db,
        order_id,
        OrderExecutionAttemptKind::PrimaryExecution,
        2,
        Some(OrderExecutionAttemptStatus::Failed),
        Duration::from_secs(120),
    )
    .await;
    let refund_attempt = wait_for_execution_attempt(
        db,
        order_id,
        OrderExecutionAttemptKind::RefundRecovery,
        3,
        None,
        Duration::from_secs(120),
    )
    .await;
    let refund_operation = wait_for_provider_operation_for_attempt(
        db,
        order_id,
        refund_attempt.id,
        ProviderOperationType::AcrossBridge,
        Duration::from_secs(120),
    )
    .await;
    let refund_step = db
        .orders()
        .get_execution_step(
            refund_operation
                .execution_step_id
                .expect("refund Across operation should be linked to a step"),
        )
        .await
        .expect("load refund Across step");
    let depositor = refund_step
        .request
        .get("depositor_address")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| Address::from_str(value).ok())
        .expect("refund Across step should carry an EVM depositor_address");
    wait_for_across_deposit_indexed(mocks, depositor, Duration::from_secs(90)).await;
    if fund_destination_vault {
        fund_destination_execution_vault_from_across(db, devnet, order_id, &refund_operation).await;
    }

    let refund_workflow = client.get_workflow_handle::<RefundWorkflow>(&refund_workflow_id(
        order_id.into(),
        parent_attempt.id.into(),
    ));
    let signal_result = refund_workflow
        .signal(
            RefundWorkflow::provider_operation_hint,
            ProviderOperationHintSignal {
                order_id: order_id.into(),
                hint_id: Uuid::now_v7().into(),
                provider_operation_id: Some(refund_operation.id.into()),
                provider: ProviderKind::Bridge,
                hint_kind: ProviderHintKind::AcrossFill,
                provider_ref: refund_operation.provider_ref,
                evidence: None,
            },
            WorkflowSignalOptions::default(),
        )
        .await;
    assert_signal_sent_or_workflow_completed(
        signal_result,
        "send refund Across provider-operation hint signal",
    );
}

async fn signal_external_custody_refund_cctp_attestation(
    client: &Client,
    db: &Database,
    mocks: &MockIntegratorServer,
    workflow_id: &str,
    order_id: Uuid,
) {
    let parent_attempt = wait_for_execution_attempt(
        db,
        order_id,
        OrderExecutionAttemptKind::PrimaryExecution,
        2,
        Some(OrderExecutionAttemptStatus::Failed),
        Duration::from_secs(120),
    )
    .await;
    let order_workflow = client.get_workflow_handle::<OrderWorkflow>(workflow_id);
    let refund_attempt = tokio::select! {
        attempt = wait_for_execution_attempt(
            db,
            order_id,
            OrderExecutionAttemptKind::RefundRecovery,
            3,
            None,
            Duration::from_secs(120),
        ) => attempt,
        result = order_workflow.get_result(WorkflowGetResultOptions::default()) => {
            let state = workflow_state_summary(db, order_id).await;
            panic!("OrderWorkflow ended before the CCTP refund attempt was materialized: {result:?}\n{state}");
        }
    };
    let refund_operation = wait_for_provider_operation_with_ref(
        db,
        order_id,
        Some(refund_attempt.id),
        ProviderOperationType::CctpBridge,
        Duration::from_secs(120),
    )
    .await;
    wait_for_cctp_burn_indexed(
        mocks,
        refund_operation
            .provider_ref
            .as_deref()
            .expect("refund CCTP operation should carry burn tx hash"),
        Duration::from_secs(90),
    )
    .await;

    let refund_workflow = client.get_workflow_handle::<RefundWorkflow>(&refund_workflow_id(
        order_id.into(),
        parent_attempt.id.into(),
    ));
    let signal_result = refund_workflow
        .signal(
            RefundWorkflow::provider_operation_hint,
            ProviderOperationHintSignal {
                order_id: order_id.into(),
                hint_id: Uuid::now_v7().into(),
                provider_operation_id: Some(refund_operation.id.into()),
                provider: ProviderKind::Bridge,
                hint_kind: ProviderHintKind::CctpAttestation,
                provider_ref: refund_operation.provider_ref,
                evidence: None,
            },
            WorkflowSignalOptions::default(),
        )
        .await;
    assert_signal_sent_or_workflow_completed(
        signal_result,
        "send refund CCTP provider-operation hint signal",
    );
}

async fn signal_hyperliquid_spot_refund_unit_withdrawal(
    client: &Client,
    db: &Database,
    mocks: &MockIntegratorServer,
    order_id: Uuid,
) {
    let parent_attempt = wait_for_execution_attempt(
        db,
        order_id,
        OrderExecutionAttemptKind::PrimaryExecution,
        2,
        Some(OrderExecutionAttemptStatus::Failed),
        Duration::from_secs(120),
    )
    .await;
    let refund_attempt = wait_for_execution_attempt(
        db,
        order_id,
        OrderExecutionAttemptKind::RefundRecovery,
        3,
        None,
        Duration::from_secs(120),
    )
    .await;
    let refund_operation = wait_for_provider_operation_for_attempt(
        db,
        order_id,
        refund_attempt.id,
        ProviderOperationType::UnitWithdrawal,
        Duration::from_secs(120),
    )
    .await;
    let provider_ref = refund_operation
        .provider_ref
        .as_deref()
        .expect("UnitWithdrawal refund operation should carry provider_ref");
    let amount = refund_operation
        .request
        .get("amount")
        .and_then(serde_json::Value::as_str)
        .expect("UnitWithdrawal refund operation request should carry amount");
    mocks
        .complete_unit_operation_with_source_amount(provider_ref, amount)
        .await
        .expect("complete mock Unit withdrawal operation");

    let refund_workflow = client.get_workflow_handle::<RefundWorkflow>(&refund_workflow_id(
        order_id.into(),
        parent_attempt.id.into(),
    ));
    let signal_result = refund_workflow
        .signal(
            RefundWorkflow::provider_operation_hint,
            ProviderOperationHintSignal {
                order_id: order_id.into(),
                hint_id: Uuid::now_v7().into(),
                provider_operation_id: Some(refund_operation.id.into()),
                provider: ProviderKind::Unit,
                hint_kind: ProviderHintKind::ProviderObservation,
                provider_ref: refund_operation.provider_ref,
                evidence: None,
            },
            WorkflowSignalOptions::default(),
        )
        .await;
    assert_signal_sent_or_workflow_completed(
        signal_result,
        "send refund UnitWithdrawal provider-operation hint signal",
    );
}

async fn wait_for_execution_attempt(
    db: &Database,
    order_id: Uuid,
    attempt_kind: OrderExecutionAttemptKind,
    attempt_index: i32,
    status: Option<OrderExecutionAttemptStatus>,
    max_wait: Duration,
) -> OrderExecutionAttempt {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        let attempts = db
            .orders()
            .get_execution_attempts(order_id)
            .await
            .expect("load execution attempts");
        if let Some(attempt) = attempts.into_iter().find(|attempt| {
            attempt.attempt_kind == attempt_kind
                && attempt.attempt_index == attempt_index
                && status.is_none_or(|status| attempt.status == status)
        }) {
            return attempt;
        }
        if tokio::time::Instant::now() >= deadline {
            let state = workflow_state_summary(db, order_id).await;
            panic!(
                "timed out waiting for {attempt_kind:?} attempt {attempt_index} on order {order_id}\n{state}"
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_provider_operation_for_attempt(
    db: &Database,
    order_id: Uuid,
    attempt_id: Uuid,
    operation_type: ProviderOperationType,
    max_wait: Duration,
) -> OrderProviderOperation {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        let operations = db
            .orders()
            .get_provider_operations(order_id)
            .await
            .expect("load provider operations");
        if let Some(operation) = operations.into_iter().find(|operation| {
            operation.execution_attempt_id == Some(attempt_id)
                && operation.operation_type == operation_type
        }) {
            return operation;
        }
        if tokio::time::Instant::now() >= deadline {
            let state = workflow_state_summary(db, order_id).await;
            panic!(
                "timed out waiting for provider operation {operation_type:?} on attempt {attempt_id}\n{state}"
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_provider_operation_with_ref(
    db: &Database,
    order_id: Uuid,
    attempt_id: Option<Uuid>,
    operation_type: ProviderOperationType,
    max_wait: Duration,
) -> OrderProviderOperation {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        let operations = db
            .orders()
            .get_provider_operations(order_id)
            .await
            .expect("load provider operations");
        if let Some(operation) = operations.into_iter().find(|operation| {
            operation.operation_type == operation_type
                && attempt_id
                    .is_none_or(|attempt_id| operation.execution_attempt_id == Some(attempt_id))
                && operation.provider_ref.is_some()
        }) {
            return operation;
        }
        if tokio::time::Instant::now() >= deadline {
            let state = workflow_state_summary(db, order_id).await;
            panic!(
                "timed out waiting for provider operation {operation_type:?} with provider_ref on order {order_id}\n{state}"
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_cctp_burn_indexed(
    mocks: &MockIntegratorServer,
    burn_tx_hash: &str,
    max_wait: Duration,
) {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        if mocks
            .cctp_burns()
            .await
            .iter()
            .any(|record| record.burn_tx_hash.eq_ignore_ascii_case(burn_tx_hash))
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for CCTP mock to index burn {burn_tx_hash}"
        );
        sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_across_deposit_indexed(
    mocks: &MockIntegratorServer,
    depositor: Address,
    max_wait: Duration,
) {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        if !mocks.across_deposits_from(depositor).await.is_empty() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for Across mock to index deposit from {depositor:#x}"
        );
        sleep(Duration::from_millis(250)).await;
    }
}

async fn fund_destination_execution_vault_from_across(
    db: &Database,
    devnet: &RiftDevnet,
    order_id: Uuid,
    operation: &OrderProviderOperation,
) {
    let step = db
        .orders()
        .get_execution_step(
            operation
                .execution_step_id
                .expect("Across operation should be linked to a step"),
        )
        .await
        .expect("load Across operation step");
    let output_asset = step
        .output_asset
        .clone()
        .expect("Across operation step should carry output_asset");
    let destination_vault = db
        .orders()
        .get_custody_vaults(order_id)
        .await
        .expect("load custody vaults")
        .into_iter()
        .find(|vault| {
            vault.role == CustodyVaultRole::DestinationExecution
                && vault.chain == output_asset.chain
                && vault.asset.as_ref() == Some(&output_asset.asset)
        })
        .expect("Across step should create destination execution custody vault");
    let provider_response = operation
        .response
        .get("provider_response")
        .unwrap_or(&operation.response);
    let output_amount = provider_response
        .get("expectedOutputAmount")
        .or_else(|| provider_response.get("outputAmount"))
        .and_then(serde_json::Value::as_str)
        .and_then(|amount| U256::from_str_radix(amount, 10).ok())
        .expect("Across response should include expected output amount");
    let destination_devnet = evm_devnet_for_chain(devnet, destination_vault.chain.as_str());
    let address = Address::from_str(&destination_vault.address)
        .expect("destination execution vault address should be EVM address");
    let gas_balance = U256::from(EVM_NATIVE_GAS_BUFFER_WEI);

    match destination_vault.asset.as_ref().unwrap_or(&AssetId::Native) {
        AssetId::Native => {
            send_native(destination_devnet, address, output_amount + gas_balance).await;
        }
        AssetId::Reference(token_address) => {
            send_native(destination_devnet, address, gas_balance).await;
            mint_erc20(
                destination_devnet,
                token_address.parse().expect("destination token address"),
                address,
                output_amount,
            )
            .await;
        }
    }
    mine_evm_confirmation_block(destination_devnet).await;
}

fn evm_devnet_for_chain<'a>(devnet: &'a RiftDevnet, chain: &str) -> &'a devnet::EthDevnet {
    match chain {
        "evm:1" => &devnet.ethereum,
        "evm:8453" => &devnet.base,
        "evm:42161" => &devnet.arbitrum,
        other => panic!("unsupported EVM chain {other}"),
    }
}

async fn simulate_across_lost_intent_checkpoint(
    database_url: &str,
    db: &Database,
    operation: &OrderProviderOperation,
) -> OrderProviderOperation {
    let deposit_tx_hash = operation
        .observed_state
        .get("deposit_tx_hash")
        .or_else(|| operation.observed_state.get("tx_hash"))
        .and_then(Value::as_str)
        .expect("Across operation should carry deposit tx hash before lost-intent simulation")
        .to_string();
    let checkpoint_response = json!({
        "kind": "provider_receipt_checkpoint",
        "provider": &operation.provider,
        "operation_id": operation.id,
        "provider_response": &operation.response,
        "tx_hash": &deposit_tx_hash,
        "tx_hashes": [&deposit_tx_hash],
    });
    let pool = PgPool::connect(database_url)
        .await
        .expect("connect raw pool for lost-intent simulation");
    sqlx_core::query::query(
        r#"
        UPDATE order_provider_operations
        SET provider_ref = NULL,
            status = $2,
            response_json = $3,
            observed_state_json = '{}'::jsonb,
            updated_at = $4
        WHERE id = $1
        "#,
    )
    .bind(operation.id)
    .bind(ProviderOperationStatus::WaitingExternal.to_db_string())
    .bind(checkpoint_response)
    .bind(Utc::now())
    .execute(&pool)
    .await
    .expect("simulate Across lost-intent checkpoint");
    db.orders()
        .get_provider_operation(operation.id)
        .await
        .expect("reload simulated Across lost-intent operation")
}

async fn provider_operation_by_type(
    db: &Database,
    order_id: Uuid,
    operation_type: ProviderOperationType,
) -> OrderProviderOperation {
    db.orders()
        .get_provider_operations(order_id)
        .await
        .expect("load provider operations")
        .into_iter()
        .find(|operation| operation.operation_type == operation_type)
        .unwrap_or_else(|| {
            panic!("expected provider operation {operation_type:?} for order {order_id}")
        })
}

async fn workflow_state_summary(db: &Database, order_id: Uuid) -> String {
    let mut lines = Vec::new();
    match db.orders().get(order_id).await {
        Ok(order) => lines.push(format!(
            "order: status={:?} funding_vault_id={:?}",
            order.status, order.funding_vault_id
        )),
        Err(err) => lines.push(format!("order load failed: {err}")),
    }
    match db.orders().get_execution_attempts(order_id).await {
        Ok(attempts) => {
            lines.push(format!("attempts: {}", attempts.len()));
            for attempt in attempts {
                lines.push(format!(
                    "  attempt index={} kind={:?} status={:?} failure={}",
                    attempt.attempt_index,
                    attempt.attempt_kind,
                    attempt.status,
                    attempt.failure_reason
                ));
                match db
                    .orders()
                    .get_execution_steps_for_attempt(attempt.id)
                    .await
                {
                    Ok(steps) => {
                        for step in steps {
                            lines.push(format!(
                                "    step index={} type={:?} provider={} status={:?} error={} response={}",
                                step.step_index,
                                step.step_type,
                                step.provider,
                                step.status,
                                step.error,
                                step.response
                            ));
                        }
                    }
                    Err(err) => lines.push(format!("    steps load failed: {err}")),
                }
            }
        }
        Err(err) => lines.push(format!("attempts load failed: {err}")),
    }
    match db.orders().get_custody_vaults(order_id).await {
        Ok(vaults) => {
            lines.push(format!("custody_vaults: {}", vaults.len()));
            for vault in vaults {
                lines.push(format!(
                    "  vault id={} role={:?} chain={} asset={:?} address={} status={:?}",
                    vault.id, vault.role, vault.chain, vault.asset, vault.address, vault.status
                ));
            }
        }
        Err(err) => lines.push(format!("custody vaults load failed: {err}")),
    }
    match db.orders().get_provider_operations(order_id).await {
        Ok(operations) => {
            lines.push(format!("provider_operations: {}", operations.len()));
            for operation in operations {
                lines.push(format!(
                    "  operation type={:?} provider={} status={:?} ref={:?} request={} response={}",
                    operation.operation_type,
                    operation.provider,
                    operation.status,
                    operation.provider_ref,
                    operation.request,
                    operation.response
                ));
            }
        }
        Err(err) => lines.push(format!("provider operations load failed: {err}")),
    }
    lines.join("\n")
}

async fn seed_funded_single_step_order(
    db: &Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    action_providers: Arc<ActionProviderRegistry>,
    devnet: &RiftDevnet,
    order_kind: MarketOrderQuoteKind,
) -> SeededOrder {
    let order_manager = OrderManager::with_action_providers(
        db.clone(),
        settings.clone(),
        chain_registry.clone(),
        action_providers,
    );
    let vault_manager = VaultManager::new(db.clone(), settings, chain_registry);
    let source_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").expect("base chain id"),
        asset: AssetId::Native,
    };
    let destination_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").expect("base chain id"),
        asset: AssetId::parse(BASE_USDC_ADDRESS).expect("base USDC asset"),
    };
    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: destination_asset,
            recipient_address: valid_evm_address(),
            order_kind,
        })
        .await
        .expect("quote single-step order");
    let market_quote = match quote.quote {
        RouterOrderQuote::MarketOrder(quote) => quote,
        RouterOrderQuote::LimitOrder(_) => panic!("expected market quote"),
    };
    assert!(
        market_quote
            .provider_id
            .contains("universal_router_swap:velora"),
        "expected a single Velora step, got {}",
        market_quote.provider_id
    );

    let (order, _) = order_manager
        .create_order_from_quote(CreateOrderRequest {
            quote_id: market_quote.id,
            refund_address: valid_evm_address(),
            idempotency_key: None,
            metadata: json!({ "test": "temporal_order_workflow" }),
        })
        .await
        .expect("create order from quote");
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: source_asset,
            action: VaultAction::Null,
            recovery_address: valid_evm_address(),
            cancellation_commitment:
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            cancel_after: None,
            metadata: json!({ "test": "temporal_order_workflow" }),
        })
        .await
        .expect("create funding vault");
    fund_source_vault(
        devnet,
        &vault.deposit_vault_address,
        &market_quote.amount_in,
    )
    .await;

    let now = Utc::now();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            now,
        )
        .await
        .expect("mark vault funded");
    db.orders()
        .transition_status(
            order.id,
            RouterOrderStatus::PendingFunding,
            RouterOrderStatus::Funded,
            now,
        )
        .await
        .expect("mark order funded");
    SeededOrder {
        order_id: order.id,
        funding_vault_address: vault.deposit_vault_address,
    }
}

async fn seed_funded_across_order(
    db: &Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    action_providers: Arc<ActionProviderRegistry>,
    devnet: &RiftDevnet,
) -> SeededOrder {
    let order_manager = OrderManager::with_action_providers(
        db.clone(),
        settings.clone(),
        chain_registry.clone(),
        action_providers,
    );
    let vault_manager = VaultManager::new(db.clone(), settings, chain_registry);
    let source_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").expect("base chain id"),
        asset: AssetId::Native,
    };
    let destination_asset = DepositAsset {
        chain: ChainId::parse("evm:1").expect("ethereum chain id"),
        asset: AssetId::parse(ETHEREUM_USDC_ADDRESS).expect("ethereum USDC asset"),
    };
    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: destination_asset,
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: ORDER_AMOUNT_IN_WEI.to_string(),
                slippage_bps: Some(100),
            },
        })
        .await
        .expect("quote Across order");
    let market_quote = match quote.quote {
        RouterOrderQuote::MarketOrder(quote) => quote,
        RouterOrderQuote::LimitOrder(_) => panic!("expected market quote"),
    };
    assert!(
        market_quote.provider_id.contains("across_bridge:across")
            && market_quote
                .provider_id
                .contains("universal_router_swap:velora"),
        "expected an Across + Velora route, got {}",
        market_quote.provider_id
    );

    let (order, _) = order_manager
        .create_order_from_quote(CreateOrderRequest {
            quote_id: market_quote.id,
            refund_address: valid_evm_address(),
            idempotency_key: None,
            metadata: json!({ "test": "temporal_order_workflow_across" }),
        })
        .await
        .expect("create Across order from quote");
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: source_asset,
            action: VaultAction::Null,
            recovery_address: valid_evm_address(),
            cancellation_commitment:
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            cancel_after: None,
            metadata: json!({ "test": "temporal_order_workflow_across" }),
        })
        .await
        .expect("create Across funding vault");
    fund_source_vault(
        devnet,
        &vault.deposit_vault_address,
        &market_quote.amount_in,
    )
    .await;

    let now = Utc::now();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            now,
        )
        .await
        .expect("mark Across vault funded");
    db.orders()
        .transition_status(
            order.id,
            RouterOrderStatus::PendingFunding,
            RouterOrderStatus::Funded,
            now,
        )
        .await
        .expect("mark Across order funded");
    SeededOrder {
        order_id: order.id,
        funding_vault_address: vault.deposit_vault_address,
    }
}

async fn seed_funded_cctp_order(
    db: &Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    action_providers: Arc<ActionProviderRegistry>,
    devnet: &RiftDevnet,
) -> SeededOrder {
    let order_manager = OrderManager::with_action_providers(
        db.clone(),
        settings.clone(),
        chain_registry.clone(),
        action_providers,
    );
    let vault_manager = VaultManager::new(db.clone(), settings, chain_registry);
    let source_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").expect("base chain id"),
        asset: AssetId::parse(BASE_USDC_ADDRESS).expect("base USDC asset"),
    };
    let destination_asset = DepositAsset {
        chain: ChainId::parse("evm:42161").expect("arbitrum chain id"),
        asset: AssetId::Native,
    };
    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: destination_asset,
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: ORDER_USDC_AMOUNT_RAW.to_string(),
                slippage_bps: Some(100),
            },
        })
        .await
        .expect("quote CCTP order");
    let market_quote = match quote.quote {
        RouterOrderQuote::MarketOrder(quote) => quote,
        RouterOrderQuote::LimitOrder(_) => panic!("expected market quote"),
    };
    assert!(
        market_quote.provider_id.contains("cctp_bridge:cctp")
            && market_quote
                .provider_id
                .contains("universal_router_swap:velora"),
        "expected a CCTP + Velora route, got {}",
        market_quote.provider_id
    );

    let (order, _) = order_manager
        .create_order_from_quote(CreateOrderRequest {
            quote_id: market_quote.id,
            refund_address: valid_evm_address(),
            idempotency_key: None,
            metadata: json!({ "test": "temporal_order_workflow_cctp" }),
        })
        .await
        .expect("create CCTP order from quote");
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: source_asset,
            action: VaultAction::Null,
            recovery_address: valid_evm_address(),
            cancellation_commitment:
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            cancel_after: None,
            metadata: json!({ "test": "temporal_order_workflow_cctp" }),
        })
        .await
        .expect("create CCTP funding vault");
    fund_erc20_source_vault(
        &devnet.base,
        BASE_USDC_ADDRESS.parse().expect("valid Base USDC address"),
        &vault.deposit_vault_address,
        &market_quote.amount_in,
    )
    .await;

    let now = Utc::now();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            now,
        )
        .await
        .expect("mark CCTP vault funded");
    db.orders()
        .transition_status(
            order.id,
            RouterOrderStatus::PendingFunding,
            RouterOrderStatus::Funded,
            now,
        )
        .await
        .expect("mark CCTP order funded");
    SeededOrder {
        order_id: order.id,
        funding_vault_address: vault.deposit_vault_address,
    }
}

async fn seed_funded_cctp_hyperliquid_unit_order(
    db: &Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    action_providers: Arc<ActionProviderRegistry>,
    devnet: &RiftDevnet,
) -> SeededOrder {
    let order_manager = OrderManager::with_action_providers(
        db.clone(),
        settings.clone(),
        chain_registry.clone(),
        action_providers,
    );
    let vault_manager = VaultManager::new(db.clone(), settings, chain_registry);
    let source_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").expect("base chain id"),
        asset: AssetId::parse(BASE_USDC_ADDRESS).expect("base USDC asset"),
    };
    let destination_asset = DepositAsset {
        chain: ChainId::parse("bitcoin").expect("bitcoin chain id"),
        asset: AssetId::Native,
    };
    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: destination_asset,
            recipient_address: devnet.bitcoin.miner_address.to_string(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: ORDER_USDC_AMOUNT_RAW.to_string(),
                slippage_bps: Some(100),
            },
        })
        .await
        .expect("quote CCTP + Hyperliquid + Unit order");
    let market_quote = match quote.quote {
        RouterOrderQuote::MarketOrder(quote) => quote,
        RouterOrderQuote::LimitOrder(_) => panic!("expected market quote"),
    };
    assert!(
        market_quote.provider_id.contains("cctp_bridge:cctp")
            && market_quote
                .provider_id
                .contains("hyperliquid_bridge_deposit:hyperliquid_bridge")
            && market_quote
                .provider_id
                .contains("hyperliquid_trade:hyperliquid")
            && market_quote.provider_id.contains("unit_withdrawal:unit"),
        "expected CCTP + HyperliquidBridgeDeposit + HyperliquidTrade + UnitWithdrawal route, got {}",
        market_quote.provider_id
    );

    let (order, _) = order_manager
        .create_order_from_quote(CreateOrderRequest {
            quote_id: market_quote.id,
            refund_address: valid_evm_address(),
            idempotency_key: None,
            metadata: json!({ "test": "temporal_order_workflow_cctp_hyperliquid_unit" }),
        })
        .await
        .expect("create CCTP + Hyperliquid + Unit order from quote");
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: source_asset,
            action: VaultAction::Null,
            recovery_address: valid_evm_address(),
            cancellation_commitment:
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            cancel_after: None,
            metadata: json!({ "test": "temporal_order_workflow_cctp_hyperliquid_unit" }),
        })
        .await
        .expect("create CCTP + Hyperliquid + Unit funding vault");
    fund_erc20_source_vault(
        &devnet.base,
        BASE_USDC_ADDRESS.parse().expect("valid Base USDC address"),
        &vault.deposit_vault_address,
        &market_quote.amount_in,
    )
    .await;

    let now = Utc::now();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            now,
        )
        .await
        .expect("mark CCTP + Hyperliquid + Unit vault funded");
    db.orders()
        .transition_status(
            order.id,
            RouterOrderStatus::PendingFunding,
            RouterOrderStatus::Funded,
            now,
        )
        .await
        .expect("mark CCTP + Hyperliquid + Unit order funded");
    SeededOrder {
        order_id: order.id,
        funding_vault_address: vault.deposit_vault_address,
    }
}

async fn seed_funded_arbitrum_hyperliquid_unit_order(
    db: &Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    action_providers: Arc<ActionProviderRegistry>,
    devnet: &RiftDevnet,
) -> SeededOrder {
    let order_manager = OrderManager::with_action_providers(
        db.clone(),
        settings.clone(),
        chain_registry.clone(),
        action_providers,
    );
    let vault_manager = VaultManager::new(db.clone(), settings, chain_registry);
    let source_asset = DepositAsset {
        chain: ChainId::parse("evm:42161").expect("arbitrum chain id"),
        asset: AssetId::parse(ARBITRUM_USDC_ADDRESS).expect("Arbitrum USDC asset"),
    };
    let destination_asset = DepositAsset {
        chain: ChainId::parse("bitcoin").expect("bitcoin chain id"),
        asset: AssetId::Native,
    };
    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: destination_asset,
            recipient_address: devnet.bitcoin.miner_address.to_string(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: ORDER_USDC_AMOUNT_RAW.to_string(),
                slippage_bps: Some(100),
            },
        })
        .await
        .expect("quote Arbitrum USDC + Hyperliquid + Unit order");
    let market_quote = match quote.quote {
        RouterOrderQuote::MarketOrder(quote) => quote,
        RouterOrderQuote::LimitOrder(_) => panic!("expected market quote"),
    };
    assert!(
        !market_quote.provider_id.contains("cctp_bridge:cctp")
            && market_quote
                .provider_id
                .contains("hyperliquid_bridge_deposit:hyperliquid_bridge")
            && market_quote
                .provider_id
                .contains("hyperliquid_trade:hyperliquid")
            && market_quote.provider_id.contains("unit_withdrawal:unit"),
        "expected direct Arbitrum HyperliquidBridgeDeposit + HyperliquidTrade + UnitWithdrawal route, got {}",
        market_quote.provider_id
    );

    let (order, _) = order_manager
        .create_order_from_quote(CreateOrderRequest {
            quote_id: market_quote.id,
            refund_address: valid_evm_address(),
            idempotency_key: None,
            metadata: json!({ "test": "temporal_order_workflow_arbitrum_hyperliquid_unit" }),
        })
        .await
        .expect("create Arbitrum USDC + Hyperliquid + Unit order from quote");
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: source_asset,
            action: VaultAction::Null,
            recovery_address: valid_evm_address(),
            cancellation_commitment:
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            cancel_after: None,
            metadata: json!({ "test": "temporal_order_workflow_arbitrum_hyperliquid_unit" }),
        })
        .await
        .expect("create Arbitrum USDC + Hyperliquid + Unit funding vault");
    fund_erc20_source_vault(
        &devnet.arbitrum,
        ARBITRUM_USDC_ADDRESS
            .parse()
            .expect("valid Arbitrum USDC address"),
        &vault.deposit_vault_address,
        &market_quote.amount_in,
    )
    .await;

    let now = Utc::now();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            now,
        )
        .await
        .expect("mark Arbitrum USDC + Hyperliquid + Unit vault funded");
    db.orders()
        .transition_status(
            order.id,
            RouterOrderStatus::PendingFunding,
            RouterOrderStatus::Funded,
            now,
        )
        .await
        .expect("mark Arbitrum USDC + Hyperliquid + Unit order funded");
    SeededOrder {
        order_id: order.id,
        funding_vault_address: vault.deposit_vault_address,
    }
}

async fn seed_funded_unit_order(
    db: &Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    action_providers: Arc<ActionProviderRegistry>,
    devnet: &RiftDevnet,
) -> SeededOrder {
    let order_manager = OrderManager::with_action_providers(
        db.clone(),
        settings.clone(),
        chain_registry.clone(),
        action_providers,
    );
    let vault_manager = VaultManager::new(db.clone(), settings, chain_registry);
    let source_asset = DepositAsset {
        chain: ChainId::parse("bitcoin").expect("bitcoin chain id"),
        asset: AssetId::Native,
    };
    let destination_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").expect("base chain id"),
        asset: AssetId::Native,
    };
    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: destination_asset,
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: ORDER_BTC_AMOUNT_SATS.to_string(),
                slippage_bps: Some(100),
            },
        })
        .await
        .expect("quote Unit order");
    let market_quote = match quote.quote {
        RouterOrderQuote::MarketOrder(quote) => quote,
        RouterOrderQuote::LimitOrder(_) => panic!("expected market quote"),
    };
    assert!(
        market_quote.provider_id.contains("unit_deposit:unit")
            && market_quote
                .provider_id
                .contains("hyperliquid_trade:hyperliquid")
            && market_quote.provider_id.contains("unit_withdrawal:unit"),
        "expected a UnitDeposit + HyperliquidTrade + UnitWithdrawal route, got {}",
        market_quote.provider_id
    );

    let (order, _) = order_manager
        .create_order_from_quote(CreateOrderRequest {
            quote_id: market_quote.id,
            refund_address: devnet.bitcoin.miner_address.to_string(),
            idempotency_key: None,
            metadata: json!({ "test": "temporal_order_workflow_unit" }),
        })
        .await
        .expect("create Unit order from quote");
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: source_asset,
            action: VaultAction::Null,
            recovery_address: devnet.bitcoin.miner_address.to_string(),
            cancellation_commitment:
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            cancel_after: None,
            metadata: json!({ "test": "temporal_order_workflow_unit" }),
        })
        .await
        .expect("create Unit funding vault");
    let funding_address = BitcoinAddress::from_str(&vault.deposit_vault_address)
        .expect("funding vault Bitcoin address")
        .require_network(BitcoinNetwork::Regtest)
        .expect("regtest funding vault address");
    let amount_sats = market_quote
        .amount_in
        .parse::<u64>()
        .expect("Unit quote amount_in should fit in sats")
        + 50_000;
    devnet
        .bitcoin
        .deal_bitcoin(&funding_address, &BitcoinAmount::from_sat(amount_sats))
        .await
        .expect("fund Bitcoin source vault");

    let now = Utc::now();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            now,
        )
        .await
        .expect("mark Unit vault funded");
    db.orders()
        .transition_status(
            order.id,
            RouterOrderStatus::PendingFunding,
            RouterOrderStatus::Funded,
            now,
        )
        .await
        .expect("mark Unit order funded");
    SeededOrder {
        order_id: order.id,
        funding_vault_address: vault.deposit_vault_address,
    }
}

async fn fund_source_vault(devnet: &RiftDevnet, vault_address: &str, amount_in: &str) {
    let vault_address = Address::from_str(vault_address).expect("funding vault EVM address");
    let amount = U256::from_str_radix(amount_in, 10).expect("amount parses");
    send_native(
        &devnet.base,
        vault_address,
        amount + U256::from(EVM_NATIVE_GAS_BUFFER_WEI),
    )
    .await;
    mine_evm_confirmation_block(&devnet.base).await;
}

async fn fund_erc20_source_vault(
    devnet: &devnet::EthDevnet,
    token_address: Address,
    vault_address: &str,
    amount_in: &str,
) {
    let vault_address = Address::from_str(vault_address).expect("funding vault EVM address");
    let amount = U256::from_str_radix(amount_in, 10).expect("amount parses");
    send_native(devnet, vault_address, U256::from(EVM_NATIVE_GAS_BUFFER_WEI)).await;
    mint_erc20(devnet, token_address, vault_address, amount).await;
    mine_evm_confirmation_block(devnet).await;
}

async fn send_native(devnet: &devnet::EthDevnet, recipient: Address, amount: U256) {
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(anvil_signer(devnet)))
        .connect_http(devnet.anvil.endpoint_url());
    let tx = TransactionRequest::default()
        .with_to(recipient)
        .with_value(amount);
    let receipt = provider
        .send_transaction(tx)
        .await
        .expect("send native transfer")
        .get_receipt()
        .await
        .expect("native transfer receipt");
    assert!(receipt.status(), "native transfer reverted");
}

async fn set_native_balance(devnet: &devnet::EthDevnet, address: Address, amount: U256) {
    let provider = ProviderBuilder::new().connect_http(devnet.anvil.endpoint_url());
    provider
        .anvil_set_balance(address, amount)
        .await
        .expect("set native balance");
    mine_evm_confirmation_block(devnet).await;
}

async fn mine_evm_confirmation_block(devnet: &devnet::EthDevnet) {
    let provider = ProviderBuilder::new().connect_http(devnet.anvil.endpoint_url());
    provider
        .anvil_mine(Some(1), None)
        .await
        .expect("mine EVM confirmation block");
}

async fn native_balance(devnet: &devnet::EthDevnet, address: &str) -> U256 {
    let provider = ProviderBuilder::new().connect_http(devnet.anvil.endpoint_url());
    provider
        .get_balance(Address::from_str(address).expect("valid EVM balance address"))
        .await
        .expect("read native balance")
}

async fn erc20_balance(devnet: &devnet::EthDevnet, token_address: Address, owner: Address) -> U256 {
    let provider = ProviderBuilder::new()
        .connect_http(devnet.anvil.endpoint_url())
        .erased();
    let token = GenericEIP3009ERC20Instance::new(token_address, provider);
    token
        .balanceOf(owner)
        .call()
        .await
        .expect("read ERC20 balance")
}

async fn mint_erc20(
    devnet: &devnet::EthDevnet,
    token_address: Address,
    recipient: Address,
    amount: U256,
) {
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(anvil_signer(devnet)))
        .connect_http(devnet.anvil.endpoint_url())
        .erased();
    let token = GenericEIP3009ERC20Instance::new(token_address, provider);
    let receipt = token
        .mint(recipient, amount)
        .send()
        .await
        .expect("send mint")
        .get_receipt()
        .await
        .expect("mint receipt");
    assert!(receipt.status(), "ERC20 mint reverted");
}

async fn install_mock_usdc_clone(devnet: &devnet::EthDevnet, token_address: Address) {
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(anvil_signer(devnet)))
        .connect_http(devnet.anvil.endpoint_url())
        .erased();
    let implementation_code = provider
        .get_code_at(*devnet.mock_erc20_contract.address())
        .await
        .expect("load mock ERC20 bytecode");
    provider
        .anvil_set_code(token_address, implementation_code)
        .await
        .expect("install mock ERC20 bytecode at canonical address");

    let token = GenericEIP3009ERC20Instance::new(token_address, provider.clone());
    let admin = devnet.funded_address;
    token
        .initialize(
            "Mock USD Coin".to_string(),
            "USDC".to_string(),
            "USDC".to_string(),
            6,
            admin,
            admin,
            admin,
            admin,
        )
        .send()
        .await
        .expect("send mock USDC initialize")
        .get_receipt()
        .await
        .expect("mock USDC initialize receipt");
    let minters = [
        admin,
        *devnet.mock_velora_swap_contract.address(),
        Address::from_str(MOCK_CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS)
            .expect("valid mock CCTP MessageTransmitterV2 address"),
    ];
    for minter in minters {
        token
            .configureMinter(minter, U256::MAX)
            .send()
            .await
            .expect("send mock USDC configureMinter")
            .get_receipt()
            .await
            .expect("mock USDC configureMinter receipt");
    }
}

fn anvil_signer(devnet: &devnet::EthDevnet) -> PrivateKeySigner {
    format!(
        "0x{}",
        alloy::hex::encode(devnet.anvil.keys()[0].to_bytes())
    )
    .parse()
    .expect("valid Anvil private key")
}

fn valid_evm_address() -> String {
    "0x1111111111111111111111111111111111111111".to_string()
}
