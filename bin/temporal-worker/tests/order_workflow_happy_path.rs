use std::{process::Command, str::FromStr, sync::Arc, time::Duration};

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, U256},
    providers::{ext::AnvilApi, Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use chains::{
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
use serde_json::json;
use sqlx_core::connection::Connection;
use sqlx_postgres::{PgConnectOptions, PgConnection, PgPool};
use temporal_worker::{
    order_execution::{
        activities::{OrderActivities, OrderActivityDeps},
        build_worker, order_workflow_id, refund_workflow_id,
        types::{
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
const ORDER_USDC_AMOUNT_RAW: &str = "150000000";
const TEST_RESIDUAL_SINK_ADDRESS: &str = "0x2222222222222222222222222222222222222222";

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
    AcrossBaseEthToEthereumUsdc,
    CctpBaseUsdcToArbitrumEth,
}

#[derive(Default)]
struct WorkflowOptions {
    route: WorkflowRoute,
    velora_transaction_failures: usize,
    velora_stale_quote_failures: usize,
    expire_quote_legs: bool,
    refreshed_eth_usd_micro: Option<u128>,
    expect_external_custody_across_refund: bool,
    expect_hyperliquid_spot_unit_refund: bool,
    expect_external_custody_cctp_refund: bool,
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
        velora_stale_quote_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS,
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
async fn order_workflow_refreshes_stale_quote_multi_leg_then_completes() {
    let run = run_order_workflow(WorkflowOptions {
        route: WorkflowRoute::AcrossBaseEthToEthereumUsdc,
        velora_stale_quote_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS,
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
async fn order_workflow_refresh_untenable_routes_to_refund() {
    let run = run_order_workflow(WorkflowOptions {
        velora_stale_quote_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS,
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
        velora_stale_quote_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS,
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
    if matches!(options.route, WorkflowRoute::AcrossBaseEthToEthereumUsdc) {
        install_mock_usdc_clone(
            &devnet.ethereum,
            ETHEREUM_USDC_ADDRESS
                .parse()
                .expect("valid Ethereum USDC address"),
        )
        .await;
    }
    if matches!(options.route, WorkflowRoute::CctpBaseUsdcToArbitrumEth) {
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
    let hyperliquid_enabled = options.expect_hyperliquid_spot_unit_refund;
    let cctp_enabled = matches!(options.route, WorkflowRoute::CctpBaseUsdcToArbitrumEth);
    let chain_registry =
        Arc::new(test_chain_registry(&devnet, options.route, hyperliquid_enabled).await);
    let action_providers = Arc::new(test_action_providers(
        &mocks,
        options.route,
        hyperliquid_enabled,
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
    if options.expire_quote_legs {
        expire_market_order_quote_legs(&database_url, order_id).await;
    }
    if let Some(usd_micro) = options.refreshed_eth_usd_micro {
        mocks.set_velora_usd_price_micro("ETH", usd_micro).await;
    }

    let task_queue = format!("{DEFAULT_TASK_QUEUE}-{}", Uuid::now_v7());
    let connection = TemporalConnection {
        temporal_address: "http://127.0.0.1:7233".to_string(),
        namespace: "default".to_string(),
    };
    let activity_deps = OrderActivityDeps::new(db.clone(), action_providers, custody_executor);

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
            let workflow_id = order_workflow_id(order_id);
            let handle = client
                .start_workflow(
                    OrderWorkflow::run,
                    OrderWorkflowInput { order_id },
                    workflow_start_options(&task_queue, &workflow_id),
                )
                .await
                .expect("start OrderWorkflow");
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
                handle
                    .signal(
                        OrderWorkflow::provider_operation_hint,
                        ProviderOperationHintSignal {
                            order_id,
                            hint_id: Uuid::now_v7(),
                            provider_operation_id: Some(across_operation.id),
                            provider: ProviderKind::Bridge,
                            hint_kind: ProviderHintKind::AcrossFill,
                            provider_ref: across_operation.provider_ref,
                        },
                        WorkflowSignalOptions::default(),
                    )
                    .await
                    .expect("send Across provider-operation hint signal");
            }
            if matches!(options.route, WorkflowRoute::CctpBaseUsdcToArbitrumEth) {
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
            if options.expect_external_custody_across_refund {
                signal_external_custody_refund_across_fill(
                    &client,
                    &db_for_workflow,
                    &mocks,
                    devnet_for_workflow,
                    &seeded.funding_vault_address,
                    order_id,
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

fn ensure_temporal_up() {
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
    route: WorkflowRoute,
    hyperliquid_enabled: bool,
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
    if matches!(route, WorkflowRoute::AcrossBaseEthToEthereumUsdc) {
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
    if matches!(route, WorkflowRoute::CctpBaseUsdcToArbitrumEth) {
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
    registry
}

fn test_action_providers(
    mocks: &MockIntegratorServer,
    route: WorkflowRoute,
    hyperliquid_enabled: bool,
    cctp_enabled: bool,
) -> ActionProviderRegistry {
    ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
        across: matches!(route, WorkflowRoute::AcrossBaseEthToEthereumUsdc).then(|| {
            AcrossHttpProviderConfig::new(mocks.base_url().to_string(), "temporal-worker-across")
        }),
        cctp: cctp_enabled.then(|| CctpHttpProviderConfig::mock(mocks.base_url().to_string())),
        hyperunit_base_url: hyperliquid_enabled.then(|| mocks.base_url().to_string()),
        hyperunit_proxy_url: None,
        hyperliquid_base_url: hyperliquid_enabled.then(|| mocks.base_url().to_string()),
        velora: matches!(
            route,
            WorkflowRoute::VeloraBaseEthToBaseUsdc
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
    if matches!(options.route, WorkflowRoute::VeloraBaseEthToBaseUsdc) {
        config = config.with_velora_swap_contract_address(
            devnet.base.anvil.chain_id(),
            format!("{:#x}", devnet.base.mock_velora_swap_contract.address()),
        );
    }
    if matches!(options.route, WorkflowRoute::AcrossBaseEthToEthereumUsdc) {
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
    if matches!(options.route, WorkflowRoute::CctpBaseUsdcToArbitrumEth) {
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
    if options.expect_hyperliquid_spot_unit_refund {
        config = config
            .with_unit_evm_rpc_url(
                hyperunit_client::UnitChain::Base,
                devnet.base.anvil.endpoint_url().to_string(),
            )
            .with_mainnet_hyperliquid(false);
    }
    config = config
        .with_velora_transaction_fail_next_n(options.velora_transaction_failures)
        .with_velora_transaction_stale_quote_fail_next_n(options.velora_stale_quote_failures);
    MockIntegratorServer::spawn_with_config(config)
        .await
        .expect("spawn mock integrators")
}

async fn expire_market_order_quote_legs(database_url: &str, order_id: Uuid) {
    let pool = PgPool::connect(database_url)
        .await
        .expect("connect raw pool for quote expiry");
    let expired_at = (Utc::now() - chrono::Duration::minutes(1)).to_rfc3339();
    sqlx_core::query::query(
        r#"
        UPDATE market_order_quotes
        SET provider_quote = jsonb_set(
            provider_quote,
            '{legs}',
            (
                SELECT jsonb_agg(jsonb_set(leg.value, '{expires_at}', to_jsonb($2::text), false))
                FROM jsonb_array_elements(provider_quote->'legs') AS leg(value)
            ),
            false
        )
        WHERE order_id = $1
        "#,
    )
    .bind(order_id)
    .bind(expired_at)
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
    handle_before_hint
        .signal(
            OrderWorkflow::provider_operation_hint,
            ProviderOperationHintSignal {
                order_id,
                hint_id: Uuid::now_v7(),
                provider_operation_id: Some(cctp_operation.id),
                provider: ProviderKind::Bridge,
                hint_kind: ProviderHintKind::ProviderObservation,
                provider_ref: cctp_operation.provider_ref,
            },
            WorkflowSignalOptions::default(),
        )
        .await
        .expect("send CCTP provider-operation hint signal");
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

    let refund_workflow = client
        .get_workflow_handle::<RefundWorkflow>(&refund_workflow_id(order_id, parent_attempt.id));
    refund_workflow
        .signal(
            RefundWorkflow::provider_operation_hint,
            ProviderOperationHintSignal {
                order_id,
                hint_id: Uuid::now_v7(),
                provider_operation_id: Some(refund_operation.id),
                provider: ProviderKind::Bridge,
                hint_kind: ProviderHintKind::AcrossFill,
                provider_ref: refund_operation.provider_ref,
            },
            WorkflowSignalOptions::default(),
        )
        .await
        .expect("send refund Across provider-operation hint signal");
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

    let refund_workflow = client
        .get_workflow_handle::<RefundWorkflow>(&refund_workflow_id(order_id, parent_attempt.id));
    refund_workflow
        .signal(
            RefundWorkflow::provider_operation_hint,
            ProviderOperationHintSignal {
                order_id,
                hint_id: Uuid::now_v7(),
                provider_operation_id: Some(refund_operation.id),
                provider: ProviderKind::Bridge,
                hint_kind: ProviderHintKind::ProviderObservation,
                provider_ref: refund_operation.provider_ref,
            },
            WorkflowSignalOptions::default(),
        )
        .await
        .expect("send refund CCTP provider-operation hint signal");
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

    let refund_workflow = client
        .get_workflow_handle::<RefundWorkflow>(&refund_workflow_id(order_id, parent_attempt.id));
    refund_workflow
        .signal(
            RefundWorkflow::provider_operation_hint,
            ProviderOperationHintSignal {
                order_id,
                hint_id: Uuid::now_v7(),
                provider_operation_id: Some(refund_operation.id),
                provider: ProviderKind::Unit,
                hint_kind: ProviderHintKind::ProviderObservation,
                provider_ref: refund_operation.provider_ref,
            },
            WorkflowSignalOptions::default(),
        )
        .await
        .expect("send refund UnitWithdrawal provider-operation hint signal");
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
    let destination_vault = db
        .orders()
        .get_custody_vaults(order_id)
        .await
        .expect("load custody vaults")
        .into_iter()
        .find(|vault| vault.role == CustodyVaultRole::DestinationExecution)
        .expect("Across step should create destination execution custody vault");
    let output_amount = operation
        .response
        .get("expectedOutputAmount")
        .or_else(|| operation.response.get("outputAmount"))
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
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: ORDER_AMOUNT_IN_WEI.to_string(),
                slippage_bps: Some(100),
            },
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
