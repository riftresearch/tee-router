use crate::batched_tx::{checked_u256_add, estimate_evm_gas_or_cap, max_fee_per_gas_with_headroom};
use crate::config::PaymasterBatchConfig;
use crate::delegation::ensure_eip7702_delegation_with_wallet;
use crate::error::{clone_batch_error, PaymasterError, Result};
use crate::metrics::{
    record_paymaster_balance_gwei, record_paymaster_batch_executed,
    record_paymaster_funding_failed, record_paymaster_funding_sent,
    record_paymaster_funding_skipped, record_paymaster_gas_estimate,
};
use alloy::network::TransactionBuilder;
use alloy::primitives::{Address, Bytes, TxHash, U256};
use alloy::providers::{DynProvider, Provider};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use eip7702_delegator_contract::EIP7702Delegator::Execution;
use std::collections::HashMap;
use tokio::sync::{
    mpsc::{self, error::TryRecvError},
    oneshot,
};
use tracing::{debug, info, warn};

pub const PAYMASTER_COMMAND_BUFFER: usize = 128;

#[derive(Clone, Debug)]
pub struct PaymasterFundingRequest {
    pub vault_address: Address,
    pub target_address: Address,
    pub value: U256,
    pub calldata: Bytes,
    pub operation: &'static str,
}

pub struct Paymaster;

#[derive(Clone)]
pub struct PaymasterHandle {
    command_tx: mpsc::Sender<PaymasterCmd>,
}

pub struct PaymasterSubmission<T> {
    response_rx: oneshot::Receiver<Result<T>>,
}

impl<T> PaymasterSubmission<T> {
    pub async fn wait(self) -> Result<T> {
        match self.response_rx.await {
            Ok(result) => result,
            Err(_) => Err(PaymasterError::Actor {
                message: "actor response channel closed".to_string(),
            }),
        }
    }
}

#[derive(Clone)]
pub(crate) struct PaymasterActor {
    pub(crate) provider: DynProvider,
    pub(crate) wallet_provider: DynProvider,
    pub(crate) sponsor_signer: PrivateKeySigner,
    pub(crate) sponsor_address: Address,
    pub(crate) vault_gas_buffer_wei: U256,
    pub(crate) batch_config: PaymasterBatchConfig,
    pub(crate) metric_label: &'static str,
}

struct PendingFundingRequest {
    request: PaymasterFundingRequest,
    response_tx: oneshot::Sender<Result<Option<TxHash>>>,
}

struct PendingExecutionRequest {
    execution: Execution,
    response_tx: oneshot::Sender<Result<TxHash>>,
}

enum PaymasterCmd {
    FundTransactions {
        requests: Vec<PendingFundingRequest>,
    },
    SubmitExecutions {
        executions: Vec<PendingExecutionRequest>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct PaymasterExecution {
    pub(crate) execution: Execution,
    pub(crate) top_up_amount: U256,
}

#[derive(Debug)]
enum PaymasterPrepareOutcome {
    Skip,
    Submit(PaymasterExecution),
}

struct PendingBatchResponse {
    response_tx: PendingBatchResponseTx,
    top_up_amount: Option<U256>,
}

enum PendingBatchResponseTx {
    Funding(oneshot::Sender<Result<Option<TxHash>>>),
    Execution(oneshot::Sender<Result<TxHash>>),
}

impl Paymaster {
    pub fn spawn(
        provider: DynProvider,
        wallet_provider: DynProvider,
        sponsor_signer: PrivateKeySigner,
        vault_gas_buffer_wei: U256,
        metric_label: &'static str,
        config: PaymasterBatchConfig,
    ) -> PaymasterHandle {
        let actor = PaymasterActor {
            provider,
            wallet_provider,
            sponsor_address: sponsor_signer.address(),
            sponsor_signer,
            vault_gas_buffer_wei,
            batch_config: config,
            metric_label,
        };
        let (command_tx, command_rx) = mpsc::channel(PAYMASTER_COMMAND_BUFFER);
        tokio::spawn(run_paymaster_actor(actor, command_rx));
        PaymasterHandle { command_tx }
    }
}

impl PaymasterHandle {
    pub async fn submit(&self, execution: Execution) -> Result<TxHash> {
        let mut submissions = self.submit_submissions(vec![execution]).await?;
        submissions
            .pop()
            .ok_or(PaymasterError::Actor {
                message: "actor response channel closed".to_string(),
            })?
            .wait()
            .await
    }

    pub async fn fund_transaction(
        &self,
        request: PaymasterFundingRequest,
    ) -> Result<Option<TxHash>> {
        let mut submissions = self.fund_transaction_submissions(vec![request]).await?;
        submissions
            .pop()
            .ok_or(PaymasterError::Actor {
                message: "actor response channel closed".to_string(),
            })?
            .wait()
            .await
    }

    pub async fn fund_transaction_submissions(
        &self,
        requests: Vec<PaymasterFundingRequest>,
    ) -> Result<Vec<PaymasterSubmission<Option<TxHash>>>> {
        let mut submissions = Vec::with_capacity(requests.len());
        let requests = requests
            .into_iter()
            .map(|request| {
                let (response_tx, response_rx) = oneshot::channel();
                submissions.push(PaymasterSubmission { response_rx });
                PendingFundingRequest {
                    request,
                    response_tx,
                }
            })
            .collect();

        if self
            .command_tx
            .send(PaymasterCmd::FundTransactions { requests })
            .await
            .is_err()
        {
            return Err(PaymasterError::Actor {
                message: "actor command queue closed".to_string(),
            });
        }

        Ok(submissions)
    }

    async fn submit_submissions(
        &self,
        executions: Vec<Execution>,
    ) -> Result<Vec<PaymasterSubmission<TxHash>>> {
        let mut submissions = Vec::with_capacity(executions.len());
        let executions = executions
            .into_iter()
            .map(|execution| {
                let (response_tx, response_rx) = oneshot::channel();
                submissions.push(PaymasterSubmission { response_rx });
                PendingExecutionRequest {
                    execution,
                    response_tx,
                }
            })
            .collect();

        if self
            .command_tx
            .send(PaymasterCmd::SubmitExecutions { executions })
            .await
            .is_err()
        {
            return Err(PaymasterError::Actor {
                message: "actor command queue closed".to_string(),
            });
        }

        Ok(submissions)
    }
}

async fn run_paymaster_actor(actor: PaymasterActor, mut command_rx: mpsc::Receiver<PaymasterCmd>) {
    match ensure_eip7702_delegation_with_wallet(
        &actor.provider,
        &actor.wallet_provider,
        &actor.sponsor_signer,
        actor.metric_label,
    )
    .await
    {
        Ok(()) => {
            info!(
                chain = actor.metric_label,
                sponsor = %actor.sponsor_address,
                batch_max_size = actor.batch_config.max_size,
                "EVM paymaster actor using EIP-7702 batched funding"
            );
        }
        Err(error) => {
            warn!(
                chain = actor.metric_label,
                sponsor = %actor.sponsor_address,
                error = %error,
                "failed to set up EIP-7702 delegation; falling back to serial paymaster funding"
            );
            run_paymaster_actor_serial(actor, command_rx).await;
            return;
        }
    }

    while let Some(batch) =
        drain_paymaster_batch(&mut command_rx, actor.batch_config.max_size).await
    {
        process_paymaster_batch(&actor, batch).await;
    }
}

async fn run_paymaster_actor_serial(
    actor: PaymasterActor,
    mut command_rx: mpsc::Receiver<PaymasterCmd>,
) {
    while let Some(command) = command_rx.recv().await {
        match command {
            PaymasterCmd::FundTransactions { requests } => {
                for pending in requests {
                    let result = actor.fund_transaction_legacy(pending.request).await;
                    let _ = pending.response_tx.send(result);
                }
            }
            PaymasterCmd::SubmitExecutions { executions } => {
                for pending in executions {
                    let result = actor.send_legacy_execution(pending.execution).await;
                    let _ = pending.response_tx.send(result);
                }
            }
        }
    }
}

async fn drain_paymaster_batch(
    command_rx: &mut mpsc::Receiver<PaymasterCmd>,
    max_size: usize,
) -> Option<Vec<PaymasterCmd>> {
    drain_ready_batch(command_rx, max_size, PAYMASTER_COMMAND_BUFFER).await
}

async fn drain_ready_batch<T>(
    command_rx: &mut mpsc::Receiver<T>,
    max_size: usize,
    capacity_hint: usize,
) -> Option<Vec<T>> {
    let max_size = max_size.max(1);
    let first_command = command_rx.recv().await?;
    let mut batch = Vec::with_capacity(max_size.min(capacity_hint));
    batch.push(first_command);

    while batch.len() < max_size {
        match command_rx.try_recv() {
            Ok(command) => batch.push(command),
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        }
    }

    Some(batch)
}

async fn process_paymaster_batch(actor: &PaymasterActor, commands: Vec<PaymasterCmd>) {
    let mut pending_responses = Vec::new();
    let mut executions = Vec::new();
    let mut virtual_vault_balances = HashMap::new();

    for command in commands {
        match command {
            PaymasterCmd::FundTransactions { requests } => {
                for pending in requests {
                    match actor
                        .prepare_transaction_execution(pending.request, &mut virtual_vault_balances)
                        .await
                    {
                        Ok(PaymasterPrepareOutcome::Skip) => {
                            let _ = pending.response_tx.send(Ok(None));
                        }
                        Ok(PaymasterPrepareOutcome::Submit(prepared)) => {
                            let PaymasterExecution {
                                execution,
                                top_up_amount,
                            } = prepared;
                            executions.push(execution);
                            pending_responses.push(PendingBatchResponse {
                                response_tx: PendingBatchResponseTx::Funding(pending.response_tx),
                                top_up_amount: Some(top_up_amount),
                            });
                        }
                        Err(error) => {
                            let _ = pending.response_tx.send(Err(error));
                        }
                    }
                }
            }
            PaymasterCmd::SubmitExecutions {
                executions: pending_executions,
            } => {
                for pending in pending_executions {
                    executions.push(pending.execution);
                    pending_responses.push(PendingBatchResponse {
                        response_tx: PendingBatchResponseTx::Execution(pending.response_tx),
                        top_up_amount: None,
                    });
                }
            }
        }
    }

    if executions.is_empty() {
        return;
    }

    match actor.send_batched_tx(&executions).await {
        Ok(tx_hash) => {
            record_paymaster_batch_executed(actor.metric_label, executions.len(), true);
            for pending in pending_responses {
                pending.send_ok(tx_hash, actor.metric_label);
            }
        }
        Err(error) => {
            record_paymaster_batch_executed(actor.metric_label, executions.len(), false);
            for pending in pending_responses {
                pending.send_error(clone_batch_error(&error));
            }
        }
    }
}

impl PendingBatchResponse {
    fn send_ok(self, tx_hash: TxHash, metric_label: &'static str) {
        if let Some(top_up_amount) = self.top_up_amount {
            record_paymaster_funding_sent(metric_label, top_up_amount);
        }
        match self.response_tx {
            PendingBatchResponseTx::Funding(response_tx) => {
                let _ = response_tx.send(Ok(Some(tx_hash)));
            }
            PendingBatchResponseTx::Execution(response_tx) => {
                let _ = response_tx.send(Ok(tx_hash));
            }
        }
    }

    fn send_error(self, error: PaymasterError) {
        match self.response_tx {
            PendingBatchResponseTx::Funding(response_tx) => {
                let _ = response_tx.send(Err(error));
            }
            PendingBatchResponseTx::Execution(response_tx) => {
                let _ = response_tx.send(Err(error));
            }
        }
    }
}

impl PaymasterActor {
    async fn prepare_transaction_execution(
        &self,
        request: PaymasterFundingRequest,
        virtual_vault_balances: &mut HashMap<Address, U256>,
    ) -> Result<PaymasterPrepareOutcome> {
        let PaymasterFundingRequest {
            vault_address,
            target_address,
            value,
            calldata,
            operation,
        } = request;

        debug!(
            chain = self.metric_label,
            %vault_address,
            %target_address,
            %value,
            operation,
            "EVM paymaster actor processing transaction funding request"
        );

        let action_estimate_request = TransactionRequest::default()
            .with_from(vault_address)
            .with_to(target_address)
            .with_value(value)
            .with_gas_limit(crate::batched_tx::EVM_CALL_ESTIMATE_GAS_CAP)
            .with_input(calldata);
        let action_gas_limit = estimate_evm_gas_or_cap(
            &self.provider,
            self.metric_label,
            action_estimate_request,
            operation,
        )
        .await?;
        record_paymaster_gas_estimate(self.metric_label, operation, action_gas_limit);
        let fee_estimate = crate::batched_tx::retry_evm_rpc(
            self.metric_label,
            "estimate_eip1559_fees",
            || async { self.provider.estimate_eip1559_fees().await },
        )
        .await
        .map_err(|source| PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        })?;
        let action_max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;
        let action_reserved_fee = crate::batched_tx::checked_gas_fee(
            action_gas_limit,
            action_max_fee_per_gas,
            "paymaster action gas reservation",
        )?;
        let required_vault_balance = checked_u256_add(
            value,
            action_reserved_fee,
            "paymaster vault required balance",
        )?;
        let required_vault_balance = checked_u256_add(
            required_vault_balance,
            self.vault_gas_buffer_wei,
            "paymaster vault gas buffer",
        )?;

        let effective_balance = match virtual_vault_balances.entry(vault_address) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let current_balance =
                    crate::batched_tx::retry_evm_rpc(self.metric_label, "get_balance", || async {
                        self.provider.get_balance(vault_address).await
                    })
                    .await
                    .map_err(|source| PaymasterError::EVMRpcError {
                        source,
                        loc: snafu::location!(),
                    })?;
                record_paymaster_balance_gwei(self.metric_label, "vault", current_balance);
                entry.insert(current_balance)
            }
        };

        if *effective_balance < value {
            record_paymaster_funding_failed(self.metric_label, "insufficient_action_value");
            return Err(PaymasterError::InsufficientBalance {
                required: value,
                available: *effective_balance,
            });
        }

        match prepare_funding_execution_from_balance(
            vault_address,
            value,
            required_vault_balance,
            *effective_balance,
        )? {
            PaymasterPrepareOutcome::Skip => {
                record_paymaster_funding_skipped(self.metric_label, "already_funded");
                Ok(PaymasterPrepareOutcome::Skip)
            }
            PaymasterPrepareOutcome::Submit(prepared) => {
                *effective_balance = checked_u256_add(
                    *effective_balance,
                    prepared.top_up_amount,
                    "paymaster virtual vault balance",
                )?;
                Ok(PaymasterPrepareOutcome::Submit(prepared))
            }
        }
    }

    async fn fund_transaction_legacy(
        &self,
        request: PaymasterFundingRequest,
    ) -> Result<Option<TxHash>> {
        let mut virtual_vault_balances = HashMap::new();
        match self
            .prepare_transaction_execution(request, &mut virtual_vault_balances)
            .await?
        {
            PaymasterPrepareOutcome::Skip => Ok(None),
            PaymasterPrepareOutcome::Submit(prepared) => {
                self.send_legacy_top_up(prepared).await.map(Some)
            }
        }
    }
}

fn prepare_funding_execution_from_balance(
    vault_address: Address,
    action_value: U256,
    required_vault_balance: U256,
    current_balance: U256,
) -> Result<PaymasterPrepareOutcome> {
    if current_balance < action_value {
        return Err(PaymasterError::InsufficientBalance {
            required: action_value,
            available: current_balance,
        });
    }
    if current_balance >= required_vault_balance {
        return Ok(PaymasterPrepareOutcome::Skip);
    }

    let top_up_amount = crate::batched_tx::checked_u256_sub(
        required_vault_balance,
        current_balance,
        "paymaster vault top-up amount",
    )?;
    Ok(PaymasterPrepareOutcome::Submit(PaymasterExecution {
        execution: Execution {
            target: vault_address,
            value: top_up_amount,
            callData: Bytes::new(),
        },
        top_up_amount,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::sol_types::SolValue;

    #[test]
    fn prepare_execution_skips_when_vault_balance_is_sufficient() {
        let vault_address = Address::repeat_byte(0x11);
        let outcome = prepare_funding_execution_from_balance(
            vault_address,
            U256::ZERO,
            U256::from(100),
            U256::from(100),
        )
        .unwrap();

        assert!(matches!(outcome, PaymasterPrepareOutcome::Skip));
    }

    #[test]
    fn prepare_execution_submits_native_top_up_when_vault_balance_is_insufficient() {
        let vault_address = Address::repeat_byte(0x22);
        let outcome = prepare_funding_execution_from_balance(
            vault_address,
            U256::ZERO,
            U256::from(100),
            U256::from(40),
        )
        .unwrap();

        let PaymasterPrepareOutcome::Submit(prepared) = outcome else {
            panic!("expected top-up execution");
        };
        assert_eq!(prepared.top_up_amount, U256::from(60));
        assert_eq!(prepared.execution.target, vault_address);
        assert_eq!(prepared.execution.value, U256::from(60));
        assert!(prepared.execution.callData.is_empty());
    }

    #[test]
    fn batched_executions_abi_encode_roundtrip() {
        let executions = vec![
            Execution {
                target: Address::repeat_byte(0x33),
                value: U256::from(1),
                callData: Bytes::from_static(&[0xaa, 0xbb]),
            },
            Execution {
                target: Address::repeat_byte(0x44),
                value: U256::from(2),
                callData: Bytes::new(),
            },
        ];

        let encoded = executions.abi_encode();
        let decoded = Vec::<Execution>::abi_decode(&encoded).unwrap();

        assert_eq!(decoded.len(), executions.len());
        for (actual, expected) in decoded.iter().zip(executions.iter()) {
            assert_eq!(actual.target, expected.target);
            assert_eq!(actual.value, expected.value);
            assert_eq!(actual.callData, expected.callData);
        }
    }

    #[tokio::test]
    async fn drain_batch_drains_available_commands_up_to_max_size() {
        let (command_tx, mut command_rx) = mpsc::channel(4);
        for byte in 0x01..=0x03 {
            command_tx
                .send(test_paymaster_command(Address::repeat_byte(byte)))
                .await
                .unwrap();
        }

        let batch = drain_paymaster_batch(&mut command_rx, 4).await.unwrap();

        assert_eq!(batch.len(), 3);
        assert!(matches!(command_rx.try_recv(), Err(TryRecvError::Empty)));

        for byte in 0x04..=0x06 {
            command_tx
                .send(test_paymaster_command(Address::repeat_byte(byte)))
                .await
                .unwrap();
        }

        let batch = drain_paymaster_batch(&mut command_rx, 2).await.unwrap();

        assert_eq!(batch.len(), 2);
        assert!(command_rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn drain_batch_returns_single_command_without_waiting_for_more() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        command_tx
            .send(test_paymaster_command(Address::repeat_byte(0x01)))
            .await
            .unwrap();

        let batch = drain_paymaster_batch(
            &mut command_rx,
            crate::config::EIP7702_PAYMASTER_BATCH_DEFAULT_MAX_SIZE,
        )
        .await
        .unwrap();

        assert_eq!(batch.len(), 1);
    }

    #[tokio::test]
    async fn drain_batch_waits_for_first_command_when_empty() {
        let (_command_tx, mut command_rx) = mpsc::channel(1);
        let mut drain = Box::pin(drain_paymaster_batch(
            &mut command_rx,
            crate::config::EIP7702_PAYMASTER_BATCH_DEFAULT_MAX_SIZE,
        ));
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);

        assert!(matches!(
            std::future::Future::poll(drain.as_mut(), &mut context),
            std::task::Poll::Pending
        ));
    }

    fn test_paymaster_command(vault_address: Address) -> PaymasterCmd {
        let (response_tx, _response_rx) = oneshot::channel();
        PaymasterCmd::FundTransactions {
            requests: vec![PendingFundingRequest {
                request: PaymasterFundingRequest {
                    vault_address,
                    target_address: Address::repeat_byte(0xaa),
                    value: U256::ZERO,
                    calldata: Bytes::new(),
                    operation: "test",
                },
                response_tx,
            }],
        }
    }
}
