use crate::traits::{UserDepositCandidateStatus, VerifiedUserDeposit};
use crate::{key_derivation, ChainOperations, Result};
use alloy::consensus::Transaction as _;
use alloy::network::{EthereumWallet, TransactionBuilder, TransactionBuilder7702};
use alloy::primitives::{Address, Bytes, TxHash, U256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::{
    Authorization, Filter, Log as RpcLog, TransactionReceipt, TransactionRequest,
};
use alloy::signers::local::{LocalSigner, PrivateKeySigner};
use alloy::signers::SignerSync;
use alloy::sol_types::SolValue;
use alloy::{hex, sol};
use async_trait::async_trait;
use blockchain_utils::create_transfer_with_authorization_execution;
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use eip7702_delegator_contract::{
    EIP7702Delegator::{EIP7702DelegatorInstance, Execution},
    ModeCode, EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS,
};
use metrics::{counter, gauge, histogram};
use router_primitives::{
    ChainType, ConfirmedTxStatus, Currency, Lot, PendingTxStatus, TokenIdentifier, TxStatus, Wallet,
};
use serde::{Deserialize, Serialize};
use snafu::location;
use std::{
    fmt,
    future::Future,
    str::FromStr,
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::{
    sync::{
        mpsc::{self, error::TryRecvError},
        oneshot,
    },
    task::{JoinError, JoinSet},
    time::sleep,
};
use tracing::{debug, info, warn};

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

const EVM_PAYMASTER_COMMAND_BUFFER: usize = 128;
const EVM_PAYMASTER_RESPONSE_CHANNEL_CLOSED: &str = "actor response channel closed";
const EVM_RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const EVM_RECEIPT_TIMEOUT: Duration = Duration::from_secs(300);
const EVM_RPC_RETRY_ATTEMPTS: usize = 6;
const EVM_RPC_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(500);
const EVM_CALL_ESTIMATE_GAS_CAP: u64 = 1_000_000;
const EVM_PAYMASTER_BATCH_DEFAULT_MAX_SIZE: usize = 64;
const ROUTER_PAYMASTER_BATCH_MAX_SIZE_ENV: &str = "ROUTER_PAYMASTER_BATCH_MAX_SIZE";
const WEI_PER_GWEI: f64 = 1_000_000_000.0;
const FLASHBOTS_ETHEREUM_RPC_URL: &str = "https://rpc.flashbots.net";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvmBroadcastPolicy {
    #[default]
    Standard,
    FlashbotsIfEthereum,
}

pub struct EvmChain {
    provider: DynProvider,
    rpc_url: String,
    allowed_token: Address,
    chain_type: ChainType,
    wallet_seed_tag: Vec<u8>,
    min_confirmations: u32,
    est_block_time: Duration,
    gas_sponsor: Option<EvmGasSponsor>,
}

#[derive(Clone)]
pub struct EvmGasSponsorConfig {
    pub private_key: String,
    pub vault_gas_buffer_wei: U256,
}

impl fmt::Debug for EvmGasSponsorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EvmGasSponsorConfig")
            .field("private_key", &"<redacted>")
            .field("vault_gas_buffer_wei", &self.vault_gas_buffer_wei)
            .finish()
    }
}

struct EvmGasSponsor {
    actor: EvmPaymasterActor,
    actor_runtime: Mutex<Option<EvmPaymasterRuntime>>,
    chain_type: ChainType,
}

struct EvmPaymasterRuntime {
    command_tx: mpsc::Sender<EvmPaymasterCommand>,
    actor_tasks: JoinSet<()>,
}

#[derive(Clone)]
struct EvmPaymasterActor {
    provider: DynProvider,
    wallet_provider: DynProvider,
    sponsor_signer: PrivateKeySigner,
    sponsor_address: Address,
    vault_gas_buffer_wei: U256,
    batch_config: EvmPaymasterBatchConfig,
    chain_type: ChainType,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EvmPaymasterBatchConfig {
    max_size: usize,
}

#[derive(Clone)]
struct FundVaultAction {
    token_address: Address,
    vault_address: Address,
    recipient_address: Address,
    token_amount: U256,
}

#[derive(Clone)]
struct FundEvmTransactionAction {
    vault_address: Address,
    target_address: Address,
    value: U256,
    calldata: Bytes,
    operation: &'static str,
}

enum EvmPaymasterCommand {
    FundVaultAction {
        request: FundVaultAction,
        response_tx: oneshot::Sender<Result<Option<String>>>,
    },
    FundEvmTransaction {
        request: FundEvmTransactionAction,
        response_tx: oneshot::Sender<Result<Option<String>>>,
    },
}

impl Default for EvmPaymasterBatchConfig {
    fn default() -> Self {
        Self {
            max_size: EVM_PAYMASTER_BATCH_DEFAULT_MAX_SIZE,
        }
    }
}

impl EvmPaymasterBatchConfig {
    fn from_env() -> Self {
        let default = Self::default();
        Self {
            max_size: parse_paymaster_batch_max_size_env(default.max_size),
        }
    }
}

impl EvmChain {
    pub async fn new(
        rpc_url: &str,
        allowed_token: &str,
        chain_type: ChainType,
        wallet_seed_tag: &[u8],
        min_confirmations: u32,
        est_block_time: Duration,
    ) -> Result<Self> {
        Self::new_with_gas_sponsor(
            rpc_url,
            allowed_token,
            chain_type,
            wallet_seed_tag,
            min_confirmations,
            est_block_time,
            None,
        )
        .await
    }

    pub async fn new_with_gas_sponsor(
        rpc_url: &str,
        allowed_token: &str,
        chain_type: ChainType,
        wallet_seed_tag: &[u8],
        min_confirmations: u32,
        est_block_time: Duration,
        gas_sponsor: Option<EvmGasSponsorConfig>,
    ) -> Result<Self> {
        let url = rpc_url.parse().map_err(|_| crate::Error::Serialization {
            message: "Invalid RPC URL".to_string(),
        })?;

        let client = alloy::rpc::client::ClientBuilder::default()
            .layer(crate::rpc_metrics_layer::RpcMetricsLayer::new(
                chain_type.to_db_string().to_string(),
            ))
            .http(url);

        let provider = ProviderBuilder::new().connect_client(client).erased();
        let allowed_token =
            Address::from_str(allowed_token).map_err(|_| crate::Error::Serialization {
                message: "Invalid allowed token address".to_string(),
            })?;
        let gas_sponsor = gas_sponsor
            .map(|config| {
                EvmGasSponsor::start(config, provider.clone(), rpc_url.to_string(), chain_type)
            })
            .transpose()?;

        Ok(Self {
            provider,
            rpc_url: rpc_url.to_string(),
            allowed_token,
            chain_type,
            wallet_seed_tag: wallet_seed_tag.to_vec(),
            min_confirmations,
            est_block_time,
            gas_sponsor,
        })
    }

    pub async fn native_balance(&self, address: &str) -> Result<U256> {
        let address = Address::from_str(address).map_err(|_| crate::Error::Serialization {
            message: "Invalid address".to_string(),
        })?;
        retry_evm_rpc(self.chain_type, "get_balance", || async {
            self.provider.get_balance(address).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })
    }

    pub async fn erc20_balance(&self, token_address: &str, address: &str) -> Result<U256> {
        let token_address =
            Address::from_str(token_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid token address".to_string(),
            })?;
        let address = Address::from_str(address).map_err(|_| crate::Error::Serialization {
            message: "Invalid address".to_string(),
        })?;
        let token_contract = GenericEIP3009ERC20Instance::new(token_address, &self.provider);
        retry_evm_rpc(self.chain_type, "erc20_balanceOf", || async {
            token_contract.balanceOf(address).call().await
        })
        .await
        .map_err(|e| crate::Error::DumpToAddress {
            message: format!("Failed to get token balance: {e}"),
        })
    }

    pub async fn erc20_decimals(&self, token_address: &str) -> Result<u8> {
        let token_address =
            Address::from_str(token_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid token address".to_string(),
            })?;
        let token_contract = GenericEIP3009ERC20Instance::new(token_address, &self.provider);
        retry_evm_rpc(self.chain_type, "erc20_decimals", || async {
            token_contract.decimals().call().await
        })
        .await
        .map_err(|e| crate::Error::DumpToAddress {
            message: format!("Failed to get token decimals: {e}"),
        })
    }

    pub async fn transaction_receipt(&self, tx_hash: &str) -> Result<Option<TransactionReceipt>> {
        let tx_hash = TxHash::from_str(tx_hash).map_err(|_| crate::Error::Serialization {
            message: "Invalid transaction hash".to_string(),
        })?;
        retry_evm_rpc(self.chain_type, "get_transaction_receipt", || async {
            self.provider.get_transaction_receipt(tx_hash).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })
    }

    pub async fn logs(&self, filter: &Filter) -> Result<Vec<RpcLog>> {
        retry_evm_rpc(self.chain_type, "get_logs", || async {
            self.provider.get_logs(filter).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })
    }

    async fn read_erc20_balance_with_provider<P: Provider>(
        &self,
        provider: &P,
        token_address: Address,
        owner: Address,
    ) -> Result<U256> {
        let token_contract = GenericEIP3009ERC20Instance::new(token_address, provider);
        retry_evm_rpc(self.chain_type, "erc20_balanceOf", || async {
            token_contract.balanceOf(owner).call().await
        })
        .await
        .map_err(|e| crate::Error::DumpToAddress {
            message: format!("Failed to get token balance: {e}"),
        })
    }

    async fn send_erc20_transfer_with_provider<P: Provider>(
        &self,
        provider: &P,
        token_address: Address,
        sender: Address,
        recipient: Address,
        amount: U256,
    ) -> Result<String> {
        let token_contract = GenericEIP3009ERC20Instance::new(token_address, provider);
        let calldata = token_contract
            .transfer(recipient, amount)
            .calldata()
            .clone();
        let estimate_request = TransactionRequest::default()
            .with_from(sender)
            .with_to(token_address)
            .with_input(calldata.clone());
        let gas_limit = retry_evm_rpc(self.chain_type, "estimate_gas", || async {
            self.provider.estimate_gas(estimate_request.clone()).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let fee_estimate = retry_evm_rpc(self.chain_type, "estimate_eip1559_fees", || async {
            self.provider.estimate_eip1559_fees().await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;
        let transaction_request = TransactionRequest::default()
            .with_from(sender)
            .with_to(token_address)
            .with_input(calldata)
            .with_nonce(self.pending_nonce(sender).await?)
            .with_gas_limit(gas_limit)
            .with_max_fee_per_gas(max_fee_per_gas)
            .with_max_priority_fee_per_gas(fee_estimate.max_priority_fee_per_gas);

        let pending_tx = provider
            .send_transaction(transaction_request)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;
        let tx_hash = *pending_tx.tx_hash();
        wait_for_evm_receipt(provider, self.chain_type, tx_hash).await?;

        Ok(tx_hash.to_string())
    }

    async fn wait_for_native_balance_at_least(
        &self,
        address: Address,
        required_balance: U256,
        rpc_method: &'static str,
    ) -> Result<U256> {
        let mut delay = EVM_RPC_RETRY_INITIAL_DELAY;
        let mut last_balance = U256::ZERO;
        for attempt in 1..=EVM_RPC_RETRY_ATTEMPTS {
            let balance = retry_evm_rpc(self.chain_type, rpc_method, || async {
                self.provider.get_balance(address).await
            })
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;
            if balance >= required_balance {
                return Ok(balance);
            }
            last_balance = balance;
            if attempt < EVM_RPC_RETRY_ATTEMPTS {
                record_evm_rpc_retry(self.chain_type, rpc_method, "state_lag");
                warn!(
                    chain = %self.chain_type.to_db_string(),
                    rpc_method,
                    attempt,
                    max_attempts = EVM_RPC_RETRY_ATTEMPTS,
                    required_balance = %required_balance,
                    available_balance = %balance,
                    "waiting for native balance to become visible after funding"
                );
                sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
        }

        Err(crate::Error::InsufficientBalance {
            required: required_balance,
            available: last_balance,
        })
    }

    async fn pending_nonce(&self, address: Address) -> Result<u64> {
        retry_evm_rpc(self.chain_type, "get_transaction_count_pending", || async {
            self.provider.get_transaction_count(address).pending().await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })
    }

    pub async fn ensure_native_gas_for_erc20_refund(
        &self,
        token_address: &str,
        vault_address: &str,
        recipient_address: &str,
        token_amount: U256,
    ) -> Result<Option<String>> {
        self.ensure_native_gas_for_erc20_transfer(
            token_address,
            vault_address,
            recipient_address,
            token_amount,
        )
        .await
    }

    pub async fn ensure_native_gas_for_erc20_transfer(
        &self,
        token_address: &str,
        vault_address: &str,
        recipient_address: &str,
        token_amount: U256,
    ) -> Result<Option<String>> {
        let Some(gas_sponsor) = &self.gas_sponsor else {
            return Ok(None);
        };

        let token_address =
            Address::from_str(token_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid token address".to_string(),
            })?;
        let vault_address =
            Address::from_str(vault_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid address".to_string(),
            })?;
        let recipient_address =
            Address::from_str(recipient_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid recipient address".to_string(),
            })?;

        gas_sponsor
            .fund_vault_action(FundVaultAction {
                token_address,
                vault_address,
                recipient_address,
                token_amount,
            })
            .await
    }

    pub async fn ensure_native_gas_for_transaction(
        &self,
        vault_address: &str,
        target_address: &str,
        value: U256,
        calldata: Bytes,
        operation: &'static str,
    ) -> Result<Option<String>> {
        let Some(gas_sponsor) = &self.gas_sponsor else {
            return Ok(None);
        };

        let vault_address =
            Address::from_str(vault_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid address".to_string(),
            })?;
        let target_address =
            Address::from_str(target_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid target address".to_string(),
            })?;

        gas_sponsor
            .fund_evm_transaction(FundEvmTransactionAction {
                vault_address,
                target_address,
                value,
                calldata,
                operation,
            })
            .await
    }

    pub async fn refund_to_address(
        &self,
        token: &TokenIdentifier,
        private_key: &str,
        recipient_address: &str,
    ) -> Result<String> {
        let sender_signer =
            LocalSigner::from_str(private_key).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid private key".to_string(),
            })?;
        let sender_address = sender_signer.address();
        let recipient_address =
            Address::from_str(recipient_address).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid recipient address".to_string(),
            })?;

        let url = self
            .rpc_url
            .parse()
            .map_err(|_| crate::Error::Serialization {
                message: "Invalid RPC URL".to_string(),
            })?;
        let wallet_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::new(sender_signer))
            .connect_http(url);

        match token {
            TokenIdentifier::Native => {
                let native_balance = retry_evm_rpc(self.chain_type, "get_balance", || async {
                    self.provider.get_balance(sender_address).await
                })
                .await
                .map_err(|e| crate::Error::EVMRpcError {
                    source: e,
                    loc: location!(),
                })?;
                if native_balance.is_zero() {
                    return Err(crate::Error::DumpToAddress {
                        message: "No native balance found".to_string(),
                    });
                }

                let estimate_request = TransactionRequest::default()
                    .with_from(sender_address)
                    .with_to(recipient_address)
                    .with_value(U256::ZERO);
                let gas_limit = retry_evm_rpc(self.chain_type, "estimate_gas", || async {
                    self.provider.estimate_gas(estimate_request.clone()).await
                })
                .await
                .map_err(|e| crate::Error::EVMRpcError {
                    source: e,
                    loc: location!(),
                })?;
                let fee_estimate =
                    retry_evm_rpc(self.chain_type, "estimate_eip1559_fees", || async {
                        self.provider.estimate_eip1559_fees().await
                    })
                    .await
                    .map_err(|e| crate::Error::EVMRpcError {
                        source: e,
                        loc: location!(),
                    })?;
                let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;
                let reserved_fee =
                    checked_gas_fee(gas_limit, max_fee_per_gas, "native dump gas reservation")?;
                if native_balance <= reserved_fee {
                    return Err(crate::Error::DumpToAddress {
                        message: "Insufficient native balance to cover refund gas".to_string(),
                    });
                }
                let dump_value =
                    checked_u256_sub(native_balance, reserved_fee, "native dump transfer value")?;

                let transaction_request = TransactionRequest::default()
                    .with_from(sender_address)
                    .with_to(recipient_address)
                    .with_value(dump_value)
                    .with_nonce(self.pending_nonce(sender_address).await?)
                    .with_gas_limit(gas_limit)
                    .with_max_fee_per_gas(max_fee_per_gas)
                    .with_max_priority_fee_per_gas(fee_estimate.max_priority_fee_per_gas);

                let pending_tx = wallet_provider
                    .send_transaction(transaction_request)
                    .await
                    .map_err(|e| crate::Error::EVMRpcError {
                        source: e,
                        loc: location!(),
                    })?;
                let tx_hash = *pending_tx.tx_hash();
                wait_for_evm_receipt(&wallet_provider, self.chain_type, tx_hash).await?;

                Ok(tx_hash.to_string())
            }
            TokenIdentifier::Address(token_address) => {
                let token_address =
                    Address::from_str(token_address).map_err(|_| crate::Error::DumpToAddress {
                        message: "Invalid token address".to_string(),
                    })?;
                let token_balance = self
                    .read_erc20_balance_with_provider(
                        &wallet_provider,
                        token_address,
                        sender_address,
                    )
                    .await
                    .map_err(|source| crate::Error::DumpToAddress {
                        message: source.to_string(),
                    })?;
                if token_balance.is_zero() {
                    return Err(crate::Error::DumpToAddress {
                        message: "No token balance found".to_string(),
                    });
                }

                self.send_erc20_transfer_with_provider(
                    &wallet_provider,
                    token_address,
                    sender_address,
                    recipient_address,
                    token_balance,
                )
                .await
            }
        }
    }

    pub async fn transfer_native_amount(
        &self,
        private_key: &str,
        recipient_address: &str,
        amount: U256,
    ) -> Result<String> {
        if amount.is_zero() {
            return Err(crate::Error::DumpToAddress {
                message: "Native transfer amount must be greater than zero".to_string(),
            });
        }

        let sender_signer =
            LocalSigner::from_str(private_key).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid private key".to_string(),
            })?;
        let sender_address = sender_signer.address();
        let recipient_address =
            Address::from_str(recipient_address).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid recipient address".to_string(),
            })?;

        let native_balance = retry_evm_rpc(self.chain_type, "get_balance", || async {
            self.provider.get_balance(sender_address).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        if native_balance < amount {
            return Err(crate::Error::InsufficientBalance {
                required: amount,
                available: native_balance,
            });
        }

        let estimate_request = TransactionRequest::default()
            .with_from(sender_address)
            .with_to(recipient_address)
            .with_value(amount);
        let gas_limit = retry_evm_rpc(self.chain_type, "estimate_gas", || async {
            self.provider.estimate_gas(estimate_request.clone()).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let fee_estimate = retry_evm_rpc(self.chain_type, "estimate_eip1559_fees", || async {
            self.provider.estimate_eip1559_fees().await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;
        let reserved_fee = checked_gas_fee(
            gas_limit,
            max_fee_per_gas,
            "native transfer gas reservation",
        )?;
        let required_balance =
            checked_u256_add(amount, reserved_fee, "native transfer required balance")?;
        let _native_balance = if native_balance < required_balance {
            self.wait_for_native_balance_at_least(
                sender_address,
                required_balance,
                "get_balance_after_native_transfer_funding",
            )
            .await?
        } else {
            native_balance
        };

        let url = self
            .rpc_url
            .parse()
            .map_err(|_| crate::Error::Serialization {
                message: "Invalid RPC URL".to_string(),
            })?;
        let wallet_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::new(sender_signer))
            .connect_http(url);

        let transaction_request = TransactionRequest::default()
            .with_from(sender_address)
            .with_to(recipient_address)
            .with_value(amount)
            .with_nonce(self.pending_nonce(sender_address).await?)
            .with_gas_limit(gas_limit)
            .with_max_fee_per_gas(max_fee_per_gas)
            .with_max_priority_fee_per_gas(fee_estimate.max_priority_fee_per_gas);

        let pending_tx = wallet_provider
            .send_transaction(transaction_request)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;
        let tx_hash = *pending_tx.tx_hash();
        wait_for_evm_receipt(&wallet_provider, self.chain_type, tx_hash).await?;

        Ok(tx_hash.to_string())
    }

    pub async fn estimate_native_transfer_fee(
        &self,
        sender_address: &str,
        recipient_address: &str,
        amount: U256,
    ) -> Result<U256> {
        let sender_address =
            Address::from_str(sender_address).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid sender address".to_string(),
            })?;
        let recipient_address =
            Address::from_str(recipient_address).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid recipient address".to_string(),
            })?;

        let estimate_request = TransactionRequest::default()
            .with_from(sender_address)
            .with_to(recipient_address)
            .with_value(amount);
        let gas_limit = retry_evm_rpc(self.chain_type, "estimate_gas", || async {
            self.provider.estimate_gas(estimate_request.clone()).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let fee_estimate = retry_evm_rpc(self.chain_type, "estimate_eip1559_fees", || async {
            self.provider.estimate_eip1559_fees().await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;
        checked_gas_fee(gas_limit, max_fee_per_gas, "native transfer fee estimate")
    }

    pub async fn transfer_erc20_amount(
        &self,
        token_address: &str,
        private_key: &str,
        recipient_address: &str,
        amount: U256,
    ) -> Result<String> {
        if amount.is_zero() {
            return Err(crate::Error::DumpToAddress {
                message: "ERC-20 transfer amount must be greater than zero".to_string(),
            });
        }

        let token_address =
            Address::from_str(token_address).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid token address".to_string(),
            })?;
        let sender_signer =
            LocalSigner::from_str(private_key).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid private key".to_string(),
            })?;
        let sender_address = sender_signer.address();
        let recipient_address =
            Address::from_str(recipient_address).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid recipient address".to_string(),
            })?;

        let url = self
            .rpc_url
            .parse()
            .map_err(|_| crate::Error::Serialization {
                message: "Invalid RPC URL".to_string(),
            })?;
        let wallet_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::new(sender_signer))
            .connect_http(url);
        let token_balance = self
            .read_erc20_balance_with_provider(&wallet_provider, token_address, sender_address)
            .await
            .map_err(|source| crate::Error::DumpToAddress {
                message: source.to_string(),
            })?;
        if token_balance < amount {
            return Err(crate::Error::InsufficientBalance {
                required: amount,
                available: token_balance,
            });
        }

        self.send_erc20_transfer_with_provider(
            &wallet_provider,
            token_address,
            sender_address,
            recipient_address,
            amount,
        )
        .await
    }

    pub async fn send_call(
        &self,
        private_key: &str,
        to_address: &str,
        value: U256,
        calldata: Bytes,
    ) -> Result<EvmCallOutcome> {
        self.send_call_with_broadcast_policy(
            private_key,
            to_address,
            value,
            calldata,
            EvmBroadcastPolicy::Standard,
        )
        .await
    }

    pub async fn send_call_with_broadcast_policy(
        &self,
        private_key: &str,
        to_address: &str,
        value: U256,
        calldata: Bytes,
        broadcast_policy: EvmBroadcastPolicy,
    ) -> Result<EvmCallOutcome> {
        let sender_signer =
            LocalSigner::from_str(private_key).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid private key".to_string(),
            })?;
        let sender_address = sender_signer.address();
        let to_address =
            Address::from_str(to_address).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid call target address".to_string(),
            })?;

        let native_balance = retry_evm_rpc(self.chain_type, "get_balance", || async {
            self.provider.get_balance(sender_address).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        if native_balance < value {
            return Err(crate::Error::InsufficientBalance {
                required: value,
                available: native_balance,
            });
        }

        let estimate_request = TransactionRequest::default()
            .with_from(sender_address)
            .with_to(to_address)
            .with_value(value)
            .with_gas_limit(EVM_CALL_ESTIMATE_GAS_CAP)
            .with_input(calldata.clone());
        let gas_limit = estimate_evm_gas_or_cap(
            &self.provider,
            self.chain_type,
            estimate_request,
            "evm_call",
        )
        .await?;
        let fee_estimate = retry_evm_rpc(self.chain_type, "estimate_eip1559_fees", || async {
            self.provider.estimate_eip1559_fees().await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;
        let reserved_fee = checked_gas_fee(gas_limit, max_fee_per_gas, "EVM call gas reservation")?;
        let required_balance = checked_u256_add(value, reserved_fee, "EVM call required balance")?;
        let _native_balance = if native_balance < required_balance {
            self.wait_for_native_balance_at_least(
                sender_address,
                required_balance,
                "get_balance_after_call_funding",
            )
            .await?
        } else {
            native_balance
        };

        let broadcast_rpc_url = self.broadcast_rpc_url(broadcast_policy);
        let url = broadcast_rpc_url
            .parse()
            .map_err(|_| crate::Error::Serialization {
                message: "Invalid EVM broadcast RPC URL".to_string(),
            })?;
        let wallet_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::new(sender_signer))
            .connect_http(url);

        let transaction_request = TransactionRequest::default()
            .with_from(sender_address)
            .with_to(to_address)
            .with_value(value)
            .with_input(calldata)
            .with_nonce(self.pending_nonce(sender_address).await?)
            .with_gas_limit(gas_limit)
            .with_max_fee_per_gas(max_fee_per_gas)
            .with_max_priority_fee_per_gas(fee_estimate.max_priority_fee_per_gas);

        let pending_tx = wallet_provider
            .send_transaction(transaction_request)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;
        let tx_hash = *pending_tx.tx_hash();
        let receipt = wait_for_evm_receipt(&self.provider, self.chain_type, tx_hash).await?;

        Ok(EvmCallOutcome {
            tx_hash: tx_hash.to_string(),
            logs: receipt.logs().to_vec(),
        })
    }

    fn broadcast_rpc_url(&self, broadcast_policy: EvmBroadcastPolicy) -> &str {
        select_broadcast_rpc_url(&self.rpc_url, self.chain_type, broadcast_policy)
    }
}

fn select_broadcast_rpc_url(
    rpc_url: &str,
    chain_type: ChainType,
    broadcast_policy: EvmBroadcastPolicy,
) -> &str {
    if matches!(broadcast_policy, EvmBroadcastPolicy::FlashbotsIfEthereum)
        && chain_type == ChainType::Ethereum
        && !is_local_evm_rpc_url(rpc_url)
    {
        FLASHBOTS_ETHEREUM_RPC_URL
    } else {
        rpc_url
    }
}

fn is_local_evm_rpc_url(rpc_url: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(rpc_url) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.to_ascii_lowercase();
    host == "localhost"
        || host == "devnet"
        || host == "host.docker.internal"
        || host.starts_with("127.")
        || host == "0.0.0.0"
        || host == "::1"
}

/// Result of a custody-controlled EVM call. Includes the receipt's logs so that
/// downstream action providers (e.g. Across bridge) can decode domain events
/// emitted during execution without issuing a second `eth_getTransactionReceipt`.
#[derive(Debug, Clone)]
pub struct EvmCallOutcome {
    pub tx_hash: String,
    pub logs: Vec<RpcLog>,
}

impl EvmGasSponsor {
    fn start(
        config: EvmGasSponsorConfig,
        provider: DynProvider,
        rpc_url: String,
        chain_type: ChainType,
    ) -> Result<Self> {
        let signer = PrivateKeySigner::from_str(&config.private_key).map_err(|_| {
            crate::Error::Serialization {
                message: "Invalid gas sponsor private key".to_string(),
            }
        })?;
        let sponsor_address = signer.address();
        let url = rpc_url.parse().map_err(|_| crate::Error::Serialization {
            message: "Invalid RPC URL".to_string(),
        })?;
        let wallet_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::new(signer.clone()))
            .connect_http(url)
            .erased();
        let batch_config = EvmPaymasterBatchConfig::from_env();
        let (command_tx, command_rx) = mpsc::channel(EVM_PAYMASTER_COMMAND_BUFFER);
        let mut actor_tasks = JoinSet::new();
        let actor = EvmPaymasterActor {
            provider,
            wallet_provider,
            sponsor_signer: signer,
            sponsor_address,
            vault_gas_buffer_wei: config.vault_gas_buffer_wei,
            batch_config,
            chain_type,
        };
        actor_tasks.spawn(run_evm_paymaster_actor(actor.clone(), command_rx));
        record_paymaster_actor_start(chain_type, "initial");
        record_paymaster_queue_depth(chain_type, &command_tx);

        Ok(Self {
            actor,
            actor_runtime: Mutex::new(Some(EvmPaymasterRuntime {
                command_tx,
                actor_tasks,
            })),
            chain_type,
        })
    }

    fn command_tx(&self) -> Result<mpsc::Sender<EvmPaymasterCommand>> {
        let mut actor_runtime =
            self.actor_runtime
                .lock()
                .map_err(|_| crate::Error::PaymasterActor {
                    chain: self.chain_type,
                    message: "actor runtime lock poisoned".to_string(),
                })?;

        if let Some(runtime) = actor_runtime.as_mut() {
            match runtime.actor_tasks.try_join_next() {
                None if !runtime.actor_tasks.is_empty() && !runtime.command_tx.is_closed() => {
                    return Ok(runtime.command_tx.clone());
                }
                None => {}
                Some(Ok(())) => {
                    record_paymaster_actor_exit(self.chain_type, "exited");
                    return Err(crate::Error::PaymasterActor {
                        chain: self.chain_type,
                        message: "actor exited unexpectedly".to_string(),
                    });
                }
                Some(Err(error)) if error.is_cancelled() => {
                    record_paymaster_actor_exit(self.chain_type, "cancelled");
                }
                Some(Err(error)) => {
                    record_paymaster_actor_exit(self.chain_type, "join_error");
                    return Err(actor_join_error(self.chain_type, error));
                }
            }
        }

        Ok(self.restart_actor(&mut actor_runtime))
    }

    fn restart_actor(
        &self,
        actor_runtime: &mut Option<EvmPaymasterRuntime>,
    ) -> mpsc::Sender<EvmPaymasterCommand> {
        let (command_tx, command_rx) = mpsc::channel(EVM_PAYMASTER_COMMAND_BUFFER);
        let mut actor_tasks = JoinSet::new();
        actor_tasks.spawn(run_evm_paymaster_actor(self.actor.clone(), command_rx));
        record_paymaster_actor_start(self.chain_type, "restart");
        record_paymaster_queue_depth(self.chain_type, &command_tx);

        *actor_runtime = Some(EvmPaymasterRuntime {
            command_tx: command_tx.clone(),
            actor_tasks,
        });

        command_tx
    }

    async fn fund_vault_action(&self, request: FundVaultAction) -> Result<Option<String>> {
        let retry_request = request.clone();
        match self.fund_vault_action_once(request).await {
            Err(crate::Error::PaymasterActor { message, .. })
                if message == EVM_PAYMASTER_RESPONSE_CHANNEL_CLOSED =>
            {
                record_paymaster_command_retry(
                    self.chain_type,
                    "fund_vault_action",
                    "response_channel_closed",
                );
                self.force_restart_actor()?;
                self.fund_vault_action_once(retry_request).await
            }
            result => result,
        }
    }

    async fn fund_evm_transaction(
        &self,
        request: FundEvmTransactionAction,
    ) -> Result<Option<String>> {
        let retry_request = request.clone();
        match self.fund_evm_transaction_once(request).await {
            Err(crate::Error::PaymasterActor { message, .. })
                if message == EVM_PAYMASTER_RESPONSE_CHANNEL_CLOSED =>
            {
                record_paymaster_command_retry(
                    self.chain_type,
                    "fund_evm_transaction",
                    "response_channel_closed",
                );
                self.force_restart_actor()?;
                self.fund_evm_transaction_once(retry_request).await
            }
            result => result,
        }
    }

    fn force_restart_actor(&self) -> Result<()> {
        let mut actor_runtime =
            self.actor_runtime
                .lock()
                .map_err(|_| crate::Error::PaymasterActor {
                    chain: self.chain_type,
                    message: "actor runtime lock poisoned".to_string(),
                })?;
        self.restart_actor(&mut actor_runtime);
        Ok(())
    }

    async fn fund_vault_action_once(&self, request: FundVaultAction) -> Result<Option<String>> {
        let command_tx = self.command_tx()?;
        let started = Instant::now();
        record_paymaster_command_submitted(self.chain_type, "fund_vault_action");

        let (response_tx, response_rx) = oneshot::channel();
        if command_tx
            .send(EvmPaymasterCommand::FundVaultAction {
                request,
                response_tx,
            })
            .await
            .is_err()
        {
            record_paymaster_command_roundtrip(
                self.chain_type,
                "fund_vault_action",
                false,
                started.elapsed(),
            );
            return Err(crate::Error::PaymasterActor {
                chain: self.chain_type,
                message: "actor command queue closed".to_string(),
            });
        }
        record_paymaster_queue_depth(self.chain_type, &command_tx);

        match response_rx.await {
            Ok(result) => {
                record_paymaster_command_roundtrip(
                    self.chain_type,
                    "fund_vault_action",
                    result.is_ok(),
                    started.elapsed(),
                );
                result
            }
            Err(_) => {
                record_paymaster_command_roundtrip(
                    self.chain_type,
                    "fund_vault_action",
                    false,
                    started.elapsed(),
                );
                Err(crate::Error::PaymasterActor {
                    chain: self.chain_type,
                    message: EVM_PAYMASTER_RESPONSE_CHANNEL_CLOSED.to_string(),
                })
            }
        }
    }

    async fn fund_evm_transaction_once(
        &self,
        request: FundEvmTransactionAction,
    ) -> Result<Option<String>> {
        let command_tx = self.command_tx()?;
        let started = Instant::now();
        record_paymaster_command_submitted(self.chain_type, "fund_evm_transaction");

        let (response_tx, response_rx) = oneshot::channel();
        if command_tx
            .send(EvmPaymasterCommand::FundEvmTransaction {
                request,
                response_tx,
            })
            .await
            .is_err()
        {
            record_paymaster_command_roundtrip(
                self.chain_type,
                "fund_evm_transaction",
                false,
                started.elapsed(),
            );
            return Err(crate::Error::PaymasterActor {
                chain: self.chain_type,
                message: "actor command queue closed".to_string(),
            });
        }
        record_paymaster_queue_depth(self.chain_type, &command_tx);

        match response_rx.await {
            Ok(result) => {
                record_paymaster_command_roundtrip(
                    self.chain_type,
                    "fund_evm_transaction",
                    result.is_ok(),
                    started.elapsed(),
                );
                result
            }
            Err(_) => {
                record_paymaster_command_roundtrip(
                    self.chain_type,
                    "fund_evm_transaction",
                    false,
                    started.elapsed(),
                );
                Err(crate::Error::PaymasterActor {
                    chain: self.chain_type,
                    message: EVM_PAYMASTER_RESPONSE_CHANNEL_CLOSED.to_string(),
                })
            }
        }
    }
}

fn actor_join_error(chain_type: ChainType, error: JoinError) -> crate::Error {
    crate::Error::PaymasterActor {
        chain: chain_type,
        message: format!("actor panicked or was cancelled: {error}"),
    }
}

fn parse_paymaster_batch_max_size_env(default: usize) -> usize {
    match std::env::var(ROUTER_PAYMASTER_BATCH_MAX_SIZE_ENV) {
        Ok(value) => match value.parse::<usize>() {
            Ok(parsed) if parsed > 0 => parsed,
            Ok(_) | Err(_) => {
                warn!(
                    env_var = ROUTER_PAYMASTER_BATCH_MAX_SIZE_ENV,
                    value, default, "invalid EVM paymaster batch max size; using default"
                );
                default
            }
        },
        Err(_) => default,
    }
}

fn record_paymaster_actor_start(chain_type: ChainType, start_kind: &'static str) {
    counter!(
        "tee_router_paymaster_actor_start_total",
        "chain" => chain_label(chain_type),
        "start_kind" => start_kind,
    )
    .increment(1);
}

fn record_paymaster_actor_exit(chain_type: ChainType, exit_kind: &'static str) {
    counter!(
        "tee_router_paymaster_actor_exit_total",
        "chain" => chain_label(chain_type),
        "exit_kind" => exit_kind,
    )
    .increment(1);
}

fn record_paymaster_queue_depth(
    chain_type: ChainType,
    command_tx: &mpsc::Sender<EvmPaymasterCommand>,
) {
    let depth = command_tx
        .max_capacity()
        .saturating_sub(command_tx.capacity());
    gauge!(
        "tee_router_paymaster_queue_depth",
        "chain" => chain_label(chain_type),
    )
    .set(depth as f64);
}

fn record_paymaster_command_submitted(chain_type: ChainType, command: &'static str) {
    counter!(
        "tee_router_paymaster_command_submitted_total",
        "chain" => chain_label(chain_type),
        "command" => command,
    )
    .increment(1);
}

fn record_paymaster_command_started(chain_type: ChainType, command: &'static str) {
    counter!(
        "tee_router_paymaster_command_started_total",
        "chain" => chain_label(chain_type),
        "command" => command,
    )
    .increment(1);
}

fn record_paymaster_command_retry(
    chain_type: ChainType,
    command: &'static str,
    retry_reason: &'static str,
) {
    counter!(
        "tee_router_paymaster_command_retry_total",
        "chain" => chain_label(chain_type),
        "command" => command,
        "retry_reason" => retry_reason,
    )
    .increment(1);
}

fn record_paymaster_command_roundtrip(
    chain_type: ChainType,
    command: &'static str,
    success: bool,
    duration: Duration,
) {
    let status = success_status(success);
    counter!(
        "tee_router_paymaster_command_roundtrip_total",
        "chain" => chain_label(chain_type),
        "command" => command,
        "status" => status,
    )
    .increment(1);
    histogram!(
        "tee_router_paymaster_command_roundtrip_duration_seconds",
        "chain" => chain_label(chain_type),
        "command" => command,
        "status" => status,
    )
    .record(duration.as_secs_f64());
}

fn record_paymaster_command_processed(
    chain_type: ChainType,
    command: &'static str,
    success: bool,
    duration: Duration,
) {
    let status = success_status(success);
    counter!(
        "tee_router_paymaster_command_processed_total",
        "chain" => chain_label(chain_type),
        "command" => command,
        "status" => status,
    )
    .increment(1);
    histogram!(
        "tee_router_paymaster_command_processing_duration_seconds",
        "chain" => chain_label(chain_type),
        "command" => command,
        "status" => status,
    )
    .record(duration.as_secs_f64());
}

fn record_paymaster_gas_estimate(chain_type: ChainType, operation: &'static str, gas_units: u64) {
    histogram!(
        "tee_router_paymaster_estimated_gas_units",
        "chain" => chain_label(chain_type),
        "operation" => operation,
    )
    .record(gas_units as f64);
}

fn record_paymaster_balance_gwei(chain_type: ChainType, wallet_kind: &'static str, balance: U256) {
    gauge!(
        "tee_router_paymaster_balance_gwei",
        "chain" => chain_label(chain_type),
        "wallet_kind" => wallet_kind,
    )
    .set(u256_to_gwei_f64(balance));
}

fn record_paymaster_funding_skipped(chain_type: ChainType, reason: &'static str) {
    counter!(
        "tee_router_paymaster_funding_skipped_total",
        "chain" => chain_label(chain_type),
        "reason" => reason,
    )
    .increment(1);
}

fn record_paymaster_funding_failed(chain_type: ChainType, failure_kind: &'static str) {
    counter!(
        "tee_router_paymaster_funding_failed_total",
        "chain" => chain_label(chain_type),
        "failure_kind" => failure_kind,
    )
    .increment(1);
}

fn record_paymaster_funding_sent(chain_type: ChainType, top_up_amount: U256) {
    counter!(
        "tee_router_paymaster_funding_tx_sent_total",
        "chain" => chain_label(chain_type),
    )
    .increment(1);
    histogram!(
        "tee_router_paymaster_funding_amount_gwei",
        "chain" => chain_label(chain_type),
    )
    .record(u256_to_gwei_f64(top_up_amount));
}

fn record_paymaster_batch_sent(chain_type: ChainType, execution_count: usize) {
    counter!(
        "tee_router_paymaster_batch_sent_total",
        "chain" => chain_label(chain_type),
    )
    .increment(1);
    histogram!(
        "tee_router_paymaster_batch_size",
        "chain" => chain_label(chain_type),
        "stage" => "sent",
    )
    .record(execution_count as f64);
}

fn record_paymaster_batch_executed(chain_type: ChainType, execution_count: usize, success: bool) {
    let status = success_status(success);
    counter!(
        "tee_router_paymaster_batch_executed_total",
        "chain" => chain_label(chain_type),
        "status" => status,
    )
    .increment(1);
    histogram!(
        "tee_router_paymaster_batch_size",
        "chain" => chain_label(chain_type),
        "stage" => "executed",
        "status" => status,
    )
    .record(execution_count as f64);
}

fn success_status(success: bool) -> &'static str {
    if success {
        "success"
    } else {
        "error"
    }
}

fn chain_label(chain_type: ChainType) -> String {
    chain_type.to_db_string().to_string()
}

fn u256_to_gwei_f64(value: U256) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(f64::MAX) / WEI_PER_GWEI
}

#[derive(Debug, Clone)]
struct PaymasterExecution {
    execution: Execution,
    top_up_amount: U256,
}

#[derive(Debug)]
enum PaymasterPrepareOutcome {
    Skip,
    Submit(PaymasterExecution),
}

struct PendingPaymasterBatchResponse {
    response_tx: oneshot::Sender<Result<Option<String>>>,
    command: &'static str,
    started: Instant,
    top_up_amount: U256,
}

async fn run_evm_paymaster_actor(
    actor: EvmPaymasterActor,
    mut command_rx: mpsc::Receiver<EvmPaymasterCommand>,
) {
    match actor.ensure_eip7702_delegation().await {
        Ok(()) => {
            info!(
                chain = %actor.chain_type.to_db_string(),
                sponsor = %actor.sponsor_address,
                batch_max_size = actor.batch_config.max_size,
                "EVM paymaster actor using EIP-7702 batched funding"
            );
        }
        Err(error) => {
            warn!(
                chain = %actor.chain_type.to_db_string(),
                sponsor = %actor.sponsor_address,
                error = %error,
                "failed to set up EIP-7702 delegation; falling back to serial paymaster funding"
            );
            run_evm_paymaster_actor_serial(actor, command_rx).await;
            return;
        }
    }

    while let Some(batch) =
        drain_paymaster_batch(&mut command_rx, actor.batch_config.max_size).await
    {
        process_evm_paymaster_batch(&actor, batch).await;
    }
}

async fn run_evm_paymaster_actor_serial(
    actor: EvmPaymasterActor,
    mut command_rx: mpsc::Receiver<EvmPaymasterCommand>,
) {
    while let Some(command) = command_rx.recv().await {
        match command {
            EvmPaymasterCommand::FundVaultAction {
                request,
                response_tx,
            } => {
                let started = Instant::now();
                record_paymaster_command_started(actor.chain_type, "fund_vault_action");
                let result = actor.fund_vault_action_legacy(request).await;
                record_paymaster_command_processed(
                    actor.chain_type,
                    "fund_vault_action",
                    result.is_ok(),
                    started.elapsed(),
                );
                let _ = response_tx.send(result);
            }
            EvmPaymasterCommand::FundEvmTransaction {
                request,
                response_tx,
            } => {
                let started = Instant::now();
                record_paymaster_command_started(actor.chain_type, "fund_evm_transaction");
                let result = actor.fund_evm_transaction_legacy(request).await;
                record_paymaster_command_processed(
                    actor.chain_type,
                    "fund_evm_transaction",
                    result.is_ok(),
                    started.elapsed(),
                );
                let _ = response_tx.send(result);
            }
        }
    }
}

async fn drain_paymaster_batch(
    command_rx: &mut mpsc::Receiver<EvmPaymasterCommand>,
    max_size: usize,
) -> Option<Vec<EvmPaymasterCommand>> {
    let max_size = max_size.max(1);
    let first_command = command_rx.recv().await?;
    let mut batch = Vec::with_capacity(max_size.min(EVM_PAYMASTER_COMMAND_BUFFER));
    batch.push(first_command);

    while batch.len() < max_size {
        match command_rx.try_recv() {
            Ok(command) => batch.push(command),
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        }
    }

    Some(batch)
}

async fn process_evm_paymaster_batch(
    actor: &EvmPaymasterActor,
    commands: Vec<EvmPaymasterCommand>,
) {
    let mut pending_responses = Vec::new();
    let mut executions = Vec::new();
    let mut virtual_vault_balances = std::collections::HashMap::new();

    for command in commands {
        match command {
            EvmPaymasterCommand::FundVaultAction {
                request,
                response_tx,
            } => {
                let started = Instant::now();
                let command_name = "fund_vault_action";
                record_paymaster_command_started(actor.chain_type, command_name);
                match actor
                    .prepare_vault_action_execution(request, &mut virtual_vault_balances)
                    .await
                {
                    Ok(PaymasterPrepareOutcome::Skip) => {
                        record_paymaster_command_processed(
                            actor.chain_type,
                            command_name,
                            true,
                            started.elapsed(),
                        );
                        let _ = response_tx.send(Ok(None));
                    }
                    Ok(PaymasterPrepareOutcome::Submit(prepared)) => {
                        let PaymasterExecution {
                            execution,
                            top_up_amount,
                        } = prepared;
                        executions.push(execution);
                        pending_responses.push(PendingPaymasterBatchResponse {
                            response_tx,
                            command: command_name,
                            started,
                            top_up_amount,
                        });
                    }
                    Err(error) => {
                        record_paymaster_command_processed(
                            actor.chain_type,
                            command_name,
                            false,
                            started.elapsed(),
                        );
                        let _ = response_tx.send(Err(error));
                    }
                }
            }
            EvmPaymasterCommand::FundEvmTransaction {
                request,
                response_tx,
            } => {
                let started = Instant::now();
                let command_name = "fund_evm_transaction";
                record_paymaster_command_started(actor.chain_type, command_name);
                match actor
                    .prepare_evm_transaction_execution(request, &mut virtual_vault_balances)
                    .await
                {
                    Ok(PaymasterPrepareOutcome::Skip) => {
                        record_paymaster_command_processed(
                            actor.chain_type,
                            command_name,
                            true,
                            started.elapsed(),
                        );
                        let _ = response_tx.send(Ok(None));
                    }
                    Ok(PaymasterPrepareOutcome::Submit(prepared)) => {
                        let PaymasterExecution {
                            execution,
                            top_up_amount,
                        } = prepared;
                        executions.push(execution);
                        pending_responses.push(PendingPaymasterBatchResponse {
                            response_tx,
                            command: command_name,
                            started,
                            top_up_amount,
                        });
                    }
                    Err(error) => {
                        record_paymaster_command_processed(
                            actor.chain_type,
                            command_name,
                            false,
                            started.elapsed(),
                        );
                        let _ = response_tx.send(Err(error));
                    }
                }
            }
        }
    }

    if executions.is_empty() {
        return;
    }

    match actor.send_batched_tx(&executions).await {
        Ok(tx_hash) => {
            record_paymaster_batch_executed(actor.chain_type, executions.len(), true);
            let tx_hash = tx_hash.to_string();
            for pending in pending_responses {
                record_paymaster_funding_sent(actor.chain_type, pending.top_up_amount);
                record_paymaster_command_processed(
                    actor.chain_type,
                    pending.command,
                    true,
                    pending.started.elapsed(),
                );
                let _ = pending.response_tx.send(Ok(Some(tx_hash.clone())));
            }
        }
        Err(error) => {
            record_paymaster_batch_executed(actor.chain_type, executions.len(), false);
            for pending in pending_responses {
                record_paymaster_command_processed(
                    actor.chain_type,
                    pending.command,
                    false,
                    pending.started.elapsed(),
                );
                let _ = pending
                    .response_tx
                    .send(Err(clone_paymaster_error(actor.chain_type, &error)));
            }
        }
    }
}

impl EvmPaymasterActor {
    async fn ensure_eip7702_delegation(&self) -> Result<()> {
        let derived_address = self.sponsor_signer.address();
        if derived_address != self.sponsor_address {
            return Err(crate::Error::PaymasterActor {
                chain: self.chain_type,
                message: format!(
                    "gas sponsor signer address mismatch: expected {}, got {}",
                    self.sponsor_address, derived_address
                ),
            });
        }

        let delegator_contract_address = Address::from_str(EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS)
            .map_err(|_| crate::Error::Serialization {
                message: "Invalid EIP-7702 delegator address".to_string(),
            })?;
        let code = retry_evm_rpc(self.chain_type, "get_code_at", || async {
            self.provider.get_code_at(self.sponsor_address).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;

        let mut delegation_pattern = hex!("ef0100").to_vec();
        delegation_pattern.extend_from_slice(delegator_contract_address.as_slice());
        if code.starts_with(&delegation_pattern) {
            debug!(
                chain = %self.chain_type.to_db_string(),
                sponsor = %self.sponsor_address,
                "EVM paymaster sponsor already delegated to EIP-7702 delegator"
            );
            return Ok(());
        }

        let nonce = retry_evm_rpc(self.chain_type, "get_transaction_count", || async {
            self.provider
                .get_transaction_count(self.sponsor_address)
                .await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let chain_id = retry_evm_rpc(self.chain_type, "get_chain_id", || async {
            self.provider.get_chain_id().await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;

        let authorization = Authorization {
            chain_id: U256::from(chain_id),
            address: delegator_contract_address,
            nonce: nonce.checked_add(1).ok_or(crate::Error::NumericOverflow {
                context: "EIP-7702 authorization nonce",
            })?,
        };
        let signature = self
            .sponsor_signer
            .sign_hash_sync(&authorization.signature_hash())
            .map_err(|error| crate::Error::PaymasterActor {
                chain: self.chain_type,
                message: format!("failed to sign EIP-7702 authorization: {error}"),
            })?;
        let signed_authorization = authorization.into_signed(signature);
        let tx = TransactionRequest::default()
            .with_from(self.sponsor_address)
            .with_to(self.sponsor_address)
            .with_authorization_list(vec![signed_authorization]);

        let pending_tx = self
            .wallet_provider
            .send_transaction(tx)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;
        let tx_hash = *pending_tx.tx_hash();
        wait_for_evm_receipt(&self.wallet_provider, self.chain_type, tx_hash).await?;

        info!(
            chain = %self.chain_type.to_db_string(),
            sponsor = %self.sponsor_address,
            tx_hash = %tx_hash,
            "EVM paymaster sponsor delegated to EIP-7702 delegator"
        );
        Ok(())
    }

    async fn prepare_vault_action_execution(
        &self,
        request: FundVaultAction,
        virtual_vault_balances: &mut std::collections::HashMap<Address, U256>,
    ) -> Result<PaymasterPrepareOutcome> {
        let FundVaultAction {
            token_address,
            vault_address,
            recipient_address,
            token_amount,
        } = request;

        debug!(
            chain = self.chain_type.to_db_string(),
            %token_address,
            %vault_address,
            %recipient_address,
            %token_amount,
            "EVM paymaster actor processing vault action funding request"
        );

        let token_contract = GenericEIP3009ERC20Instance::new(token_address, &self.provider);
        let transfer_calldata = token_contract
            .transfer(recipient_address, token_amount)
            .calldata()
            .clone();
        self.prepare_evm_transaction_execution(
            FundEvmTransactionAction {
                vault_address,
                target_address: token_address,
                value: U256::ZERO,
                calldata: transfer_calldata,
                operation: "erc20_transfer",
            },
            virtual_vault_balances,
        )
        .await
    }

    async fn prepare_evm_transaction_execution(
        &self,
        request: FundEvmTransactionAction,
        virtual_vault_balances: &mut std::collections::HashMap<Address, U256>,
    ) -> Result<PaymasterPrepareOutcome> {
        let FundEvmTransactionAction {
            vault_address,
            target_address,
            value,
            calldata,
            operation,
        } = request;

        debug!(
            chain = self.chain_type.to_db_string(),
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
            .with_gas_limit(EVM_CALL_ESTIMATE_GAS_CAP)
            .with_input(calldata);
        let action_gas_limit = estimate_evm_gas_or_cap(
            &self.provider,
            self.chain_type,
            action_estimate_request,
            operation,
        )
        .await?;
        record_paymaster_gas_estimate(self.chain_type, operation, action_gas_limit);
        let fee_estimate = retry_evm_rpc(self.chain_type, "estimate_eip1559_fees", || async {
            self.provider.estimate_eip1559_fees().await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let action_max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;
        let action_reserved_fee = checked_gas_fee(
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
                let current_balance = retry_evm_rpc(self.chain_type, "get_balance", || async {
                    self.provider.get_balance(vault_address).await
                })
                .await
                .map_err(|e| crate::Error::EVMRpcError {
                    source: e,
                    loc: location!(),
                })?;
                record_paymaster_balance_gwei(self.chain_type, "vault", current_balance);
                entry.insert(current_balance)
            }
        };

        if *effective_balance < value {
            record_paymaster_funding_failed(self.chain_type, "insufficient_action_value");
            return Err(crate::Error::InsufficientBalance {
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
                record_paymaster_funding_skipped(self.chain_type, "already_funded");
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

    async fn send_batched_tx(&self, executions: &[Execution]) -> Result<TxHash> {
        let delegator = EIP7702DelegatorInstance::new(
            Address::from_str(EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS).map_err(|_| {
                crate::Error::Serialization {
                    message: "Invalid EIP-7702 delegator address".to_string(),
                }
            })?,
            self.wallet_provider.clone(),
        );

        let mut tx = delegator
            .execute_1(
                ModeCode::Batch.as_fixed_bytes32(),
                executions.to_vec().abi_encode().into(),
            )
            .into_transaction_request()
            .with_from(self.sponsor_address);
        tx.set_to(self.sponsor_address);

        let gas_limit = estimate_batched_paymaster_gas(self, tx.clone(), executions).await?;
        record_paymaster_gas_estimate(self.chain_type, "eip7702_batch_top_up", gas_limit);
        let fee_estimate = retry_evm_rpc(self.chain_type, "estimate_eip1559_fees", || async {
            self.provider.estimate_eip1559_fees().await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;

        let sponsor_balance = retry_evm_rpc(self.chain_type, "get_balance", || async {
            self.provider.get_balance(self.sponsor_address).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        record_paymaster_balance_gwei(self.chain_type, "sponsor", sponsor_balance);
        let batch_value = executions.iter().try_fold(U256::ZERO, |acc, execution| {
            checked_u256_add(acc, execution.value, "paymaster batch execution value")
        })?;
        let reserved_fee = checked_gas_fee(
            gas_limit,
            max_fee_per_gas,
            "paymaster batch gas reservation",
        )?;
        let required_balance = checked_u256_add(
            batch_value,
            reserved_fee,
            "paymaster batch sponsor required balance",
        )?;
        if sponsor_balance < required_balance {
            record_paymaster_funding_failed(self.chain_type, "insufficient_balance");
            return Err(crate::Error::InsufficientBalance {
                required: required_balance,
                available: sponsor_balance,
            });
        }

        let nonce = retry_evm_rpc(self.chain_type, "get_transaction_count_pending", || async {
            self.provider
                .get_transaction_count(self.sponsor_address)
                .pending()
                .await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;

        tx = tx
            .with_nonce(nonce)
            .with_gas_limit(gas_limit)
            .with_max_fee_per_gas(max_fee_per_gas)
            .with_max_priority_fee_per_gas(fee_estimate.max_priority_fee_per_gas);

        let pending_tx = self
            .wallet_provider
            .send_transaction(tx)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;
        let tx_hash = *pending_tx.tx_hash();
        record_paymaster_batch_sent(self.chain_type, executions.len());
        wait_for_evm_receipt(&self.wallet_provider, self.chain_type, tx_hash).await?;
        Ok(tx_hash)
    }

    async fn fund_vault_action_legacy(&self, request: FundVaultAction) -> Result<Option<String>> {
        let mut virtual_vault_balances = std::collections::HashMap::new();
        match self
            .prepare_vault_action_execution(request, &mut virtual_vault_balances)
            .await?
        {
            PaymasterPrepareOutcome::Skip => Ok(None),
            PaymasterPrepareOutcome::Submit(prepared) => {
                self.send_legacy_top_up(prepared).await.map(Some)
            }
        }
    }

    async fn fund_evm_transaction_legacy(
        &self,
        request: FundEvmTransactionAction,
    ) -> Result<Option<String>> {
        let mut virtual_vault_balances = std::collections::HashMap::new();
        match self
            .prepare_evm_transaction_execution(request, &mut virtual_vault_balances)
            .await?
        {
            PaymasterPrepareOutcome::Skip => Ok(None),
            PaymasterPrepareOutcome::Submit(prepared) => {
                self.send_legacy_top_up(prepared).await.map(Some)
            }
        }
    }

    async fn send_legacy_top_up(&self, prepared: PaymasterExecution) -> Result<String> {
        let estimate_request = TransactionRequest::default()
            .with_from(self.sponsor_address)
            .with_to(prepared.execution.target)
            .with_value(prepared.execution.value)
            .with_input(prepared.execution.callData.clone());
        let gas_limit = retry_evm_rpc(self.chain_type, "estimate_gas", || async {
            self.provider.estimate_gas(estimate_request.clone()).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        record_paymaster_gas_estimate(self.chain_type, "native_top_up", gas_limit);
        let fee_estimate = retry_evm_rpc(self.chain_type, "estimate_eip1559_fees", || async {
            self.provider.estimate_eip1559_fees().await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;
        let reserved_fee = checked_gas_fee(
            gas_limit,
            max_fee_per_gas,
            "paymaster top-up gas reservation",
        )?;
        let required_balance = checked_u256_add(
            prepared.top_up_amount,
            reserved_fee,
            "paymaster sponsor required balance",
        )?;
        let sponsor_balance = retry_evm_rpc(self.chain_type, "get_balance", || async {
            self.provider.get_balance(self.sponsor_address).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        record_paymaster_balance_gwei(self.chain_type, "sponsor", sponsor_balance);
        if sponsor_balance < required_balance {
            record_paymaster_funding_failed(self.chain_type, "insufficient_balance");
            return Err(crate::Error::InsufficientBalance {
                required: required_balance,
                available: sponsor_balance,
            });
        }

        let nonce = retry_evm_rpc(self.chain_type, "get_transaction_count_pending", || async {
            self.provider
                .get_transaction_count(self.sponsor_address)
                .pending()
                .await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;

        let transaction_request = TransactionRequest::default()
            .with_from(self.sponsor_address)
            .with_to(prepared.execution.target)
            .with_value(prepared.execution.value)
            .with_input(prepared.execution.callData)
            .with_nonce(nonce)
            .with_gas_limit(gas_limit)
            .with_max_fee_per_gas(max_fee_per_gas)
            .with_max_priority_fee_per_gas(fee_estimate.max_priority_fee_per_gas);

        let pending_tx = self
            .wallet_provider
            .send_transaction(transaction_request)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;
        let tx_hash = *pending_tx.tx_hash();
        wait_for_evm_receipt(&self.wallet_provider, self.chain_type, tx_hash).await?;

        record_paymaster_funding_sent(self.chain_type, prepared.top_up_amount);
        Ok(tx_hash.to_string())
    }
}

fn prepare_funding_execution_from_balance(
    vault_address: Address,
    action_value: U256,
    required_vault_balance: U256,
    current_balance: U256,
) -> Result<PaymasterPrepareOutcome> {
    if current_balance < action_value {
        return Err(crate::Error::InsufficientBalance {
            required: action_value,
            available: current_balance,
        });
    }
    if current_balance >= required_vault_balance {
        return Ok(PaymasterPrepareOutcome::Skip);
    }

    let top_up_amount = checked_u256_sub(
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

fn clone_paymaster_error(chain_type: ChainType, error: &crate::Error) -> crate::Error {
    match error {
        crate::Error::InvalidAddress {
            address,
            network,
            reason,
        } => crate::Error::InvalidAddress {
            address: address.clone(),
            network: *network,
            reason: reason.clone(),
        },
        crate::Error::WalletCreation { message } => crate::Error::WalletCreation {
            message: message.clone(),
        },
        crate::Error::EVMRpcError { .. } => crate::Error::PaymasterActor {
            chain: chain_type,
            message: error.to_string(),
        },
        crate::Error::BitcoinRpcError { .. } => crate::Error::PaymasterActor {
            chain: chain_type,
            message: error.to_string(),
        },
        crate::Error::EsploraClientError { .. } => crate::Error::PaymasterActor {
            chain: chain_type,
            message: error.to_string(),
        },
        crate::Error::EVMTokenIndexerClientError { .. } => crate::Error::PaymasterActor {
            chain: chain_type,
            message: error.to_string(),
        },
        crate::Error::InvalidCurrency { currency, network } => crate::Error::InvalidCurrency {
            currency: currency.clone(),
            network: *network,
        },
        crate::Error::TransactionNotFound { tx_hash } => crate::Error::TransactionNotFound {
            tx_hash: tx_hash.clone(),
        },
        crate::Error::TransactionReverted { tx_hash } => crate::Error::TransactionReverted {
            tx_hash: tx_hash.clone(),
        },
        crate::Error::TransactionDeserializationFailed { context, .. } => {
            crate::Error::TransactionDeserializationFailed {
                context: context.clone(),
                loc: location!(),
            }
        }
        crate::Error::InsufficientBalance {
            required,
            available,
        } => crate::Error::InsufficientBalance {
            required: *required,
            available: *available,
        },
        crate::Error::NumericOverflow { context } => {
            crate::Error::NumericOverflow { context: *context }
        }
        crate::Error::ChainNotSupported { chain } => crate::Error::ChainNotSupported {
            chain: chain.clone(),
        },
        crate::Error::Serialization { message } => crate::Error::Serialization {
            message: message.clone(),
        },
        crate::Error::KeyDerivation { message } => crate::Error::KeyDerivation {
            message: message.clone(),
        },
        crate::Error::DumpToAddress { message } => crate::Error::DumpToAddress {
            message: message.clone(),
        },
        crate::Error::PaymasterActor { chain, message } => crate::Error::PaymasterActor {
            chain: *chain,
            message: message.clone(),
        },
    }
}

async fn estimate_batched_paymaster_gas(
    actor: &EvmPaymasterActor,
    batch_tx: TransactionRequest,
    executions: &[Execution],
) -> Result<u64> {
    match retry_evm_rpc(actor.chain_type, "estimate_gas", || async {
        actor.provider.estimate_gas(batch_tx.clone()).await
    })
    .await
    {
        Ok(gas_limit) => Ok(gas_limit),
        Err(batch_error) => {
            warn!(
                chain = %actor.chain_type.to_db_string(),
                execution_count = executions.len(),
                error = %batch_error,
                "batched paymaster gas estimate failed; falling back to per-execution sum"
            );
            estimate_batched_paymaster_gas_from_executions(actor, executions).await
        }
    }
}

async fn estimate_batched_paymaster_gas_from_executions(
    actor: &EvmPaymasterActor,
    executions: &[Execution],
) -> Result<u64> {
    let mut total_gas = 0u64;
    for execution in executions {
        let estimate_request = TransactionRequest::default()
            .with_from(actor.sponsor_address)
            .with_to(execution.target)
            .with_value(execution.value)
            .with_input(execution.callData.clone());
        let gas_limit = retry_evm_rpc(actor.chain_type, "estimate_gas", || async {
            actor.provider.estimate_gas(estimate_request.clone()).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        total_gas = total_gas
            .checked_add(gas_limit)
            .ok_or(crate::Error::NumericOverflow {
                context: "paymaster batch fallback gas sum",
            })?;
    }

    total_gas
        .checked_mul(12)
        .map(|gas| gas.div_ceil(10))
        .ok_or(crate::Error::NumericOverflow {
            context: "paymaster batch fallback gas headroom",
        })
}

async fn wait_for_evm_receipt<P>(
    provider: &P,
    chain_type: ChainType,
    tx_hash: TxHash,
) -> Result<TransactionReceipt>
where
    P: Provider,
{
    let started = Instant::now();
    loop {
        let receipt = retry_evm_rpc(chain_type, "get_transaction_receipt", || async {
            provider.get_transaction_receipt(tx_hash).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        if let Some(receipt) = receipt {
            ensure_evm_receipt_success(tx_hash, receipt.status())?;
            return Ok(receipt);
        }

        if started.elapsed() >= EVM_RECEIPT_TIMEOUT {
            return Err(crate::Error::DumpToAddress {
                message: format!("timed out waiting for EVM transaction receipt for {tx_hash}"),
            });
        }

        sleep(EVM_RECEIPT_POLL_INTERVAL).await;
    }
}

async fn retry_evm_rpc<T, E, Fut, Op>(
    chain_type: ChainType,
    rpc_method: &'static str,
    mut op: Op,
) -> std::result::Result<T, E>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    let mut delay = EVM_RPC_RETRY_INITIAL_DELAY;
    let mut attempt = 1;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt < EVM_RPC_RETRY_ATTEMPTS && is_retryable_evm_rpc_error(&err) => {
                let error_kind = classify_evm_rpc_error(&err);
                record_evm_rpc_retry(chain_type, rpc_method, error_kind);
                warn!(
                    chain = %chain_type.to_db_string(),
                    rpc_method,
                    attempt,
                    max_attempts = EVM_RPC_RETRY_ATTEMPTS,
                    error_kind,
                    error = %err,
                    "retrying EVM RPC after retryable error"
                );
                sleep(delay).await;
                delay = delay.saturating_mul(2);
                attempt += 1;
            }
            Err(err) => {
                let error_kind = classify_evm_rpc_error(&err);
                record_evm_rpc_failure(chain_type, rpc_method, error_kind);
                warn!(
                    chain = %chain_type.to_db_string(),
                    rpc_method,
                    error_kind,
                    error = %err,
                    "EVM RPC failed"
                );
                return Err(err);
            }
        }
    }
}

async fn estimate_evm_gas_or_cap(
    provider: &DynProvider,
    chain_type: ChainType,
    request: TransactionRequest,
    operation: &'static str,
) -> Result<u64> {
    match retry_evm_rpc(chain_type, "estimate_gas", || async {
        provider.estimate_gas(request.clone()).await
    })
    .await
    {
        Ok(gas_limit) => Ok(gas_limit),
        Err(err) if is_balance_bound_estimate_error(&err) => {
            warn!(
                chain = %chain_type.to_db_string(),
                operation,
                gas_cap = EVM_CALL_ESTIMATE_GAS_CAP,
                error = %err,
                "falling back to capped EVM gas estimate after balance-bound simulation failure"
            );
            Ok(EVM_CALL_ESTIMATE_GAS_CAP)
        }
        Err(err) => Err(crate::Error::EVMRpcError {
            source: err,
            loc: location!(),
        }),
    }
}

fn is_balance_bound_estimate_error<E: std::fmt::Display>(err: &E) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("insufficient funds")
        || message.contains("insufficient balance")
        || message.contains("transfer amount exceeds balance")
}

fn is_retryable_evm_rpc_error<E: std::fmt::Display>(err: &E) -> bool {
    let error_kind = classify_evm_rpc_error(err);
    matches!(
        error_kind,
        "rate_limited" | "timeout" | "upstream_unavailable" | "connection" | "state_lag"
    )
}

fn classify_evm_rpc_error<E: std::fmt::Display>(err: &E) -> &'static str {
    let message = err.to_string().to_ascii_lowercase();
    if is_evm_execution_revert(&message) {
        "execution_reverted"
    } else if contains_status_code(&message, "429")
        || message.contains("rate limit")
        || message.contains("rate-limited")
        || message.contains("too many requests")
    {
        "rate_limited"
    } else if message.contains("timeout") || message.contains("timed out") {
        "timeout"
    } else if message.contains("temporarily unavailable")
        || message.contains("temporary internal error")
        || message.contains("incorrect response body")
        || message.contains("wrong json-rpc response")
        || contains_status_code(&message, "502")
        || contains_status_code(&message, "503")
        || contains_status_code(&message, "504")
        || contains_status_code(&message, "500")
    {
        "upstream_unavailable"
    } else if message.contains("connection reset")
        || message.contains("connection closed")
        || message.contains("connection refused")
    {
        "connection"
    } else if message.contains("insufficient funds") || message.contains("insufficient balance") {
        "state_lag"
    } else {
        "other"
    }
}

fn is_evm_execution_revert(message: &str) -> bool {
    message.contains("execution reverted")
        || message.contains("transaction reverted")
        || message.contains("revert reason")
        || message.contains("evm revert")
}

fn contains_status_code(message: &str, code: &str) -> bool {
    message.match_indices(code).any(|(idx, _)| {
        let before = message[..idx].chars().next_back();
        let after = message[idx + code.len()..].chars().next();
        before.is_none_or(|ch| !ch.is_ascii_alphanumeric())
            && after.is_none_or(|ch| !ch.is_ascii_alphanumeric())
    })
}

fn record_evm_rpc_retry(chain_type: ChainType, rpc_method: &'static str, error_kind: &'static str) {
    counter!(
        "tee_router_chain_rpc_retries_total",
        "rpc_method" => rpc_method,
        "chain" => chain_type.to_db_string(),
        "error_kind" => error_kind,
    )
    .increment(1);
    counter!(
        "ethereum_rpc_retries_total",
        "method" => rpc_method,
        "chain" => chain_type.to_db_string(),
        "error_kind" => error_kind,
    )
    .increment(1);
}

fn record_evm_rpc_failure(
    chain_type: ChainType,
    rpc_method: &'static str,
    error_kind: &'static str,
) {
    counter!(
        "tee_router_chain_rpc_errors_total",
        "rpc_method" => rpc_method,
        "status" => "retry_exhausted",
        "chain" => chain_type.to_db_string(),
        "error_kind" => error_kind,
    )
    .increment(1);
    counter!(
        "ethereum_rpc_errors_total",
        "method" => rpc_method,
        "status" => "retry_exhausted",
        "chain" => chain_type.to_db_string(),
        "error_kind" => error_kind,
    )
    .increment(1);
}

impl EvmChain {
    async fn verify_native_deposit_candidate(
        &self,
        recipient_address: &str,
        tx_hash: &str,
        transfer_index: u64,
    ) -> Result<UserDepositCandidateStatus> {
        let recipient_address =
            Address::from_str(recipient_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid address".to_string(),
            })?;
        let transaction_hash: TxHash =
            tx_hash
                .parse()
                .map_err(|_| crate::Error::TransactionDeserializationFailed {
                    context: format!("Failed to parse EVM transaction hash {tx_hash}"),
                    loc: location!(),
                })?;

        let transaction_receipt = self
            .provider
            .get_transaction_receipt(transaction_hash)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;
        let Some(transaction_receipt) = transaction_receipt else {
            return Ok(UserDepositCandidateStatus::TxNotFound);
        };
        if !transaction_receipt.status() {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        }
        if !evm_transfer_index_matches(transaction_receipt.transaction_index, transfer_index)? {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        }

        let transaction = self
            .provider
            .get_transaction_by_hash(transaction_hash)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;
        let Some(transaction) = transaction else {
            return Ok(UserDepositCandidateStatus::TxNotFound);
        };
        let Some(to) = transaction.to() else {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        };
        if to != recipient_address {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        }
        let amount = transaction.value();
        if amount.is_zero() {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        }

        let current_block_height =
            self.provider
                .get_block_number()
                .await
                .map_err(|e| crate::Error::EVMRpcError {
                    source: e,
                    loc: location!(),
                })?;
        let Some(inclusion_height) = transaction_receipt.block_number else {
            return Ok(UserDepositCandidateStatus::TxNotFound);
        };
        let confirmations = evm_confirmation_count(current_block_height, inclusion_height)?;

        Ok(UserDepositCandidateStatus::Verified(VerifiedUserDeposit {
            amount,
            confirmations,
        }))
    }
}

#[async_trait]
impl ChainOperations for EvmChain {
    fn create_wallet(&self) -> Result<(Wallet, [u8; 32])> {
        // Generate a random salt
        let mut salt = [0u8; 32];
        getrandom::getrandom(&mut salt).map_err(|_| crate::Error::Serialization {
            message: "Failed to generate random salt".to_string(),
        })?;

        // Create a new random signer
        let signer = PrivateKeySigner::random();
        let address = signer.address();
        let private_key = alloy::primitives::hex::encode(signer.to_bytes());

        info!(
            "Created new {} wallet: {}",
            self.chain_type.to_db_string(),
            address
        );

        let wallet = Wallet::new(format!("{address:?}"), format!("0x{private_key}"));
        Ok((wallet, salt))
    }

    fn derive_wallet(&self, master_key: &[u8], salt: &[u8; 32]) -> Result<Wallet> {
        // Derive private key using HKDF
        let private_key_bytes =
            key_derivation::derive_private_key(master_key, salt, &self.wallet_seed_tag)?;

        // Create signer from derived key
        let signer = PrivateKeySigner::from_bytes(&private_key_bytes.into()).map_err(|_| {
            crate::Error::Serialization {
                message: "Failed to create signer from derived key".to_string(),
            }
        })?;

        let address = format!("{:?}", signer.address());
        let private_key = format!("0x{}", alloy::hex::encode(private_key_bytes));

        debug!(
            "Derived {} wallet: {}",
            self.chain_type.to_db_string(),
            address
        );

        Ok(Wallet::new(address, private_key))
    }

    async fn get_tx_status(&self, tx_hash: &str) -> Result<TxStatus> {
        let tx_hash_parsed = tx_hash.parse().map_err(|_| crate::Error::Serialization {
            message: "Invalid transaction hash".to_string(),
        })?;
        let receipt = retry_evm_rpc(self.chain_type, "get_transaction_receipt", || async {
            self.provider.get_transaction_receipt(tx_hash_parsed).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let current_block_height = retry_evm_rpc(self.chain_type, "get_block_number", || async {
            self.provider.get_block_number().await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;

        if let Some(receipt) = receipt {
            return evm_tx_status_from_receipt_fields(
                receipt.status(),
                receipt.block_number,
                current_block_height,
            );
        }

        let pending_tx = retry_evm_rpc(self.chain_type, "get_transaction_by_hash", || async {
            self.provider.get_transaction_by_hash(tx_hash_parsed).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;

        if pending_tx.is_some() {
            Ok(TxStatus::Pending(PendingTxStatus {
                current_height: current_block_height,
            }))
        } else {
            Ok(TxStatus::NotFound)
        }
    }
    async fn verify_user_deposit_candidate(
        &self,
        recipient_address: &str,
        currency: &Currency,
        tx_hash: &str,
        transfer_index: u64,
    ) -> Result<UserDepositCandidateStatus> {
        if currency.chain != self.chain_type {
            return Err(crate::Error::InvalidCurrency {
                currency: currency.clone(),
                network: self.chain_type,
            });
        }

        if matches!(currency.token, TokenIdentifier::Native) {
            return self
                .verify_native_deposit_candidate(recipient_address, tx_hash, transfer_index)
                .await;
        }

        let TokenIdentifier::Address(token_address) = &currency.token else {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        };
        let token_address =
            Address::from_str(token_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid token address".to_string(),
            })?;

        if token_address != self.allowed_token {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        }

        let recipient_address =
            Address::from_str(recipient_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid address".to_string(),
            })?;

        let transaction_hash: TxHash =
            tx_hash
                .parse()
                .map_err(|_| crate::Error::TransactionDeserializationFailed {
                    context: format!("Failed to parse EVM transaction hash {tx_hash}"),
                    loc: location!(),
                })?;

        let transaction_receipt = self
            .provider
            .get_transaction_receipt(transaction_hash)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;

        let Some(transaction_receipt) = transaction_receipt else {
            return Ok(UserDepositCandidateStatus::TxNotFound);
        };

        if !transaction_receipt.status() {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        }

        let matching_transfer =
            extract_all_transfers_from_transaction_receipt(&transaction_receipt)
                .into_iter()
                .find(|transfer_log| {
                    transfer_log.address() == self.allowed_token
                        && transfer_log.log_index == Some(transfer_index)
                        && transfer_log.inner.data.to == recipient_address
                });

        let Some(transfer_log) = matching_transfer else {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        };

        let current_block_height =
            self.provider
                .get_block_number()
                .await
                .map_err(|e| crate::Error::EVMRpcError {
                    source: e,
                    loc: location!(),
                })?;
        let Some(inclusion_height) = transaction_receipt.block_number else {
            return Ok(UserDepositCandidateStatus::TxNotFound);
        };
        let confirmations = evm_confirmation_count(current_block_height, inclusion_height)?;

        Ok(UserDepositCandidateStatus::Verified(VerifiedUserDeposit {
            amount: transfer_log.inner.data.value,
            confirmations,
        }))
    }

    /// Dump to address here just gives permission for the recipient to call receiveWithAuthorization
    /// What is returned is an unsigned transaction calldata that the recipient can sign and send
    /// note that it CANNOT just be broadcasted, it must be signed the the recipient
    async fn dump_to_address(
        &self,
        token: &TokenIdentifier,
        private_key: &str,
        recipient_address: &str,
        _fee: U256, //ignore fee b/c we dont sign a full transaction here
    ) -> Result<String> {
        let token_address = match token {
            TokenIdentifier::Address(address) => {
                Address::from_str(address).map_err(|_| crate::Error::DumpToAddress {
                    message: "Invalid token address".to_string(),
                })?
            }
            TokenIdentifier::Native => {
                return Err(crate::Error::DumpToAddress {
                    message: "Native token not supported".to_string(),
                })
            }
        };
        let sender_signer =
            LocalSigner::from_str(private_key).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid private key".to_string(),
            })?;
        let sender_address = sender_signer.address();
        let recipient_address =
            Address::from_str(recipient_address).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid recipient address".to_string(),
            })?;
        let token_contract = GenericEIP3009ERC20Instance::new(token_address, &self.provider);
        let token_balance = token_contract
            .balanceOf(sender_address)
            .call()
            .await
            .map_err(|_| crate::Error::DumpToAddress {
                message: "Failed to get token balance".to_string(),
            })?;

        let lot = Lot {
            currency: Currency {
                chain: self.chain_type,
                token: TokenIdentifier::address(token_address.to_string()),
                decimals: 8,
            },
            amount: token_balance,
        };

        let execution = create_transfer_with_authorization_execution(
            &lot,
            &sender_signer,
            &self.provider,
            &recipient_address,
        )
        .await
        .map_err(|e| crate::Error::DumpToAddress {
            message: e.to_string(),
        })?;

        Ok(hex::encode(execution.callData))
    }

    fn validate_address(&self, address: &str) -> bool {
        Address::from_str(address).is_ok()
    }

    fn minimum_block_confirmations(&self) -> u32 {
        self.min_confirmations
    }

    fn estimated_block_time(&self) -> Duration {
        self.est_block_time
    }

    async fn get_block_height(&self) -> Result<u64> {
        self.provider
            .get_block_number()
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })
    }

    async fn get_best_hash(&self) -> Result<String> {
        let Some(block) = self
            .provider
            .get_block_by_number(alloy::eips::BlockNumberOrTag::Latest)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?
        else {
            return Err(crate::Error::Serialization {
                message: "latest EVM block missing from RPC response".to_string(),
            });
        };
        Ok(hex::encode(block.hash()))
    }
}

fn extract_all_transfers_from_transaction_receipt(
    transaction_receipt: &TransactionReceipt,
) -> Vec<RpcLog<Transfer>> {
    transaction_receipt
        .logs()
        .iter()
        .filter_map(|log| log.log_decode::<Transfer>().ok())
        .collect()
}

fn max_fee_per_gas_with_headroom(max_fee_per_gas: u128) -> Result<u128> {
    max_fee_per_gas
        .checked_mul(11)
        .map(|value| value.div_ceil(10))
        .ok_or(crate::Error::NumericOverflow {
            context: "EVM max fee per gas headroom",
        })
}

fn checked_gas_fee(gas_limit: u64, max_fee_per_gas: u128, context: &'static str) -> Result<U256> {
    U256::from(gas_limit)
        .checked_mul(U256::from(max_fee_per_gas))
        .ok_or(crate::Error::NumericOverflow { context })
}

fn checked_u256_add(left: U256, right: U256, context: &'static str) -> Result<U256> {
    left.checked_add(right)
        .ok_or(crate::Error::NumericOverflow { context })
}

fn checked_u256_sub(left: U256, right: U256, context: &'static str) -> Result<U256> {
    left.checked_sub(right)
        .ok_or(crate::Error::NumericOverflow { context })
}

fn ensure_evm_receipt_success(tx_hash: TxHash, succeeded: bool) -> Result<()> {
    if succeeded {
        Ok(())
    } else {
        Err(crate::Error::TransactionReverted {
            tx_hash: tx_hash.to_string(),
        })
    }
}

fn evm_transfer_index_matches(observed: Option<u64>, expected: u64) -> Result<bool> {
    let Some(observed) = observed else {
        return Err(crate::Error::TransactionDeserializationFailed {
            context: "EVM transaction receipt missing transaction index".to_string(),
            loc: location!(),
        });
    };
    Ok(observed == expected)
}

fn evm_confirmation_count(current_block_height: u64, inclusion_height: u64) -> Result<u64> {
    current_block_height
        .checked_sub(inclusion_height)
        .ok_or(crate::Error::NumericOverflow {
            context: "evm confirmation count",
        })
}

fn evm_tx_status_from_receipt_fields(
    succeeded: bool,
    inclusion_height: Option<u64>,
    current_block_height: u64,
) -> Result<TxStatus> {
    let Some(inclusion_height) = inclusion_height else {
        return Ok(TxStatus::Pending(PendingTxStatus {
            current_height: current_block_height,
        }));
    };
    let confirmed = ConfirmedTxStatus {
        confirmations: evm_confirmation_count(current_block_height, inclusion_height)?,
        current_height: current_block_height,
        inclusion_height,
    };
    if succeeded {
        Ok(TxStatus::Confirmed(confirmed))
    } else {
        Ok(TxStatus::Failed(confirmed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverted_evm_receipt_is_error() {
        let tx_hash = TxHash::repeat_byte(0x11);
        let error = ensure_evm_receipt_success(tx_hash, false).unwrap_err();

        assert!(
            error.to_string().contains("Transaction reverted"),
            "{error}"
        );
        assert!(error.to_string().contains(&tx_hash.to_string()), "{error}");
    }

    #[test]
    fn successful_evm_receipt_is_ok() {
        assert!(ensure_evm_receipt_success(TxHash::repeat_byte(0x22), true).is_ok());
    }

    #[test]
    fn max_fee_per_gas_headroom_uses_checked_ceiling_math() {
        assert_eq!(max_fee_per_gas_with_headroom(0).unwrap(), 0);
        assert_eq!(max_fee_per_gas_with_headroom(1).unwrap(), 2);
        assert_eq!(max_fee_per_gas_with_headroom(10).unwrap(), 11);
    }

    #[test]
    fn max_fee_per_gas_headroom_rejects_overflow() {
        let error = max_fee_per_gas_with_headroom(u128::MAX).unwrap_err();

        assert!(
            matches!(error, crate::Error::NumericOverflow { context } if context == "EVM max fee per gas headroom")
        );
    }

    #[test]
    fn gas_sponsor_config_debug_redacts_private_key() {
        let config = EvmGasSponsorConfig {
            private_key: "0x59c6995e998f97a5a0044976f7ad0a7df4976fbe66f6cc18ff3c16f18a6b9e3f"
                .to_string(),
            vault_gas_buffer_wei: U256::from(10),
        };

        let rendered = format!("{config:?}");
        assert!(rendered.contains("private_key"));
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("59c6995e"));
        assert!(rendered.contains("vault_gas_buffer_wei"));
    }

    #[test]
    fn u256_balance_arithmetic_rejects_overflow_and_underflow() {
        assert!(matches!(
            checked_u256_add(U256::MAX, U256::from(1), "test add").unwrap_err(),
            crate::Error::NumericOverflow { context } if context == "test add"
        ));
        assert!(matches!(
            checked_u256_sub(U256::ZERO, U256::from(1), "test sub").unwrap_err(),
            crate::Error::NumericOverflow { context } if context == "test sub"
        ));
    }

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

        let batch = drain_paymaster_batch(&mut command_rx, EVM_PAYMASTER_BATCH_DEFAULT_MAX_SIZE)
            .await
            .unwrap();

        assert_eq!(batch.len(), 1);
    }

    #[tokio::test]
    async fn drain_batch_waits_for_first_command_when_empty() {
        let (_command_tx, mut command_rx) = mpsc::channel(1);
        let mut drain = Box::pin(drain_paymaster_batch(
            &mut command_rx,
            EVM_PAYMASTER_BATCH_DEFAULT_MAX_SIZE,
        ));
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);

        assert!(matches!(
            std::future::Future::poll(drain.as_mut(), &mut context),
            std::task::Poll::Pending
        ));
    }

    #[test]
    fn missing_evm_transfer_index_is_not_treated_as_zero() {
        let error = evm_transfer_index_matches(None, 0).unwrap_err();

        assert!(
            error.to_string().contains("missing transaction index"),
            "{error}"
        );
        assert!(evm_transfer_index_matches(Some(0), 0).unwrap());
        assert!(!evm_transfer_index_matches(Some(1), 0).unwrap());
    }

    #[test]
    fn erc20_transfer_amount_exceeds_balance_revert_is_not_retryable() {
        let error = "server returned an error response: error code 3: execution reverted: ERC20: transfer amount exceeds balance, data: 0x08c379a00000000000000000000000000000000000000000000000000000000000000204524332303a207472616e7366657220616d6f756e7420657863656564732062616c616e6365";

        assert_eq!(classify_evm_rpc_error(&error), "execution_reverted");
        assert!(!is_retryable_evm_rpc_error(&error));
        assert!(is_balance_bound_estimate_error(&error));
    }

    #[test]
    fn revert_data_containing_status_digits_is_not_upstream_unavailable() {
        let error = "server returned an error response: error code 3: execution reverted: ERC20: transfer amount exceeds balance, data: 0x0000000000000000000000000000000000000000000000000000000000050000";

        assert_eq!(classify_evm_rpc_error(&error), "execution_reverted");
        assert!(!is_retryable_evm_rpc_error(&error));
    }

    #[test]
    fn delimited_http_status_code_is_retryable() {
        let error = "upstream responded with HTTP 500";

        assert_eq!(classify_evm_rpc_error(&error), "upstream_unavailable");
        assert!(is_retryable_evm_rpc_error(&error));
    }

    #[test]
    fn status_digits_embedded_in_payload_are_not_http_status_codes() {
        let error = "bad rpc payload 0x0000000000050000";

        assert_eq!(classify_evm_rpc_error(&error), "other");
        assert!(!is_retryable_evm_rpc_error(&error));
    }

    #[test]
    fn reverted_evm_tx_status_is_failed_not_confirmed() {
        assert_eq!(
            evm_tx_status_from_receipt_fields(false, Some(10), 12).unwrap(),
            TxStatus::Failed(ConfirmedTxStatus {
                confirmations: 2,
                current_height: 12,
                inclusion_height: 10,
            })
        );
    }

    #[test]
    fn successful_evm_tx_status_is_confirmed() {
        assert_eq!(
            evm_tx_status_from_receipt_fields(true, Some(10), 12).unwrap(),
            TxStatus::Confirmed(ConfirmedTxStatus {
                confirmations: 2,
                current_height: 12,
                inclusion_height: 10,
            })
        );
    }

    #[test]
    fn receipt_without_block_height_is_pending() {
        assert_eq!(
            evm_tx_status_from_receipt_fields(true, None, 12).unwrap(),
            TxStatus::Pending(PendingTxStatus { current_height: 12 })
        );
    }

    #[test]
    fn evm_confirmation_count_rejects_receipts_above_current_head() {
        assert_eq!(evm_confirmation_count(12, 12).unwrap(), 0);
        let error = evm_confirmation_count(11, 12).unwrap_err();
        assert!(matches!(
            error,
            crate::Error::NumericOverflow { context }
                if context == "evm confirmation count"
        ));
        assert!(evm_tx_status_from_receipt_fields(true, Some(12), 11).is_err());
    }

    #[test]
    fn flashbots_policy_only_selects_flashbots_for_non_local_ethereum() {
        assert_eq!(
            select_broadcast_rpc_url(
                "https://ethereum-mainnet.example",
                ChainType::Ethereum,
                EvmBroadcastPolicy::FlashbotsIfEthereum,
            ),
            FLASHBOTS_ETHEREUM_RPC_URL
        );
        assert_eq!(
            select_broadcast_rpc_url(
                "https://base-mainnet.example",
                ChainType::Base,
                EvmBroadcastPolicy::FlashbotsIfEthereum,
            ),
            "https://base-mainnet.example"
        );
        assert_eq!(
            select_broadcast_rpc_url(
                "http://localhost:50101",
                ChainType::Ethereum,
                EvmBroadcastPolicy::FlashbotsIfEthereum,
            ),
            "http://localhost:50101"
        );
        assert_eq!(
            select_broadcast_rpc_url(
                "https://ethereum-mainnet.example",
                ChainType::Ethereum,
                EvmBroadcastPolicy::Standard,
            ),
            "https://ethereum-mainnet.example"
        );
    }

    fn test_paymaster_command(vault_address: Address) -> EvmPaymasterCommand {
        let (response_tx, _response_rx) = oneshot::channel();
        EvmPaymasterCommand::FundEvmTransaction {
            request: FundEvmTransactionAction {
                vault_address,
                target_address: Address::repeat_byte(0xaa),
                value: U256::ZERO,
                calldata: Bytes::new(),
                operation: "test",
            },
            response_tx,
        }
    }
}
