use crate::traits::{UserDepositCandidateStatus, VerifiedUserDeposit};
use crate::{key_derivation, ChainOperations, Result};
use alloy::consensus::Transaction as _;
use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::{Address, Bytes, TxHash, U256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::{Log as RpcLog, TransactionReceipt, TransactionRequest};
use alloy::signers::local::{LocalSigner, PrivateKeySigner};
use alloy::{hex, sol};
use async_trait::async_trait;
use blockchain_utils::create_transfer_with_authorization_execution;
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use metrics::{counter, gauge, histogram};
use router_primitives::{
    ChainType, ConfirmedTxStatus, Currency, Lot, PendingTxStatus, TokenIdentifier, TxStatus, Wallet,
};
use snafu::location;
use std::{
    future::Future,
    str::FromStr,
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, oneshot},
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
const WEI_PER_GWEI: f64 = 1_000_000_000.0;

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

#[derive(Clone, Debug)]
pub struct EvmGasSponsorConfig {
    pub private_key: String,
    pub vault_gas_buffer_wei: U256,
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
    sponsor_address: Address,
    vault_gas_buffer_wei: U256,
    chain_type: ChainType,
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
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas);
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
                let reserved_fee = U256::from(gas_limit).saturating_mul(U256::from(
                    max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas),
                ));
                if native_balance <= reserved_fee {
                    return Err(crate::Error::DumpToAddress {
                        message: "Insufficient native balance to cover refund gas".to_string(),
                    });
                }

                let transaction_request = TransactionRequest::default()
                    .with_from(sender_address)
                    .with_to(recipient_address)
                    .with_value(native_balance.saturating_sub(reserved_fee))
                    .with_nonce(self.pending_nonce(sender_address).await?)
                    .with_gas_limit(gas_limit)
                    .with_max_fee_per_gas(fee_estimate.max_fee_per_gas)
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
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas);
        let reserved_fee = U256::from(gas_limit).saturating_mul(U256::from(max_fee_per_gas));
        let required_balance = amount.saturating_add(reserved_fee);
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
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas);
        Ok(U256::from(gas_limit).saturating_mul(U256::from(max_fee_per_gas)))
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
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas);
        let reserved_fee = U256::from(gas_limit).saturating_mul(U256::from(max_fee_per_gas));
        let required_balance = value.saturating_add(reserved_fee);
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
        let receipt = wait_for_evm_receipt(&wallet_provider, self.chain_type, tx_hash).await?;

        Ok(EvmCallOutcome {
            tx_hash: tx_hash.to_string(),
            logs: receipt.logs().to_vec(),
        })
    }
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
            .wallet(EthereumWallet::new(signer))
            .connect_http(url)
            .erased();
        let (command_tx, command_rx) = mpsc::channel(EVM_PAYMASTER_COMMAND_BUFFER);
        let mut actor_tasks = JoinSet::new();
        let actor = EvmPaymasterActor {
            provider,
            wallet_provider,
            sponsor_address,
            vault_gas_buffer_wei: config.vault_gas_buffer_wei,
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

async fn run_evm_paymaster_actor(
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
                let result = actor.fund_vault_action(request).await;
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
                let result = actor.fund_evm_transaction(request).await;
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

impl EvmPaymasterActor {
    async fn fund_vault_action(&self, request: FundVaultAction) -> Result<Option<String>> {
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
        self.fund_evm_transaction(FundEvmTransactionAction {
            vault_address,
            target_address: token_address,
            value: U256::ZERO,
            calldata: transfer_calldata,
            operation: "erc20_transfer",
        })
        .await
    }

    async fn fund_evm_transaction(
        &self,
        request: FundEvmTransactionAction,
    ) -> Result<Option<String>> {
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
            .with_input(calldata);
        let action_gas_limit = retry_evm_rpc(self.chain_type, "estimate_gas", || async {
            self.provider
                .estimate_gas(action_estimate_request.clone())
                .await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        record_paymaster_gas_estimate(self.chain_type, operation, action_gas_limit);
        let fee_estimate = retry_evm_rpc(self.chain_type, "estimate_eip1559_fees", || async {
            self.provider.estimate_eip1559_fees().await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        let action_max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas);
        let required_vault_balance = value
            .saturating_add(
                U256::from(action_gas_limit).saturating_mul(U256::from(action_max_fee_per_gas)),
            )
            .saturating_add(self.vault_gas_buffer_wei);

        let current_balance = retry_evm_rpc(self.chain_type, "get_balance", || async {
            self.provider.get_balance(vault_address).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        record_paymaster_balance_gwei(self.chain_type, "vault", current_balance);
        if current_balance < value {
            record_paymaster_funding_failed(self.chain_type, "insufficient_action_value");
            return Err(crate::Error::InsufficientBalance {
                required: value,
                available: current_balance,
            });
        }
        if current_balance >= required_vault_balance {
            record_paymaster_funding_skipped(self.chain_type, "already_funded");
            return Ok(None);
        }

        let top_up_amount = required_vault_balance.saturating_sub(current_balance);
        let sponsor_balance = retry_evm_rpc(self.chain_type, "get_balance", || async {
            self.provider.get_balance(self.sponsor_address).await
        })
        .await
        .map_err(|e| crate::Error::EVMRpcError {
            source: e,
            loc: location!(),
        })?;
        record_paymaster_balance_gwei(self.chain_type, "sponsor", sponsor_balance);

        let estimate_request = TransactionRequest::default()
            .with_from(self.sponsor_address)
            .with_to(vault_address)
            .with_value(top_up_amount);
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
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas);
        let reserved_fee = U256::from(gas_limit).saturating_mul(U256::from(max_fee_per_gas));
        let required_balance = top_up_amount.saturating_add(reserved_fee);
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
            .with_to(vault_address)
            .with_value(top_up_amount)
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

        record_paymaster_funding_sent(self.chain_type, top_up_amount);
        Ok(Some(tx_hash.to_string()))
    }
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
    for attempt in 1..=EVM_RPC_RETRY_ATTEMPTS {
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
    unreachable!("retry loop always returns before exhausting attempts")
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
    if message.contains("429")
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
        || message.contains("502")
        || message.contains("503")
        || message.contains("504")
        || message.contains("500")
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
        if transaction_receipt.transaction_index.unwrap_or(0) != transfer_index {
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
        let confirmations =
            current_block_height.saturating_sub(transaction_receipt.block_number.unwrap());

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
        let receipt = self
            .provider
            .get_transaction_receipt(tx_hash_parsed)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;
        let current_block_height =
            self.provider
                .get_block_number()
                .await
                .map_err(|e| crate::Error::EVMRpcError {
                    source: e,
                    loc: location!(),
                })?;

        if let Some(receipt) = receipt {
            return Ok(TxStatus::Confirmed(ConfirmedTxStatus {
                confirmations: current_block_height - receipt.block_number.unwrap(),
                current_height: current_block_height,
                inclusion_height: receipt.block_number.unwrap(),
            }));
        }

        let pending_tx = self
            .provider
            .get_transaction_by_hash(tx_hash_parsed)
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

        let token_address = match &currency.token {
            TokenIdentifier::Address(address) => address,
            TokenIdentifier::Native => unreachable!("native deposits are handled above"),
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
        let confirmations = current_block_height - transaction_receipt.block_number.unwrap();

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
        Ok(hex::encode(
            self.provider
                .get_block_by_number(alloy::eips::BlockNumberOrTag::Latest)
                .await
                .map_err(|e| crate::Error::EVMRpcError {
                    source: e,
                    loc: location!(),
                })?
                .unwrap()
                .hash(),
        ))
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

fn max_fee_per_gas_with_headroom(max_fee_per_gas: u128) -> u128 {
    max_fee_per_gas.saturating_mul(11).div_ceil(10)
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
}
