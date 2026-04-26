use crate::{
    key_derivation,
    traits::{UserDepositCandidateStatus, VerifiedUserDeposit},
    ChainOperations, Result,
};
use alloy::hex;
use alloy::primitives::U256;
use async_trait::async_trait;
use bdk_wallet::{signer::SignOptions, CreateParams, TxOrdering, Wallet as BdkWallet};
use bitcoin::base64::engine::general_purpose::STANDARD;
use bitcoin::base64::Engine;
use bitcoin::consensus::encode;
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use bitcoin::{Address, Amount, CompressedPublicKey, Network, OutPoint, PrivateKey, Transaction};
use bitcoincore_rpc_async::{jsonrpc, Auth, Client, RpcApi};
use metrics::{counter, histogram};
use reqwest::Url;
use router_primitives::{
    ChainType, ConfirmedTxStatus, Currency, PendingTxStatus, TokenIdentifier, TxStatus, Wallet,
};
use snafu::location;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tracing::{debug, info};

struct ReqwestRpcTransport {
    url: Url,
    client: reqwest::Client,
    auth_header: Option<String>,
}

impl ReqwestRpcTransport {
    pub fn new(url: Url, auth: Auth) -> Self {
        let auth = auth.get_user_pass().unwrap();
        let auth_header = if let Some((user, password)) = auth {
            let auth_header = format!(
                "Basic {}",
                STANDARD.encode(format!("{}:{}", user, password))
            );
            Some(auth_header)
        } else {
            None
        };
        Self {
            url,
            client: reqwest::Client::builder()
                .use_rustls_tls()
                .build()
                .expect("failed to construct bitcoin reqwest client with rustls"),
            auth_header,
        }
    }
}

#[async_trait]
impl jsonrpc::Transport for ReqwestRpcTransport {
    async fn send_request(
        &self,
        r: jsonrpc::Request<'_>,
    ) -> std::result::Result<jsonrpc::Response, jsonrpc::Error> {
        let mut request = self.client.post(self.url.clone()).json(&r);
        if let Some(auth_header) = &self.auth_header {
            request = request.header("Authorization", auth_header);
        }
        let started = Instant::now();
        let result = async {
            let response = request
                .send()
                .await
                .map_err(|e| jsonrpc::Error::Transport(e.into()))?;
            response
                .json()
                .await
                .map_err(|e| jsonrpc::Error::Transport(e.into()))
        }
        .await;
        record_bitcoin_rpc_request("single", result.is_ok(), started.elapsed());

        result
    }

    async fn send_batch(
        &self,
        rs: &[jsonrpc::Request<'_>],
    ) -> std::result::Result<Vec<jsonrpc::Response>, jsonrpc::Error> {
        let mut request = self.client.post(self.url.clone()).json(rs);
        if let Some(auth_header) = &self.auth_header {
            request = request.header("Authorization", auth_header);
        }
        let started = Instant::now();
        let result = async {
            let responses = request
                .send()
                .await
                .map_err(|e| jsonrpc::Error::Transport(e.into()))?;
            responses
                .json()
                .await
                .map_err(|e| jsonrpc::Error::Transport(e.into()))
        }
        .await;
        record_bitcoin_rpc_request("batch", result.is_ok(), started.elapsed());

        result
    }

    fn fmt_target(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.url)
    }
}

fn record_bitcoin_rpc_request(rpc_method: &'static str, success: bool, duration: Duration) {
    let status = if success { "success" } else { "error" };
    counter!(
        "tee_router_chain_rpc_requests_total",
        "chain" => "bitcoin",
        "rpc_method" => rpc_method,
        "status" => status,
    )
    .increment(1);
    histogram!(
        "tee_router_chain_rpc_request_duration_seconds",
        "chain" => "bitcoin",
        "rpc_method" => rpc_method,
        "status" => status,
    )
    .record(duration.as_secs_f64());
}

fn u256_to_u64_sats(field: &'static str, value: U256) -> Result<u64> {
    if value > U256::from(u64::MAX) {
        return Err(crate::Error::DumpToAddress {
            message: format!("{field} exceeds u64 satoshi range"),
        });
    }
    Ok(value.to::<u64>())
}

pub struct BitcoinChain {
    rpc_client: Client,
    untrusted_esplora_client: esplora_client::AsyncClient,
    network: Network,
}

impl BitcoinChain {
    pub fn new(
        bitcoin_core_rpc_url: &str,
        bitcoin_core_rpc_auth: Auth,
        untrusted_esplora_url: &str,
        network: Network,
    ) -> Result<Self> {
        // create a reqwest client
        let rpc_transport = ReqwestRpcTransport::new(
            Url::parse(bitcoin_core_rpc_url).unwrap(),
            bitcoin_core_rpc_auth,
        );
        let jsonrpc_client = jsonrpc::Client::with_transport(rpc_transport);
        let rpc_client = Client::from_jsonrpc(jsonrpc_client);

        let esplora_client = esplora_client::Builder::new(untrusted_esplora_url)
            .build_async()
            .map_err(|e| crate::Error::EsploraClientError {
                source: e,
                loc: location!(),
            })?;

        Ok(Self {
            rpc_client,
            untrusted_esplora_client: esplora_client,
            network,
        })
    }

    pub async fn address_balance_sats(&self, address: &str) -> Result<u64> {
        let address = Address::from_str(address)?.assume_checked();
        let started = Instant::now();
        let result = self
            .untrusted_esplora_client
            .get_address_utxo(&address)
            .await
            .map_err(|e| crate::Error::EsploraClientError {
                source: e,
                loc: location!(),
            });
        record_bitcoin_rpc_request(
            "esplora_get_address_utxo",
            result.is_ok(),
            started.elapsed(),
        );
        let utxos = result?;
        Ok(utxos.iter().map(|utxo| utxo.value).sum())
    }

    pub async fn estimate_full_balance_sweep_fee_sats(&self, address: &str) -> Result<u64> {
        let address = Address::from_str(address)?.assume_checked();
        let started = Instant::now();
        let result = self
            .untrusted_esplora_client
            .get_address_utxo(&address)
            .await
            .map_err(|e| crate::Error::EsploraClientError {
                source: e,
                loc: location!(),
            });
        record_bitcoin_rpc_request(
            "esplora_get_address_utxo",
            result.is_ok(),
            started.elapsed(),
        );
        let utxos = result?;
        if utxos.is_empty() {
            return Ok(0);
        }

        self.estimate_p2wpkh_transfer_fee_sats(utxos.len(), 1).await
    }

    pub async fn estimate_p2wpkh_transfer_fee_sats(
        &self,
        input_count: usize,
        output_count: usize,
    ) -> Result<u64> {
        let fee_rate_sat_per_vb = self.estimate_fee_rate_sat_per_vb().await?;
        let input_count = input_count.max(1) as u64;
        let output_count = output_count.max(1) as u64;
        let estimated_vbytes = 11_u64
            .saturating_add(31_u64.saturating_mul(output_count))
            .saturating_add(68_u64.saturating_mul(input_count));
        Ok(fee_rate_sat_per_vb.saturating_mul(estimated_vbytes))
    }

    pub async fn estimate_native_transfer_fee_sats(&self, address: &str) -> Result<u64> {
        let address = Address::from_str(address)?.assume_checked();
        let started = Instant::now();
        let result = self
            .untrusted_esplora_client
            .get_address_utxo(&address)
            .await
            .map_err(|e| crate::Error::EsploraClientError {
                source: e,
                loc: location!(),
            });
        record_bitcoin_rpc_request(
            "esplora_get_address_utxo",
            result.is_ok(),
            started.elapsed(),
        );
        let utxos = result?;
        if utxos.is_empty() {
            return Ok(0);
        }
        self.estimate_p2wpkh_transfer_fee_sats(utxos.len(), 2).await
    }

    async fn estimate_fee_rate_sat_per_vb(&self) -> Result<u64> {
        let fee_rate_btc_per_kb = self
            .rpc_client
            .estimate_smart_fee(2)
            .await
            .map_err(|e| crate::Error::BitcoinRpcError {
                source: e,
                loc: location!(),
            })?
            .fee_rate
            .unwrap_or(0.00001_f64);
        Ok(((fee_rate_btc_per_kb * 100_000_000.0) / 1000.0)
            .ceil()
            .max(1.0) as u64)
    }

    pub async fn broadcast_signed_transaction(&self, tx_hex: &str) -> Result<String> {
        let transaction: Transaction = encode::deserialize_hex(tx_hex).map_err(|e| {
            crate::Error::TransactionDeserializationFailed {
                context: format!("Failed to deserialize signed bitcoin transaction: {e}"),
                loc: location!(),
            }
        })?;

        let txid = self
            .rpc_client
            .send_raw_transaction(&transaction)
            .await
            .map_err(|e| crate::Error::BitcoinRpcError {
                source: e,
                loc: location!(),
            })?;

        Ok(txid.to_string())
    }

    pub async fn transfer_native_amount(
        &self,
        private_key: &str,
        recipient_address: &str,
        amount: U256,
    ) -> Result<String> {
        let amount_sats = u256_to_u64_sats("amount", amount)?;
        if amount_sats == 0 {
            return Err(crate::Error::DumpToAddress {
                message: "amount must be positive".to_string(),
            });
        }

        let private_key =
            PrivateKey::from_wif(private_key).map_err(|e| crate::Error::DumpToAddress {
                message: format!("Invalid signer private key: {e}"),
            })?;
        let recipient_address = Address::from_str(recipient_address)?.assume_checked();

        let secp = Secp256k1::new();
        let sender_address = Address::p2wpkh(
            &CompressedPublicKey::from_private_key(&secp, &private_key).unwrap(),
            self.network,
        );

        let started = Instant::now();
        let result = self
            .untrusted_esplora_client
            .get_address_utxo(&sender_address)
            .await
            .map_err(|e| crate::Error::EsploraClientError {
                source: e,
                loc: location!(),
            });
        record_bitcoin_rpc_request(
            "esplora_get_address_utxo",
            result.is_ok(),
            started.elapsed(),
        );
        let utxos = result?;
        if utxos.is_empty() {
            return Err(crate::Error::DumpToAddress {
                message: "No UTXOs found".to_string(),
            });
        }

        let total_in: u64 = utxos.iter().map(|utxo| utxo.value).sum();
        let fee_sats = self
            .estimate_p2wpkh_transfer_fee_sats(utxos.len(), 2)
            .await?;
        let required_sats =
            amount_sats
                .checked_add(fee_sats)
                .ok_or_else(|| crate::Error::DumpToAddress {
                    message: format!(
                        "Insufficient balance: amount {amount_sats} plus fee {fee_sats} overflows"
                    ),
                })?;
        if total_in < required_sats {
            return Err(crate::Error::InsufficientBalance {
                required: U256::from(required_sats),
                available: U256::from(total_in),
            });
        }

        let descriptor = format!("wpkh({})", private_key.to_wif());
        let mut temp_wallet = BdkWallet::create_with_params(
            CreateParams::new_single(descriptor).network(self.network),
        )
        .map_err(|e| crate::Error::DumpToAddress {
            message: format!("Failed to create temp wallet: {e}"),
        })?;

        let mut tx_builder = temp_wallet.build_tx();
        tx_builder.manually_selected_only();
        tx_builder.ordering(TxOrdering::Untouched);
        tx_builder.add_recipient(
            recipient_address.script_pubkey(),
            Amount::from_sat(amount_sats),
        );
        tx_builder.fee_absolute(Amount::from_sat(fee_sats));

        for utxo in utxos {
            let tx_hex = self
                .rpc_client
                .get_raw_transaction_hex(&utxo.txid, None)
                .await
                .map_err(|e| crate::Error::DumpToAddress {
                    message: format!("Failed to fetch raw transaction for {}: {e}", utxo.txid),
                })?;

            let tx_bytes =
                alloy::hex::decode(&tx_hex).map_err(|e| crate::Error::DumpToAddress {
                    message: format!(
                        "Failed to decode raw transaction hex for {}: {e}",
                        utxo.txid
                    ),
                })?;

            let full_tx =
                bitcoin::consensus::deserialize::<Transaction>(&tx_bytes).map_err(|e| {
                    crate::Error::DumpToAddress {
                        message: format!(
                            "Failed to deserialize raw transaction for {}: {e}",
                            utxo.txid
                        ),
                    }
                })?;

            let output = full_tx
                .output
                .get(utxo.vout as usize)
                .cloned()
                .ok_or_else(|| crate::Error::DumpToAddress {
                    message: format!(
                        "Transaction {} missing vout {} for foreign UTXO",
                        utxo.txid, utxo.vout
                    ),
                })?;

            let psbt_input = bdk_wallet::bitcoin::psbt::Input {
                witness_utxo: Some(output),
                non_witness_utxo: Some(full_tx.clone()),
                ..Default::default()
            };

            let satisfaction_weight = bdk_wallet::bitcoin::Weight::from_wu(108);

            tx_builder
                .add_foreign_utxo(
                    OutPoint::new(utxo.txid, utxo.vout),
                    psbt_input,
                    satisfaction_weight,
                )
                .map_err(|e| crate::Error::DumpToAddress {
                    message: format!(
                        "Failed to add foreign UTXO {}:{}: {e}",
                        utxo.txid, utxo.vout
                    ),
                })?;
        }

        let mut psbt = tx_builder
            .finish()
            .map_err(|e| crate::Error::DumpToAddress {
                message: format!("Failed to build transaction: {e}"),
            })?;

        let finalized = temp_wallet
            .sign(&mut psbt, SignOptions::default())
            .map_err(|e| crate::Error::DumpToAddress {
                message: format!("Failed to sign PSBT: {e}"),
            })?;

        if !finalized {
            return Err(crate::Error::DumpToAddress {
                message: "PSBT not fully finalized after signing".to_string(),
            });
        }

        let tx = psbt.extract_tx().map_err(|e| crate::Error::DumpToAddress {
            message: format!("Failed to extract transaction: {e}"),
        })?;
        let raw = bitcoin::consensus::serialize(&tx);
        self.broadcast_signed_transaction(&hex::encode(raw)).await
    }
}

#[async_trait]
impl ChainOperations for BitcoinChain {
    fn esplora_client(&self) -> Option<&esplora_client::AsyncClient> {
        Some(&self.untrusted_esplora_client)
    }

    fn create_wallet(&self) -> Result<(Wallet, [u8; 32])> {
        // Generate a random salt
        let mut salt = [0u8; 32];
        getrandom::getrandom(&mut salt).map_err(|_| crate::Error::Serialization {
            message: "Failed to generate random salt".to_string(),
        })?;

        // Generate a new private key
        let secp = Secp256k1::new();
        let secret_key = bitcoin::secp256k1::SecretKey::from_slice(&salt).unwrap();
        let private_key = PrivateKey::new(secret_key, self.network);

        // Derive public key and address
        let compressed_pk = CompressedPublicKey::from_private_key(&secp, &private_key).unwrap();
        let address = Address::p2wpkh(&compressed_pk, self.network);

        info!("Created new Bitcoin wallet: {}", address);

        let wallet = Wallet::new(address.to_string(), private_key.to_wif());
        Ok((wallet, salt))
    }

    fn derive_wallet(&self, master_key: &[u8], salt: &[u8; 32]) -> Result<Wallet> {
        // Derive private key using HKDF
        let private_key_bytes =
            key_derivation::derive_private_key(master_key, salt, b"bitcoin-wallet")?;

        // Create secp256k1 secret key
        let secret_key =
            SecretKey::from_slice(&private_key_bytes).map_err(|_| crate::Error::Serialization {
                message: "Failed to create secret key from derived bytes".to_string(),
            })?;

        let private_key = PrivateKey::new(secret_key, self.network);

        // Derive public key and address
        let secp = Secp256k1::new();
        let compressed_pk = CompressedPublicKey::from_private_key(&secp, &private_key).unwrap();
        let address = Address::p2wpkh(&compressed_pk, self.network);

        debug!("Derived Bitcoin wallet: {}", address);

        Ok(Wallet::new(address.to_string(), private_key.to_wif()))
    }

    async fn get_tx_status(&self, tx_hash: &str) -> Result<TxStatus> {
        let tx_verbose_result = self
            .rpc_client
            .get_raw_transaction_verbose(&bitcoin::Txid::from_str(tx_hash).unwrap())
            .await;
        let tx = match tx_verbose_result {
            Ok(tx_verbose) => tx_verbose,
            Err(e) => {
                if e.to_string()
                    .contains("No such mempool or blockchain transaction")
                {
                    return Ok(TxStatus::NotFound);
                }
                return Err(crate::Error::BitcoinRpcError {
                    source: e,
                    loc: location!(),
                });
            }
        };
        let current_height =
            self.rpc_client
                .get_block_count()
                .await
                .map_err(|e| crate::Error::BitcoinRpcError {
                    source: e,
                    loc: location!(),
                })?;

        if tx.confirmations.unwrap_or(0) > 0 {
            Ok(TxStatus::Confirmed(ConfirmedTxStatus {
                confirmations: tx.confirmations.unwrap_or(0),
                current_height,
                inclusion_height: current_height
                    .saturating_sub(tx.confirmations.unwrap_or(0))
                    .saturating_add(1),
            }))
        } else {
            Ok(TxStatus::Pending(PendingTxStatus { current_height }))
        }
    }

    async fn dump_to_address(
        &self,
        token: &TokenIdentifier,
        private_key: &str,
        recipient_address: &str,
        fee: U256,
    ) -> Result<String> {
        if token != &TokenIdentifier::Native {
            return Err(crate::Error::DumpToAddress {
                message: "Native token not supported".to_string(),
            });
        }
        let private_key =
            PrivateKey::from_wif(private_key).map_err(|e| crate::Error::DumpToAddress {
                message: format!("Invalid signer private key: {e}"),
            })?;
        let recipient_address = Address::from_str(recipient_address)?.assume_checked();

        // Determine the sender address from the provided private key and collect its UTXOs
        let secp = Secp256k1::new();
        let sender_address = Address::p2wpkh(
            &CompressedPublicKey::from_private_key(&secp, &private_key).unwrap(),
            self.network,
        );

        let started = Instant::now();
        let result = self
            .untrusted_esplora_client
            .get_address_utxo(&sender_address)
            .await
            .map_err(|e| crate::Error::EsploraClientError {
                source: e,
                loc: location!(),
            });
        record_bitcoin_rpc_request(
            "esplora_get_address_utxo",
            result.is_ok(),
            started.elapsed(),
        );
        let utxos = result?;
        if utxos.is_empty() {
            return Err(crate::Error::DumpToAddress {
                message: "No UTXOs found".to_string(),
            });
        }
        if utxos.iter().map(|utxo| utxo.value).sum::<u64>() < fee.to::<u64>() {
            return Err(crate::Error::DumpToAddress {
                message: "Insufficient balance to cover fee".to_string(),
            });
        }
        // Calculate totals
        let total_in: u64 = utxos.iter().map(|u| u.value).sum();
        let fee_sats: u64 = fee.to::<u64>();
        if total_in <= fee_sats {
            return Err(crate::Error::DumpToAddress {
                message: format!(
                    "Insufficient balance: inputs {total_in} sats, fee {fee_sats} sats"
                ),
            });
        }

        let send_amount = total_in - fee_sats;

        // Build a descriptor from the provided WIF — our addresses are P2WPKH
        let descriptor = format!("wpkh({})", private_key.to_wif());

        // Create a temporary in-memory BDK wallet for signing and building
        let mut temp_wallet = BdkWallet::create_with_params(
            CreateParams::new_single(descriptor).network(self.network),
        )
        .map_err(|e| crate::Error::DumpToAddress {
            message: format!("Failed to create temp wallet: {e}"),
        })?;

        let mut tx_builder = temp_wallet.build_tx();
        tx_builder.manually_selected_only();
        tx_builder.add_recipient(
            recipient_address.script_pubkey(),
            Amount::from_sat(send_amount),
        );
        tx_builder.fee_absolute(Amount::from_sat(fee_sats));

        // Add inputs as foreign UTXOs with full PSBT metadata for reliability
        for utxo in utxos {
            let tx_hex = self
                .rpc_client
                .get_raw_transaction_hex(&utxo.txid, None)
                .await
                .map_err(|e| crate::Error::DumpToAddress {
                    message: format!("Failed to fetch raw transaction for {}: {e}", utxo.txid),
                })?;

            let tx_bytes =
                alloy::hex::decode(&tx_hex).map_err(|e| crate::Error::DumpToAddress {
                    message: format!(
                        "Failed to decode raw transaction hex for {}: {e}",
                        utxo.txid
                    ),
                })?;

            let full_tx =
                bitcoin::consensus::deserialize::<Transaction>(&tx_bytes).map_err(|e| {
                    crate::Error::DumpToAddress {
                        message: format!(
                            "Failed to deserialize raw transaction for {}: {e}",
                            utxo.txid
                        ),
                    }
                })?;

            let output = full_tx
                .output
                .get(utxo.vout as usize)
                .cloned()
                .ok_or_else(|| crate::Error::DumpToAddress {
                    message: format!(
                        "Transaction {} missing vout {} for foreign UTXO",
                        utxo.txid, utxo.vout
                    ),
                })?;

            let psbt_input = bdk_wallet::bitcoin::psbt::Input {
                witness_utxo: Some(output),
                non_witness_utxo: Some(full_tx.clone()),
                ..Default::default()
            };

            let satisfaction_weight = bdk_wallet::bitcoin::Weight::from_wu(108);

            tx_builder
                .add_foreign_utxo(
                    OutPoint::new(utxo.txid, utxo.vout),
                    psbt_input,
                    satisfaction_weight,
                )
                .map_err(|e| crate::Error::DumpToAddress {
                    message: format!(
                        "Failed to add foreign UTXO {}:{}: {e}",
                        utxo.txid, utxo.vout
                    ),
                })?;
        }

        let mut psbt = tx_builder
            .finish()
            .map_err(|e| crate::Error::DumpToAddress {
                message: format!("Failed to build transaction: {e}"),
            })?;

        // Sign with the temporary wallet
        let finalized = temp_wallet
            .sign(&mut psbt, SignOptions::default())
            .map_err(|e| crate::Error::DumpToAddress {
                message: format!("Failed to sign PSBT: {e}"),
            })?;

        if !finalized {
            return Err(crate::Error::DumpToAddress {
                message: "PSBT not fully finalized after signing".to_string(),
            });
        }

        let tx = psbt.extract_tx().map_err(|e| crate::Error::DumpToAddress {
            message: format!("Failed to extract transaction: {e}"),
        })?;

        // Return raw signed transaction hex
        let raw = bitcoin::consensus::serialize(&tx);
        Ok(hex::encode(raw))
    }

    async fn verify_user_deposit_candidate(
        &self,
        recipient_address: &str,
        currency: &Currency,
        tx_hash: &str,
        transfer_index: u64,
    ) -> Result<UserDepositCandidateStatus> {
        if !matches!(currency.chain, ChainType::Bitcoin)
            || !matches!(currency.token, TokenIdentifier::Native)
        {
            return Err(crate::Error::InvalidCurrency {
                currency: currency.clone(),
                network: ChainType::Bitcoin,
            });
        }

        let txid = bitcoin::Txid::from_str(tx_hash).map_err(|e| {
            crate::Error::TransactionDeserializationFailed {
                context: format!("Failed to parse txid: {e}"),
                loc: location!(),
            }
        })?;

        let tx_verbose = match self.rpc_client.get_raw_transaction_verbose(&txid).await {
            Ok(tx_verbose) => tx_verbose,
            Err(bitcoincore_rpc_async::Error::JsonRpc(jsonrpc::Error::Rpc(rpc_err)))
                if rpc_err.code == -5 =>
            {
                return Ok(UserDepositCandidateStatus::TxNotFound);
            }
            Err(e) => {
                return Err(crate::Error::BitcoinRpcError {
                    source: e,
                    loc: location!(),
                });
            }
        };

        let confirmations = tx_verbose.confirmations.unwrap_or(0);
        let tx_bytes = hex::decode(tx_verbose.hex).map_err(|e| {
            crate::Error::TransactionDeserializationFailed {
                context: format!("Failed to decode bitcoin tx data hex to bytes: {e}"),
                loc: location!(),
            }
        })?;
        let tx = bitcoin::consensus::deserialize::<Transaction>(&tx_bytes).map_err(|e| {
            crate::Error::TransactionDeserializationFailed {
                context: format!("Failed to deserialize tx data as bitcoin transaction: {e}"),
                loc: location!(),
            }
        })?;

        let Some(output) = tx.output.get(transfer_index as usize) else {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        };

        let output_address =
            match bitcoin::Address::from_script(&output.script_pubkey, self.network) {
                Ok(address) => address,
                Err(_) => return Ok(UserDepositCandidateStatus::TransferNotFound),
            };

        if output_address.to_string() != recipient_address {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        }

        Ok(UserDepositCandidateStatus::Verified(VerifiedUserDeposit {
            amount: U256::from(output.value.to_sat()),
            confirmations,
        }))
    }

    fn validate_address(&self, address: &str) -> bool {
        match Address::from_str(address) {
            Ok(addr) => addr.is_valid_for_network(self.network),
            Err(_) => false,
        }
    }

    fn minimum_block_confirmations(&self) -> u32 {
        2
    }

    fn estimated_block_time(&self) -> Duration {
        Duration::from_secs(600) // 10 minutes
    }

    async fn get_block_height(&self) -> Result<u64> {
        self.rpc_client
            .get_block_count()
            .await
            .map_err(|e| crate::Error::BitcoinRpcError {
                source: e,
                loc: location!(),
            })
    }

    async fn get_best_hash(&self) -> Result<String> {
        Ok(self
            .rpc_client
            .get_best_block_hash()
            .await
            .map_err(|e| crate::Error::BitcoinRpcError {
                source: e,
                loc: location!(),
            })?
            .to_string())
    }
}
