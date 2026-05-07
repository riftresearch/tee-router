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
use serde::{de::DeserializeOwned, Deserialize};
use snafu::location;
use std::{collections::HashSet, io, str::FromStr, time::Duration, time::Instant};
use tracing::{debug, info};

const BITCOIN_RPC_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const BITCOIN_RPC_MAX_RESPONSE_BODY_BYTES: usize = 256 * 1024;
const BITCOIN_RPC_ERROR_BODY_PREVIEW_BYTES: usize = 2 * 1024;

#[derive(Debug, Deserialize)]
struct TestMempoolAcceptResult {
    allowed: bool,
}

#[derive(Clone)]
struct BitcoinSpendableOutput {
    txid: bitcoin::Txid,
    vout: u32,
    full_tx: Transaction,
    output: bitcoin::TxOut,
}

impl BitcoinSpendableOutput {
    fn value_sats(&self) -> u64 {
        self.output.value.to_sat()
    }
}

struct ReqwestRpcTransport {
    url: Url,
    client: reqwest::Client,
    auth_header: Option<String>,
}

impl ReqwestRpcTransport {
    pub fn new(url: Url, auth: Auth) -> Result<Self> {
        let auth = auth
            .get_user_pass()
            .map_err(|e| crate::Error::Serialization {
                message: format!("invalid bitcoin RPC authentication: {e}"),
            })?;
        let auth_header = if let Some((user, password)) = auth {
            let auth_header = format!(
                "Basic {}",
                STANDARD.encode(format!("{}:{}", user, password))
            );
            Some(auth_header)
        } else {
            None
        };
        Ok(Self {
            url,
            client: reqwest::Client::builder()
                .use_rustls_tls()
                .timeout(BITCOIN_RPC_HTTP_TIMEOUT)
                .build()
                .map_err(|e| crate::Error::Serialization {
                    message: format!("failed to construct bitcoin reqwest client: {e}"),
                })?,
            auth_header,
        })
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
            read_json_rpc_response(response).await
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
            read_json_rpc_response(responses).await
        }
        .await;
        record_bitcoin_rpc_request("batch", result.is_ok(), started.elapsed());

        result
    }

    fn fmt_target(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", redacted_url_for_debug(&self.url))
    }
}

async fn read_json_rpc_response<T>(
    response: reqwest::Response,
) -> std::result::Result<T, jsonrpc::Error>
where
    T: DeserializeOwned,
{
    let status = response.status();
    let body = read_limited_response_body(response, BITCOIN_RPC_MAX_RESPONSE_BODY_BYTES).await?;
    if !status.is_success() {
        return Err(jsonrpc_transport_error(format!(
            "Bitcoin RPC returned HTTP {}: {}",
            status.as_u16(),
            response_body_preview(&body)
        )));
    }

    serde_json::from_slice(&body).map_err(jsonrpc::Error::Json)
}

async fn read_limited_response_body(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> std::result::Result<Vec<u8>, jsonrpc::Error> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| jsonrpc::Error::Transport(e.into()))?
    {
        if !append_limited_body_chunk(&mut body, chunk.as_ref(), max_bytes) {
            return Err(jsonrpc_transport_error(format!(
                "Bitcoin RPC response body exceeded {max_bytes} bytes"
            )));
        }
    }
    Ok(body)
}

fn append_limited_body_chunk(body: &mut Vec<u8>, chunk: &[u8], max_bytes: usize) -> bool {
    if body.len().saturating_add(chunk.len()) > max_bytes {
        return false;
    }
    body.extend_from_slice(chunk);
    true
}

fn jsonrpc_transport_error(message: String) -> jsonrpc::Error {
    jsonrpc::Error::Transport(io::Error::other(message).into())
}

fn response_body_preview(body: &[u8]) -> String {
    let truncated = body.len() > BITCOIN_RPC_ERROR_BODY_PREVIEW_BYTES;
    let preview = if truncated {
        &body[..BITCOIN_RPC_ERROR_BODY_PREVIEW_BYTES]
    } else {
        body
    };
    let mut text = String::from_utf8_lossy(preview).into_owned();
    if truncated {
        text.push_str("...<truncated>");
    }
    text
}

fn redacted_url_for_debug(url: &Url) -> String {
    let host = url.host_str().unwrap_or("<missing-host>");
    let mut redacted = format!("{}://{}", url.scheme(), host);
    if let Some(port) = url.port() {
        redacted.push(':');
        redacted.push_str(&port.to_string());
    }
    if url.path() != "/" {
        redacted.push_str("/<redacted-path>");
    }
    if url.query().is_some() {
        redacted.push_str("?<redacted-query>");
    }
    if url.fragment().is_some() {
        redacted.push_str("#<redacted-fragment>");
    }
    redacted
}

fn bitcoin_p2wpkh_transfer_fee_sats(
    fee_rate_sat_per_vb: u64,
    input_count: usize,
    output_count: usize,
) -> Result<u64> {
    let input_count =
        u64::try_from(input_count.max(1)).map_err(|_| crate::Error::NumericOverflow {
            context: "bitcoin fee input count",
        })?;
    let output_count =
        u64::try_from(output_count.max(1)).map_err(|_| crate::Error::NumericOverflow {
            context: "bitcoin fee output count",
        })?;
    let input_vbytes = 68_u64
        .checked_mul(input_count)
        .ok_or(crate::Error::NumericOverflow {
            context: "bitcoin fee input vbytes",
        })?;
    let output_vbytes = 31_u64
        .checked_mul(output_count)
        .ok_or(crate::Error::NumericOverflow {
            context: "bitcoin fee output vbytes",
        })?;
    let estimated_vbytes = checked_add_sats(
        checked_add_sats(11, output_vbytes, "bitcoin fee base output vbytes")?,
        input_vbytes,
        "bitcoin fee estimated vbytes",
    )?;
    fee_rate_sat_per_vb
        .checked_mul(estimated_vbytes)
        .ok_or(crate::Error::NumericOverflow {
            context: "bitcoin fee sats",
        })
}

fn fee_rate_btc_per_kb_to_sat_per_vb(fee_rate_btc_per_kb: f64) -> Result<u64> {
    if !fee_rate_btc_per_kb.is_finite() || fee_rate_btc_per_kb <= 0.0 {
        return Err(crate::Error::Serialization {
            message: format!("bitcoin fee rate must be finite and positive: {fee_rate_btc_per_kb}"),
        });
    }
    let sat_per_vb = (fee_rate_btc_per_kb * 100_000.0).ceil().max(1.0);
    if !sat_per_vb.is_finite() || sat_per_vb > u64::MAX as f64 {
        return Err(crate::Error::NumericOverflow {
            context: "bitcoin fee rate sat/vbyte",
        });
    }
    Ok(sat_per_vb as u64)
}

fn sum_sats(values: impl IntoIterator<Item = u64>, context: &'static str) -> Result<u64> {
    values.into_iter().try_fold(0_u64, |total, value| {
        checked_add_sats(total, value, context)
    })
}

fn checked_add_sats(left: u64, right: u64, context: &'static str) -> Result<u64> {
    left.checked_add(right)
        .ok_or(crate::Error::NumericOverflow { context })
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

fn bitcoin_inclusion_height(current_height: u64, confirmations: u64) -> Result<u64> {
    current_height
        .checked_add(1)
        .and_then(|chain_length| chain_length.checked_sub(confirmations))
        .ok_or(crate::Error::NumericOverflow {
            context: "bitcoin inclusion height",
        })
}

fn bitcoin_output_index(transfer_index: u64) -> Option<usize> {
    let vout = u32::try_from(transfer_index).ok()?;
    usize::try_from(vout).ok()
}

fn compressed_public_key_for(private_key: &PrivateKey) -> Result<CompressedPublicKey> {
    let secp = Secp256k1::new();
    CompressedPublicKey::from_private_key(&secp, private_key).map_err(|_| {
        crate::Error::Serialization {
            message: "failed to derive compressed bitcoin public key".to_string(),
        }
    })
}

fn parse_bitcoin_address_for_network(address: &str, network: Network) -> Result<Address> {
    Address::from_str(address)
        .and_then(|address| address.require_network(network))
        .map_err(|error| crate::Error::InvalidAddress {
            address: address.to_string(),
            network: ChainType::Bitcoin,
            reason: error.to_string(),
        })
}

fn random_bitcoin_secret_key() -> Result<(SecretKey, [u8; 32])> {
    for _ in 0..16 {
        let mut salt = [0u8; 32];
        getrandom::getrandom(&mut salt).map_err(|_| crate::Error::Serialization {
            message: "Failed to generate random salt".to_string(),
        })?;
        if let Ok(secret_key) = SecretKey::from_slice(&salt) {
            return Ok((secret_key, salt));
        }
    }

    Err(crate::Error::WalletCreation {
        message: "failed to generate a valid secp256k1 secret key".to_string(),
    })
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
            Url::parse(bitcoin_core_rpc_url).map_err(|e| crate::Error::Serialization {
                message: format!("invalid bitcoin RPC URL: {e}"),
            })?,
            bitcoin_core_rpc_auth,
        )?;
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
        let address = parse_bitcoin_address_for_network(address, self.network)?;
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
        sum_sats(
            utxos.iter().map(|utxo| utxo.value),
            "bitcoin address balance",
        )
    }

    pub async fn estimate_full_balance_sweep_fee_sats(&self, address: &str) -> Result<u64> {
        let address = parse_bitcoin_address_for_network(address, self.network)?;
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
        bitcoin_p2wpkh_transfer_fee_sats(
            self.estimate_fee_rate_sat_per_vb().await?,
            input_count,
            output_count,
        )
    }

    pub async fn estimate_native_transfer_fee_sats(&self, address: &str) -> Result<u64> {
        let address = parse_bitcoin_address_for_network(address, self.network)?;
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
        let Some(fee_rate_btc_per_kb) = self
            .rpc_client
            .estimate_smart_fee(2)
            .await
            .map_err(|e| crate::Error::BitcoinRpcError {
                source: e,
                loc: location!(),
            })?
            .fee_rate
        else {
            return Ok(1);
        };
        fee_rate_btc_per_kb_to_sat_per_vb(fee_rate_btc_per_kb)
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

    pub async fn can_spend_outpoint_now(
        &self,
        private_key: &str,
        recipient_address: &str,
        tx_hash: &str,
        vout: u32,
        expected_amount_sats: u64,
    ) -> Result<bool> {
        let private_key =
            PrivateKey::from_wif(private_key).map_err(|e| crate::Error::DumpToAddress {
                message: format!("Invalid signer private key: {e}"),
            })?;
        let recipient_address = parse_bitcoin_address_for_network(recipient_address, self.network)?;
        let txid = bitcoin::Txid::from_str(tx_hash).map_err(|e| {
            crate::Error::TransactionDeserializationFailed {
                context: format!("Failed to parse bitcoin txid {tx_hash}: {e}"),
                loc: location!(),
            }
        })?;

        let tx_out = self
            .rpc_client
            .get_tx_out(&txid, vout, Some(true))
            .await
            .map_err(|e| crate::Error::BitcoinRpcError {
                source: e,
                loc: location!(),
            })?;
        if tx_out.is_none() {
            return Ok(false);
        }

        let full_tx = self.raw_transaction(&txid).await?;
        let output_index =
            bitcoin_output_index(u64::from(vout)).ok_or_else(|| crate::Error::DumpToAddress {
                message: format!("Transaction {txid} missing vout {vout}"),
            })?;
        let output = full_tx.output.get(output_index).cloned().ok_or_else(|| {
            crate::Error::DumpToAddress {
                message: format!("Transaction {txid} missing vout {vout}"),
            }
        })?;
        if output.value.to_sat() != expected_amount_sats {
            return Ok(false);
        }

        let sender_address =
            Address::p2wpkh(&compressed_public_key_for(&private_key)?, self.network);
        if output.script_pubkey != sender_address.script_pubkey() {
            return Ok(false);
        }

        let fee_sats = self.estimate_p2wpkh_transfer_fee_sats(1, 1).await?;
        if expected_amount_sats <= fee_sats {
            return Ok(false);
        }

        let raw_tx = self.build_signed_p2wpkh_sweep_from_outputs(
            private_key,
            recipient_address,
            vec![BitcoinSpendableOutput {
                txid,
                vout,
                full_tx,
                output,
            }],
            fee_sats,
        )?;
        let raw_tx_hex = raw_tx;
        let accepted = self
            .rpc_client
            .call::<Vec<TestMempoolAcceptResult>>(
                "testmempoolaccept",
                &[serde_json::json!([raw_tx_hex])],
            )
            .await
            .map_err(|e| crate::Error::BitcoinRpcError {
                source: e,
                loc: location!(),
            })?;

        Ok(accepted.first().is_some_and(|result| result.allowed))
    }

    async fn spendable_output_from_outpoint(
        &self,
        sender_address: &Address,
        tx_hash: &str,
        vout: u32,
        expected_amount_sats: u64,
    ) -> Result<BitcoinSpendableOutput> {
        let txid = bitcoin::Txid::from_str(tx_hash).map_err(|e| {
            crate::Error::TransactionDeserializationFailed {
                context: format!("Failed to parse bitcoin txid {tx_hash}: {e}"),
                loc: location!(),
            }
        })?;
        let tx_out = self
            .rpc_client
            .get_tx_out(&txid, vout, Some(true))
            .await
            .map_err(|e| crate::Error::BitcoinRpcError {
                source: e,
                loc: location!(),
            })?;
        if tx_out.is_none() {
            return Err(crate::Error::DumpToAddress {
                message: format!("Observed bitcoin outpoint {txid}:{vout} is not spendable"),
            });
        }

        let full_tx = self.raw_transaction(&txid).await?;
        let output_index =
            bitcoin_output_index(u64::from(vout)).ok_or_else(|| crate::Error::DumpToAddress {
                message: format!("Transaction {txid} missing vout {vout}"),
            })?;
        let output = full_tx.output.get(output_index).cloned().ok_or_else(|| {
            crate::Error::DumpToAddress {
                message: format!("Transaction {txid} missing vout {vout}"),
            }
        })?;
        if output.value.to_sat() != expected_amount_sats {
            return Err(crate::Error::DumpToAddress {
                message: format!(
                    "Observed bitcoin outpoint {txid}:{vout} amount {} did not match expected {expected_amount_sats}",
                    output.value.to_sat()
                ),
            });
        }
        if output.script_pubkey != sender_address.script_pubkey() {
            return Err(crate::Error::DumpToAddress {
                message: format!(
                    "Observed bitcoin outpoint {txid}:{vout} does not pay the signer address"
                ),
            });
        }

        Ok(BitcoinSpendableOutput {
            txid,
            vout,
            full_tx,
            output,
        })
    }

    pub async fn transfer_native_amount_from_outpoint(
        &self,
        private_key: &str,
        recipient_address: &str,
        amount: U256,
        tx_hash: &str,
        vout: u32,
        expected_amount_sats: u64,
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
        let recipient_address = parse_bitcoin_address_for_network(recipient_address, self.network)?;
        let sender_address =
            Address::p2wpkh(&compressed_public_key_for(&private_key)?, self.network);
        let utxo = self
            .spendable_output_from_outpoint(&sender_address, tx_hash, vout, expected_amount_sats)
            .await?;

        let total_in = utxo.value_sats();
        let fee_sats = self.estimate_p2wpkh_transfer_fee_sats(1, 2).await?;
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

        let psbt_input = bdk_wallet::bitcoin::psbt::Input {
            witness_utxo: Some(utxo.output.clone()),
            non_witness_utxo: Some(utxo.full_tx.clone()),
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

    pub async fn dump_to_address_from_outpoint(
        &self,
        token: &TokenIdentifier,
        private_key: &str,
        recipient_address: &str,
        fee: U256,
        tx_hash: &str,
        vout: u32,
        expected_amount_sats: u64,
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
        let recipient_address = parse_bitcoin_address_for_network(recipient_address, self.network)?;
        let sender_address =
            Address::p2wpkh(&compressed_public_key_for(&private_key)?, self.network);
        let fee_sats = u256_to_u64_sats("fee", fee)?;
        let utxo = self
            .spendable_output_from_outpoint(&sender_address, tx_hash, vout, expected_amount_sats)
            .await?;

        self.build_signed_p2wpkh_sweep_from_outputs(
            private_key,
            recipient_address,
            vec![utxo],
            fee_sats,
        )
    }

    async fn raw_transaction(&self, txid: &bitcoin::Txid) -> Result<Transaction> {
        let tx_hex = self
            .rpc_client
            .get_raw_transaction_hex(txid, None)
            .await
            .map_err(|e| crate::Error::DumpToAddress {
                message: format!("Failed to fetch raw transaction for {txid}: {e}"),
            })?;

        let tx_bytes = alloy::hex::decode(&tx_hex).map_err(|e| crate::Error::DumpToAddress {
            message: format!("Failed to decode raw transaction hex for {txid}: {e}"),
        })?;

        bitcoin::consensus::deserialize::<Transaction>(&tx_bytes).map_err(|e| {
            crate::Error::DumpToAddress {
                message: format!("Failed to deserialize raw transaction for {txid}: {e}"),
            }
        })
    }

    async fn spendable_outputs_from_esplora(
        &self,
        address: &Address,
    ) -> Result<Vec<BitcoinSpendableOutput>> {
        let started = Instant::now();
        let result = self
            .untrusted_esplora_client
            .get_address_utxo(address)
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

        let script_pubkey = address.script_pubkey();
        let mut outputs = Vec::new();
        for utxo in result? {
            let full_tx = self.raw_transaction(&utxo.txid).await?;
            let output_index = bitcoin_output_index(u64::from(utxo.vout)).ok_or_else(|| {
                crate::Error::DumpToAddress {
                    message: format!(
                        "Transaction {} missing vout {} for foreign UTXO",
                        utxo.txid, utxo.vout
                    ),
                }
            })?;
            let output = full_tx.output.get(output_index).cloned().ok_or_else(|| {
                crate::Error::DumpToAddress {
                    message: format!(
                        "Transaction {} missing vout {} for foreign UTXO",
                        utxo.txid, utxo.vout
                    ),
                }
            })?;
            if output.script_pubkey != script_pubkey {
                debug!(
                    txid = %utxo.txid,
                    vout = utxo.vout,
                    address = %address,
                    "Skipping Esplora UTXO whose output script does not match the wallet address"
                );
                continue;
            }
            outputs.push(BitcoinSpendableOutput {
                txid: utxo.txid,
                vout: utxo.vout,
                full_tx,
                output,
            });
        }
        Ok(outputs)
    }

    async fn mempool_outputs_for_address(
        &self,
        address: &Address,
        seen: &mut HashSet<OutPoint>,
    ) -> Result<Vec<BitcoinSpendableOutput>> {
        let txids =
            self.rpc_client
                .get_raw_mempool()
                .await
                .map_err(|e| crate::Error::BitcoinRpcError {
                    source: e,
                    loc: location!(),
                })?;
        if txids.is_empty() {
            return Ok(Vec::new());
        }

        let script_pubkey = address.script_pubkey();
        let mut outputs = Vec::new();
        for txid in txids {
            let full_tx = match self.raw_transaction(&txid).await {
                Ok(tx) => tx,
                Err(err) => {
                    debug!(
                        txid = %txid,
                        error = %err,
                        "Skipping mempool transaction whose raw body could not be loaded"
                    );
                    continue;
                }
            };
            for (vout_index, output) in full_tx.output.iter().enumerate() {
                if output.script_pubkey != script_pubkey {
                    continue;
                }
                let vout = u32::try_from(vout_index).map_err(|_| crate::Error::DumpToAddress {
                    message: format!("Bitcoin transaction {txid} has vout index above u32"),
                })?;
                let outpoint = OutPoint::new(txid, vout);
                if !seen.insert(outpoint) {
                    continue;
                }
                let still_unspent = self
                    .rpc_client
                    .get_tx_out(&txid, vout, Some(true))
                    .await
                    .map_err(|e| crate::Error::BitcoinRpcError {
                        source: e,
                        loc: location!(),
                    })?
                    .is_some();
                if !still_unspent {
                    continue;
                }
                outputs.push(BitcoinSpendableOutput {
                    txid,
                    vout,
                    full_tx: full_tx.clone(),
                    output: output.clone(),
                });
            }
        }
        Ok(outputs)
    }

    async fn spendable_outputs_for_address(
        &self,
        address: &Address,
        minimum_sats: u64,
    ) -> Result<Vec<BitcoinSpendableOutput>> {
        let mut outputs = self.spendable_outputs_from_esplora(address).await?;
        let mut seen: HashSet<OutPoint> = outputs
            .iter()
            .map(|output| OutPoint::new(output.txid, output.vout))
            .collect();
        let total = sum_sats(
            outputs.iter().map(BitcoinSpendableOutput::value_sats),
            "bitcoin spendable output total",
        )?;
        if total >= minimum_sats {
            return Ok(outputs);
        }

        outputs.extend(self.mempool_outputs_for_address(address, &mut seen).await?);
        Ok(outputs)
    }

    fn build_signed_p2wpkh_sweep_from_outputs(
        &self,
        private_key: PrivateKey,
        recipient_address: Address,
        utxos: Vec<BitcoinSpendableOutput>,
        fee_sats: u64,
    ) -> Result<String> {
        let total_in = utxos
            .iter()
            .try_fold(0_u64, |total, output| {
                total.checked_add(output.value_sats())
            })
            .ok_or_else(|| crate::Error::DumpToAddress {
                message: "Bitcoin sweep input amount overflow".to_string(),
            })?;
        if total_in <= fee_sats {
            return Err(crate::Error::DumpToAddress {
                message: format!(
                    "Insufficient balance: inputs {total_in} sats, fee {fee_sats} sats"
                ),
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
            Amount::from_sat(total_in - fee_sats),
        );
        tx_builder.fee_absolute(Amount::from_sat(fee_sats));

        for utxo in utxos {
            let psbt_input = bdk_wallet::bitcoin::psbt::Input {
                witness_utxo: Some(utxo.output),
                non_witness_utxo: Some(utxo.full_tx),
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
        Ok(hex::encode(bitcoin::consensus::serialize(&tx)))
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
        let recipient_address = parse_bitcoin_address_for_network(recipient_address, self.network)?;

        let sender_address =
            Address::p2wpkh(&compressed_public_key_for(&private_key)?, self.network);

        let mut utxos = self
            .spendable_outputs_for_address(&sender_address, amount_sats)
            .await?;
        if utxos.is_empty() {
            return Err(crate::Error::DumpToAddress {
                message: "No UTXOs found".to_string(),
            });
        }

        let mut total_in = sum_sats(
            utxos.iter().map(BitcoinSpendableOutput::value_sats),
            "bitcoin transfer input total",
        )?;
        let mut fee_sats = self
            .estimate_p2wpkh_transfer_fee_sats(utxos.len(), 2)
            .await?;
        let mut required_sats =
            amount_sats
                .checked_add(fee_sats)
                .ok_or_else(|| crate::Error::DumpToAddress {
                    message: format!(
                        "Insufficient balance: amount {amount_sats} plus fee {fee_sats} overflows"
                    ),
                })?;
        if total_in < required_sats {
            utxos = self
                .spendable_outputs_for_address(&sender_address, required_sats)
                .await?;
            total_in = sum_sats(
                utxos.iter().map(BitcoinSpendableOutput::value_sats),
                "bitcoin transfer input total",
            )?;
            fee_sats = self
                .estimate_p2wpkh_transfer_fee_sats(utxos.len(), 2)
                .await?;
            required_sats =
                amount_sats
                    .checked_add(fee_sats)
                    .ok_or_else(|| crate::Error::DumpToAddress {
                        message: format!(
                        "Insufficient balance: amount {amount_sats} plus fee {fee_sats} overflows"
                    ),
                    })?;
        }
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
            let psbt_input = bdk_wallet::bitcoin::psbt::Input {
                witness_utxo: Some(utxo.output),
                non_witness_utxo: Some(utxo.full_tx),
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
        // Generate a new private key
        let (secret_key, salt) = random_bitcoin_secret_key()?;
        let private_key = PrivateKey::new(secret_key, self.network);

        // Derive public key and address
        let compressed_pk = compressed_public_key_for(&private_key)?;
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
        let compressed_pk = compressed_public_key_for(&private_key)?;
        let address = Address::p2wpkh(&compressed_pk, self.network);

        debug!("Derived Bitcoin wallet: {}", address);

        Ok(Wallet::new(address.to_string(), private_key.to_wif()))
    }

    async fn get_tx_status(&self, tx_hash: &str) -> Result<TxStatus> {
        let txid = bitcoin::Txid::from_str(tx_hash).map_err(|e| {
            crate::Error::TransactionDeserializationFailed {
                context: format!("Failed to parse txid: {e}"),
                loc: location!(),
            }
        })?;
        let tx_verbose_result = self.rpc_client.get_raw_transaction_verbose(&txid).await;
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

        let confirmations = tx.confirmations.unwrap_or(0);
        if confirmations > 0 {
            let inclusion_height = bitcoin_inclusion_height(current_height, confirmations)?;
            Ok(TxStatus::Confirmed(ConfirmedTxStatus {
                confirmations,
                current_height,
                inclusion_height,
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
        let recipient_address = parse_bitcoin_address_for_network(recipient_address, self.network)?;

        // Determine the sender address from the provided private key and collect its UTXOs
        let sender_address =
            Address::p2wpkh(&compressed_public_key_for(&private_key)?, self.network);

        let fee_sats = u256_to_u64_sats("fee", fee)?;
        let minimum_sats = fee_sats
            .checked_add(1)
            .ok_or(crate::Error::NumericOverflow {
                context: "bitcoin dump minimum sats",
            })?;
        let utxos = self
            .spendable_outputs_for_address(&sender_address, minimum_sats)
            .await?;
        if utxos.is_empty() {
            return Err(crate::Error::DumpToAddress {
                message: "No UTXOs found".to_string(),
            });
        }
        let total_in = sum_sats(
            utxos.iter().map(BitcoinSpendableOutput::value_sats),
            "bitcoin dump input total",
        )?;
        if total_in < fee_sats {
            return Err(crate::Error::DumpToAddress {
                message: "Insufficient balance to cover fee".to_string(),
            });
        }
        // Calculate totals
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
            let psbt_input = bdk_wallet::bitcoin::psbt::Input {
                witness_utxo: Some(utxo.output),
                non_witness_utxo: Some(utxo.full_tx),
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

        let Some(output_index) = bitcoin_output_index(transfer_index) else {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        };
        let Some(output) = tx.output.get(output_index) else {
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
        parse_bitcoin_address_for_network(address, self.network).is_ok()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limited_body_chunk_rejects_oversized_append_without_mutating() {
        let mut body = b"abcd".to_vec();

        assert!(!append_limited_body_chunk(&mut body, b"ef", 5));
        assert_eq!(body, b"abcd");
    }

    #[test]
    fn limited_body_chunk_accepts_exact_limit() {
        let mut body = b"abcd".to_vec();

        assert!(append_limited_body_chunk(&mut body, b"ef", 6));
        assert_eq!(body, b"abcdef");
    }

    #[test]
    fn response_body_preview_marks_truncated_large_bodies() {
        let body = vec![b'a'; BITCOIN_RPC_ERROR_BODY_PREVIEW_BYTES + 1];

        let preview = response_body_preview(&body);

        assert_eq!(
            preview.len(),
            BITCOIN_RPC_ERROR_BODY_PREVIEW_BYTES + "...<truncated>".len()
        );
        assert!(preview.ends_with("...<truncated>"));
    }

    #[test]
    fn bitcoin_rpc_target_debug_redacts_url_credentials() {
        let url = Url::parse(
            "http://rpc-user:rpc-pass@bitcoin.example:18443/wallet/path-secret?token=query-secret#fragment-secret",
        )
        .expect("url");

        let redacted = redacted_url_for_debug(&url);

        assert_eq!(
            redacted,
            "http://bitcoin.example:18443/<redacted-path>?<redacted-query>#<redacted-fragment>"
        );
        assert!(!redacted.contains("rpc-user"));
        assert!(!redacted.contains("rpc-pass"));
        assert!(!redacted.contains("path-secret"));
        assert!(!redacted.contains("query-secret"));
        assert!(!redacted.contains("fragment-secret"));
    }

    #[test]
    fn bitcoin_fee_estimate_rejects_overflow() {
        let error = bitcoin_p2wpkh_transfer_fee_sats(u64::MAX, 1, 1).unwrap_err();

        assert!(matches!(
            error,
            crate::Error::NumericOverflow { context } if context == "bitcoin fee sats"
        ));
    }

    #[test]
    fn bitcoin_satoshi_sum_rejects_overflow() {
        let error = sum_sats([u64::MAX, 1], "test sats").unwrap_err();

        assert!(matches!(
            error,
            crate::Error::NumericOverflow { context } if context == "test sats"
        ));
    }

    #[test]
    fn bitcoin_fee_rate_conversion_rejects_invalid_rpc_values() {
        assert_eq!(fee_rate_btc_per_kb_to_sat_per_vb(0.00001).unwrap(), 1);
        assert_eq!(fee_rate_btc_per_kb_to_sat_per_vb(0.00001001).unwrap(), 2);

        assert!(fee_rate_btc_per_kb_to_sat_per_vb(0.0).is_err());
        assert!(fee_rate_btc_per_kb_to_sat_per_vb(f64::NAN).is_err());
        assert!(fee_rate_btc_per_kb_to_sat_per_vb(f64::INFINITY).is_err());
    }

    #[test]
    fn bitcoin_inclusion_height_uses_checked_chain_length_math() {
        assert_eq!(bitcoin_inclusion_height(100, 1).unwrap(), 100);
        assert_eq!(bitcoin_inclusion_height(100, 101).unwrap(), 0);
    }

    #[test]
    fn bitcoin_inclusion_height_rejects_impossible_confirmations() {
        let error = bitcoin_inclusion_height(100, 102).unwrap_err();

        assert!(matches!(
            error,
            crate::Error::NumericOverflow { context } if context == "bitcoin inclusion height"
        ));
    }

    #[test]
    fn bitcoin_output_index_rejects_values_outside_vout_range() {
        assert_eq!(bitcoin_output_index(0), Some(0));
        assert_eq!(
            bitcoin_output_index(u64::from(u32::MAX)),
            usize::try_from(u32::MAX).ok()
        );
        assert_eq!(bitcoin_output_index(u64::from(u32::MAX) + 1), None);
    }

    #[test]
    fn checked_bitcoin_address_accepts_configured_network() {
        let secret_key = SecretKey::from_slice(&[1_u8; 32]).expect("valid secret key");
        let private_key = PrivateKey::new(secret_key, Network::Regtest);
        let public_key = compressed_public_key_for(&private_key).expect("public key");
        let address = Address::p2wpkh(&public_key, Network::Regtest).to_string();

        assert!(parse_bitcoin_address_for_network(&address, Network::Regtest).is_ok());
    }

    #[test]
    fn checked_bitcoin_address_rejects_wrong_network() {
        let secret_key = SecretKey::from_slice(&[1_u8; 32]).expect("valid secret key");
        let private_key = PrivateKey::new(secret_key, Network::Bitcoin);
        let public_key = compressed_public_key_for(&private_key).expect("public key");
        let address = Address::p2wpkh(&public_key, Network::Bitcoin).to_string();

        let error =
            parse_bitcoin_address_for_network(&address, Network::Regtest).expect_err("network");

        assert!(
            error.to_string().contains("Invalid address format"),
            "{error}"
        );
    }
}
