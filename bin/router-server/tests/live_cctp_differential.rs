use std::{
    env,
    error::Error,
    fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy::{
    hex,
    network::{EthereumWallet, TransactionBuilder},
    primitives::{address, Address, Bytes, FixedBytes, B256, U256},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
    sol,
    sol_types::SolCall,
};
use router_server::{
    models::{MarketOrderKind, ProviderOperationStatus, ProviderOperationType},
    protocol::{AssetId, ChainId, DepositAsset},
    services::action_providers::{
        BridgeProvider, BridgeQuoteRequest, CctpProvider, ProviderOperationObservationRequest,
    },
};
use serde::Deserialize;
use serde_json::{json, Value};
use url::Url;
use uuid::Uuid;

mod support;
use support::{box_error, retry_rpc, wait_for_successful_receipt};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_PROVIDER_TESTS: &str = "LIVE_PROVIDER_TESTS";
const CCTP_API_URL: &str = "CCTP_API_URL";
const DEFAULT_CCTP_API_URL: &str = "https://iris-api.circle.com";
const CCTP_BASE_RPC_URL: &str = "CCTP_LIVE_BASE_RPC_URL";
const CCTP_ARBITRUM_RPC_URL: &str = "CCTP_LIVE_ARBITRUM_RPC_URL";
const CCTP_PRIVATE_KEY: &str = "CCTP_LIVE_PRIVATE_KEY";
const CCTP_AMOUNT_RAW: &str = "CCTP_LIVE_USDC_AMOUNT_RAW";
const CCTP_BURN_TX_HASH: &str = "CCTP_LIVE_BURN_TX_HASH";
const CCTP_RECEIVE_TX_HASH: &str = "CCTP_LIVE_RECEIVE_TX_HASH";
const CCTP_SPEND_CONFIRMATION: &str = "CCTP_LIVE_I_UNDERSTAND_THIS_BRIDGES_REAL_FUNDS";
const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const LIVE_RECOVERY_DIR: &str = "ROUTER_LIVE_RECOVERY_DIR";

const DEFAULT_AMOUNT_RAW: &str = "100000";
const BASE_CHAIN_ID: u64 = 8453;
const ARBITRUM_CHAIN_ID: u64 = 42161;
const BASE_DOMAIN: u32 = 6;
const ARBITRUM_DOMAIN: u32 = 3;
const STANDARD_FINALITY_THRESHOLD: u32 = 2_000;
const ATTESTATION_POLL_INTERVAL: Duration = Duration::from_secs(15);
const DEFAULT_ATTESTATION_TIMEOUT_SECS: u64 = 2 * 60 * 60;

const BASE_USDC: Address = address!("833589fcd6edb6e08f4c7c32d4f71b54bda02913");
const ARBITRUM_USDC: Address = address!("af88d065e77c8cc2239327c5edb3a432268e5831");
const TOKEN_MESSENGER_V2: Address = address!("28b5a0e9c621a5badaa536219b3a228c8168cf5d");
const MESSAGE_TRANSMITTER_V2: Address = address!("81d40f21f12a8f0e3252bccb954d722d4c464b64");

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 amount) external returns (bool);
        function balanceOf(address account) external view returns (uint256);
    }
}

sol! {
    interface ICircleTokenMessengerV2 {
        function depositForBurn(
            uint256 amount,
            uint32 destinationDomain,
            bytes32 mintRecipient,
            address burnToken,
            bytes32 destinationCaller,
            uint256 maxFee,
            uint32 minFinalityThreshold
        ) external returns (uint64);
    }
}

sol! {
    interface ICircleMessageTransmitterV2 {
        function receiveMessage(bytes calldata message, bytes calldata attestation) external returns (bool);
    }
}

#[tokio::test]
#[ignore = "live CCTP quote and Iris observer test; run with LIVE_PROVIDER_TESTS=1"]
async fn live_cctp_base_usdc_to_arbitrum_usdc_quote_and_observer_transcript() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }

    let amount = configured_amount()?;
    let iris_base_url = live_cctp_base_url()?.to_string();
    let provider = live_cctp_provider()?;
    let quote = provider
        .quote_bridge(BridgeQuoteRequest {
            source_asset: base_usdc_asset(),
            destination_asset: arbitrum_usdc_asset(),
            order_kind: MarketOrderKind::ExactIn {
                amount_in: amount.to_string(),
                min_amount_out: "1".to_string(),
            },
            recipient_address: "0x0000000000000000000000000000000000000001".to_string(),
            depositor_address: "0x0000000000000000000000000000000000000002".to_string(),
            partial_fills_enabled: false,
        })
        .await
        .map_err(|error| format!("CCTP quote failed: {error}"))?
        .ok_or("CCTP quote returned None")?;

    assert_eq!(quote.provider_id, "cctp");
    assert_eq!(quote.amount_in, amount.to_string());
    assert_eq!(quote.amount_out, amount.to_string());
    assert_eq!(quote.provider_quote["source_domain"], json!(BASE_DOMAIN));
    assert_eq!(
        quote.provider_quote["destination_domain"],
        json!(ARBITRUM_DOMAIN)
    );
    assert_eq!(quote.provider_quote["max_fee"], json!("0"));

    let mut observation = Value::Null;
    if let Some(tx_hash) = env_var_any(&[CCTP_BURN_TX_HASH]) {
        let request = ProviderOperationObservationRequest {
            operation_id: Uuid::now_v7(),
            operation_type: ProviderOperationType::CctpBridge,
            provider_ref: Some(tx_hash.clone()),
            request: json!({
                "source_domain": BASE_DOMAIN,
                "transition_decl_id": "live-cctp-transcript",
            }),
            response: json!({}),
            observed_state: json!({
                "burn_tx_hash": tx_hash,
            }),
            hint_evidence: json!({}),
        };
        let observed = provider
            .observe_bridge_operation(request)
            .await
            .map_err(|error| format!("CCTP observe failed: {error}"))?
            .ok_or("CCTP observe returned None")?;
        assert!(
            matches!(
                observed.status,
                ProviderOperationStatus::WaitingExternal | ProviderOperationStatus::Completed
            ),
            "unexpected CCTP observation status: {:?}",
            observed.status
        );
        observation = json!({
            "status": observed.status.to_db_string(),
            "provider_ref": observed.provider_ref,
            "observed_state": observed.observed_state,
            "response": observed.response,
        });
    }
    let iris_not_found_probe_tx = format!("0x{}", "00".repeat(32));
    let iris_not_found_probe = IrisClient::new(live_cctp_base_url()?)
        .message(BASE_DOMAIN, &iris_not_found_probe_tx)
        .await?;
    assert!(
        iris_not_found_probe.is_none(),
        "zero-hash Iris probe unexpectedly returned a complete message"
    );

    emit_transcript(
        "cctp.base_usdc_arbitrum_usdc.quote_and_observe",
        json!({
            "iris_base_url": iris_base_url,
            "request": {
                "source_domain": BASE_DOMAIN,
                "destination_domain": ARBITRUM_DOMAIN,
                "source_token": format!("{BASE_USDC:#x}"),
                "destination_token": format!("{ARBITRUM_USDC:#x}"),
                "amount_raw": amount.to_string(),
            },
            "typed_quote": {
                "provider_id": quote.provider_id,
                "amount_in": quote.amount_in,
                "amount_out": quote.amount_out,
                "expires_at": quote.expires_at.to_rfc3339(),
                "provider_quote": quote.provider_quote,
            },
            "optional_observation": observation,
            "iris_not_found_probe": {
                "source_domain": BASE_DOMAIN,
                "transaction_hash": iris_not_found_probe_tx,
                "result": "not_found",
            },
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "SPENDS REAL FUNDS by bridging Base USDC to Arbitrum USDC through live CCTP"]
async fn live_cctp_base_usdc_to_arbitrum_usdc_bridge_transcript_spends_funds() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        CCTP_SPEND_CONFIRMATION,
        "YES",
        "refusing to submit a live CCTP bridge",
    )?;

    let signer = required_private_key()?.parse::<PrivateKeySigner>()?;
    let user = signer.address();
    let wallet = EthereumWallet::new(signer);
    let base = ProviderBuilder::new()
        .wallet(wallet.clone())
        .connect_http(live_base_rpc_url()?);
    let arbitrum = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(live_arbitrum_rpc_url()?);
    assert_chain_id(&base, BASE_CHAIN_ID, "Base").await?;
    assert_chain_id(&arbitrum, ARBITRUM_CHAIN_ID, "Arbitrum").await?;

    let amount = configured_amount()?;
    let resume_burn_hash = env_var_any(&[CCTP_BURN_TX_HASH]);
    let resume_receive_hash = env_var_any(&[CCTP_RECEIVE_TX_HASH]);
    let iris_base_url = live_cctp_base_url()?.to_string();
    let base_usdc_before = erc20_balance(&base, BASE_USDC, user).await?;
    let arbitrum_usdc_before = erc20_balance(&arbitrum, ARBITRUM_USDC, user).await?;
    if resume_burn_hash.is_none() && base_usdc_before < amount {
        return Err(format!(
            "insufficient Base USDC for CCTP live bridge: balance={}, amount={}",
            base_usdc_before, amount
        )
        .into());
    }

    let (approve_hash, burn_hash, submitted_burn) = if let Some(burn_hash) = resume_burn_hash {
        eprintln!("resuming CCTP depositForBurn transaction {burn_hash}");
        (None, burn_hash, false)
    } else {
        let approve_data = IERC20::approveCall {
            spender: TOKEN_MESSENGER_V2,
            amount,
        }
        .abi_encode();
        let approve_hash = send_evm_transaction(
            &base,
            BASE_USDC,
            U256::ZERO,
            Bytes::from(approve_data),
            "CCTP approve",
        )
        .await?;

        let burn_call = ICircleTokenMessengerV2::depositForBurnCall {
            amount,
            destinationDomain: ARBITRUM_DOMAIN,
            mintRecipient: evm_address_to_bytes32(user),
            burnToken: BASE_USDC,
            destinationCaller: FixedBytes::ZERO,
            maxFee: U256::ZERO,
            minFinalityThreshold: STANDARD_FINALITY_THRESHOLD,
        };
        let burn_hash = send_evm_transaction(
            &base,
            TOKEN_MESSENGER_V2,
            U256::ZERO,
            Bytes::from(burn_call.abi_encode()),
            "CCTP depositForBurn",
        )
        .await?;
        (Some(approve_hash), burn_hash, true)
    };

    let iris = IrisClient::new(live_cctp_base_url()?);
    let attested = iris
        .wait_for_complete_message(BASE_DOMAIN, &burn_hash)
        .await?;
    let (receive_hash, submitted_receive) = if let Some(receive_hash) = resume_receive_hash {
        eprintln!("resuming CCTP receiveMessage transaction {receive_hash}");
        let parsed_hash = receive_hash.parse::<B256>()?;
        wait_for_successful_receipt(&arbitrum, parsed_hash, "CCTP receiveMessage").await?;
        (receive_hash, false)
    } else {
        let receive_call = ICircleMessageTransmitterV2::receiveMessageCall {
            message: hex_bytes(&attested.message)?,
            attestation: hex_bytes(&attested.attestation)?,
        };
        let receive_hash = send_evm_transaction(
            &arbitrum,
            MESSAGE_TRANSMITTER_V2,
            U256::ZERO,
            Bytes::from(receive_call.abi_encode()),
            "CCTP receiveMessage",
        )
        .await?;
        (receive_hash, true)
    };

    let base_usdc_after = erc20_balance(&base, BASE_USDC, user).await?;
    let arbitrum_usdc_after = erc20_balance(&arbitrum, ARBITRUM_USDC, user).await?;
    if submitted_burn {
        assert!(
            base_usdc_after <= base_usdc_before.saturating_sub(amount),
            "Base USDC was not burned/transferred as expected"
        );
    }
    if submitted_receive {
        assert!(
            arbitrum_usdc_after >= arbitrum_usdc_before.saturating_add(amount),
            "Arbitrum USDC did not increase by bridged amount"
        );
    }

    emit_transcript(
        "cctp.base_usdc_to_arbitrum_usdc.lifecycle",
        json!({
            "iris_base_url": iris_base_url,
            "source": {
                "chain_id": BASE_CHAIN_ID,
                "domain": BASE_DOMAIN,
                "token": format!("{BASE_USDC:#x}"),
            },
            "destination": {
                "chain_id": ARBITRUM_CHAIN_ID,
                "domain": ARBITRUM_DOMAIN,
                "token": format!("{ARBITRUM_USDC:#x}"),
            },
            "user": format!("{user:#x}"),
            "amount_raw": amount.to_string(),
            "transactions": {
                "approve": approve_hash,
                "burn": burn_hash,
                "receive": receive_hash,
            },
            "iris_message": attested.raw,
            "balances": {
                "base_usdc_before_raw": base_usdc_before.to_string(),
                "base_usdc_after_raw": base_usdc_after.to_string(),
                "arbitrum_usdc_before_raw": arbitrum_usdc_before.to_string(),
                "arbitrum_usdc_after_raw": arbitrum_usdc_after.to_string(),
            },
        }),
    );

    Ok(())
}

#[derive(Clone)]
struct IrisClient {
    http: reqwest::Client,
    base_url: Url,
}

impl IrisClient {
    fn new(base_url: Url) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url,
        }
    }

    async fn wait_for_complete_message(
        &self,
        source_domain: u32,
        burn_tx_hash: &str,
    ) -> TestResult<CompleteIrisMessage> {
        let deadline = tokio::time::Instant::now()
            + Duration::from_secs(
                env::var("CCTP_LIVE_ATTESTATION_TIMEOUT_SECS")
                    .ok()
                    .and_then(|raw| raw.parse().ok())
                    .unwrap_or(DEFAULT_ATTESTATION_TIMEOUT_SECS),
            );
        loop {
            if let Some(message) = self.message(source_domain, burn_tx_hash).await? {
                return Ok(message);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "CCTP attestation was not complete before timeout for burn tx {burn_tx_hash}"
                )
                .into());
            }
            tokio::time::sleep(ATTESTATION_POLL_INTERVAL).await;
        }
    }

    async fn message(
        &self,
        source_domain: u32,
        burn_tx_hash: &str,
    ) -> TestResult<Option<CompleteIrisMessage>> {
        let mut url = self.base_url.clone();
        url.set_path(&format!("/v2/messages/{source_domain}"));
        url.query_pairs_mut()
            .append_pair("transactionHash", burn_tx_hash);
        let response = self.http.get(url).send().await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(format!("Iris /v2/messages failed with {status}: {body}").into());
        }
        let parsed: IrisMessagesResponse = serde_json::from_str(&body)?;
        let raw: Value = serde_json::from_str(&body)?;
        Ok(parsed
            .messages
            .into_iter()
            .find(|message| {
                message.status.eq_ignore_ascii_case("complete")
                    && message
                        .message
                        .as_deref()
                        .is_some_and(|value| !value.is_empty())
                    && message
                        .attestation
                        .as_deref()
                        .is_some_and(|value| !value.is_empty())
            })
            .map(|message| CompleteIrisMessage {
                message: message.message.expect("checked message"),
                attestation: message.attestation.expect("checked attestation"),
                raw,
            }))
    }
}

#[derive(Debug, Deserialize)]
struct IrisMessagesResponse {
    #[serde(default)]
    messages: Vec<IrisMessage>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IrisMessage {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    attestation: Option<String>,
    #[serde(default)]
    status: String,
}

struct CompleteIrisMessage {
    message: String,
    attestation: String,
    raw: Value,
}

fn live_cctp_provider() -> TestResult<CctpProvider> {
    CctpProvider::new(
        live_cctp_base_url()?.to_string(),
        format!("{TOKEN_MESSENGER_V2:#x}"),
        format!("{MESSAGE_TRANSMITTER_V2:#x}"),
        std::sync::Arc::new(router_server::services::AssetRegistry::default()),
    )
    .map_err(|error| format!("build CCTP provider: {error}").into())
}

fn base_usdc_asset() -> DepositAsset {
    DepositAsset {
        chain: ChainId::parse("evm:8453").expect("static Base chain id"),
        asset: AssetId::reference(format!("{BASE_USDC:#x}")),
    }
}

fn arbitrum_usdc_asset() -> DepositAsset {
    DepositAsset {
        chain: ChainId::parse("evm:42161").expect("static Arbitrum chain id"),
        asset: AssetId::reference(format!("{ARBITRUM_USDC:#x}")),
    }
}

async fn assert_chain_id<P>(provider: &P, expected: u64, label: &str) -> TestResult<()>
where
    P: Provider,
{
    let actual = retry_rpc(&format!("{label} chain id"), || async {
        provider.get_chain_id().await.map_err(box_error)
    })
    .await?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{label} RPC is on chain {actual}, expected {expected}").into())
    }
}

async fn erc20_balance<P>(provider: &P, token: Address, owner: Address) -> TestResult<U256>
where
    P: Provider,
{
    let token = IERC20::new(token, provider);
    retry_rpc("CCTP ERC20 balanceOf", || async {
        token.balanceOf(owner).call().await.map_err(box_error)
    })
    .await
}

async fn send_evm_transaction<P>(
    provider: &P,
    to: Address,
    value: U256,
    data: Bytes,
    label: &str,
) -> TestResult<String>
where
    P: Provider,
{
    let tx = TransactionRequest::default()
        .with_to(to)
        .with_value(value)
        .with_input(data);
    let pending = provider.send_transaction(tx).await?;
    let tx_hash = *pending.tx_hash();
    eprintln!("submitted {label} transaction {tx_hash}");
    wait_for_successful_receipt(provider, tx_hash, label).await?;
    Ok(format!("{tx_hash:#x}"))
}

fn evm_address_to_bytes32(address: Address) -> FixedBytes<32> {
    let mut bytes = [0_u8; 32];
    bytes[12..].copy_from_slice(address.as_slice());
    FixedBytes::from(bytes)
}

fn hex_bytes(value: &str) -> TestResult<Bytes> {
    let trimmed = value.trim();
    let raw = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    Ok(Bytes::from(hex::decode(raw)?))
}

fn configured_amount() -> TestResult<U256> {
    U256::from_str_radix(
        &env::var(CCTP_AMOUNT_RAW).unwrap_or_else(|_| DEFAULT_AMOUNT_RAW.to_string()),
        10,
    )
    .map_err(|error| format!("invalid {CCTP_AMOUNT_RAW}: {error}").into())
}

fn live_base_rpc_url() -> TestResult<Url> {
    env_var_any(&[
        CCTP_BASE_RPC_URL,
        "ROUTER_LIVE_BASE_RPC_URL",
        "BASE_RPC_URL",
    ])
    .ok_or_else(|| {
        format!(
            "missing required env var {}, ROUTER_LIVE_BASE_RPC_URL, or BASE_RPC_URL",
            CCTP_BASE_RPC_URL
        )
        .into()
    })
    .and_then(|raw| raw.parse::<Url>().map_err(|error| error.into()))
}

fn live_arbitrum_rpc_url() -> TestResult<Url> {
    env_var_any(&[
        CCTP_ARBITRUM_RPC_URL,
        "ROUTER_LIVE_ARBITRUM_RPC_URL",
        "ARBITRUM_RPC_URL",
    ])
    .ok_or_else(|| {
        format!(
            "missing required env var {}, ROUTER_LIVE_ARBITRUM_RPC_URL, or ARBITRUM_RPC_URL",
            CCTP_ARBITRUM_RPC_URL
        )
        .into()
    })
    .and_then(|raw| raw.parse::<Url>().map_err(|error| error.into()))
}

fn live_cctp_base_url() -> TestResult<Url> {
    env_var_any(&[CCTP_API_URL])
        .unwrap_or_else(|| DEFAULT_CCTP_API_URL.to_string())
        .parse::<Url>()
        .map_err(|error| format!("invalid {CCTP_API_URL}: {error}").into())
}

fn live_enabled() -> bool {
    if env::var(LIVE_PROVIDER_TESTS).as_deref() == Ok("1") {
        true
    } else {
        eprintln!("skipping live provider test; set {LIVE_PROVIDER_TESTS}=1 to enable");
        false
    }
}

fn required_private_key() -> TestResult<String> {
    let value = env_var_any(&[
        CCTP_PRIVATE_KEY,
        "ROUTER_LIVE_BASE_PRIVATE_KEY",
        "BASE_LIVE_PRIVATE_KEY",
        "ROUTER_LIVE_EVM_PRIVATE_KEY",
        LIVE_TEST_PRIVATE_KEY,
    ])
    .ok_or_else(|| {
        format!(
            "missing required env var {}, ROUTER_LIVE_BASE_PRIVATE_KEY, BASE_LIVE_PRIVATE_KEY, ROUTER_LIVE_EVM_PRIVATE_KEY, or {}",
            CCTP_PRIVATE_KEY, LIVE_TEST_PRIVATE_KEY
        )
    })?;
    assert_hex_private_key(&value)?;
    Ok(value)
}

fn require_confirmation(key: &str, expected: &str, context: &str) -> TestResult<()> {
    let actual = env::var(key).map_err(|_| format!("missing required env var {key}"))?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{context}; expected {key}={expected}, got {actual:?}").into())
    }
}

fn env_var_any(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn assert_hex_private_key(value: &str) -> TestResult<()> {
    let trimmed = value.strip_prefix("0x").unwrap_or(value);
    if trimmed.len() != 64 || !trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err("live EVM private key must be a 32-byte hex string".into());
    }
    Ok(())
}

fn emit_transcript(label: &str, value: Value) {
    println!(
        "LIVE_PROVIDER_TRANSCRIPT {label}\n{}",
        serde_json::to_string_pretty(&value).expect("transcript serializes")
    );
    if let Err(error) = write_transcript_artifact(label, &value) {
        eprintln!("failed to write live provider transcript artifact: {error}");
    }
}

fn write_transcript_artifact(label: &str, value: &Value) -> TestResult<()> {
    let created_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock before unix epoch: {error}"))?
        .as_millis();
    let dir = env::var(LIVE_RECOVERY_DIR)
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/live-recovery")
        })
        .join("provider-transcripts");
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!(
        "{}-{}.json",
        created_at_ms,
        sanitize_artifact_label(label)
    ));
    let payload = json!({
        "schema_version": 1,
        "kind": "live_provider_transcript",
        "provider": "cctp",
        "label": label,
        "created_at_ms": created_at_ms,
        "private_key_env_candidates": [
            CCTP_PRIVATE_KEY,
            "ROUTER_LIVE_BASE_PRIVATE_KEY",
            "BASE_LIVE_PRIVATE_KEY",
            "ROUTER_LIVE_EVM_PRIVATE_KEY",
            LIVE_TEST_PRIVATE_KEY
        ],
        "transcript": value,
    });
    fs::write(&path, serde_json::to_vec_pretty(&payload)?)?;
    eprintln!(
        "live provider transcript artifact written to {}",
        path.display()
    );
    Ok(())
}

fn sanitize_artifact_label(label: &str) -> String {
    label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}
