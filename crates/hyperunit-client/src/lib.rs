use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::Snafu;
use std::{collections::BTreeSet, fmt};
use url::Url;

pub type HyperUnitResult<T> = Result<T, HyperUnitClientError>;

#[derive(Debug, Snafu)]
pub enum HyperUnitClientError {
    #[snafu(display("failed to join base URL {base_url} with path {path}: {source}"))]
    InvalidUrl {
        base_url: String,
        path: String,
        source: url::ParseError,
    },
    #[snafu(display("invalid HyperUnit proxy URL {proxy_url}: {source}"))]
    InvalidProxyUrl {
        proxy_url: String,
        source: url::ParseError,
    },
    #[snafu(display(
        "unsupported HyperUnit proxy scheme {scheme:?} for {proxy_url}; expected socks5"
    ))]
    UnsupportedProxyScheme { proxy_url: String, scheme: String },
    #[snafu(display("failed to configure HyperUnit proxy {proxy_url}: {source}"))]
    ProxyConfiguration {
        proxy_url: String,
        source: reqwest::Error,
    },
    #[snafu(display("failed to build HyperUnit HTTP client: {source}"))]
    HttpClientBuild { source: reqwest::Error },
    #[snafu(display("HTTP request to HyperUnit failed: {source}"))]
    Http { source: reqwest::Error },
    #[snafu(display("HyperUnit returned HTTP {status}: {body}"))]
    HttpStatus { status: u16, body: String },
    #[snafu(display("HyperUnit returned an invalid JSON body: {source}; body={body}"))]
    InvalidBody {
        source: serde_json::Error,
        body: String,
    },
}

impl From<reqwest::Error> for HyperUnitClientError {
    fn from(source: reqwest::Error) -> Self {
        Self::Http { source }
    }
}

/// Chains recognised by HyperUnit's REST API.
///
/// The wire representation is the lowercase chain name used in `/gen` path
/// segments. `#[non_exhaustive]` leaves room to add Solana, Plasma, Base, or
/// Avalanche without a breaking change.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnitChain {
    Bitcoin,
    Ethereum,
    Base,
    Hyperliquid,
}

impl UnitChain {
    #[must_use]
    pub const fn as_wire_str(self) -> &'static str {
        match self {
            Self::Bitcoin => "bitcoin",
            Self::Ethereum => "ethereum",
            Self::Base => "base",
            Self::Hyperliquid => "hyperliquid",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "bitcoin" => Some(Self::Bitcoin),
            "ethereum" => Some(Self::Ethereum),
            "base" => Some(Self::Base),
            "hyperliquid" => Some(Self::Hyperliquid),
            _ => None,
        }
    }
}

impl fmt::Display for UnitChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_wire_str())
    }
}

/// Assets recognised by HyperUnit's REST API.
///
/// Wire representation matches the lowercase symbol in `/gen` path segments.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnitAsset {
    Btc,
    Eth,
}

impl UnitAsset {
    #[must_use]
    pub const fn as_wire_str(self) -> &'static str {
        match self {
            Self::Btc => "btc",
            Self::Eth => "eth",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "btc" => Some(Self::Btc),
            "eth" => Some(Self::Eth),
            _ => None,
        }
    }
}

impl fmt::Display for UnitAsset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_wire_str())
    }
}

/// Path parameters for `GET /gen/{src_chain}/{dst_chain}/{asset}/{dst_addr}`.
///
/// Mirrors HyperUnit's public HTTP API. The mock server in
/// `crates/devnet/src/mock_integrators.rs` must serve this exact shape so the
/// same client code exercises both production and integration-test paths.
#[derive(Debug, Clone)]
pub struct UnitGenerateAddressRequest {
    pub src_chain: UnitChain,
    pub dst_chain: UnitChain,
    pub asset: UnitAsset,
    pub dst_addr: String,
}

impl UnitGenerateAddressRequest {
    #[must_use]
    pub fn into_path(&self) -> String {
        format!(
            "/gen/{src}/{dst}/{asset}/{dst_addr}",
            src = self.src_chain.as_wire_str(),
            dst = self.dst_chain.as_wire_str(),
            asset = self.asset.as_wire_str(),
            dst_addr = sanitize_path_segment(&self.dst_addr),
        )
    }
}

/// Response body from `GET /gen/...`.
///
/// `signatures` is kept as a free-form JSON value because HyperUnit may add or
/// rename guardian node keys (currently `field-node`, `hl-node`/`unit-node`,
/// `node-1`) without warning. Callers that need to verify the guardian
/// signatures can parse it themselves; the router trusts the TLS cert for now.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnitGenerateAddressResponse {
    pub address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default)]
    pub signatures: Value,
}

/// Path parameters for `GET /operations/{address}`.
#[derive(Debug, Clone)]
pub struct UnitOperationsRequest {
    pub address: String,
}

impl UnitOperationsRequest {
    #[must_use]
    pub fn into_path(&self) -> String {
        format!(
            "/operations/{address}",
            address = sanitize_path_segment(&self.address)
        )
    }
}

/// Response body from `GET /operations/{address}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct UnitOperationsResponse {
    #[serde(default)]
    pub addresses: Vec<Value>,
    #[serde(default)]
    pub operations: Vec<UnitOperation>,
}

/// Individual operation entry returned by `GET /operations/...`.
///
/// Every field is optional because HyperUnit populates them progressively as
/// the operation advances through its lifecycle. The router resolves
/// completion from `state`; callers may also choose to treat a populated
/// destination transaction hash as sufficient broadcast evidence.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UnitOperation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_chain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_chain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_fee_amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sweep_fee_amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_tx_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_tx_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_tx_confirmations: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_tx_confirmations: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_in_withdraw_queue: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broadcast_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asset: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_updated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_next_attempt_at: Option<String>,
}

impl UnitOperation {
    /// True when the operation's `protocol_address` matches the supplied address
    /// case-insensitively. Returns false for operations with no protocol address.
    #[must_use]
    pub fn matches_protocol_address(&self, protocol_address: &str) -> bool {
        self.protocol_address
            .as_deref()
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(protocol_address))
    }

    /// Classify this operation's lifecycle state. Unknown or absent states
    /// resolve to `UnitOperationState::Unknown`.
    #[must_use]
    pub fn classified_state(&self) -> UnitOperationState {
        match self.state.as_deref() {
            Some(raw) => UnitOperationState::parse(raw),
            None => UnitOperationState::Unknown,
        }
    }

    #[must_use]
    pub fn fingerprints(&self) -> Vec<String> {
        let mut fingerprints = Vec::new();
        push_operation_fingerprint(
            &mut fingerprints,
            "operationId",
            self.operation_id.as_deref(),
        );
        push_operation_fingerprint(
            &mut fingerprints,
            "sourceTxHash",
            self.source_tx_hash.as_deref(),
        );
        push_operation_fingerprint(
            &mut fingerprints,
            "destinationTxHash",
            self.destination_tx_hash.as_deref(),
        );
        push_operation_fingerprint(
            &mut fingerprints,
            "opCreatedAt",
            self.op_created_at.as_deref(),
        );
        fingerprints
    }

    #[must_use]
    pub fn has_seen_fingerprint(&self, seen_operations: &BTreeSet<String>) -> bool {
        self.fingerprints()
            .iter()
            .any(|fingerprint| seen_operations.contains(fingerprint))
    }
}

fn push_operation_fingerprint(fingerprints: &mut Vec<String>, field: &str, value: Option<&str>) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    fingerprints.push(format!("{field}:{value}"));
}

/// Normalized operation lifecycle state.
///
/// HyperUnit's raw state strings mix camelCase and lowercase (`done`, `failure`,
/// `sourceTxDiscovered`, ...). This enum matches case-insensitively and preserves
/// the original string on `Unknown` so operators can observe unexpected states
/// without code changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitOperationState {
    SourceTxDiscovered,
    WaitForSrcTxFinalization,
    BuildingDstTx,
    AdditionalChecks,
    SignTx,
    BroadcastTx,
    WaitForDstTxFinalization,
    ReadyForWithdrawQueue,
    QueuedForWithdraw,
    Done,
    Failure,
    Unknown,
}

impl UnitOperationState {
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        let lower = raw.to_ascii_lowercase();
        match lower.as_str() {
            "sourcetxdiscovered" => Self::SourceTxDiscovered,
            "waitforsrctxfinalization" => Self::WaitForSrcTxFinalization,
            "buildingdsttx" => Self::BuildingDstTx,
            "additionalchecks" => Self::AdditionalChecks,
            "signtx" => Self::SignTx,
            "broadcasttx" => Self::BroadcastTx,
            "waitfordsttxfinalization" => Self::WaitForDstTxFinalization,
            "readyforwithdrawqueue" => Self::ReadyForWithdrawQueue,
            "queuedforwithdraw" => Self::QueuedForWithdraw,
            "done" => Self::Done,
            "failure" => Self::Failure,
            _ => Self::Unknown,
        }
    }

    #[must_use]
    pub const fn is_terminal_success(&self) -> bool {
        matches!(self, Self::Done)
    }

    #[must_use]
    pub const fn is_terminal_failure(&self) -> bool {
        matches!(self, Self::Failure)
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.is_terminal_success() || self.is_terminal_failure()
    }
}

#[derive(Debug, Clone)]
pub struct HyperUnitClient {
    http: reqwest::Client,
    base_url: String,
}

impl HyperUnitClient {
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::builder()
                .use_rustls_tls()
                .build()
                .expect("hyperunit rustls client should build"),
            base_url: normalize_base_url(base_url),
        }
    }

    pub fn new_with_proxy_url(
        base_url: impl Into<String>,
        proxy_url: Option<String>,
    ) -> HyperUnitResult<Self> {
        let base_url = normalize_base_url(base_url);
        // Pin this client to Rustls + the vendored Mozilla root set from
        // `webpki-roots` so proxy and direct connections share one
        // deterministic trust boundary instead of inheriting host OS CAs.
        let mut builder = reqwest::Client::builder().use_rustls_tls();
        if let Some(proxy_url) = normalize_proxy_url(proxy_url)? {
            let proxy = reqwest::Proxy::all(&proxy_url).map_err(|source| {
                HyperUnitClientError::ProxyConfiguration {
                    proxy_url: proxy_url.clone(),
                    source,
                }
            })?;
            builder = builder.proxy(proxy);
        }
        let http = builder
            .build()
            .map_err(|source| HyperUnitClientError::HttpClientBuild { source })?;
        Ok(Self { http, base_url })
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn generate_address(
        &self,
        request: UnitGenerateAddressRequest,
    ) -> HyperUnitResult<UnitGenerateAddressResponse> {
        let path = request.into_path();
        self.get_json(&path).await
    }

    pub async fn operations(
        &self,
        request: UnitOperationsRequest,
    ) -> HyperUnitResult<UnitOperationsResponse> {
        let path = request.into_path();
        self.get_json(&path).await
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> HyperUnitResult<T> {
        let endpoint = build_endpoint(&self.base_url, path)?;
        let response = self.http.get(endpoint).send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(HyperUnitClientError::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }
        serde_json::from_str(&body)
            .map_err(|source| HyperUnitClientError::InvalidBody { source, body })
    }
}

fn normalize_base_url(base_url: impl Into<String>) -> String {
    base_url.into().trim_end_matches('/').to_string()
}

fn normalize_proxy_url(proxy_url: Option<String>) -> HyperUnitResult<Option<String>> {
    let Some(proxy_url) = proxy_url else {
        return Ok(None);
    };
    let trimmed = proxy_url.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let parsed = Url::parse(trimmed).map_err(|source| HyperUnitClientError::InvalidProxyUrl {
        proxy_url: trimmed.to_string(),
        source,
    })?;
    match parsed.scheme() {
        "socks5" => Ok(Some(trimmed.to_string())),
        scheme => Err(HyperUnitClientError::UnsupportedProxyScheme {
            proxy_url: trimmed.to_string(),
            scheme: scheme.to_string(),
        }),
    }
}

fn build_endpoint(base_url: &str, path: &str) -> HyperUnitResult<Url> {
    Url::parse(&format!("{base_url}{path}")).map_err(|source| HyperUnitClientError::InvalidUrl {
        base_url: base_url.to_string(),
        path: path.to_string(),
        source,
    })
}

/// Strip characters that would break a path segment (slashes, whitespace).
///
/// Addresses are hex / bech32 strings so this is a lightweight guard; malicious
/// input never reaches the router because `dst_addr` is always derived from
/// validated upstream data, but defense-in-depth is cheap here.
fn sanitize_path_segment(raw: &str) -> String {
    raw.chars()
        .filter(|c| !c.is_whitespace() && *c != '/' && *c != '?' && *c != '#')
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn generate_address_request_builds_path() {
        let request = UnitGenerateAddressRequest {
            src_chain: UnitChain::Ethereum,
            dst_chain: UnitChain::Hyperliquid,
            asset: UnitAsset::Eth,
            dst_addr: "0xabcDEF1234567890AbCdef1234567890aBCDef12".to_string(),
        };
        assert_eq!(
            request.into_path(),
            "/gen/ethereum/hyperliquid/eth/0xabcDEF1234567890AbCdef1234567890aBCDef12"
        );
    }

    #[test]
    fn generate_address_request_builds_path_for_bitcoin() {
        let request = UnitGenerateAddressRequest {
            src_chain: UnitChain::Bitcoin,
            dst_chain: UnitChain::Hyperliquid,
            asset: UnitAsset::Btc,
            dst_addr: "0x1111111111111111111111111111111111111111".to_string(),
        };
        assert_eq!(
            request.into_path(),
            "/gen/bitcoin/hyperliquid/btc/0x1111111111111111111111111111111111111111"
        );
    }

    #[test]
    fn generate_address_request_strips_path_breakers() {
        let request = UnitGenerateAddressRequest {
            src_chain: UnitChain::Ethereum,
            dst_chain: UnitChain::Hyperliquid,
            asset: UnitAsset::Eth,
            dst_addr: "0xabc /def#frag?q=1\n".to_string(),
        };
        assert_eq!(
            request.into_path(),
            "/gen/ethereum/hyperliquid/eth/0xabcdeffragq=1"
        );
    }

    #[test]
    fn operations_request_builds_path() {
        let request = UnitOperationsRequest {
            address: "tb1pwv4p3sgpy8g323mxsa5z4dnm0qnzlqcxk70gwdqvrmaf92psselsxcwf75".to_string(),
        };
        assert_eq!(
            request.into_path(),
            "/operations/tb1pwv4p3sgpy8g323mxsa5z4dnm0qnzlqcxk70gwdqvrmaf92psselsxcwf75"
        );
    }

    #[test]
    fn generate_address_response_deserializes_real_shape() {
        // Sample payload derived verbatim from HyperUnit docs at
        // docs.hyperunit.xyz/developers/api/generate-address.
        let body = json!({
            "address": "tb1pwv4p3sgpy8g323mxsa5z4dnm0qnzlqcxk70gwdqvrmaf92psselsxcwf75",
            "signatures": {
                "field-node": "6FdyoeFFKkzT1fylI5xmMNSLMPC3aNICBL0Nj6KokYYpA9dqEpjdLoKBw4uGPDIdZEARc1QagKIirNzqoW5cNw==",
                "hl-node": "gkMJhxswzNqqMBx03wJy5zzHIcXyTrzfgb077F2CC4kDSyXqMPKE/HQOvaLpgMC21v3Fb/G8ujAuXafBIz2tVg==",
                "node-1": "6AbTFibJ7BQEXGFXRP5lROQ6w2aqVMDyy1xMLJL0mnyB1MeIPFIEPKbHKtAbsAdB37NhxSeL9iMKUkM5XnUnFg=="
            },
            "status": "OK"
        });
        let response: UnitGenerateAddressResponse = serde_json::from_value(body).unwrap();
        assert_eq!(
            response.address,
            "tb1pwv4p3sgpy8g323mxsa5z4dnm0qnzlqcxk70gwdqvrmaf92psselsxcwf75"
        );
        assert_eq!(response.status.as_deref(), Some("OK"));
        assert!(response.signatures.get("field-node").is_some());
        assert!(response.signatures.get("hl-node").is_some());
        assert!(response.signatures.get("node-1").is_some());
    }

    #[test]
    fn generate_address_response_accepts_partial_signature_set() {
        let body = json!({
            "address": "0xc7dbCFD81cB7C4D88b2e20C12201Cff67B4716C2",
            "signatures": {
                "field-node": "sig-a",
                "unit-node": "sig-b"
            },
            "status": "OK"
        });
        let response: UnitGenerateAddressResponse = serde_json::from_value(body).unwrap();
        assert_eq!(
            response.address,
            "0xc7dbCFD81cB7C4D88b2e20C12201Cff67B4716C2"
        );
        assert_eq!(response.status.as_deref(), Some("OK"));
        assert!(response.signatures.get("field-node").is_some());
        assert!(response.signatures.get("unit-node").is_some());
        assert!(response.signatures.get("hl-node").is_none());
    }

    #[test]
    fn generate_address_response_deserializes_minimal_shape() {
        let body = json!({ "address": "0xabc" });
        let response: UnitGenerateAddressResponse = serde_json::from_value(body).unwrap();
        assert_eq!(response.address, "0xabc");
        assert!(response.status.is_none());
        assert!(response.signatures.is_null());
    }

    #[test]
    fn generate_address_response_roundtrips() {
        let original = UnitGenerateAddressResponse {
            address: "0xprotocol".to_string(),
            status: Some("OK".to_string()),
            signatures: json!({ "field-node": "sig" }),
        };
        let serialized = serde_json::to_value(&original).unwrap();
        let roundtripped: UnitGenerateAddressResponse = serde_json::from_value(serialized).unwrap();
        assert_eq!(original, roundtripped);
    }

    #[test]
    fn operations_response_deserializes_real_shape() {
        let body = json!({
            "addresses": [
                {
                    "sourceCoinType": "ethereum",
                    "destinationChain": "hyperliquid",
                    "address": "0xprotocol",
                    "signatures": {
                        "field-node": "sig-a",
                        "hl-node": "sig-b",
                        "unit-node": "sig-c"
                    }
                }
            ],
            "operations": [
                {
                    "operationId": "op-42",
                    "opCreatedAt": "2026-04-17T12:00:00Z",
                    "protocolAddress": "0xprotocol",
                    "sourceAddress": "0xsource",
                    "destinationAddress": "0xdest",
                    "sourceChain": "ethereum",
                    "destinationChain": "hyperliquid",
                    "sourceAmount": "50000000000000000",
                    "destinationFeeAmount": "1000",
                    "sweepFeeAmount": "2000",
                    "state": "done",
                    "sourceTxHash": "0xaaa",
                    "destinationTxHash": "0xbbb",
                    "sourceTxConfirmations": 14,
                    "destinationTxConfirmations": 10,
                    "positionInWithdrawQueue": 0,
                    "broadcastAt": "2026-04-17T12:05:00Z",
                    "asset": "eth",
                    "stateStartedAt": "2026-04-17T12:00:10Z",
                    "stateUpdatedAt": "2026-04-17T12:06:00Z",
                    "stateNextAttemptAt": "2026-04-17T12:10:00Z"
                }
            ]
        });
        let response: UnitOperationsResponse = serde_json::from_value(body).unwrap();
        assert_eq!(response.addresses.len(), 1);
        assert_eq!(response.operations.len(), 1);
        let op = &response.operations[0];
        assert_eq!(op.operation_id.as_deref(), Some("op-42"));
        assert_eq!(op.protocol_address.as_deref(), Some("0xprotocol"));
        assert_eq!(op.source_chain.as_deref(), Some("ethereum"));
        assert_eq!(op.destination_chain.as_deref(), Some("hyperliquid"));
        assert_eq!(op.source_amount.as_deref(), Some("50000000000000000"));
        assert_eq!(op.state.as_deref(), Some("done"));
        assert_eq!(op.source_tx_confirmations, Some(14));
        assert_eq!(op.classified_state(), UnitOperationState::Done);
    }

    #[test]
    fn operations_response_defaults_to_empty_lists() {
        let body = json!({});
        let response: UnitOperationsResponse = serde_json::from_value(body).unwrap();
        assert!(response.addresses.is_empty());
        assert!(response.operations.is_empty());
    }

    #[test]
    fn unit_operation_matches_protocol_address_case_insensitively() {
        let op = UnitOperation {
            operation_id: None,
            op_created_at: None,
            protocol_address: Some("0xAbCdEf".to_string()),
            source_address: None,
            destination_address: None,
            source_chain: None,
            destination_chain: None,
            source_amount: None,
            destination_fee_amount: None,
            sweep_fee_amount: None,
            state: None,
            source_tx_hash: None,
            destination_tx_hash: None,
            source_tx_confirmations: None,
            destination_tx_confirmations: None,
            position_in_withdraw_queue: None,
            broadcast_at: None,
            asset: None,
            state_started_at: None,
            state_updated_at: None,
            state_next_attempt_at: None,
        };
        assert!(op.matches_protocol_address("0xABCDEF"));
        assert!(op.matches_protocol_address("0xabcdef"));
        assert!(!op.matches_protocol_address("0xdeadbeef"));
    }

    #[test]
    fn unit_operation_state_parses_all_documented_states() {
        // Native-asset deposit lifecycle per HyperUnit docs
        assert_eq!(
            UnitOperationState::parse("sourceTxDiscovered"),
            UnitOperationState::SourceTxDiscovered
        );
        assert_eq!(
            UnitOperationState::parse("waitForSrcTxFinalization"),
            UnitOperationState::WaitForSrcTxFinalization
        );
        assert_eq!(
            UnitOperationState::parse("buildingDstTx"),
            UnitOperationState::BuildingDstTx
        );
        assert_eq!(
            UnitOperationState::parse("additionalChecks"),
            UnitOperationState::AdditionalChecks
        );
        assert_eq!(
            UnitOperationState::parse("signTx"),
            UnitOperationState::SignTx
        );
        assert_eq!(
            UnitOperationState::parse("broadcastTx"),
            UnitOperationState::BroadcastTx
        );
        assert_eq!(
            UnitOperationState::parse("waitForDstTxFinalization"),
            UnitOperationState::WaitForDstTxFinalization
        );
        // ERC20 lifecycle variants
        assert_eq!(
            UnitOperationState::parse("readyForWithdrawQueue"),
            UnitOperationState::ReadyForWithdrawQueue
        );
        assert_eq!(
            UnitOperationState::parse("queuedForWithdraw"),
            UnitOperationState::QueuedForWithdraw
        );
        // Terminal states
        assert_eq!(UnitOperationState::parse("done"), UnitOperationState::Done);
        assert_eq!(
            UnitOperationState::parse("failure"),
            UnitOperationState::Failure
        );
    }

    #[test]
    fn unit_operation_state_case_insensitive() {
        assert_eq!(UnitOperationState::parse("DONE"), UnitOperationState::Done);
        assert_eq!(UnitOperationState::parse("Done"), UnitOperationState::Done);
        assert_eq!(
            UnitOperationState::parse("FAILURE"),
            UnitOperationState::Failure
        );
    }

    #[test]
    fn unit_operation_state_unknown_for_unrecognized_value() {
        assert_eq!(
            UnitOperationState::parse("brand-new-state"),
            UnitOperationState::Unknown
        );
        assert_eq!(UnitOperationState::parse(""), UnitOperationState::Unknown);
    }

    #[test]
    fn unit_operation_state_terminal_predicates() {
        assert!(UnitOperationState::Done.is_terminal());
        assert!(UnitOperationState::Done.is_terminal_success());
        assert!(!UnitOperationState::Done.is_terminal_failure());

        assert!(UnitOperationState::Failure.is_terminal());
        assert!(UnitOperationState::Failure.is_terminal_failure());
        assert!(!UnitOperationState::Failure.is_terminal_success());

        assert!(!UnitOperationState::SignTx.is_terminal());
        assert!(!UnitOperationState::Unknown.is_terminal());
    }

    #[test]
    fn unit_chain_and_asset_roundtrip_through_parse() {
        assert_eq!(UnitChain::parse("bitcoin"), Some(UnitChain::Bitcoin));
        assert_eq!(UnitChain::parse("ethereum"), Some(UnitChain::Ethereum));
        assert_eq!(
            UnitChain::parse("hyperliquid"),
            Some(UnitChain::Hyperliquid)
        );
        assert_eq!(
            UnitChain::parse("Ethereum"),
            None,
            "parse is case-sensitive"
        );
        assert_eq!(UnitChain::parse("solana"), None, "solana unsupported today");

        assert_eq!(UnitAsset::parse("btc"), Some(UnitAsset::Btc));
        assert_eq!(UnitAsset::parse("eth"), Some(UnitAsset::Eth));
        assert_eq!(UnitAsset::parse("ETH"), None);
    }

    #[test]
    fn client_normalizes_trailing_slash() {
        let client = HyperUnitClient::new("https://api.hyperunit.xyz/");
        assert_eq!(client.base_url(), "https://api.hyperunit.xyz");

        let client = HyperUnitClient::new("https://api.hyperunit.xyz///");
        assert_eq!(client.base_url(), "https://api.hyperunit.xyz");
    }

    #[test]
    fn client_with_proxy_normalizes_trailing_slash() {
        let client = HyperUnitClient::new_with_proxy_url(
            "https://api.hyperunit.xyz///",
            Some("socks5://127.0.0.1:1080".to_string()),
        )
        .expect("proxy client");
        assert_eq!(client.base_url(), "https://api.hyperunit.xyz");
    }

    #[test]
    fn client_with_proxy_rejects_invalid_proxy_url() {
        let error = HyperUnitClient::new_with_proxy_url(
            "https://api.hyperunit.xyz",
            Some("not a url".to_string()),
        )
        .expect_err("invalid proxy must fail");
        assert!(matches!(
            error,
            HyperUnitClientError::InvalidProxyUrl { .. }
        ));
    }

    #[test]
    fn client_with_proxy_rejects_non_socks_schemes() {
        let error = HyperUnitClient::new_with_proxy_url(
            "https://api.hyperunit.xyz",
            Some("http://127.0.0.1:8080".to_string()),
        )
        .expect_err("non-socks proxy must fail");
        assert!(matches!(
            error,
            HyperUnitClientError::UnsupportedProxyScheme { .. }
        ));
    }

    #[test]
    fn client_with_proxy_rejects_socks5h_scheme() {
        let error = HyperUnitClient::new_with_proxy_url(
            "https://api.hyperunit.xyz",
            Some("socks5h://127.0.0.1:1080".to_string()),
        )
        .expect_err("socks5h proxy must fail");
        assert!(matches!(
            error,
            HyperUnitClientError::UnsupportedProxyScheme { .. }
        ));
    }
}
